"""
Розширений клас операцій з бонусами для роботи з DynamoDB + KeyCRM
"""
import sys
import os
import logging
from typing import Dict, Any, Optional

# Додаємо шлях до сервісу балансів
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'bonus_balance_service'))

# Імпортуємо основний клас
from bonus_operations import BonusOperations as OriginalBonusOperations

# Імпортуємо нові сервіси
from bonus_balance_manager import BonusBalanceManager
from sync_service import BonusSyncService

# Імпортуємо конфігурацію
from config import (
    BONUS_PERCENTAGE,
    OPERATION_EMOJIS
)
from utils import create_response

logger = logging.getLogger()

class EnhancedBonusOperations(OriginalBonusOperations):
    """
    Розширений клас для операцій з бонусами з використанням DynamoDB як основного сховища
    
    Логіка роботи:
    1. DynamoDB - основне сховище балансів та історії
    2. KeyCRM - дублює дані для кожного клієнта (включно з дублікатами)
    3. При кожній операції: спочатку оновлюється DynamoDB, потім синхронізується KeyCRM
    """
    
    def __init__(self):
        super().__init__()
        self.balance_manager = BonusBalanceManager()
        self.sync_service = BonusSyncService()
    
    def handle_order_completion(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нарахування бонусів за виконане замовлення з використанням нової системи
        """
        try:
            logger.info(f"Обробка виконаного замовлення (Enhanced): {body}")
            
            # Отримуємо контекст замовлення
            order_context = body.get('context', {})
            
            # Валідація обов'язкових полів
            order_id = order_context.get('id')
            if not order_id:
                return create_response(400, {
                    'error': 'Відсутній номер замовлення (id)',
                    'success': False
                })
            
            order_total = order_context.get('grand_total')
            if order_total is None:
                return create_response(400, {
                    'error': 'Відсутня сума замовлення (grand_total)',
                    'success': False
                })
            
            client_id = order_context.get('client_id')
            if not client_id:
                return create_response(400, {
                    'error': 'Відсутній ID клієнта (client_id)',
                    'success': False
                })
            
            # Отримуємо дані клієнта з KeyCRM для отримання номера телефону
            buyer_result = self.keycrm.make_request(f"{self.keycrm.base_url}/buyer/{client_id}?include=custom_fields")
            if not buyer_result['success']:
                return create_response(500, {
                    'error': f"Помилка отримання даних клієнта з KeyCRM: {buyer_result.get('error')}",
                    'success': False
                })
            
            buyer_data = buyer_result['data']
            phone_numbers = buyer_data.get('phone', [])
            if not phone_numbers or not phone_numbers[0]:
                return create_response(400, {
                    'error': 'У клієнта відсутній номер телефону',
                    'success': False
                })
            
            phone_number = phone_numbers[0]
            
            # Розраховуємо параметри операції
            discount_amount = order_context.get('discount_amount', 0)
            bonus_amount = int(float(order_total or 0) * BONUS_PERCENTAGE)
            used_bonus_amount = int(float(discount_amount or 0))
            
            logger.info(f"Параметри операції: phone={phone_number}, order_total={order_total}, "
                       f"bonus_amount={bonus_amount}, used_bonus_amount={used_bonus_amount}")
            
            return self.process_order_completion_enhanced(
                phone_number, client_id, order_id, 
                bonus_amount, used_bonus_amount, float(order_total or 0)
            )
                
        except Exception as e:
            logger.error(f"Помилка обробки замовлення (Enhanced): {str(e)}")
            return create_response(500, {
                'error': f'Внутрішня помилка сервера: {str(e)}',
                'success': False
            })
    
    def process_order_completion_enhanced(self, phone_number: str, keycrm_client_id: str, 
                                        order_id: str, bonus_amount: int, 
                                        used_bonus_amount: int, order_total: float) -> Dict[str, Any]:
        """
        Обробка виконання замовлення з використанням DynamoDB + синхронізація в KeyCRM
        """
        try:
            # 1. Оновлюємо баланс в DynamoDB (основне сховище)
            
            # Спочатку списуємо з резерву (якщо використовувались бонуси)
            if used_bonus_amount > 0:
                # Переводимо з резерву в використані
                transaction_used = {
                    'type': 'used',
                    'amount': -used_bonus_amount,
                    'order_id': order_id,
                    'description': f"{OPERATION_EMOJIS.get('completed', '✅')} Використано {used_bonus_amount} бонусів за замовленням {order_id}"
                }
                
                use_result = self.balance_manager.update_balance(
                    phone_number,
                    active_bonus_delta=0,
                    reserved_bonus_delta=-used_bonus_amount,
                    transaction=transaction_used
                )
                
                if not use_result['success']:
                    return create_response(500, {
                        'error': f"Помилка списання бонусів з резерву в DynamoDB: {use_result.get('error')}",
                        'success': False
                    })
            
            # Потім нараховуємо нові бонуси
            if bonus_amount > 0:
                transaction_earned = {
                    'type': 'earned',
                    'amount': bonus_amount,
                    'order_id': order_id,
                    'description': f"{OPERATION_EMOJIS.get('completed', '✅')} Виконано замовлення {order_id}: +{bonus_amount} бонусів (сума: {order_total:.2f}₴)"
                }
                
                earn_result = self.balance_manager.update_balance(
                    phone_number,
                    active_bonus_delta=bonus_amount,
                    transaction=transaction_earned
                )
                
                if not earn_result['success']:
                    return create_response(500, {
                        'error': f"Помилка нарахування бонусів в DynamoDB: {earn_result.get('error')}",
                        'success': False
                    })
            
            # 2. Синхронізуємо оновлені дані в KeyCRM
            sync_result = self.sync_service.sync_to_all_client_duplicates(phone_number)
            
            if not sync_result['success']:
                logger.error(f"Помилка синхронізації з KeyCRM: {sync_result.get('error')}")
                # Не повертаємо помилку, оскільки основна операція в DynamoDB пройшла успішно
            else:
                logger.info(f"Синхронізовано дані в {sync_result.get('total_synced', 0)} клієнтів KeyCRM")
            
            # 3. Отримуємо підсумковий стан для відповіді
            final_balance = self.balance_manager.get_balance(phone_number)
            if not final_balance['success']:
                logger.error(f"Помилка отримання підсумкового балансу: {final_balance.get('error')}")
                balance_data = {'active_bonus': 0, 'reserved_bonus': 0}
            else:
                balance_data = final_balance['data']
            
            return create_response(200, {
                'success': True,
                'message': f'Замовлення {order_id} успішно оброблено',
                'order_id': order_id,
                'phone_number': phone_number,
                'operations': {
                    'bonus_earned': bonus_amount,
                    'bonus_used': used_bonus_amount,
                    'order_total': order_total
                },
                'final_balance': {
                    'active_bonus': balance_data['active_bonus'],
                    'reserved_bonus': balance_data['reserved_bonus'],
                    'bonus_expiry_date': balance_data.get('bonus_expiry_date')
                },
                'sync_info': {
                    'keycrm_synced': sync_result.get('success', False),
                    'synced_clients': sync_result.get('total_synced', 0)
                }
            })
            
        except Exception as e:
            logger.error(f"Помилка обробки замовлення Enhanced: {str(e)}")
            return create_response(500, {
                'error': f'Внутрішня помилка обробки: {str(e)}',
                'success': False
            })
    
    def handle_bonus_reservation(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Резервування бонусів для замовлення з використанням нової системи
        """
        try:
            logger.info(f"Резервування бонусів (Enhanced): {body}")
            
            # Отримуємо параметри з body
            phone_number = body.get('phone')
            order_id = body.get('order_id')
            bonus_amount = int(body.get('bonus_amount', 0))
            
            if not phone_number:
                return create_response(400, {
                    'error': 'Відсутній номер телефону',
                    'success': False
                })
            
            if not order_id:
                return create_response(400, {
                    'error': 'Відсутній ID замовлення',
                    'success': False
                })
            
            if bonus_amount <= 0:
                return create_response(400, {
                    'error': 'Некоректна сума бонусів для резервування',
                    'success': False
                })
            
            # 1. Перевіряємо доступний баланс в DynamoDB
            balance_result = self.balance_manager.get_balance(phone_number)
            if not balance_result['success']:
                return create_response(500, {
                    'error': f"Помилка перевірки балансу: {balance_result.get('error')}",
                    'success': False
                })
            
            current_balance = balance_result['data']
            available_bonus = current_balance['active_bonus']
            
            if available_bonus < bonus_amount:
                return create_response(400, {
                    'error': f'Недостатньо бонусів для резервування. Доступно: {available_bonus}, запитано: {bonus_amount}',
                    'success': False,
                    'available_bonus': available_bonus,
                    'requested_bonus': bonus_amount
                })
            
            # 2. Резервуємо бонуси в DynamoDB (переносимо з активних в зарезервовані)
            transaction = {
                'type': 'reserved',
                'amount': bonus_amount,
                'order_id': order_id,
                'description': f"{OPERATION_EMOJIS.get('reserved', '🔒')} Зарезервовано {bonus_amount} бонусів для замовлення {order_id}"
            }
            
            reserve_result = self.balance_manager.update_balance(
                phone_number,
                active_bonus_delta=-bonus_amount,  # Зменшуємо активні
                reserved_bonus_delta=bonus_amount,  # Збільшуємо зарезервовані
                transaction=transaction
            )
            
            if not reserve_result['success']:
                return create_response(500, {
                    'error': f"Помилка резервування в DynamoDB: {reserve_result.get('error')}",
                    'success': False
                })
            
            # 3. Синхронізуємо в KeyCRM
            sync_result = self.sync_service.sync_to_all_client_duplicates(phone_number)
            
            if not sync_result['success']:
                logger.warning(f"Помилка синхронізації резервування в KeyCRM: {sync_result.get('error')}")
            
            # 4. Повертаємо результат
            updated_balance = reserve_result['data']
            
            return create_response(200, {
                'success': True,
                'message': f'Успішно зарезервовано {bonus_amount} бонусів для замовлення {order_id}',
                'reserved_amount': bonus_amount,
                'order_id': order_id,
                'phone_number': phone_number,
                'balance_after_reservation': {
                    'active_bonus': updated_balance['active_bonus'],
                    'reserved_bonus': updated_balance['reserved_bonus'],
                    'bonus_expiry_date': updated_balance.get('bonus_expiry_date')
                },
                'sync_info': {
                    'keycrm_synced': sync_result.get('success', False),
                    'synced_clients': sync_result.get('total_synced', 0)
                }
            })
            
        except Exception as e:
            logger.error(f"Помилка резервування бонусів (Enhanced): {str(e)}")
            return create_response(500, {
                'error': f'Внутрішня помилка резервування: {str(e)}',
                'success': False
            })
    
    def get_client_balance_enhanced(self, phone_number: str) -> Dict[str, Any]:
        """
        Отримання балансу клієнта з DynamoDB з перевіркою прострочених бонусів
        """
        try:
            # Спочатку перевіряємо та очищаємо прострочені бонуси
            expiry_check = self.balance_manager.check_expired_bonuses(phone_number)
            
            if not expiry_check['success']:
                logger.error(f"Помилка перевірки прострочених бонусів: {expiry_check.get('error')}")
            elif expiry_check.get('expired_amount', 0) > 0:
                logger.info(f"Очищено {expiry_check['expired_amount']} прострочених бонусів для {phone_number}")
                # Синхронізуємо оновлення в KeyCRM
                self.sync_service.sync_to_all_client_duplicates(phone_number)
            
            # Отримуємо актуальний баланс
            balance_result = self.balance_manager.get_balance(phone_number)
            
            if not balance_result['success']:
                return create_response(500, {
                    'error': f"Помилка отримання балансу: {balance_result.get('error')}",
                    'success': False
                })
            
            return create_response(200, {
                'success': True,
                'balance': balance_result['data'],
                'expired_bonuses_cleared': expiry_check.get('expired_amount', 0)
            })
            
        except Exception as e:
            logger.error(f"Помилка отримання балансу (Enhanced): {str(e)}")
            return create_response(500, {
                'error': f'Помилка отримання балансу: {str(e)}',
                'success': False
            })


# Глобальний екземпляр розширеного класу
enhanced_bonus_operations = EnhancedBonusOperations()
