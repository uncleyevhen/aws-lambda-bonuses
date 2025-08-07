"""
Бізнес-логіка операцій з бонусами
"""
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from config import (
    BONUS_PERCENTAGE,
    OPERATION_EMOJIS,
    MAX_HISTORY_RECORDS,
    MAX_HISTORY_LENGTH
)
from keycrm_client import KeyCRMClient
from utils import create_response

logger = logging.getLogger()


class BonusOperations:
    """Клас для виконання операцій з бонусами"""
    
    def __init__(self):
        self.keycrm = KeyCRMClient()
    
    def handle_order_completion(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нарахування бонусів за виконане замовлення та списання використаних бонусів з резерву
        """
        try:
            logger.info(f"Обробка виконаного замовлення: {body}")
            
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
            
            # Розраховуємо параметри операції
            discount_amount = order_context.get('discount_amount', 0)
            bonus_amount = float(order_total or 0) * BONUS_PERCENTAGE
            
            logger.info(f"Параметри операції: order_total={order_total}, discount_amount={discount_amount}, "
                       f"bonus_amount={bonus_amount}")
            
            # Отримуємо дані клієнта за ID для пошуку реально зарезервованих бонусів
            buyer_result = self.keycrm.make_request(f"{self.keycrm.base_url}/buyer/{client_id}?include=custom_fields")
            if not buyer_result['success']:
                return create_response(500, {
                    'error': f"Помилка отримання даних клієнта з KeyCRM: {buyer_result.get('error', 'Невідома помилка')}",
                    'success': False
                })
            
            buyer_data = buyer_result['data']
            
            # Отримуємо історію бонусів для пошуку реально зарезервованих сум
            history = self.keycrm.get_bonus_history(buyer_data['id']) or ''
            
            # Знаходимо реально зарезервовані бонуси для цього замовлення
            reserved_for_order = self.find_reserved_amount_for_order(history, order_id)
            
            if reserved_for_order is None:
                # Якщо не знайшли в історії, але є знижка - використовуємо її
                reserved_for_order = float(discount_amount or 0)
                logger.warning(f"Не знайдено запис резерву для замовлення {order_id} в історії, використовуємо знижку замовлення: {reserved_for_order}")
            else:
                logger.info(f"Знайдено зарезервовані бонуси для замовлення {order_id}: {reserved_for_order}")
            
            # Використовуємо реально зарезервовану суму для списання
            used_bonus_amount = reserved_for_order
            
            logger.info(f"Списуватимемо з резерву: {used_bonus_amount} (реально зарезервовано для замовлення)")
            
            # Виконуємо логіку нарахування бонусів та списання з резерву
            return self.handle_order_completion_logic(
                buyer_data, order_id, used_bonus_amount, bonus_amount, float(order_total or 0)
            )
                
        except Exception as e:
            logger.error(f"Помилка обробки замовлення: {str(e)}")
            return create_response(500, {
                'error': f'Внутрішня помилка сервера: {str(e)}',
                'success': False
            })
    
    def handle_order_completion_logic(self, buyer_data: Dict, order_id: str, 
                                    used_bonus_amount: float, bonus_amount: float, 
                                    order_total: float) -> Dict[str, Any]:
        """
        Логіка обробки виконання замовлення:
        1. Списуємо використані бонуси з резерву
        2. Нараховуємо нові бонуси до активних
        """
        try:
            # Отримуємо поточний баланс бонусів клієнта з переданих даних
            buyer_id = buyer_data['id']
            
            # Отримуємо поточні баланси з кастомних полів
            current_bonus = 0
            current_reserved = 0
            
            if buyer_data.get('custom_fields'):
                for field in buyer_data['custom_fields']:
                    if field.get('uuid') == 'CT_1023':  # BONUS_FIELD_UUID
                        current_bonus = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == 'CT_1034':  # RESERVED_BONUS_FIELD_UUID
                        current_reserved = int(field.get('value', 0) or 0)
            
            logger.info(f"Поточний баланс: активних={current_bonus}, зарезервованих={current_reserved}")
            
            # Розраховуємо нові баланси
            new_reserved = max(0, current_reserved - used_bonus_amount)
            new_bonus = current_bonus + bonus_amount
            
            logger.info(f"Розрахунок нових балансів: активні {current_bonus} + {bonus_amount} = {new_bonus}, "
                       f"резерв {current_reserved} - {used_bonus_amount} = {new_reserved}")
            
            # Оновлюємо баланси в KeyCRM
            update_result = self.keycrm.update_buyer_bonus(buyer_id, int(new_bonus), int(new_reserved))
            if not update_result['success']:
                return create_response(500, {
                    'error': f"Помилка оновлення бонусів в KeyCRM: {update_result.get('error', 'Невідома помилка')}",
                    'success': False
                })
            
            # Логуємо транзакцію в історії бонусів
            self.log_transaction(
                buyer_id, 
                order_id, 
                bonus_amount, 
                used_bonus_amount,
                current_bonus, 
                int(new_bonus),
                order_total,
                'completed'
            )
            
            logger.info(f"✅ Бонуси успішно оброблені для виконаного замовлення {order_id}: "
                       f"активні {current_bonus} → {new_bonus}, резерв {current_reserved} → {new_reserved}")
            
            return create_response(200, {
                'message': 'Бонуси успішно оброблені для виконаного замовлення',
                'success': True,
                'operation': 'order_completed',
                'orderId': order_id,
                'buyerId': buyer_id,
                'previousBonus': current_bonus,
                'previousReserved': current_reserved,
                'usedBonus': used_bonus_amount,
                'accruedBonus': bonus_amount,
                'newBonus': int(new_bonus),
                'newReserved': int(new_reserved)
            })
            
        except Exception as e:
            logger.error(f"Помилка при обробці виконання замовлення: {str(e)}")
            return create_response(500, {
                'error': f'Помилка обробки виконання замовлення: {str(e)}',
                'success': False
            })
    
    def handle_order_cancellation_by_client_id(self, client_id: str, order_id: str, 
                                              used_bonus_amount: float, bonus_amount: float, 
                                              order_total: float) -> Dict[str, Any]:
        """
        Логіка обробки скасування замовлення за ID клієнта (безпечний метод):
        1. Отримуємо дані клієнта безпосередньо за ID
        2. Знаходимо в історії запис про резервування для цього замовлення
        3. Повертаємо зарезервовані бонуси до активних
        4. НЕ нараховуємо нові бонуси
        """
        try:
            # Отримуємо дані клієнта за ID
            buyer_result = self.keycrm.make_request(f"{self.keycrm.base_url}/buyer/{client_id}?include=custom_fields")
            if not buyer_result['success']:
                return create_response(500, {
                    'error': f"Помилка отримання даних клієнта з KeyCRM: {buyer_result.get('error', 'Невідома помилка')}",
                    'success': False
                })
            
            buyer_data = buyer_result['data']
            buyer_id = buyer_data['id']
            
            # Отримуємо поточні баланси з кастомних полів
            current_bonus = 0
            current_reserved = 0
            
            if buyer_data.get('custom_fields'):
                for field in buyer_data['custom_fields']:
                    if field.get('uuid') == 'CT_1023':  # BONUS_FIELD_UUID
                        current_bonus = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == 'CT_1034':  # RESERVED_BONUS_FIELD_UUID
                        current_reserved = int(field.get('value', 0) or 0)
            
            logger.info(f"Поточний баланс: активних={current_bonus}, зарезервованих={current_reserved}")
            
            # Отримуємо історію бонусів
            history = self.keycrm.get_bonus_history(buyer_id) or ''
            
            # Знаходимо суму резерву для цього замовлення в історії
            reserved_for_order = self.find_reserved_amount_for_order(history, order_id)
            
            if reserved_for_order is None:
                # Якщо не знайшли в історії, використовуємо передану суму
                reserved_for_order = used_bonus_amount
                logger.warning(f"Не знайдено запис резерву для замовлення {order_id} в історії, використовуємо передану суму: {reserved_for_order}")
            else:
                logger.info(f"Знайдено резерв для замовлення {order_id}: {reserved_for_order}")
            
            # Розраховуємо нові баланси для скасування
            return_amount = min(reserved_for_order, current_reserved)
            new_bonus = current_bonus + return_amount
            new_reserved = current_reserved - return_amount
            
            logger.info(f"Розрахунок для скасування: повертаємо {return_amount} з резерву до активних. "
                       f"Активні {current_bonus} + {return_amount} = {new_bonus}, "
                       f"резерв {current_reserved} - {return_amount} = {new_reserved}")
            
            # Оновлюємо баланси в KeyCRM
            update_result = self.keycrm.update_buyer_bonus(buyer_id, int(new_bonus), int(new_reserved))
            if not update_result['success']:
                return create_response(500, {
                    'error': f"Помилка оновлення бонусів в KeyCRM: {update_result.get('error', 'Невідома помилка')}",
                    'success': False
                })
            
            # Логуємо транзакцію в історії бонусів
            self.log_transaction(
                buyer_id, 
                order_id, 
                0,  # Не нараховуємо нові бонуси при скасуванні
                -return_amount,  # Вказуємо від'ємне значення для показу повернення
                current_bonus, 
                int(new_bonus),
                order_total,
                'cancelled'
            )
            
            logger.info(f"✅ Бонуси успішно оброблені для скасованого замовлення {order_id}: повернуто {return_amount} з резерву до активних")
            
            return create_response(200, {
                'message': 'Бонуси успішно оброблені для скасованого замовлення',
                'success': True,
                'operation': 'order_cancelled',
                'orderId': order_id,
                'buyerId': buyer_id,
                'previousBonus': current_bonus,
                'previousReserved': current_reserved,
                'returnedBonus': return_amount,
                'newBonus': int(new_bonus),
                'newReserved': int(new_reserved)
            })
            
        except Exception as e:
            logger.error(f"Помилка при обробці скасування замовлення: {str(e)}")
            return create_response(500, {
                'error': f'Помилка обробки скасування замовлення: {str(e)}',
                'success': False
            })
    

    
    def handle_order_reservation(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обробка вебхука резервування бонусів при створенні замовлення
        """
        try:
            logger.info(f"Отримано вебхук резервування: {body}")
            
            # Перевіряємо тип події
            event_type = body.get('event')
            if event_type not in ['order.create', 'order.change_order_status']:
                logger.info(f"Ігноруємо подію типу: {event_type}")
                return create_response(200, {
                    'message': f'Подія типу {event_type} ігнорується',
                    'success': True
                })
            
            order_context = body.get('context', {})
            
            # Валідація полів
            order_id = order_context.get('id')
            if not order_id:
                return create_response(400, {
                    'error': 'Відсутній номер замовлення (id)',
                    'success': False
                })
            
            order_total = order_context.get('products_total', 0)
            discount_amount = float(order_context.get('discount_amount') or 0)
            promo_code = order_context.get('promocode', '')
            
            # Перевіряємо чи є промокод і чи він бонусний
            if not promo_code or not discount_amount:
                logger.info(f"Замовлення {order_id} без промокоду або знижки, пропускаємо резервування")
                return create_response(200, {
                    'message': 'Замовлення без бонусів',
                    'success': True
                })
            
            client_id = order_context.get('client_id')
            if not client_id:
                return create_response(400, {
                    'error': 'Відсутній ID клієнта (client_id)',
                    'success': False
                })
            
            # Отримуємо дані клієнта з KeyCRM
            buyer_result = self.keycrm.make_request(f"{self.keycrm.base_url}/buyer/{client_id}?include=custom_fields")
            if not buyer_result['success']:
                return create_response(500, {
                    'error': f"Помилка отримання даних клієнта: {buyer_result.get('error', 'Невідома помилка')}",
                    'success': False
                })
            
            buyer_data = buyer_result['data']
            
            # Отримуємо поточні баланси
            current_bonus = 0
            current_reserved = 0
            
            if buyer_data.get('custom_fields'):
                for field in buyer_data['custom_fields']:
                    if field.get('uuid') == 'CT_1023':  # BONUS_FIELD_UUID
                        current_bonus = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == 'CT_1034':  # RESERVED_BONUS_FIELD_UUID
                        current_reserved = int(field.get('value', 0) or 0)
            
            # Розраховуємо суму для резервування
            bonus_to_reserve = min(discount_amount, current_bonus)
            
            if bonus_to_reserve <= 0:
                logger.info(f"Немає бонусів для резервування")
                return create_response(200, {
                    'message': 'Немає доступних бонусів для резервування',
                    'success': True
                })
            
            # Оновлюємо баланси
            new_bonus = current_bonus - bonus_to_reserve
            new_reserved = current_reserved + bonus_to_reserve
            
            # Оновлюємо в KeyCRM
            update_result = self.keycrm.update_buyer_bonus(client_id, int(new_bonus), int(new_reserved))
            if not update_result['success']:
                return create_response(500, {
                    'error': f"Помилка оновлення бонусів: {update_result.get('error', 'Невідома помилка')}",
                    'success': False
                })
            
            # Логуємо транзакцію
            self.log_transaction(
                client_id,
                order_id,
                0,  # Не нараховуємо
                bonus_to_reserve,  # Резервуємо
                current_bonus,
                int(new_bonus),
                order_total,
                'reserved'
            )
            
            logger.info(f"✅ Бонуси успішно зарезервовані для замовлення {order_id}: {bonus_to_reserve} бонусів")
            
            return create_response(200, {
                'message': 'Бонуси успішно зарезервовані',
                'success': True,
                'operation': 'order_reserved',
                'orderId': order_id,
                'reservedAmount': bonus_to_reserve,
                'previousBonus': current_bonus,
                'newBonus': new_bonus,
                'newReserved': new_reserved
            })
            
        except Exception as e:
            logger.error(f"Помилка обробки резервування: {str(e)}")
            return create_response(500, {
                'error': f'Помилка резервування: {str(e)}',
                'success': False
            })
    
    def handle_order_cancellation(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обробка прямого запиту на скасування замовлення через /order-cancel endpoint
        """
        try:
            logger.info(f"Отримано запит на скасування замовлення: {body}")
            
            # Отримуємо параметри
            order_id = body.get('order_id')
            client_id = body.get('client_id')
            used_bonus_amount = float(body.get('used_bonus_amount') or 0)
            
            if not order_id:
                return create_response(400, {
                    'error': 'Відсутній номер замовлення (order_id)',
                    'success': False
                })
            
            # Використовуємо тільки client_id для безпечного пошуку клієнта
            if client_id:
                logger.info(f"Використовуємо client_id для скасування: {client_id}")
                return self.handle_order_cancellation_by_client_id(
                    client_id, order_id, used_bonus_amount, 0, 0
                )
            else:
                return create_response(400, {
                    'error': 'Відсутній ID клієнта (client_id). Пошук за телефоном більше не підтримується з міркувань безпеки.',
                    'success': False
                })
            
        except Exception as e:
            logger.error(f"Помилка обробки запиту на скасування: {str(e)}")
            return create_response(500, {
                'error': f'Внутрішня помилка сервера: {str(e)}',
                'success': False
            })
    
    def find_reserved_amount_for_order(self, history: str, order_id: str) -> Optional[float]:
        """
        Знаходить загальну суму резерву для конкретного замовлення в історії бонусів
        Сумує всі операції резервування (автоматичні та мануальні) для цього замовлення
        Працює з форматом історії, де записи розділені подвійними переносами рядків
        """
        if not history:
            return None
        
        try:
            total_reserved = 0.0
            found_any = False
            
            # Шукаємо рядки з номером замовлення (ігноруємо порожні рядки)
            lines = history.split('\n')
            for line in lines:
                line = line.strip()  # Видаляємо зайві пробіли
                if not line:  # Пропускаємо порожні рядки
                    continue
                    
                # Перевіряємо чи є номер замовлення в рядку
                if f"#{order_id}" in line:
                    # Шукаємо операцію резерву (🔒 або 🔐)
                    if ('🔒' in line or '🔐' in line) and 'резерв' in line:
                        # Витягуємо суму резерву з рядка
                        # Формат: "резерв 100" або "ручний резерв 100"
                        match = re.search(r'резерв\s+(\d+)', line)
                        if match:
                            reserve_amount = float(match.group(1))
                            total_reserved += reserve_amount
                            found_any = True
                            logger.info(f"Знайдено резерв {reserve_amount} для замовлення {order_id}: {line.strip()}")
            
            if found_any:
                logger.info(f"Загальна сума зарезервованих бонусів для замовлення {order_id}: {total_reserved}")
                return total_reserved
            else:
                return None
            
        except Exception as e:
            logger.error(f"Помилка при пошуку резерву в історії: {str(e)}")
            return None
    
    def has_manual_reserve_for_order(self, history: str, order_id: str) -> bool:
        """
        Перевіряє чи є мануальне резервування для конкретного замовлення в історії бонусів
        Працює з форматом історії, де записи розділені подвійними переносами рядків
        """
        if not history:
            return False
        
        try:
            # Шукаємо рядки з номером замовлення та ручним резервом (ігноруємо порожні рядки)
            lines = history.split('\n')
            for line in lines:
                line = line.strip()  # Видаляємо зайві пробіли
                if not line:  # Пропускаємо порожні рядки
                    continue
                    
                # Перевіряємо чи є номер замовлення в рядку
                if f"#{order_id}" in line:
                    # Шукаємо операцію мануального резерву
                    if ('🔐' in line and 'ручний резерв' in line) or ('🔒' in line and 'ручний резерв' in line):
                        logger.info(f"Знайдено мануальне резервування для замовлення {order_id}: {line.strip()}")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Помилка при пошуку мануального резерву в історії: {str(e)}")
            return False
    
    def log_transaction(self, buyer_id: str, order_id: str, bonus_amount: float, 
                       used_bonus_amount: float, old_balance: int, new_balance: int, 
                       order_total: float, operation_type: str, lead_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Логування бонусної транзакції в кастомне поле "Історія бонусів"
        Стандартизований формат: emoji дата | #замовлення/лід | сума | операція | було→стало | термін
        """
        try:
            # Отримуємо поточну історію бонусів
            current_history = self.keycrm.get_bonus_history(buyer_id) or ''
            
            # Формуємо стандартизований запис
            date_str = datetime.utcnow().strftime('%d.%m.%y %H:%M')
            order_total_int = int(float(order_total or 0))
            
            # Визначаємо емодзі для операції
            emoji = OPERATION_EMOJIS.get(operation_type, '📝')
            
            # Формуємо ідентифікатор (замовлення або лід)
            identifier = f"#{order_id}" if order_id else ""
            if lead_id:
                identifier += f" (Лід #{lead_id})" if identifier else f"Лід #{lead_id}"
            
            # Визначаємо операцію
            operations = []
            
            # Резервування
            if operation_type in ['reserved', 'manual_reserve']:
                reserved_amount = abs(used_bonus_amount)
                operations.append(f"{'ручний ' if operation_type == 'manual_reserve' else ''}резерв {int(reserved_amount)}")
            
            # Використання/повернення
            elif used_bonus_amount != 0:
                if operation_type == 'cancelled' and used_bonus_amount < 0:
                    operations.append(f"повернуто {int(abs(used_bonus_amount))}")
                elif used_bonus_amount > 0:
                    operations.append(f"використано {int(abs(used_bonus_amount))}")
            
            # Нарахування
            if bonus_amount > 0 and operation_type == 'completed':
                operations.append(f"нараховано {int(bonus_amount)}")
            
            operation_str = ", ".join(operations) if operations else "без змін"
            
            # Формуємо зміну балансу
            balance_change = f"{int(old_balance)}→{int(new_balance)}"
            
            # Додаємо дату закінчення бонусів (через 3 місяці)
            expiry_date = (datetime.utcnow() + timedelta(days=90)).strftime('%d.%m.%y')
            
            # Формуємо фінальний запис
            transaction_entry = f"{emoji} {date_str} | {identifier} | {order_total_int}₴ | {operation_str} | {balance_change} | до {expiry_date}"
            
            logger.info(f"Формування стандартизованого запису транзакції: {transaction_entry}")
            
            # Перевіряємо дублювання для резервування
            if identifier and operation_type in ['reserved', 'manual_reserve']:
                # Для мануального резервування перевіряємо точніше
                if operation_type == 'manual_reserve':
                    if self.has_manual_reserve_for_order(current_history, order_id):
                        logger.info(f"Мануальний резерв для замовлення {order_id} вже існує в історії бонусів, пропускаємо")
                        return {
                            'success': True, 
                            'result': 'Мануальний резерв для цього замовлення вже існує',
                            'transaction_log': transaction_entry,
                            'history_updated': False
                        }
                # Для автоматичного резервування
                elif f"{identifier}" in current_history and ("резерв" in current_history and "ручний резерв" not in current_history):
                    logger.info(f"Автоматичний резерв для {identifier} вже існує в історії бонусів, пропускаємо")
                    return {
                        'success': True, 
                        'result': 'Резерв для цього замовлення вже існує',
                        'transaction_log': transaction_entry,
                        'history_updated': False
                    }
            
            # Розділяємо історію на окремі записи
            history_lines = [line.strip() for line in current_history.split('\n') if line.strip()]
            
            # Додаємо нову транзакцію на початок
            history_lines.insert(0, transaction_entry)
            
            # Обмежуємо кількість записів
            if len(history_lines) > MAX_HISTORY_RECORDS:
                history_lines = history_lines[:MAX_HISTORY_RECORDS]
            
            # Формуємо нову історію з порожніми рядками між записами для кращої читабельності
            new_history = '\n\n'.join(history_lines)
            
            # Обмежуємо розмір історії
            if len(new_history) > MAX_HISTORY_LENGTH:
                truncated_lines = history_lines[:40]
                new_history = '\n\n'.join(truncated_lines) + '\n\n...'
            
            # Оновлюємо історію в KeyCRM
            result = self.keycrm.update_bonus_history(buyer_id, new_history)
            
            if result['success']:
                logger.info(f"✅ Транзакція записана в історію бонусів клієнта {buyer_id}: {transaction_entry}")
                return {
                    'success': True, 
                    'result': 'Транзакцію записано в історію бонусів',
                    'transaction_log': transaction_entry,
                    'history_updated': True
                }
            else:
                # Fallback логування
                logger.info(f"BONUS TRANSACTION (fallback) для клієнта {buyer_id}: {transaction_entry}")
                return {
                    'success': False, 
                    'error': result['error'],
                    'fallback_log': transaction_entry
                }
                
        except Exception as e:
            logger.error(f"Помилка при записі транзакції в історію бонусів: {str(e)}")
            return {
                'success': False,
                'error': f'Помилка запису історії: {str(e)}'
            }
    
    def handle_lead_bonus_reservation(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Мануальне резервування бонусів через ліди
        Обробляє два типи воронок:
        1. Воронка з чатів - номер замовлення в target_id
        2. Воронка з сайтів - номер замовлення в кастомному полі LD_1022 воронки
        """
        try:
            logger.info(f"LEAD RESERVE - Отримано запит на мануальне резервування: {json.dumps(body, ensure_ascii=False, indent=2)}")
            
            # Перевіряємо тип події
            event_type = body.get('event')
            if event_type != 'lead.change_lead_status':
                logger.info(f"Ігноруємо подію типу: {event_type}")
                return create_response(200, {
                    'message': f'Подія типу {event_type} ігнорується',
                    'success': True
                })
            
            context = body.get('context', {})
            lead_id = context.get('id')
            contact_id = context.get('contact_id')
            pipeline_id = context.get('pipeline_id')
            target_id = context.get('target_id')
            target_type = context.get('target_type')
            status_id = context.get('status_id')
            
            if not lead_id:
                return create_response(400, {
                    'error': 'Відсутній ID ліда',
                    'success': False
                })
            
            if not contact_id:
                return create_response(400, {
                    'error': 'Відсутній ID контакта',
                    'success': False
                })
            
            logger.info(f"LEAD RESERVE - Lead ID: {lead_id}, Contact ID: {contact_id}, Pipeline: {pipeline_id}")
            logger.info(f"LEAD RESERVE - Target: {target_type} #{target_id}, Status: {status_id}")
            
            # Ініціалізуємо змінні
            order_id = None
            reserve_amount = 0
            
            # СПОЧАТКУ шукаємо в кастомних полях ліда
            logger.info(f"LEAD RESERVE - Отримуємо дані ліда {lead_id} для пошуку номера замовлення та суми резерву")
            
            pipeline_result = self.keycrm.make_request(f"{self.keycrm.base_url}/pipelines/cards/{lead_id}?include=custom_fields")
            if pipeline_result['success']:
                pipeline_data = pipeline_result['data']
                pipeline_custom_fields = pipeline_data.get('custom_fields', [])
                
                logger.info(f"LEAD RESERVE - Кастомні поля ліда {lead_id}: {pipeline_custom_fields}")
                
                # Шукаємо поле LD_1022 з номером замовлення та LD_1035 з сумою резерву
                for field in pipeline_custom_fields:
                    if field.get('uuid') == 'LD_1022':
                        field_value = field.get('value')
                        if field_value:
                            order_id = str(field_value).strip()
                            logger.info(f"LEAD RESERVE - Знайдено номер замовлення з кастомного поля LD_1022: {order_id}")
                    elif field.get('uuid') == 'LD_1035':
                        field_value = field.get('value')
                        logger.info(f"LEAD RESERVE - Значення поля LD_1035: {field_value}, тип: {type(field_value)}")
                        if field_value is not None and str(field_value).strip():
                            try:
                                reserve_amount = float(field_value)
                                logger.info(f"LEAD RESERVE - Знайдено суму резерву з поля LD_1035: {reserve_amount}")
                            except (ValueError, TypeError) as e:
                                logger.error(f"LEAD RESERVE - Помилка конвертації суми резерву з поля LD_1035: {field_value}, помилка: {e}")
                                reserve_amount = 0
                
                logger.info(f"LEAD RESERVE - Після обробки кастомних полів: order_id={order_id}, reserve_amount={reserve_amount}")
            else:
                logger.error(f"LEAD RESERVE - Помилка отримання даних ліда {lead_id}: {pipeline_result.get('error')}")
            
            # Якщо не знайшли номер замовлення в кастомних полях, шукаємо в target_id
            if not order_id and target_type == "order" and target_id:
                order_id = str(target_id)
                logger.info(f"LEAD RESERVE - Використовуємо номер замовлення з target_id: {order_id}")
            
            if not order_id:
                return create_response(400, {
                    'error': 'Не вдалося визначити номер замовлення для резервування',
                    'success': False,
                    'details': {
                        'target_type': target_type,
                        'target_id': target_id,
                        'pipeline_id': pipeline_id,
                        'lead_id': lead_id
                    }
                })
            
            if reserve_amount <= 0:
                return create_response(400, {
                    'error': 'Не вказана сума резерву в кастомному полі LD_1035 або сума дорівнює нулю',
                    'success': False,
                    'order_id': order_id,
                    'reserve_amount_found': reserve_amount,
                    'debug_info': 'Перевірте чи заповнене поле LD_1035 в ліді коректним числовим значенням більше 0'
                })
            
            # Отримуємо дані замовлення для перевірки ліміту 50%
            order_result = self.keycrm.make_request(f"{self.keycrm.base_url}/order/{order_id}")
            if not order_result['success']:
                return create_response(400, {
                    'error': f"Не вдалося отримати дані замовлення {order_id}: {order_result.get('error')}",
                    'success': False
                })
            
            order_data = order_result['data']
            order_total = float(order_data.get('products_total') or 0)
            current_discount = float(order_data.get('discount_amount') or 0)
            client_id = order_data.get('client_id') or contact_id
            
            if order_total <= 0:
                return create_response(400, {
                    'error': f"Неправильна сума замовлення: {order_total}",
                    'success': False
                })
            
            logger.info(f"LEAD RESERVE - Поточна знижка в замовленні {order_id}: {current_discount}₴")
            
            # Перевіряємо загальну знижку (поточна + мануальна) щодо ліміту 50%
            max_total_discount = order_total * 0.5
            total_discount_after = current_discount + reserve_amount
            
            # Зберігаємо початкову запитану суму для відповіді
            original_requested_amount = reserve_amount
            
            # Якщо загальна знижка перевищує 50%, коригуємо мануальну суму
            amount_was_corrected = False
            if total_discount_after > max_total_discount:
                reserve_amount = max_total_discount - current_discount
                amount_was_corrected = True
                logger.info(f"LEAD RESERVE - Корекція мануального резерву: запитано {original_requested_amount}₴, можливо {reserve_amount}₴")
                
                if reserve_amount <= 0:
                    return create_response(400, {
                        'error': f'Неможливо додати мануальну знижку. Поточна знижка ({current_discount}₴) вже досягла максимуму 50% від суми замовлення ({order_total}₴)',
                        'success': False,
                        'details': {
                            'order_total': order_total,
                            'current_discount': current_discount,
                            'max_total_discount': max_total_discount,
                            'requested_reserve': original_requested_amount
                        }
                    })
                
                # Оновлюємо поле резерву в ліді на фактичну суму
                update_lead_field_result = self.keycrm.make_request(
                    f"{self.keycrm.base_url}/pipelines/cards/{lead_id}",
                    method='PUT',
                    data={
                        'custom_fields': [
                            {
                                'uuid': 'LD_1035',
                                'value': str(reserve_amount)
                            }
                        ]
                    }
                )
                
                if update_lead_field_result['success']:
                    logger.info(f"LEAD RESERVE - Оновлено поле LD_1035 в ліді {lead_id}: {original_requested_amount}₴ → {reserve_amount}₴")
                else:
                    logger.error(f"LEAD RESERVE - Помилка оновлення поля LD_1035 в ліді {lead_id}: {update_lead_field_result.get('error')}")
            
            # Використовуємо скориговану суму для подальших розрахунків
            
            # Отримуємо дані клієнта з KeyCRM
            buyer_result = self.keycrm.make_request(f"{self.keycrm.base_url}/buyer/{client_id}?include=custom_fields")
            if not buyer_result['success']:
                return create_response(500, {
                    'error': f"Помилка отримання даних клієнта: {buyer_result.get('error', 'Невідома помилка')}",
                    'success': False
                })
            
            buyer_data = buyer_result['data']
            
            # Отримуємо поточні баланси
            current_bonus = 0
            current_reserved = 0
            
            if buyer_data.get('custom_fields'):
                for field in buyer_data['custom_fields']:
                    if field.get('uuid') == 'CT_1023':  # BONUS_FIELD_UUID
                        current_bonus = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == 'CT_1034':  # RESERVED_BONUS_FIELD_UUID
                        current_reserved = int(field.get('value', 0) or 0)
            
            # Отримуємо історію бонусів для перевірки вже зарезервованих сум для цього замовлення
            history = self.keycrm.get_bonus_history(buyer_data['id']) or ''
            already_reserved = self.find_reserved_amount_for_order(history, order_id) or 0
            
            logger.info(f"LEAD RESERVE - Вже зарезервовано для замовлення {order_id}: {already_reserved}")
            
            # Перевіряємо чи вже було резервування для цього замовлення через лід
            if self.has_manual_reserve_for_order(history, order_id):
                logger.info(f"LEAD RESERVE - Для замовлення {order_id} вже було виконано мануальне резервування через лід")
                return create_response(200, {
                    'message': f'Для замовлення {order_id} вже було виконано мануальне резервування бонусів',
                    'success': True,
                    'already_reserved': already_reserved,
                    'order_id': order_id,
                    'lead_id': lead_id,
                    'operation': 'already_reserved'
                })
            
            # Перевіряємо ліміт 50% від суми замовлення
            max_allowed_reserve = order_total * 0.5
            total_reserve_after = already_reserved + reserve_amount
            
            if total_reserve_after > max_allowed_reserve:
                return create_response(400, {
                    'error': f'Загальна сума резерву ({total_reserve_after}₴) перевищує 50% від суми замовлення ({order_total}₴)',
                    'success': False,
                    'details': {
                        'order_total': order_total,
                        'max_allowed': max_allowed_reserve,
                        'already_reserved': already_reserved,
                        'requested_reserve': reserve_amount,
                        'total_would_be': total_reserve_after
                    }
                })
            
            # Розраховуємо суму для резервування з урахуванням доступних бонусів
            bonus_to_reserve = min(reserve_amount, current_bonus)
            
            if bonus_to_reserve <= 0:
                return create_response(200, {
                    'message': 'Немає доступних бонусів для резервування',
                    'success': True,
                    'current_bonus': current_bonus,
                    'requested_amount': reserve_amount
                })
            
            # Оновлюємо баланси
            new_bonus = current_bonus - bonus_to_reserve
            new_reserved = current_reserved + bonus_to_reserve
            
            # Оновлюємо в KeyCRM
            update_result = self.keycrm.update_buyer_bonus(buyer_data['id'], int(new_bonus), int(new_reserved))
            if not update_result['success']:
                return create_response(500, {
                    'error': f"Помилка оновлення бонусів: {update_result.get('error')}",
                    'success': False
                })
            
            # Оновлюємо знижку в замовленні
            new_discount_amount = current_discount + bonus_to_reserve
            update_order_result = self.keycrm.make_request(
                f"{self.keycrm.base_url}/order/{order_id}",
                method='PUT',
                data={
                    'discount_amount': new_discount_amount
                }
            )
            
            if update_order_result['success']:
                logger.info(f"LEAD RESERVE - Знижка в замовленні {order_id} оновлена: {current_discount}₴ → {new_discount_amount}₴")
            else:
                logger.error(f"LEAD RESERVE - Помилка оновлення знижки в замовленні {order_id}: {update_order_result.get('error')}")
                # Не повертаємо помилку тут, оскільки бонуси вже зарезервовані
            
            # Логуємо транзакцію
            self.log_transaction(
                buyer_data['id'],
                order_id,
                0,  # Не нараховуємо
                bonus_to_reserve,  # Резервуємо
                current_bonus,
                int(new_bonus),
                order_total,
                'manual_reserve',
                lead_id=str(lead_id)
            )
            
            logger.info(f"✅ LEAD RESERVE - Мануальне резервування виконано для ліда {lead_id}, замовлення {order_id}: зарезервовано {bonus_to_reserve} бонусів")
            
            return create_response(200, {
                'message': 'Бонуси успішно зарезервовані через лід',
                'success': True,
                'operation': 'manual_reserve',
                'lead_id': lead_id,
                'order_id': order_id,
                'client_id': client_id,
                'reserved_amount': bonus_to_reserve,
                'previous_bonus': current_bonus,
                'new_bonus': new_bonus,
                'new_reserved': new_reserved,
                'order_total': order_total,
                'already_reserved_before': already_reserved,
                'total_reserved_now': already_reserved + bonus_to_reserve,
                'max_allowed_reserve': max_allowed_reserve,
                'pipeline_type': 'custom_field' if order_id != str(target_id) else 'target_id',
                'discount_updated': {
                    'previous_discount': current_discount,
                    'new_discount': current_discount + bonus_to_reserve,
                    'manual_discount_added': bonus_to_reserve
                },
                'amount_correction': {
                    'requested': original_requested_amount,
                    'actual': bonus_to_reserve,
                    'was_corrected': amount_was_corrected
                }
            })
            
        except Exception as e:
            logger.error(f"LEAD RESERVE - Помилка обробки резервування бонусів через лід: {str(e)}")
            return create_response(500, {
                'error': f'Помилка обробки lead bonus reserve: {str(e)}',
                'success': False
            })
