"""
Менеджер балансів бонусів для роботи з DynamoDB
Основне сховище даних для бонусної системи
"""
import json
import logging
import boto3
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, Optional, List
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()

class BonusBalanceManager:
    """
    Менеджер для роботи з балансами бонусів у DynamoDB
    
    Структура запису:
    {
        'phone_number': '+380123456789',  # Primary Key
        'active_bonus': Decimal('100'),   # Активні бонуси
        'reserved_bonus': Decimal('50'),  # Зарезервовані бонуси
        'total_earned': Decimal('1000'),  # Всього нараховано за весь час
        'total_used': Decimal('200'),     # Всього використано за весь час
        'bonus_expiry_date': '2025-03-24', # Дата до якої бонуси активні
        'transaction_history': [          # Повна історія операцій
            {
                'timestamp': '2025-01-15T10:30:00Z',
                'type': 'earned',
                'amount': Decimal('100'),
                'order_id': 'ORDER123',
                'description': '✅ Виконано замовлення ORDER123: +100 бонусів'
            }
        ],
        'last_updated': '2025-01-15T10:30:00Z',
        'created_at': '2025-01-01T00:00:00Z'
    }
    """
    
    def __init__(self, region='eu-north-1', table_name='bonus-balances'):
        self.region = region
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        
        # Константи для бонусів
        self.BONUS_EXPIRY_DAYS = 90  # 3 місяці
    
    def normalize_phone(self, phone: str) -> str:
        """Нормалізація номера телефону до стандартного формату"""
        if not phone:
            return phone
            
        # Видаляємо всі символи крім цифр
        clean_phone = ''.join(filter(str.isdigit, phone))
        
        # Стандартизуємо до українського формату
        if clean_phone.startswith('0') and len(clean_phone) == 10:
            return '+38' + clean_phone
        elif clean_phone.startswith('380') and len(clean_phone) == 12:
            return '+' + clean_phone
        elif clean_phone.startswith('38') and len(clean_phone) == 11:
            return '+3' + clean_phone
        
        return '+' + clean_phone if not phone.startswith('+') else phone
    
    def get_balance(self, phone_number: str) -> Dict[str, Any]:
        """
        Отримання балансу бонусів клієнта
        
        Returns:
            {
                'success': True/False,
                'data': {
                    'phone_number': '+380123456789',
                    'active_bonus': 100,
                    'reserved_bonus': 50,
                    'total_earned': 1000,
                    'total_used': 200,
                    'bonus_expiry_date': '2025-03-24',
                    'transaction_history': [...],
                    'last_updated': '2025-01-15T10:30:00Z'
                },
                'error': 'error message if failed'
            }
        """
        try:
            normalized_phone = self.normalize_phone(phone_number)
            
            logger.info(f"Отримання балансу для номера: {normalized_phone}")
            
            response = self.table.get_item(Key={'phone_number': normalized_phone})
            
            if 'Item' not in response:
                # Клієнта не знайдено, повертаємо нульові баланси
                return {
                    'success': True,
                    'data': {
                        'phone_number': normalized_phone,
                        'active_bonus': 0,
                        'reserved_bonus': 0,
                        'total_earned': 0,
                        'total_used': 0,
                        'bonus_expiry_date': None,
                        'transaction_history': [],
                        'last_updated': None
                    }
                }
            
            # Конвертуємо Decimal в int для JSON серіалізації
            item = response['Item']
            data = {
                'phone_number': item['phone_number'],
                'active_bonus': int(item.get('active_bonus', 0)),
                'reserved_bonus': int(item.get('reserved_bonus', 0)),
                'total_earned': int(item.get('total_earned', 0)),
                'total_used': int(item.get('total_used', 0)),
                'bonus_expiry_date': item.get('bonus_expiry_date'),
                'transaction_history': self._convert_decimals_in_history(item.get('transaction_history', [])),
                'last_updated': item.get('last_updated')
            }
            
            logger.info(f"Знайдено баланс: {data}")
            return {'success': True, 'data': data}
            
        except Exception as e:
            logger.error(f"Помилка отримання балансу для {phone_number}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def update_balance(self, phone_number: str, active_bonus_delta: int = 0, 
                      reserved_bonus_delta: int = 0, transaction: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Атомарне оновлення балансу бонусів
        
        Args:
            phone_number: номер телефону клієнта
            active_bonus_delta: зміна активних бонусів (+/-)
            reserved_bonus_delta: зміна зарезервованих бонусів (+/-)
            transaction: деталі транзакції для історії
            
        Returns:
            {'success': True/False, 'data': {...}, 'error': '...'}
        """
        try:
            normalized_phone = self.normalize_phone(phone_number)
            current_time = datetime.utcnow().isoformat() + 'Z'
            
            # Розраховуємо нову дату закінчення бонусів
            expiry_date = (datetime.utcnow() + timedelta(days=self.BONUS_EXPIRY_DAYS)).strftime('%Y-%m-%d')
            
            logger.info(f"Оновлення балансу для {normalized_phone}: "
                       f"активні {active_bonus_delta:+d}, зарезервовані {reserved_bonus_delta:+d}")
            
            # Підготовка виразу для оновлення
            update_expression = []
            expression_attribute_values = {}
            expression_attribute_names = {}
            
            # Оновлюємо активні бонуси
            if active_bonus_delta != 0:
                update_expression.append('#active_bonus = if_not_exists(#active_bonus, :zero) + :active_delta')
                expression_attribute_names['#active_bonus'] = 'active_bonus'
                expression_attribute_values[':active_delta'] = Decimal(str(active_bonus_delta))
                
                # Якщо додаємо бонуси, оновлюємо total_earned
                if active_bonus_delta > 0:
                    update_expression.append('#total_earned = if_not_exists(#total_earned, :zero) + :earned_delta')
                    expression_attribute_names['#total_earned'] = 'total_earned'
                    expression_attribute_values[':earned_delta'] = Decimal(str(active_bonus_delta))
            
            # Оновлюємо зарезервовані бонуси
            if reserved_bonus_delta != 0:
                update_expression.append('#reserved_bonus = if_not_exists(#reserved_bonus, :zero) + :reserved_delta')
                expression_attribute_names['#reserved_bonus'] = 'reserved_bonus'
                expression_attribute_values[':reserved_delta'] = Decimal(str(reserved_bonus_delta))
                
                # Якщо списуємо з резерву (зменшуємо), додаємо до total_used
                if reserved_bonus_delta < 0:
                    update_expression.append('#total_used = if_not_exists(#total_used, :zero) + :used_delta')
                    expression_attribute_names['#total_used'] = 'total_used'
                    expression_attribute_values[':used_delta'] = Decimal(str(-reserved_bonus_delta))
            
            # Завжди оновлюємо last_updated та bonus_expiry_date
            update_expression.append('#last_updated = :timestamp')
            expression_attribute_names['#last_updated'] = 'last_updated'
            expression_attribute_values[':timestamp'] = current_time
            
            if active_bonus_delta > 0:  # Оновлюємо дату закінчення лише при нарахуванні нових бонусів
                update_expression.append('#bonus_expiry_date = :expiry_date')
                expression_attribute_names['#bonus_expiry_date'] = 'bonus_expiry_date'
                expression_attribute_values[':expiry_date'] = expiry_date
            
            # Додаємо транзакцію до історії
            if transaction:
                # Конвертуємо числові значення в Decimal
                if 'amount' in transaction:
                    transaction['amount'] = Decimal(str(transaction['amount']))
                
                transaction['timestamp'] = current_time
                
                update_expression.append('#transaction_history = list_append(if_not_exists(#transaction_history, :empty_list), :new_transaction)')
                expression_attribute_names['#transaction_history'] = 'transaction_history'
                expression_attribute_values[':new_transaction'] = [transaction]
            
            # Встановлюємо created_at для нових записів
            update_expression.append('#created_at = if_not_exists(#created_at, :timestamp)')
            expression_attribute_names['#created_at'] = 'created_at'
            
            expression_attribute_values[':zero'] = Decimal('0')
            expression_attribute_values[':empty_list'] = []
            
            # Виконуємо атомарне оновлення
            response = self.table.update_item(
                Key={'phone_number': normalized_phone},
                UpdateExpression='SET ' + ', '.join(update_expression),
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='ALL_NEW'
            )
            
            # Конвертуємо результат
            updated_item = response['Attributes']
            result_data = {
                'phone_number': updated_item['phone_number'],
                'active_bonus': int(updated_item.get('active_bonus', 0)),
                'reserved_bonus': int(updated_item.get('reserved_bonus', 0)),
                'total_earned': int(updated_item.get('total_earned', 0)),
                'total_used': int(updated_item.get('total_used', 0)),
                'bonus_expiry_date': updated_item.get('bonus_expiry_date'),
                'transaction_history': self._convert_decimals_in_history(updated_item.get('transaction_history', [])),
                'last_updated': updated_item.get('last_updated')
            }
            
            logger.info(f"Успішно оновлено баланс: {result_data}")
            return {'success': True, 'data': result_data}
            
        except Exception as e:
            logger.error(f"Помилка оновлення балансу для {phone_number}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def add_transaction(self, phone_number: str, transaction_type: str, amount: int, 
                       order_id: Optional[str] = None, description: Optional[str] = None) -> Dict[str, Any]:
        """
        Додавання транзакції до історії без зміни балансу
        
        Args:
            phone_number: номер телефону
            transaction_type: тип операції ('earned', 'used', 'reserved', 'cancelled')
            amount: сума операції
            order_id: номер замовлення (опціонально)
            description: опис операції
        """
        try:
            transaction = {
                'type': transaction_type,
                'amount': amount,
                'description': description or f"{transaction_type.title()}: {amount:+d} бонусів"
            }
            
            if order_id:
                transaction['order_id'] = order_id
            
            # Додаємо транзакцію без зміни балансу
            return self.update_balance(phone_number, 0, 0, transaction)
            
        except Exception as e:
            logger.error(f"Помилка додавання транзакції для {phone_number}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_full_history(self, phone_number: str, limit: int = 100) -> Dict[str, Any]:
        """
        Отримання повної історії операцій клієнта
        
        Args:
            phone_number: номер телефону
            limit: максимальна кількість записів в історії (останні N)
            
        Returns:
            {'success': True/False, 'history': [...], 'error': '...'}
        """
        try:
            balance_result = self.get_balance(phone_number)
            if not balance_result['success']:
                return balance_result
            
            history = balance_result['data'].get('transaction_history', [])
            
            # Повертаємо останні N записів
            if len(history) > limit:
                history = history[-limit:]
            
            return {
                'success': True,
                'history': history,
                'total_records': len(history)
            }
            
        except Exception as e:
            logger.error(f"Помилка отримання історії для {phone_number}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _convert_decimals_in_history(self, history: List[Dict]) -> List[Dict]:
        """Конвертація Decimal об'єктів в історії в int для JSON серіалізації"""
        converted_history = []
        for transaction in history:
            converted_transaction = transaction.copy()
            if 'amount' in converted_transaction and isinstance(converted_transaction['amount'], Decimal):
                converted_transaction['amount'] = int(converted_transaction['amount'])
            converted_history.append(converted_transaction)
        return converted_history
    
    def check_expired_bonuses(self, phone_number: str) -> Dict[str, Any]:
        """
        Перевірка та очищення прострочених бонусів
        
        Returns:
            {'success': True/False, 'expired_amount': int, 'data': {...}}
        """
        try:
            balance_result = self.get_balance(phone_number)
            if not balance_result['success']:
                return balance_result
            
            data = balance_result['data']
            expiry_date = data.get('bonus_expiry_date')
            
            if not expiry_date:
                return {'success': True, 'expired_amount': 0, 'data': data}
            
            # Перевіряємо чи не прострочені бонуси
            expiry_datetime = datetime.strptime(expiry_date, '%Y-%m-%d')
            if datetime.utcnow().date() > expiry_datetime.date():
                # Бонуси прострочені, обнуляємо їх
                expired_amount = data['active_bonus']
                
                if expired_amount > 0:
                    transaction = {
                        'type': 'expired',
                        'amount': -expired_amount,
                        'description': f"❌ Прострочені бонуси: -{expired_amount} бонусів"
                    }
                    
                    # Обнуляємо активні бонуси
                    update_result = self.update_balance(
                        phone_number, 
                        active_bonus_delta=-expired_amount,
                        transaction=transaction
                    )
                    
                    if update_result['success']:
                        return {
                            'success': True, 
                            'expired_amount': expired_amount,
                            'data': update_result['data']
                        }
                    else:
                        return update_result
            
            return {'success': True, 'expired_amount': 0, 'data': data}
            
        except Exception as e:
            logger.error(f"Помилка перевірки прострочених бонусів для {phone_number}: {str(e)}")
            return {'success': False, 'error': str(e)}


# Глобальний екземпляр менеджера
bonus_balance_manager = BonusBalanceManager()
