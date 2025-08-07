"""
Клієнт для роботи з KeyCRM API
"""
import json
import logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from config import (
    KEYCRM_API_TOKEN,
    KEYCRM_BASE_URL,
    BONUS_FIELD_UUID,
    RESERVED_BONUS_FIELD_UUID,
    HISTORY_FIELD_UUID,
    BONUS_EXPIRY_FIELD_UUID,
    BONUS_EXPIRY_DAYS
)
from utils import normalize_phone

logger = logging.getLogger()


class KeyCRMClient:
    """Клієнт для роботи з KeyCRM API"""
    
    def __init__(self):
        self.api_token = KEYCRM_API_TOKEN
        self.base_url = KEYCRM_BASE_URL
    
    def make_request(self, url: str, method: str = 'GET', data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Універсальна функція для запитів до KeyCRM API
        """
        try:
            logger.info(f"Виконання запиту до KeyCRM API: {method} {url}")
            if data:
                logger.info(f"Тіло запиту: {data}")
            
            req = urllib.request.Request(url)
            req.add_header('Authorization', f'Bearer {self.api_token}')
            req.add_header('Accept', 'application/json')
            
            if data:
                req.add_header('Content-Type', 'application/json')
                req.data = json.dumps(data).encode()
            
            if method != 'GET':
                req.get_method = lambda: method
            
            with urllib.request.urlopen(req) as response:
                response_data = json.loads(response.read().decode())
                logger.info(f"Успішна відповідь від KeyCRM API: {response_data}")
                return {'success': True, 'data': response_data}
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.code != 404 else 'Не знайдено'
            logger.error(f"Помилка HTTP при запиті до KeyCRM API: {e.code} - {error_body}")
            return {'success': False, 'error': f'KeyCRM API помилка {e.code}: {error_body}'}
        except Exception as e:
            logger.error(f"Помилка запиту до KeyCRM API: {str(e)}")
            return {'success': False, 'error': f'Помилка запиту: {str(e)}'}
    
    def get_client_data(self, client_id: str) -> Dict[str, Any]:
        """
        Отримання даних клієнта з KeyCRM за його ID
        """
        try:
            url = f"{self.base_url}/buyer/{client_id}"
            result = self.make_request(url)
            
            if not result['success']:
                return result
            
            client_data = result['data']
            
            # Формуємо структуру даних клієнта
            customer_data = {
                'name': client_data.get('full_name', ''),
                'phone': client_data.get('phone', [''])[0] if client_data.get('phone') else '',
                'email': client_data.get('email', [''])[0] if client_data.get('email') else ''
            }
            
            return {
                'success': True,
                'customer_data': customer_data
            }
            
        except Exception as e:
            logger.error(f"Помилка отримання даних клієнта з KeyCRM: {str(e)}")
            return {
                'success': False,
                'error': f'Помилка отримання даних клієнта: {str(e)}'
            }
    
    def find_buyer(self, contact: str) -> Dict[str, Any]:
        """
        Універсальний пошук клієнта в KeyCRM за телефоном або email
        """
        # Визначаємо тип контакту та нормалізуємо
        if '@' in contact:
            filter_param = f"filter[buyer_email]={urllib.parse.quote(contact)}"
        else:
            # Очищаємо та нормалізуємо номер телефону
            clean_phone = normalize_phone(contact)
            filter_param = f"filter[buyer_phone]={urllib.parse.quote(clean_phone)}"
        
        url = f"{self.base_url}/buyer?{filter_param}&include=custom_fields"
        result = self.make_request(url)
        
        if not result['success']:
            return result
        
        keycrm_data = result['data']
        if keycrm_data.get('data') and len(keycrm_data['data']) > 0:
            buyer = keycrm_data['data'][0]
            
            # Шукаємо бонуси в custom fields
            bonus_balance = 0
            reserved_bonus_balance = 0
            bonus_expiry_date = None
            
            if buyer.get('custom_fields'):
                for field in buyer['custom_fields']:
                    if field.get('uuid') == BONUS_FIELD_UUID:
                        bonus_balance = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == RESERVED_BONUS_FIELD_UUID:
                        reserved_bonus_balance = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == BONUS_EXPIRY_FIELD_UUID:
                        bonus_expiry_date = field.get('value', '')
            
            return {
                'success': True,
                'buyer_id': buyer['id'],
                'bonus_balance': bonus_balance,
                'reserved_bonus_balance': reserved_bonus_balance,
                'bonus_expiry_date': bonus_expiry_date,
                'buyer_data': buyer
            }
        else:
            return {'success': False, 'error': 'Клієнта не знайдено'}
    
    def update_buyer_bonus(self, buyer_id: str, new_bonus_amount: int, 
                          new_reserved_amount: Optional[int] = None) -> Dict[str, Any]:
        """
        Оновлення балансу бонусів клієнта через custom field "Бонусні бали"
        та встановлення дати закінчення бонусів через 3 місяці
        """
        # Розраховуємо дату закінчення бонусів (через 3 місяці від сьогодні)
        expiry_date = datetime.utcnow() + timedelta(days=BONUS_EXPIRY_DAYS)
        expiry_date_str = expiry_date.strftime('%Y-%m-%d')
        
        # Структура оновлення - оновлюємо кастомні поля
        custom_fields = [
            {
                'uuid': BONUS_FIELD_UUID,
                'value': str(new_bonus_amount)
            },
            {
                'uuid': BONUS_EXPIRY_FIELD_UUID,
                'value': expiry_date_str
            }
        ]
        
        # Додаємо оновлення зарезервованих бонусів, якщо параметр передано
        if new_reserved_amount is not None:
            custom_fields.append({
                'uuid': RESERVED_BONUS_FIELD_UUID,
                'value': str(new_reserved_amount)
            })
        
        update_data = {
            'custom_fields': custom_fields
        }
        
        logger.info(f"Оновлення бонусів клієнта {buyer_id}, нова сума: {new_bonus_amount}, резерв: {new_reserved_amount}")
        
        # Оновлюємо клієнта
        result = self.make_request(f"{self.base_url}/buyer/{buyer_id}", 'PUT', update_data)
        
        if result['success']:
            logger.info(f"✅ Баланс бонусів клієнта {buyer_id} оновлено до {new_bonus_amount}, резерв: {new_reserved_amount}, дата закінчення: {expiry_date_str}")
            return {
                'success': True, 
                'result': f'Баланс оновлено до {new_bonus_amount}',
                'field_uuid': BONUS_FIELD_UUID,
                'expiry_date': expiry_date_str,
                'updated_at': result['data'].get('updated_at')
            }
        else:
            logger.error(f"❌ Помилка оновлення бонусів: {result['error']}")
            return result
    
    def get_bonus_history(self, buyer_id: str) -> Optional[str]:
        """
        Отримати історію бонусів з кастомного поля "Історія бонусів"
        """
        try:
            logger.info(f"Отримання історії бонусів для клієнта {buyer_id}")
            get_result = self.make_request(f"{self.base_url}/buyer/{buyer_id}?include=custom_fields")
            
            if not get_result['success']:
                logger.error(f"Помилка при отриманні даних клієнта {buyer_id}: {get_result['error']}")
                return None
            
            buyer_data = get_result['data']
            
            # Шукаємо поле "Історія бонусів"
            if buyer_data.get('custom_fields'):
                for field in buyer_data['custom_fields']:
                    if field.get('uuid') == HISTORY_FIELD_UUID:
                        history_value = field.get('value', '') or ''
                        logger.info(f"Знайдено історію бонусів: {history_value}")
                        return history_value
            
            logger.info("Поле історії бонусів не знайдено")
            return ''  # Повертаємо порожній рядок, якщо поле не знайдено
            
        except Exception as e:
            logger.error(f"Помилка при отриманні історії бонусів: {str(e)}")
            return None
    
    def update_bonus_history(self, buyer_id: str, new_history: str) -> Dict[str, Any]:
        """
        Оновлення історії бонусів клієнта
        """
        try:
            # Отримуємо поточні дані клієнта
            get_result = self.make_request(f"{self.base_url}/buyer/{buyer_id}")
            if not get_result['success']:
                return get_result
            
            buyer_data = get_result['data']
            
            custom_fields = [
                {
                    'uuid': HISTORY_FIELD_UUID,
                    'value': new_history
                }
            ]
            
            update_data = {
                'full_name': buyer_data.get('full_name', 'Клієнт'),
                'custom_fields': custom_fields
            }
            
            result = self.make_request(f"{self.base_url}/buyer/{buyer_id}", 'PUT', update_data)
            
            if result['success']:
                logger.info(f"✅ Історія бонусів клієнта {buyer_id} оновлена")
                return {
                    'success': True, 
                    'result': 'Історію бонусів оновлено'
                }
            else:
                return result
                
        except Exception as e:
            logger.error(f"Помилка оновлення історії бонусів: {str(e)}")
            return {
                'success': False,
                'error': f'Помилка оновлення історії: {str(e)}'
            }
