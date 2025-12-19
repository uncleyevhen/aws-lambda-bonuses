"""
Розширений сервіс перевірки балансу бонусів з підтримкою DynamoDB
AWS Lambda функція для перевірки балансу бонусів через DynamoDB (основне сховище) з fallback на KeyCRM
"""
import json
import logging
import sys
import os

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    """
    AWS Lambda функція для перевірки балансу бонусів з DynamoDB як основного джерела
    Обходить CORS обмеження браузера для GTM скриптів
    """
    
    # CORS заголовки - дозволяємо всі домени для GTM
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With'
    }
    
    # Обробка preflight запиту
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': ''
        }
    
    try:
        # Отримуємо номер телефону з параметрів запиту
        query_params = event.get('queryStringParameters', {})
        phone = query_params.get('phone')
        
        logger.info(f"Отримані параметри: {query_params}")
        logger.info(f"Номер телефону: {phone}")
        
        if not phone:
            logger.error("Помилка: параметр phone відсутній")
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({'error': 'Параметр phone є обов\'язковим'})
            }
        
        # Нормалізуємо номер телефону
        clean_phone = normalize_phone_number(phone)
        logger.info(f"Нормалізований номер: {clean_phone}")
        
        # Спробуємо отримати баланс з DynamoDB
        dynamodb_result = get_balance_from_dynamodb(clean_phone)
        
        if dynamodb_result['success']:
            logger.info("Баланс успішно отримано з DynamoDB")
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({
                    'phone': clean_phone,
                    'bonus_balance': dynamodb_result['data']['active_bonus'],
                    'reserved_bonus': dynamodb_result['data']['reserved_bonus'],
                    'total_earned': dynamodb_result['data']['total_earned'],
                    'total_used': dynamodb_result['data']['total_used'],
                    'bonus_expiry_date': dynamodb_result['data'].get('bonus_expiry_date'),
                    'last_updated': dynamodb_result['data'].get('last_updated'),
                    'source': 'DynamoDB',
                    'success': True
                })
            }
        else:
            # Fallback на KeyCRM для зворотної сумісності
            logger.warning(f"Не вдалося отримати дані з DynamoDB: {dynamodb_result.get('error')}")
            logger.info("Пробуємо fallback на KeyCRM")
            
            keycrm_result = get_balance_from_keycrm(clean_phone)
            
            if keycrm_result['success']:
                logger.info("Баланс успішно отримано з KeyCRM (fallback)")
                return {
                    'statusCode': 200,
                    'headers': headers,
                    'body': json.dumps({
                        'phone': clean_phone,
                        'bonus_balance': keycrm_result['bonus_balance'],
                        'reserved_bonus': keycrm_result.get('reserved_bonus', 0),
                        'source': 'KeyCRM_Fallback',
                        'success': True,
                        'fallback_reason': dynamodb_result.get('error', 'DynamoDB недоступна')
                    })
                }
            else:
                # Обидва джерела недоступні
                logger.error(f"Помилка отримання даних з обох джерел. DynamoDB: {dynamodb_result.get('error')}, KeyCRM: {keycrm_result.get('error')}")
                return {
                    'statusCode': 500,
                    'headers': headers,
                    'body': json.dumps({
                        'error': 'Не вдалося отримати дані про бонуси з жодного джерела',
                        'details': {
                            'dynamodb_error': dynamodb_result.get('error'),
                            'keycrm_error': keycrm_result.get('error')
                        },
                        'success': False
                    })
                }
        
    except Exception as e:
        logger.error(f"Загальна помилка обробки запиту: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'error': 'Внутрішня помилка сервера',
                'details': str(e),
                'success': False
            })
        }


def normalize_phone_number(phone: str) -> str:
    """
    Нормалізація номера телефону до стандартного формату
    """
    if not phone:
        return phone
    
    # Очищаємо номер телефону від всіх символів, крім цифр
    clean_phone = ''.join(filter(str.isdigit, phone))
    
    # Нормалізуємо номер (додаємо +38 якщо потрібно)
    if clean_phone.startswith('0') and len(clean_phone) == 10:
        return '+38' + clean_phone  # 0123456789 -> +380123456789
    elif clean_phone.startswith('380') and len(clean_phone) == 12:
        return '+' + clean_phone   # 380123456789 -> +380123456789
    elif clean_phone.startswith('38') and len(clean_phone) == 11:
        return '+3' + clean_phone  # 38123456789 -> +3838123456789 (потребує коригування)
    elif len(clean_phone) == 9:
        return '+380' + clean_phone  # 123456789 -> +380123456789
    
    # Якщо номер вже має +, повертаємо як є
    return phone if phone.startswith('+') else '+' + clean_phone


def get_balance_from_dynamodb(phone_number: str) -> dict:
    """
    Отримання балансу бонусів з DynamoDB
    """
    try:
        # Додаємо шлях до сервісу балансів
        current_dir = os.path.dirname(os.path.abspath(__file__))
        balance_service_path = os.path.join(current_dir, '..', 'bonus_balance_service')
        
        if balance_service_path not in sys.path:
            sys.path.append(balance_service_path)
        
        # Імпортуємо менеджер балансів
        from bonus_balance_manager import BonusBalanceManager
        
        # Створюємо екземпляр менеджера
        balance_manager = BonusBalanceManager()
        
        # Отримуємо баланс
        result = balance_manager.get_balance(phone_number)
        
        if result['success']:
            # Перевіряємо прострочені бонуси
            expiry_check = balance_manager.check_expired_bonuses(phone_number)
            
            if expiry_check.get('expired_amount', 0) > 0:
                logger.info(f"Очищено {expiry_check['expired_amount']} прострочених бонусів для {phone_number}")
                # Отримуємо оновлений баланс після очищення
                result = balance_manager.get_balance(phone_number)
        
        return result
        
    except ImportError as e:
        logger.error(f"Помилка імпорту модуля DynamoDB: {str(e)}")
        return {'success': False, 'error': f'Модуль DynamoDB недоступний: {str(e)}'}
    except Exception as e:
        logger.error(f"Помилка роботи з DynamoDB: {str(e)}")
        return {'success': False, 'error': f'Помилка DynamoDB: {str(e)}'}


def get_balance_from_keycrm(phone_number: str) -> dict:
    """
    Fallback отримання балансу бонусів з KeyCRM API (оригінальна логіка)
    """
    try:
        import urllib.request
        import urllib.parse
        import urllib.error
        import ssl
        
        # KeyCRM API конфігурація
        KEYCRM_API_TOKEN = os.environ.get('KEYCRM_API_TOKEN', 'M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ')
        KEYCRM_BASE_URL = os.environ.get('KEYCRM_BASE_URL', 'https://openapi.keycrm.app/v1')
        
        # Формуємо URL для пошуку клієнта
        search_url = f"{KEYCRM_BASE_URL}/buyers?search={urllib.parse.quote(phone_number)}&include=custom_fields"
        
        logger.info(f"Запит до KeyCRM API: {search_url}")
        
        # Створюємо запит до KeyCRM API
        req = urllib.request.Request(search_url)
        req.add_header('Authorization', f'Bearer {KEYCRM_API_TOKEN}')
        req.add_header('Accept', 'application/json')
        
        # Створюємо SSL контекст
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Виконуємо запит
        with urllib.request.urlopen(req, context=ssl_context) as response:
            response_data = json.loads(response.read().decode())
            logger.info(f"Відповідь від KeyCRM API: {response_data}")
            
            # Обробляємо відповідь
            if not response_data.get('data'):
                return {'success': False, 'error': f'Клієнта з номером {phone_number} не знайдено в KeyCRM'}
            
            # Беремо першого клієнта з результатів пошуку
            client = response_data['data'][0]
            bonus_balance = 0
            reserved_bonus = 0
            
            # Шукаємо бонусні поля в custom_fields
            if client.get('custom_fields'):
                for field in client['custom_fields']:
                    if field.get('uuid') == 'CT_1023':  # Активні бонуси
                        bonus_balance = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == 'CT_1034':  # Зарезервовані бонуси
                        reserved_bonus = int(field.get('value', 0) or 0)
            
            return {
                'success': True,
                'bonus_balance': bonus_balance,
                'reserved_bonus': reserved_bonus,
                'client_name': client.get('full_name', ''),
                'client_id': client.get('id')
            }
            
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if hasattr(e, 'read') else 'Невідома помилка HTTP'
        logger.error(f"Помилка HTTP при запиті до KeyCRM: {e.code} - {error_body}")
        return {'success': False, 'error': f'KeyCRM API помилка {e.code}: {error_body}'}
    except Exception as e:
        logger.error(f"Помилка запиту до KeyCRM: {str(e)}")
        return {'success': False, 'error': f'Помилка KeyCRM: {str(e)}'}


# Для локального тестування
if __name__ == "__main__":
    # Тестовий запит
    test_event = {
        'httpMethod': 'GET',
        'queryStringParameters': {
            'phone': '+380123456789'
        }
    }
    
    result = lambda_handler(test_event, {})
    print(json.dumps(result, indent=2, ensure_ascii=False))
