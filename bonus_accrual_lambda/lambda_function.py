import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
import os
import urllib.request
import urllib.parse
import urllib.error
from decimal import Decimal

# Налаштування логування
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Константи
BONUS_PERCENTAGE = 0.10
KEYCRM_API_TOKEN = os.environ.get('KEYCRM_API_TOKEN', 'M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ')
KEYCRM_BASE_URL = os.environ.get('KEYCRM_BASE_URL', 'https://openapi.keycrm.app/v1')

# UUID кастомних полів KeyCRM
BONUS_FIELD_UUID = "CT_1023"  # Поле "Бонусні бали" ✅
HISTORY_FIELD_UUID = "CT_1033"  # Поле "Історія бонусів" ✅
BONUS_EXPIRY_FIELD_UUID = "CT_1024"  # Поле "Дата закінчення бонусів" ✅

def lambda_handler(event, context):
    """
    Основна функція Lambda для нарахування бонусів через KeyCRM
    """
    try:
        logger.info(f"Отримано запит: {json.dumps(event)}")
        
        # Обробка CORS preflight запитів
        method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method', 'POST'))
        if method == 'OPTIONS':
            return lambda_handler_options(event, context)
        
        # Визначаємо тип запиту за шляхом
        path = event.get('path', '/')
        
        # Парсинг запиту
        if 'body' in event and event['body']:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        else:
            body = event
        
        # Маршрутизація запитів
        if path.endswith('/history') and method == 'GET':
            return handle_get_transaction_history(event, context)
        elif path.endswith('/check-expiry') and method == 'GET':
            return handle_check_bonus_expiry(event, context)
        elif method == 'POST':
            return handle_bonus_accrual(body, event, context)
        else:
            return create_response(405, {
                'error': 'Метод не підтримується',
                'success': False
            })
            
    except Exception as e:
        logger.error(f"Помилка обробки запиту: {str(e)}")
        return create_response(500, {
            'error': f'Внутрішня помилка сервера: {str(e)}',
            'success': False
        })

def handle_bonus_accrual(body: Dict[str, Any], event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Обробка запиту на нарахування бонусів
    """
    try:
        # Валідація запиту
        validation_result = validate_request(body)
        if not validation_result['valid']:
            return create_response(400, {
                'error': validation_result['error'],
                'success': False
            })
        
        order_id = body['orderId']
        
        # Перевірка на дублікати через KeyCRM (розширена перевірка)
        customer_email = body['customer'].get('email', '')
        duplicate_check = check_duplicate_in_keycrm(order_id, customer_email)
        if duplicate_check['is_duplicate']:
            logger.info(f"Замовлення {order_id} вже оброблено в KeyCRM")
            return create_response(200, {
                'message': 'Замовлення вже оброблено раніше',
                'success': True,
                'duplicate': True,
                'orderId': order_id,
                'duplicate_details': duplicate_check
            })
        
        # Визначаємо тип операції - перевіряємо і наявність, і значення
        has_deduction = 'usedBonusAmount' in body and body['usedBonusAmount'] and float(body['usedBonusAmount']) > 0
        has_accrual = 'bonusAmount' in body and body['bonusAmount'] and float(body['bonusAmount']) > 0
        
        # Комбінована обробка (списання + нарахування в одному замовленні)
        if has_deduction and has_accrual:
            # Комбінована операція: спочатку списуємо, потім нараховуємо
            logger.info(f"Комбінована операція для замовлення {order_id}: списання {body['usedBonusAmount']} + нарахування {body['bonusAmount']}")
            
            combined_result = process_combined_bonus_operation(body)
            
            if combined_result['success']:
                return create_response(200, {
                    'message': 'Комбінована операція з бонусами успішно виконана',
                    'success': True,
                    'orderId': order_id,
                    'operations': combined_result['operations'],
                    'keycrm_updated': True,
                    'keycrm_details': combined_result
                })
            else:
                return create_response(500, {
                    'error': combined_result['error'],
                    'success': False
                })
        
        elif has_deduction:
            # Тільки списання бонусів
            deduction_result = deduct_bonus(body)
            
            if deduction_result['success']:
                # Синхронізація з KeyCRM для списання
                logger.info("Початок списання бонусів в KeyCRM...")
                keycrm_result = sync_bonus_deduction_with_keycrm(body, deduction_result)
                logger.info(f"Результат списання в KeyCRM: {keycrm_result}")
                
                if keycrm_result.get('success', False):
                    return create_response(200, {
                        'message': 'Бонуси успішно списані в KeyCRM',
                        'success': True,
                        'deductedAmount': deduction_result['deducted_amount'],
                        'orderId': order_id,
                        'keycrm_updated': True,
                        'keycrm_details': keycrm_result
                    })
                else:
                    return create_response(500, {
                        'error': f"Помилка списання в KeyCRM: {keycrm_result.get('error', 'Невідома помилка')}",
                        'success': False
                    })
            else:
                return create_response(500, {
                    'error': deduction_result['error'],
                    'success': False
                })
        
        elif has_accrual:
            # Тільки нарахування бонусів (існуюча логіка)
            bonus_result = accrue_bonus(body)
            
            if bonus_result['success']:
                # Синхронізація з KeyCRM (єдине джерело даних)
                logger.info("Початок синхронізації з KeyCRM...")
                keycrm_result = sync_with_keycrm(body, bonus_result)
                logger.info(f"Результат синхронізації з KeyCRM: {keycrm_result}")
                
                if keycrm_result.get('success', False):
                    return create_response(200, {
                        'message': 'Бонуси успішно нараховані та збережені в KeyCRM',
                        'success': True,
                        'bonusAmount': bonus_result['bonus_amount'],
                        'orderId': order_id,
                        'keycrm_updated': True,
                        'keycrm_details': keycrm_result
                    })
                else:
                    return create_response(500, {
                        'error': f"Помилка збереження в KeyCRM: {keycrm_result.get('error', 'Невідома помилка')}",
                        'success': False
                    })
            else:
                return create_response(500, {
                    'error': bonus_result['error'],
                    'success': False
                })
        else:
            return create_response(400, {
                'error': 'Невідомий тип операції. Потрібно вказати bonusAmount або usedBonusAmount',
                'success': False
            })
            
    except Exception as e:
        logger.error(f"Помилка обробки запиту: {str(e)}")
        return create_response(500, {
            'error': 'Внутрішня помилка сервера',
            'success': False
        })

def validate_request(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валідація вхідного запиту (підтримує нарахування, списання та комбіновані операції)
    """
    required_fields = ['orderId', 'orderTotal', 'customer']
    
    # Перевіряємо базові поля
    for field in required_fields:
        if field not in body:
            return {
                'valid': False,
                'error': f"Відсутнє обов'язкове поле: {field}"
            }
    
    # Визначаємо тип операції - перевіряємо і наявність, і значення > 0
    has_bonus_amount = 'bonusAmount' in body and body['bonusAmount'] and float(body['bonusAmount']) > 0
    has_used_bonus = 'usedBonusAmount' in body and body['usedBonusAmount'] and float(body['usedBonusAmount']) > 0
    
    if not has_bonus_amount and not has_used_bonus:
        return {
            'valid': False,
            'error': 'Потрібно вказати bonusAmount > 0 (для нарахування) та/або usedBonusAmount > 0 (для списання)'
        }
    
    # Перевірка типів даних
    try:
        order_total = float(body['orderTotal'])
        
        if order_total <= 0:
            return {
                'valid': False,
                'error': 'Сума замовлення повинна бути більше 0'
            }
        
        # Валідація нарахування бонусів
        if has_bonus_amount:
            bonus_amount = float(body['bonusAmount'])
            
            if bonus_amount <= 0:
                return {
                    'valid': False,
                    'error': 'Сума бонусів повинна бути більше 0'
                }
            
            # Перевірка відповідності бонусів (10% від суми)
            expected_bonus = order_total * BONUS_PERCENTAGE
            if abs(bonus_amount - expected_bonus) > 1:  # Допуск 1 грн
                return {
                    'valid': False,
                    'error': f'Неправильна сума бонусів. Очікується: {expected_bonus:.2f}'
                }
        
        # Валідація списання бонусів
        if has_used_bonus:
            used_bonus_amount = float(body['usedBonusAmount'])
            
            # ВИПРАВЛЕНО: Дозволяємо 0 для випадків, коли бонуси не використовувались
            if used_bonus_amount < 0:
                return {
                    'valid': False,
                    'error': 'Сума використаних бонусів не може бути від\'ємною'
                }
        
    except (ValueError, TypeError):
        return {
            'valid': False,
            'error': 'Неправильний формат числових даних'
        }
    
    # Перевірка даних клієнта
    customer = body.get('customer', {})
    if not customer.get('phone') and not customer.get('email'):
        return {
            'valid': False,
            'error': 'Потрібен телефон або email клієнта'
        }
    
    return {'valid': True}

def check_duplicate_in_keycrm(order_id: str, buyer_email: Optional[str] = None) -> Dict[str, Any]:
    """
    Перевірка дублікатів через поле "Історія бонусів" 
    """
    try:
        # Якщо маємо email клієнта, знайдемо його та перевіримо історію
        if buyer_email:
            buyer_info = find_buyer_in_keycrm(buyer_email)
            if buyer_info['success']:
                buyer_id = buyer_info['buyer_id']
                history = get_bonus_history_from_keycrm(buyer_id)
                
                if history and f"#{order_id}" in history:
                    logger.warning(f"Знайдено дублікат замовлення {order_id} в історії бонусів клієнта {buyer_id}")
                    return {
                        'success': True,
                        'is_duplicate': True,
                        'note': f'Замовлення #{order_id} вже оброблено'
                    }
        
        # Якщо клієнта не знайдено або історії немає - дублікату немає
        logger.info(f"Дублікат для замовлення {order_id} не знайдено")
        return {
            'success': True,
            'is_duplicate': False,
            'note': 'Замовлення нове'
        }
        
    except Exception as e:
        logger.error(f"Помилка перевірки дублікату в KeyCRM: {str(e)}")
        # При помилці перевірки вважаємо що дублікату немає, щоб не блокувати операцію
        return {
            'success': False,
            'is_duplicate': False,
            'error': f'Помилка перевірки дублікату: {str(e)}'
        }

def accrue_bonus(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Логіка нарахування бонусів
    """
    try:
        order_total = float(data['orderTotal'])
        bonus_amount = float(data['bonusAmount'])
        customer = data['customer']
        
        # Тут можна додати додаткову логіку:
        # - Перевірка статусу клієнта
        # - Різні відсотки для різних категорій
        # - Максимальні ліміти тощо
        
        logger.info(f"Нарахування {bonus_amount} бонусів для замовлення {data['orderId']}")
        
        return {
            'success': True,
            'bonus_amount': bonus_amount,
            'customer_identifier': customer.get('phone') or customer.get('email')
        }
        
    except Exception as e:
        logger.error(f"Помилка нарахування бонусів: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def sync_with_keycrm(data: Dict[str, Any], bonus_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Синхронізація з KeyCRM - основна функція для оновлення бонусів клієнта
    """
    try:
        customer_phone = data['customer'].get('phone', '')
        customer_email = data['customer'].get('email', '')
        
        if not customer_phone:
            return {'success': False, 'error': 'Номер телефону не вказано'}
        
        # Очищаємо та нормалізуємо номер телефону
        clean_phone = normalize_phone(customer_phone)
        
        # Перевіряємо дублікати замовлень
        duplicate_check = check_duplicate_in_keycrm(data['orderId'], customer_email)
        if duplicate_check.get('is_duplicate', False):
            logger.warning(f"Замовлення {data['orderId']} вже оброблено для клієнта")
            return {
                'success': True,
                'message': 'Замовлення вже оброблено раніше',
                'duplicate': True,
                'orderId': data['orderId'],
                'duplicate_details': duplicate_check
            }
        
        # Шукаємо клієнта в KeyCRM
        buyer_info = find_buyer_in_keycrm(clean_phone)
        if not buyer_info['success']:
            # Якщо клієнта не знайдено, створюємо нового
            buyer_info = create_buyer_in_keycrm(data, clean_phone)
            if not buyer_info['success']:
                return buyer_info
        
        # Оновлюємо баланс бонусів
        current_bonus = buyer_info.get('bonus_balance', 0)
        new_bonus = current_bonus + bonus_result['bonus_amount']
        
        update_result = update_buyer_bonus_in_keycrm(buyer_info['buyer_id'], new_bonus)
        
        if update_result['success']:
            # Додаємо запис про транзакцію в нотатки клієнта
            log_result = log_transaction_in_keycrm(
                buyer_info['buyer_id'], 
                data['orderId'], 
                bonus_result['bonus_amount'], 
                current_bonus, 
                new_bonus,
                data.get('orderTotal')  # Передаємо суму замовлення для детального логування
            )
            
            logger.info(f"Бонуси успішно оновлено в KeyCRM для клієнта {clean_phone}: {current_bonus} + {bonus_result['bonus_amount']} = {new_bonus}")
            return {
                'success': True,
                'previous_bonus': current_bonus,
                'added_bonus': bonus_result['bonus_amount'],
                'new_bonus': new_bonus,
                'buyer_id': buyer_info['buyer_id']
            }
        else:
            return update_result
            
    except Exception as e:
        logger.error(f"Помилка синхронізації з KeyCRM: {str(e)}")
        return {'success': False, 'error': f'Помилка синхронізації з KeyCRM: {str(e)}'}

def sync_bonus_deduction_with_keycrm(data: Dict[str, Any], deduction_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Синхронізація списання бонусів з KeyCRM
    """
    try:
        customer_phone = data['customer'].get('phone', '')
        customer_email = data['customer'].get('email', '')
        
        if not customer_phone:
            return {'success': False, 'error': 'Номер телефону не вказано'}
        
        # Очищаємо та нормалізуємо номер телефону
        clean_phone = normalize_phone(customer_phone)
        
        # Шукаємо клієнта в KeyCRM
        buyer_info = find_buyer_in_keycrm(clean_phone)
        if not buyer_info['success']:
            return {'success': False, 'error': 'Клієнта не знайдено для списання бонусів'}
        
        # Перевіряємо термін дії бонусів перед списанням
        expiry_check = check_bonus_expiry(buyer_info['buyer_id'])
        if expiry_check.get('expired', False):
            return {
                'success': False,
                'error': f'Бонуси клієнта прострочені (закінчилися {expiry_check.get("expiry_date", "невідомо коли")}). Баланс обнулено.'
            }
        
        # Перевіряємо поточний баланс
        current_bonus = buyer_info.get('bonus_balance', 0)
        deducted_amount = deduction_result['deducted_amount']
        
        if current_bonus < deducted_amount:
            return {
                'success': False, 
                'error': f'Недостатньо бонусів для списання. Поточний баланс: {current_bonus}, потрібно: {deducted_amount}'
            }
        
        # Обчислюємо новий баланс
        new_bonus = current_bonus - deducted_amount
        
        # Оновлюємо баланс бонусів
        update_result = update_buyer_bonus_in_keycrm(buyer_info['buyer_id'], new_bonus)
        
        if update_result['success']:
            # Додаємо запис про списання в історію
            log_result = log_transaction_in_keycrm(
                buyer_info['buyer_id'], 
                data['orderId'], 
                -deducted_amount,  # Від'ємне значення для списання
                current_bonus, 
                new_bonus,
                data.get('orderTotal')
            )
            
            logger.info(f"Бонуси успішно списані в KeyCRM для клієнта {clean_phone}: {current_bonus} - {deducted_amount} = {new_bonus}")
            return {
                'success': True,
                'previous_bonus': current_bonus,
                'deducted_bonus': deducted_amount,
                'new_bonus': new_bonus,
                'buyer_id': buyer_info['buyer_id']
            }
        else:
            return update_result
            
    except Exception as e:
        logger.error(f"Помилка списання бонусів в KeyCRM: {str(e)}")
        return {'success': False, 'error': f'Помилка списання бонусів в KeyCRM: {str(e)}'}

def normalize_phone(phone: str) -> str:
    """
    Нормалізація номера телефону до міжнародного формату
    """
    if not phone:
        return ""
    
    # Видаляємо всі не-цифрові символи
    clean_phone = ''.join(filter(str.isdigit, phone))
    
    # Перетворюємо на міжнародний формат
    if clean_phone.startswith('380') and len(clean_phone) == 12:
        return clean_phone
    elif clean_phone.startswith('80') and len(clean_phone) == 11:
        return '3' + clean_phone
    elif clean_phone.startswith('0') and len(clean_phone) == 10:
        return '38' + clean_phone
    elif len(clean_phone) == 9:
        return '380' + clean_phone
    
    return clean_phone

def make_keycrm_request(url: str, method: str = 'GET', data: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Універсальна функція для запитів до KeyCRM API
    """
    try:
        req = urllib.request.Request(url)
        req.add_header('Authorization', f'Bearer {KEYCRM_API_TOKEN}')
        req.add_header('Accept', 'application/json')
        
        if data:
            req.add_header('Content-Type', 'application/json')
            req.data = json.dumps(data).encode()
        
        if method != 'GET':
            req.get_method = lambda: method
        
        with urllib.request.urlopen(req) as response:
            return {'success': True, 'data': json.loads(response.read().decode())}
            
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.code != 404 else 'Не знайдено'
        return {'success': False, 'error': f'KeyCRM API помилка {e.code}: {error_body}'}
    except Exception as e:
        return {'success': False, 'error': f'Помилка запиту: {str(e)}'}

def find_buyer_in_keycrm(contact: str) -> Dict[str, Any]:
    """
    Універсальний пошук клієнта в KeyCRM за телефоном або email
    """
    # Визначаємо тип контакту та нормалізуємо
    if '@' in contact:
        filter_param = f"filter[buyer_email]={urllib.parse.quote(contact)}"
    else:
        clean_phone = normalize_phone(contact)
        filter_param = f"filter[buyer_phone]={urllib.parse.quote(clean_phone)}"
    
    url = f"{KEYCRM_BASE_URL}/buyer?{filter_param}&include=custom_fields"
    result = make_keycrm_request(url)
    
    if not result['success']:
        return result
    
    keycrm_data = result['data']
    if keycrm_data.get('data') and len(keycrm_data['data']) > 0:
        buyer = keycrm_data['data'][0]
        
        # Шукаємо бонуси в custom fields
        bonus_balance = 0
        bonus_expiry_date = None
        
        if buyer.get('custom_fields'):
            for field in buyer['custom_fields']:
                if field.get('uuid') == BONUS_FIELD_UUID:
                    bonus_balance = int(field.get('value', 0) or 0)
                elif field.get('uuid') == BONUS_EXPIRY_FIELD_UUID:
                    bonus_expiry_date = field.get('value', '')
        
        return {
            'success': True,
            'buyer_id': buyer['id'],
            'bonus_balance': bonus_balance,
            'bonus_expiry_date': bonus_expiry_date,
            'buyer_data': buyer
        }
    else:
        return {'success': False, 'error': 'Клієнта не знайдено'}

def create_buyer_in_keycrm(data: Dict[str, Any], phone: str) -> Dict[str, Any]:
    """
    Створення нового клієнта в KeyCRM
    """
    clean_phone = normalize_phone(phone)
    
    buyer_data = {
        'full_name': data['customer'].get('name', 'Клієнт без імені'),
        'phone': [clean_phone],
        'email': [data['customer'].get('email', '')] if data['customer'].get('email') else []
    }
    
    url = f"{KEYCRM_BASE_URL}/buyer"
    result = make_keycrm_request(url, 'POST', buyer_data)
    
    if result['success'] and result['data'].get('id'):
        buyer_id = result['data']['id']
        logger.info(f"Створено нового клієнта в KeyCRM: {buyer_id}")
        return {
            'success': True,
            'buyer_id': buyer_id,
            'bonus_balance': 0,
            'buyer_data': result['data']
        }
    else:
        return {'success': False, 'error': result.get('error', 'Не вдалося створити клієнта')}

def update_buyer_bonus_in_keycrm(buyer_id: str, new_bonus_amount: int) -> Dict[str, Any]:
    """
    Оновлення балансу бонусів клієнта через custom field "Бонусні бали"
    та встановлення дати закінчення бонусів через 3 місяці
    """
    # Отримуємо поточні дані клієнта
    get_result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}")
    if not get_result['success']:
        return get_result
    
    buyer_data = get_result['data']
    
    # Розраховуємо дату закінчення бонусів (через 3 місяці від сьогодні)
    expiry_date = datetime.utcnow() + timedelta(days=90)  # 3 місяці ≈ 90 днів
    expiry_date_str = expiry_date.strftime('%Y-%m-%d')
    
    # Структура оновлення
    update_data = {
        'full_name': buyer_data.get('full_name', 'Клієнт'),
        'custom_fields': [
            {
                'uuid': BONUS_FIELD_UUID,
                'value': str(new_bonus_amount)
            },
            {
                'uuid': BONUS_EXPIRY_FIELD_UUID,
                'value': expiry_date_str
            }
        ]
    }
    
    # Оновлюємо клієнта
    result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}", 'PUT', update_data)
    
    if result['success']:
        logger.info(f"✅ Баланс бонусів клієнта {buyer_id} оновлено до {new_bonus_amount}, дата закінчення: {expiry_date_str}")
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

def get_bonus_history_from_keycrm(buyer_id: str) -> Optional[str]:
    """
    Отримати історію бонусів з кастомного поля "Історія бонусів"
    """
    try:
        # Отримуємо дані клієнта з кастомними полями
        get_result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}?include=custom_fields")
        if not get_result['success']:
            logger.error(f"Помилка при отриманні даних клієнта {buyer_id}: {get_result['error']}")
            return None
        
        buyer_data = get_result['data']
        
        # Шукаємо поле "Історія бонусів"
        if buyer_data.get('custom_fields'):
            for field in buyer_data['custom_fields']:
                if field.get('uuid') == HISTORY_FIELD_UUID:
                    return field.get('value', '') or ''
        
        return ''  # Повертаємо порожній рядок, якщо поле не знайдено
        
    except Exception as e:
        logger.error(f"Помилка при отриманні історії бонусів: {str(e)}")
        return None

def log_transaction_in_keycrm(buyer_id: str, order_id: str, bonus_amount: float, old_balance: int, new_balance: int, order_total: Optional[float] = None) -> Dict[str, Any]:
    """
    Логування бонусної транзакції в кастомне поле "Історія бонусів"
    """
    try:
        # Отримуємо поточні дані клієнта
        get_result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}")
        if not get_result['success']:
            return get_result
        
        buyer_data = get_result['data']
        
        # Формуємо короткий запис в новому форматі: дата | номер замовлення | сума замовлення | +нараховано | -списано | баланс (старий→новий) | термін
        date_str = datetime.utcnow().strftime('%d.%m')
        
        order_total_int = int(float(order_total)) if order_total else 0
        
        # Визначаємо тип операції та формуємо відповідний запис
        if bonus_amount >= 0:
            # Нарахування: показуємо +нараховано | -0
            added_str = f"+{int(bonus_amount)}"
            used_str = "-0"
        else:
            # Списання: показуємо +0 | -списано
            added_str = "+0"
            used_str = f"-{int(abs(bonus_amount))}"
        
        # Додаємо дату закінчення бонусів (через 3 місяці)
        expiry_date = (datetime.utcnow() + timedelta(days=90)).strftime('%d.%m.%y')
        
        transaction_entry = f"🎁 {date_str} | #{order_id} | {order_total_int}₴ | {added_str} | {used_str} | {int(old_balance)}→{int(new_balance)} | до {expiry_date}"
        
        # Отримуємо поточну історію бонусів з кастомного поля
        current_history = get_bonus_history_from_keycrm(buyer_id) or ''
        
        # Перевіряємо чи немає вже запису про це замовлення
        if f"#{order_id}" in current_history:
            logger.info(f"Запис про замовлення {order_id} вже існує в історії бонусів, пропускаємо")
            return {
                'success': True, 
                'result': 'Запис про транзакцію вже існує',
                'transaction_log': transaction_entry,
                'history_updated': False
            }
        
        # Розділяємо історію на окремі записи
        history_lines = [line.strip() for line in current_history.split('\n') if line.strip()]
        
        # Додаємо нову транзакцію на початок
        history_lines.insert(0, transaction_entry)
        
        # Обмежуємо кількість записів (останні 10 транзакцій)
        if len(history_lines) > 10:
            history_lines = history_lines[:10]
        
        # Формуємо нову історію
        new_history = '\n'.join(history_lines)
        
        # Обмежуємо розмір історії (до 1000 символів)
        if len(new_history) > 1000:
            # Обрізаємо до менших розмірів і додаємо позначку
            truncated_lines = history_lines[:7]  # Берем перші 7 записів
            new_history = '\n'.join(truncated_lines) + '\n...'
        
        # Оновлюємо кастомне поле "Історія бонусів"
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
        
        result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}", 'PUT', update_data)
        
        if result['success']:
            logger.info(f"✅ Транзакція записана в історію бонусів клієнта {buyer_id}: замовлення {order_id}, +{bonus_amount} бонусів")
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

def log_combined_transaction_in_keycrm(buyer_id: str, order_id: str, used_bonus: float, added_bonus: float, old_balance: int, new_balance: int, order_total: Optional[float] = None) -> Dict[str, Any]:
    """
    Логування комбінованої бонусної транзакції (списання + нарахування) в одному рядку історії
    Формат: 🎁 дата | #orderId | сума₴ | +нараховано | -списано | старий→новий | до дата_закінчення
    Якщо списання не було - показується "-0"
    """
    try:
        # Отримуємо поточні дані клієнта
        get_result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}")
        if not get_result['success']:
            return get_result
        
        buyer_data = get_result['data']
        
        # Формуємо запис для комбінованої операції
        date_str = datetime.utcnow().strftime('%d.%m')
        order_total_int = int(float(order_total)) if order_total else 0
        
        # Формуємо рядок: +нараховано | -списано (або -0 якщо списання не було)
        added_str = f"+{int(added_bonus)}" if added_bonus > 0 else "+0"
        used_str = f"-{int(used_bonus)}" if used_bonus > 0 else "-0"
        
        # Додаємо дату закінчення бонусів (через 3 місяці)
        expiry_date = (datetime.utcnow() + timedelta(days=90)).strftime('%d.%m.%y')
        
        transaction_entry = f"🎁 {date_str} | #{order_id} | {order_total_int}₴ | {added_str} | {used_str} | {int(old_balance)}→{int(new_balance)} | до {expiry_date}"
        
        # Отримуємо поточну історію бонусів з кастомного поля
        current_history = get_bonus_history_from_keycrm(buyer_id) or ''
        
        # Перевіряємо чи немає вже запису про це замовлення
        if f"#{order_id}" in current_history:
            logger.info(f"Запис про замовлення {order_id} вже існує в історії бонусів, пропускаємо")
            return {
                'success': True, 
                'result': 'Запис про транзакцію вже існує',
                'transaction_log': transaction_entry,
                'history_updated': False
            }
        
        # Розділяємо історію на окремі записи
        history_lines = [line.strip() for line in current_history.split('\n') if line.strip()]
        
        # Додаємо нову транзакцію на початок
        history_lines.insert(0, transaction_entry)
        
        # Обмежуємо кількість записів (останні 10 транзакцій)
        if len(history_lines) > 10:
            history_lines = history_lines[:10]
        
        # Формуємо нову історію
        new_history = '\n'.join(history_lines)
        
        # Обмежуємо розмір історії (до 1000 символів)
        if len(new_history) > 1000:
            # Обрізаємо до менших розмірів і додаємо позначку
            truncated_lines = history_lines[:7]  # Берем перші 7 записів
            new_history = '\n'.join(truncated_lines) + '\n...'
        
        # Оновлюємо кастомне поле "Історія бонусів"
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
        
        update_result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}", method='PUT', data=update_data)
        
        if update_result['success']:
            logger.info(f"Комбінована транзакція записана в історію: {transaction_entry}")
            return {
                'success': True, 
                'result': 'Комбінована транзакція записана в історію',
                'transaction_log': transaction_entry,
                'history_updated': True,
                'new_history': new_history
            }
        else:
            logger.error(f"Помилка оновлення історії бонусів: {update_result}")
            return update_result
            
    except Exception as e:
        logger.error(f"Помилка логування комбінованої транзакції: {str(e)}")
        return {'success': False, 'error': f'Помилка логування комбінованої транзакції: {str(e)}'}

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Створення HTTP відповіді
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        },
        'body': json.dumps(body, ensure_ascii=False)
    }

def lambda_handler_options(event, context):
    """
    Обробник для CORS preflight запитів
    """
    logger.info("Обробка CORS preflight запиту")
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Max-Age': '86400'
        },
        'body': ''
    }

def get_buyer_transaction_history(buyer_id: str, limit: int = 50) -> Dict[str, Any]:
    """
    Тимчасово повертаємо порожню історію (API нотаток недоступний)
    """
    try:
        logger.info(f"Запит історії транзакцій для клієнта {buyer_id} (API нотаток тимчасово недоступний)")
        
        # Тимчасово повертаємо порожню історію
        return {
            'success': True,
            'transactions': [],
            'total_count': 0,
            'note': 'API нотаток тимчасово недоступний - історія буде доступна після налаштування'
        }
        
    except Exception as e:
        logger.error(f"Помилка отримання історії транзакцій: {str(e)}")
        return {
            'success': False,
            'error': f'Помилка отримання історії: {str(e)}',
            'transactions': [],
            'total_count': 0
        }

def handle_get_transaction_history(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Обробка запиту на отримання історії бонусних транзакцій клієнта
    """
    try:
        # Отримуємо параметри з query string
        query_params = event.get('queryStringParameters') or {}
        
        # Параметри пошуку клієнта
        phone = query_params.get('phone')
        email = query_params.get('email')
        buyer_id = query_params.get('buyer_id')
        limit = int(query_params.get('limit', 50))
        
        if not any([phone, email, buyer_id]):
            return create_response(400, {
                'error': 'Необхідно вказати phone, email або buyer_id для пошуку клієнта',
                'success': False
            })
        
        # Якщо передано buyer_id, використовуємо його
        if buyer_id:
            target_buyer_id = buyer_id
        else:
            # Якщо передано phone або email, знаходимо клієнта
            search_param = phone or email
            if not search_param:
                return create_response(400, {
                    'error': 'Необхідно вказати phone, email або buyer_id для пошуку клієнта',
                    'success': False
                })
            
            buyer_info = find_buyer_in_keycrm(str(search_param))
            
            if not buyer_info['success']:
                return create_response(404, {
                    'error': 'Клієнта не знайдено',
                    'success': False
                })
            
            target_buyer_id = buyer_info['buyer_id']
        
        # Отримуємо історію транзакцій
        history_result = get_buyer_transaction_history(target_buyer_id, limit)
        
        if history_result['success']:
            return create_response(200, {
                'success': True,
                'buyer_id': target_buyer_id,
                'transactions': history_result['transactions'],
                'total_count': history_result['total_count']
            })
        else:
            return create_response(500, {
                'error': f"Помилка отримання історії: {history_result.get('error', 'Невідома помилка')}",
                'success': False
            })
            
    except Exception as e:
        logger.error(f"Помилка обробки запиту історії: {str(e)}")
        return create_response(500, {
            'error': f'Внутрішня помилка сервера: {str(e)}',
            'success': False
        })

def get_buyer_bonus_balance(phone: Optional[str] = None, email: Optional[str] = None, buyer_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Отримання поточного балансу бонусів клієнта
    """
    if buyer_id:
        # Якщо є buyer_id, робимо прямий запит
        result = make_keycrm_request(f"{KEYCRM_BASE_URL}/buyer/{buyer_id}?include=custom_fields")
        if result['success']:
            buyer = result['data']
            bonus_balance = 0
            bonus_expiry_date = None
            
            if buyer.get('custom_fields'):
                for field in buyer['custom_fields']:
                    if field.get('uuid') == BONUS_FIELD_UUID:
                        bonus_balance = int(field.get('value', 0) or 0)
                    elif field.get('uuid') == BONUS_EXPIRY_FIELD_UUID:
                        bonus_expiry_date = field.get('value', '')
            
            return {
                'success': True,
                'buyer_id': buyer_id,
                'bonus_balance': bonus_balance,
                'bonus_expiry_date': bonus_expiry_date,
                'buyer_data': buyer
            }
        else:
            return result
    else:
        # Шукаємо по контакту
        search_param = phone or email
        if not search_param:
            return {
                'success': False, 
                'error': 'Необхідно вказати phone, email або buyer_id'
            }
        
        return find_buyer_in_keycrm(search_param)

def deduct_bonus(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Логіка списання використаних бонусів
    """
    try:
        used_bonus_amount = float(data['usedBonusAmount'])
        customer = data['customer']
        
        if used_bonus_amount <= 0:
            return {
                'success': False,
                'error': 'Сума списаних бонусів повинна бути більше 0'
            }
        
        logger.info(f"Списання {used_bonus_amount} бонусів для замовлення {data['orderId']}")
        
        return {
            'success': True,
            'deducted_amount': used_bonus_amount,
            'customer_identifier': customer.get('phone') or customer.get('email')
        }
        
    except Exception as e:
        logger.error(f"Помилка списання бонусів: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def process_combined_bonus_operation(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обробка комбінованої операції: списання і нарахування бонусів в одному замовленні
    """
    try:
        customer_phone = data['customer'].get('phone', '')
        customer_email = data['customer'].get('email', '')
        
        if not customer_phone:
            return {'success': False, 'error': 'Номер телефону не вказано'}
        
        # Очищаємо та нормалізуємо номер телефону
        clean_phone = normalize_phone(customer_phone)
        
        # Перевіряємо дублікати замовлень
        duplicate_check = check_duplicate_in_keycrm(data['orderId'], customer_email)
        if duplicate_check.get('is_duplicate', False):
            logger.warning(f"Замовлення {data['orderId']} вже оброблено для клієнта")
            return {
                'success': True,
                'message': 'Замовлення вже оброблено раніше',
                'duplicate': True,
                'orderId': data['orderId'],
                'duplicate_details': duplicate_check
            }
        
        # Шукаємо клієнта в KeyCRM
        buyer_info = find_buyer_in_keycrm(clean_phone)
        if not buyer_info['success']:
            # Якщо клієнта не знайдено, створюємо нового
            buyer_info = create_buyer_in_keycrm(data, clean_phone)
            if not buyer_info['success']:
                return buyer_info
        
        # Якщо є списання бонусів - перевіряємо термін дії
        used_bonus_amount = float(data['usedBonusAmount'])
        if used_bonus_amount > 0:
            expiry_check = check_bonus_expiry(buyer_info['buyer_id'])
            if expiry_check.get('expired', False):
                return {
                    'success': False,
                    'error': f'Бонуси клієнта прострочені (закінчилися {expiry_check.get("expiry_date", "невідомо коли")}). Баланс обнулено.'
                }
        
        # Отримуємо початкові дані
        current_bonus = buyer_info.get('bonus_balance', 0)
        new_bonus_amount = float(data['bonusAmount'])
        
        # Перевірка достатності бонусів для списання
        if current_bonus < used_bonus_amount:
            return {
                'success': False, 
                'error': f'Недостатньо бонусів для списання. Поточний баланс: {current_bonus}, потрібно: {used_bonus_amount}'
            }
        
        # Обчислюємо фінальний баланс: поточний - списані + нараховані
        intermediate_balance = current_bonus - used_bonus_amount
        final_balance = intermediate_balance + new_bonus_amount
        
        # Оновлюємо баланс бонусів
        update_result = update_buyer_bonus_in_keycrm(buyer_info['buyer_id'], final_balance)
        
        if update_result['success']:
            # Записуємо комбіновану операцію в одному рядку історії
            log_combined_result = log_combined_transaction_in_keycrm(
                buyer_info['buyer_id'], 
                data['orderId'], 
                used_bonus_amount,   # Списані бонуси
                new_bonus_amount,    # Нараховані бонуси
                current_bonus,       # Початковий баланс
                final_balance,       # Фінальний баланс
                data.get('orderTotal')
            )
            
            # Логування комбінованої транзакції в одному рядку
            log_combined_result = log_combined_transaction_in_keycrm(
                buyer_info['buyer_id'],
                data['orderId'],
                used_bonus_amount,
                new_bonus_amount,
                current_bonus,
                final_balance,
                data.get('orderTotal')
            )
            
            logger.info(f"Комбінована операція виконана для клієнта {clean_phone}: {current_bonus} - {used_bonus_amount} + {new_bonus_amount} = {final_balance}")
            
            return {
                'success': True,
                'operations': [
                    {
                        'type': 'deduction',
                        'amount': used_bonus_amount,
                        'balance_before': current_bonus,
                        'balance_after': intermediate_balance
                    },
                    {
                        'type': 'accrual', 
                        'amount': new_bonus_amount,
                        'balance_before': intermediate_balance,
                        'balance_after': final_balance
                    }
                ],
                'initial_bonus': current_bonus,
                'used_bonus': used_bonus_amount,
                'added_bonus': new_bonus_amount,
                'final_bonus': final_balance,
                'buyer_id': buyer_info['buyer_id']
            }
        else:
            return update_result
            
    except Exception as e:
        logger.error(f"Помилка комбінованої операції з бонусами: {str(e)}")
        return {'success': False, 'error': f'Помилка комбінованої операції: {str(e)}'}

def check_bonus_expiry(buyer_id: str) -> Dict[str, Any]:
    """
    Перевірка та обнулення прострочених бонусів
    """
    try:
        # Отримуємо інформацію про клієнта
        buyer_result = get_buyer_bonus_balance(buyer_id=buyer_id)
        
        if not buyer_result['success']:
            return buyer_result
        
        bonus_balance = buyer_result.get('bonus_balance', 0)
        bonus_expiry_date = buyer_result.get('bonus_expiry_date')
        
        # Якщо бонусів немає або дати закінчення немає - нічого робити
        if bonus_balance <= 0 or not bonus_expiry_date:
            return {
                'success': True,
                'expired': False,
                'message': 'Бонуси відсутні або дата закінчення не встановлена'
            }
        
        # Перевіряємо чи не закінчилися бонуси
        try:
            expiry_date = datetime.strptime(bonus_expiry_date, '%Y-%m-%d')
            current_date = datetime.utcnow()
            
            if current_date > expiry_date:
                # Бонуси прострочені - обнуляємо
                logger.info(f"Бонуси клієнта {buyer_id} прострочені ({bonus_expiry_date}), обнуляємо баланс")
                
                # Оновлюємо баланс до 0 та встановлюємо нову дату закінчення
                update_result = update_buyer_bonus_in_keycrm(buyer_id, 0)
                
                if update_result['success']:
                    # Логуємо операцію обнулення
                    log_result = log_transaction_in_keycrm(
                        buyer_id,
                        f"EXPIRED_{int(current_date.timestamp())}",  # Унікальний ID для операції обнулення
                        -bonus_balance,  # Від'ємне значення - списали всі бонуси
                        bonus_balance,   # Був баланс
                        0,               # Став баланс
                        None             # Без суми замовлення
                    )
                    
                    return {
                        'success': True,
                        'expired': True,
                        'expired_amount': bonus_balance,
                        'expiry_date': bonus_expiry_date,
                        'message': f'Бонуси прострочені та обнулені: {bonus_balance} бонусів'
                    }
                else:
                    return update_result
            else:
                return {
                    'success': True,
                    'expired': False,
                    'expiry_date': bonus_expiry_date,
                    'current_balance': bonus_balance,
                    'message': 'Бонуси ще дійсні'
                }
                
        except ValueError:
            logger.error(f"Неправильний формат дати закінчення бонусів: {bonus_expiry_date}")
            return {
                'success': False,
                'error': f'Неправильний формат дати закінчення: {bonus_expiry_date}'
            }
            
    except Exception as e:
        logger.error(f"Помилка перевірки терміну дії бонусів: {str(e)}")
        return {
            'success': False,
            'error': f'Помилка перевірки терміну дії: {str(e)}'
        }

def handle_check_bonus_expiry(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Обробка запиту на перевірку терміну дії бонусів клієнта
    """
    try:
        # Отримуємо параметри з query string
        query_params = event.get('queryStringParameters') or {}
        
        # Параметри пошуку клієнта
        phone = query_params.get('phone')
        email = query_params.get('email')
        buyer_id = query_params.get('buyer_id')
        
        if not any([phone, email, buyer_id]):
            return create_response(400, {
                'error': 'Необхідно вказати phone, email або buyer_id для пошуку клієнта',
                'success': False
            })
        
        # Якщо передано buyer_id, використовуємо його
        if buyer_id:
            target_buyer_id = buyer_id
        else:
            # Якщо передано phone або email, знаходимо клієнта
            search_param = phone or email
            if not search_param:
                return create_response(400, {
                    'error': 'Необхідно вказати phone, email або buyer_id для пошуку клієнта',
                    'success': False
                })
            
            buyer_info = find_buyer_in_keycrm(str(search_param))
            
            if not buyer_info['success']:
                return create_response(404, {
                    'error': 'Клієнта не знайдено',
                    'success': False
                })
            
            target_buyer_id = buyer_info['buyer_id']
        
        # Перевіряємо термін дії бонусів
        expiry_result = check_bonus_expiry(target_buyer_id)
        
        if expiry_result['success']:
            return create_response(200, {
                'success': True,
                'buyer_id': target_buyer_id,
                'expired': expiry_result.get('expired', False),
                'expiry_date': expiry_result.get('expiry_date'),
                'current_balance': expiry_result.get('current_balance', 0),
                'expired_amount': expiry_result.get('expired_amount', 0),
                'message': expiry_result.get('message', '')
            })
        else:
            return create_response(500, {
                'error': f"Помилка перевірки терміну дії: {expiry_result.get('error', 'Невідома помилка')}",
                'success': False
            })
            
    except Exception as e:
        logger.error(f"Помилка обробки запиту перевірки терміну дії: {str(e)}")
        return create_response(500, {
            'error': f'Внутрішня помилка сервера: {str(e)}',
            'success': False
        })
