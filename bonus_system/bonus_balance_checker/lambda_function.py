import json
import urllib.request
import urllib.parse
import urllib.error
import ssl

def lambda_handler(event, context):
    """
    AWS Lambda функція для перевірки балансу бонусів клієнта через KeyCRM API
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
        
        print(f"Отримані параметри: {query_params}")
        print(f"Номер телефону: {phone}")
        
        if not phone:
            print("Помилка: параметр phone відсутній")
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({'error': 'Параметр phone є обов\'язковим'})
            }
        
        # Очищаємо номер телефону від всіх символів, крім цифр
        clean_phone = ''.join(filter(str.isdigit, phone))
        
        # Нормалізуємо номер (додаємо 380 якщо потрібно)
        if clean_phone.startswith('0') and len(clean_phone) == 10:
            clean_phone = '38' + clean_phone  # 0123456789 -> 380123456789
        elif clean_phone.startswith('380') and len(clean_phone) == 12:
            pass  # Вже в правильному форматі
        elif len(clean_phone) == 9:
            clean_phone = '380' + clean_phone  # 123456789 -> 380123456789
        
        print(f"Оригінальний номер: {phone}, Очищений: {clean_phone}")
        
        # KeyCRM API налаштування
        keycrm_api_token = 'M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ'
        keycrm_url = f"https://openapi.keycrm.app/v1/buyer?filter[buyer_phone]={urllib.parse.quote(clean_phone)}&include=loyalty,custom_fields"
        
        # Створюємо запит до KeyCRM
        req = urllib.request.Request(keycrm_url)
        req.add_header('Authorization', f'Bearer {keycrm_api_token}')
        req.add_header('Accept', 'application/json')
        
        # Виконуємо запит
        print(f"Виконуємо запит до KeyCRM: {keycrm_url}")
        with urllib.request.urlopen(req) as response:
            print(f"Отримали відповідь від KeyCRM: {response.status}")
            keycrm_data = json.loads(response.read().decode())
            print(f"Дані від KeyCRM: {keycrm_data}")
        
        # Обробляємо відповідь KeyCRM
        bonus_balance = 0
        
        if keycrm_data.get('data') and len(keycrm_data['data']) > 0:
            buyer = keycrm_data['data'][0]
            
            # Шукаємо бонуси в кастомних полях
            if buyer.get('custom_fields'):
                for field in buyer['custom_fields']:
                    if field.get('name') and (
                        'бонус' in field['name'].lower() or 
                        'bonus' in field['name'].lower() or
                        'бали' in field['name'].lower()
                    ):
                        bonus_balance = int(field.get('value', 0) or 0)
                        break
            
            # Альтернативно в loyalty програмі
            if bonus_balance == 0 and buyer.get('loyalty'):
                if len(buyer['loyalty']) > 0:
                    bonus_balance = int(buyer['loyalty'][0].get('amount', 0) or 0)
        
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'success': True,
                'bonus_balance': bonus_balance,
                'phone': clean_phone,
                'original_phone': phone
            })
        }
        
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.code != 404 else 'Покупця не знайдено'
        print(f"HTTP помилка: {e.code}, тіло помилки: {error_body}")
        return {
            'statusCode': e.code,
            'headers': headers,
            'body': json.dumps({'error': f'KeyCRM API помилка: {error_body}'})
        }
        
    except Exception as e:
        print(f"Загальна помилка: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error': f'Внутрішня помилка сервера: {str(e)}'})
        }
