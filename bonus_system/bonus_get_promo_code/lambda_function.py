import json
import logging
import os
import sys
import boto3
from promo_logic import PromoService

# --- Налаштування логера ---
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# --- Налаштування для батчової обробки ---
BATCH_THRESHOLD = int(os.getenv('BATCH_THRESHOLD', '20'))  # Поріг для запуску поповнення
MIN_CODES_THRESHOLD = int(os.getenv('MIN_CODES_THRESHOLD', '3'))  # Мінімум кодів для запуску поповнення

# --- Глобальні клієнти для пере-використання ---
lambda_client = boto3.client('lambda')

def get_cors_headers():
    """Повертає стандартні CORS headers для всіх відповідей"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Requested-With',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        'Access-Control-Max-Age': '86400'  # Cache preflight for 24 hours
    }

def lambda_handler(event, context):
    """
    Основний обробник: швидко отримує промокод з S3, записує використання і запускає поповнення при потребі.
    
    Логіка запуску поповнення:
    1. Якщо використано >= 20 промокодів (батч)
    2. Якщо для поточної суми залишилось <= 3 промокодів
    """
    logger.info(f"🚀 [Get] функція запущена (поріг батчу: {BATCH_THRESHOLD}, мін. кодів: {MIN_CODES_THRESHOLD})")
    
    # Обробка OPTIONS запиту для CORS preflight
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
    if http_method == 'OPTIONS':
        logger.info("📋 Обробка OPTIONS запиту для CORS")
        return {
            'statusCode': 200,
            'headers': get_cors_headers(),
            'body': json.dumps({'message': 'CORS preflight successful'})
        }
    
    try:
        body = json.loads(event.get('body', '{}'))
        amount = body.get('amount')

        if not amount:
            return {
                'statusCode': 400, 
                'headers': get_cors_headers(),
                'body': json.dumps({'error': 'Missing required field: amount'})
            }

        # Ініціалізація сервісу (без браузера)
        promo_service = PromoService()
        
        # Отримання та видалення промокоду з S3
        promo_code, should_trigger_low_count = promo_service.get_and_remove_code_from_s3(str(amount))
        
        if not promo_code:
            error_message = f"Промокоди для суми {amount} грн закінчилися."
            logger.error(f"❌ {error_message}")
            return {
                'statusCode': 404, 
                'headers': get_cors_headers(),
                'body': json.dumps({'error': error_message})
            }
            
        logger.info(f"✅ [Get] Успішно отримано промокод: {promo_code}")
        
        # Записуємо використання промокоду і перевіряємо, чи потрібно запускати поповнення
        try:
            should_trigger_batch = promo_service.add_used_code_count(str(amount))
            
            # Запускаємо поповнення, якщо:
            # 1. Набралося достатньо використаних промокодів (батч)
            # 2. Залишилось мало промокодів для цієї суми
            if should_trigger_batch or should_trigger_low_count:
                reason = []
                if should_trigger_batch:
                    reason.append(f"досягнуто поріг батчу ({BATCH_THRESHOLD})")
                if should_trigger_low_count:
                    reason.append(f"залишилось мало кодів (<= {MIN_CODES_THRESHOLD})")
                
                logger.info(f"🚀 [Get] Запускаємо поповнення. Причини: {', '.join(reason)}")
                
                payload = {
                    'trigger_source': 'usage_threshold_reached',
                    'trigger_reasons': reason
                }
                lambda_client.invoke(
                    FunctionName=os.getenv('REPLENISH_FUNCTION_NAME', 'replenish-promo-code'),
                    InvocationType='Event',  # Асинхронний виклик
                    Payload=json.dumps(payload)
                )
                logger.info(f"🚀 [Get] Запущено поповнення промокодів.")
            else:
                logger.info(f"⏳ [Get] Використання записано. Поповнення поки не потрібне.")
                
        except Exception as e:
            logger.error(f"❌ [Get] Помилка при роботі з лічильниками: {e}")
            # Не повертаємо помилку клієнту, оскільки основна операція успішна

        return {
            'statusCode': 200,
            'headers': get_cors_headers(),
            'body': json.dumps({'success': True, 'promo_code': promo_code, 'amount': amount})
        }

    except Exception as e:
        logger.error(f"❌ [Get] Критична помилка: {e}", exc_info=True)
        return {
            'statusCode': 500, 
            'headers': get_cors_headers(),
            'body': json.dumps({'error': 'Internal Server Error'})
        }