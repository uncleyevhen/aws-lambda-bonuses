import json
import logging
import os
import sys
import time
from functools import wraps
from promo_logic import PromoService
from browser_manager import create_browser_manager
from decorators import timeout_handler, retry_on_failure
from config_validator import validate_environment

# --- Налаштування логера ---
def setup_logger():
    """Налаштовує логер один раз для Lambda функції"""
    logger = logging.getLogger(__name__)
    if not logger.handlers:  # Уникаємо дублювання handlers
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def create_response(status, message=None, **kwargs):
    """Створює стандартизовану відповідь"""
    response = {
        'status': status,
        'timestamp': int(time.time()),
        'version': '2.0'  # Версія API для backwards compatibility
    }
    
    if message:
        response['message'] = message
    
    # Додаємо додаткові поля
    response.update(kwargs)
    
    return response

def validate_event(event):
    """Валідує вхідні дані Lambda event"""
    if not isinstance(event, dict):
        raise ValueError("Event повинен бути dictionary")
    
    # Можна додати специфічні перевірки для ваших даних
    # наприклад: required fields, format validation тощо
    return True

# Стандартний Lambda handler (AWS Lambda очікує функцію з назвою lambda_handler)
@timeout_handler(timeout_seconds=1200)
@retry_on_failure(max_retries=2, delay=2)
def lambda_handler(event, context):
    """
    Фоновий обробник: обробляє чергу запитів і створює промокоди батчами.
    """
    start_time = time.time()
    logger = setup_logger()
    
    try:
        # Валідація конфігурації середовища
        try:
            validate_environment()
        except ValueError as config_error:
            logger.error(f"❌ [Replenish] Помилка конфігурації: {config_error}")
            return create_response('error', f'Configuration error: {config_error}')
        
        # Валідація вхідних даних
        validate_event(event)
        logger.info("🚀 [Replenish] функція запущена")
    except ValueError as e:
        logger.error(f"❌ [Replenish] Невалідні вхідні дані: {e}")
        return create_response('error', f'Invalid input: {e}')
    
    browser_manager = None
    try:
        
        logger.info("📦 [Replenish] Обробка використаних промокодів")
        
        # Створюємо браузер менеджер
        browser_manager = create_browser_manager()
        page = browser_manager.get_page()
        
        promo_service = PromoService(page)
        
        # Отримуємо використані промокоди БЕЗ очищення
        used_codes_count = promo_service.get_used_codes_count()
        
        if not used_codes_count:
            logger.info("ℹ️ [Replenish] Лічільники пусті, нічого обробляти")
            execution_time = time.time() - start_time
            return create_response('success', 'No used codes to process', 
                                 execution_time=round(execution_time, 2),
                                 processed_amounts=[])
        
        logger.info(f"📊 [Replenish] Знайдено використані промокоди: {used_codes_count}")
        
        # Використовуємо розумне поповнення (рекомендований метод)
        # Він оновлює тільки ті суми, які потребують поповнення, зберігаючи інші недоторканими
        success = promo_service.replenish_promo_codes(used_codes_count)
        
        if success:
            # Після успішного поповнення, очищаємо лічільники використаних промокодів
            total_cleared = 0
            for amount_str in used_codes_count.keys():
                amount = int(amount_str)
                if promo_service.clear_used_codes_count(amount):
                    total_cleared += 1
                    logger.info(f"✅ [Replenish] Очищено лічільник для суми {amount}")
                else:
                    logger.warning(f"⚠️ [Replenish] Не вдалося очистити лічільник для суми {amount}")
            
            logger.info(f"🎉 [Replenish] Розумне поповнення завершено успішно! Очищено {total_cleared} лічільників")
            
            return {
                'status': 'success',
                'method': 'smart_replenish',
                'processed_amounts': list(used_codes_count.keys()),
                'cleared_counters': total_cleared,
                'amount_summary': used_codes_count,
                'timestamp': int(time.time())  # Додаємо timestamp для моніторингу
            }
        else:
            logger.error("❌ [Replenish] Розумне поповнення не вдалося")
            return {'status': 'error', 'message': 'Smart replenish failed'}
        
    except Exception as e:
        logger.error(f"❌ [Replenish] Критична помилка: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}
    finally:
        if browser_manager:
            browser_manager.cleanup()
            logger.info("🚪 [Replenish] Браузер закрито.")