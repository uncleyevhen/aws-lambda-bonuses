import json
import logging
import os
import sys
import time
import boto3
from functools import wraps
from promo_logic import PromoService
from browser_manager import create_browser_manager
from decorators import timeout_handler, retry_on_failure
from config_validator import validate_environment
from botocore.exceptions import ClientError

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
        
        # Контроль рівня логування через змінну середовища
        log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
        if log_level == 'ERROR':
            logger.setLevel(logging.ERROR)
        elif log_level == 'WARNING':
            logger.setLevel(logging.WARNING)  
        elif log_level == 'DEBUG':
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
    return logger

class LambdaLockManager:
    """Менеджер блокування Lambda функції через S3"""
    
    def __init__(self, bucket_name=None, lock_key=None):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name or 'lambda-promo-sessions'
        self.lock_key = lock_key or 'lambda_execution_lock.json'
        self.logger = setup_logger()
        self.execution_id = f"exec_{int(time.time())}_{os.environ.get('AWS_REQUEST_ID', 'local')}"
        self.lock_timeout = 1800  # 30 хвилин максимум
    
    def acquire_lock(self):
        """Отримує блокування перед запуском Lambda"""
        try:
            # Спробуємо отримати поточний lock
            lock_data = self._get_current_lock()
            
            if lock_data:
                # Перевіряємо чи lock не застарілий
                lock_age = time.time() - lock_data.get('timestamp', 0)
                if lock_age < self.lock_timeout:
                    locked_by = lock_data.get('execution_id', 'unknown')
                    remaining_time = self.lock_timeout - lock_age
                    
                    self.logger.warning(f"🔒 [Lock] Функція вже виконується (locked by: {locked_by})")
                    self.logger.info(f"⏰ [Lock] Очікуваний час звільнення: {remaining_time:.0f} секунд")
                    
                    return False, f"Lambda function is already running (locked by: {locked_by}). Estimated remaining time: {remaining_time:.0f}s"
                else:
                    self.logger.info(f"🕒 [Lock] Застарілий lock видалено (age: {lock_age:.0f}s)")
            
            # Створюємо новий lock
            lock_data = {
                'execution_id': self.execution_id,
                'timestamp': time.time(),
                'aws_request_id': os.environ.get('AWS_REQUEST_ID', 'local'),
                'timeout': self.lock_timeout
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.lock_key,
                Body=json.dumps(lock_data),
                ContentType='application/json'
            )
            
            self.logger.info(f"🔓 [Lock] Блокування отримано: {self.execution_id}")
            return True, "Lock acquired successfully"
            
        except Exception as e:
            self.logger.error(f"❌ [Lock] Помилка отримання блокування: {e}")
            return False, f"Failed to acquire lock: {e}"
    
    def release_lock(self):
        """Звільняє блокування після завершення Lambda"""
        try:
            # Перевіряємо чи це наш lock
            lock_data = self._get_current_lock()
            
            if lock_data and lock_data.get('execution_id') == self.execution_id:
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=self.lock_key
                )
                self.logger.info(f"🔓 [Lock] Блокування звільнено: {self.execution_id}")
                return True
            else:
                self.logger.warning(f"⚠️ [Lock] Спроба звільнити чужий lock")
                return False
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                # Lock вже не існує
                self.logger.info(f"ℹ️ [Lock] Блокування вже відсутнє")
                return True
            else:
                self.logger.error(f"❌ [Lock] Помилка звільнення блокування: {e}")
                return False
        except Exception as e:
            self.logger.error(f"❌ [Lock] Несподівана помилка звільнення блокування: {e}")
            return False
    
    def _get_current_lock(self):
        """Отримує поточний lock з S3"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.lock_key
            )
            lock_data = json.loads(response['Body'].read().decode('utf-8'))
            return lock_data
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None  # Lock не існує
            else:
                raise e
    
    def force_unlock(self):
        """Примусово видаляє блокування (використовувати обережно)"""
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=self.lock_key
            )
            self.logger.warning(f"⚠️ [Lock] ПРИМУСОВЕ звільнення блокування")
            return True
        except Exception as e:
            self.logger.error(f"❌ [Lock] Помилка примусового звільнення: {e}")
            return False
    
    def get_lock_status(self):
        """Повертає статус поточного блокування"""
        lock_data = self._get_current_lock()
        if not lock_data:
            return {'locked': False}
        
        lock_age = time.time() - lock_data.get('timestamp', 0)
        is_expired = lock_age >= self.lock_timeout
        
        return {
            'locked': not is_expired,
            'execution_id': lock_data.get('execution_id'),
            'lock_age': lock_age,
            'remaining_time': max(0, self.lock_timeout - lock_age),
            'expired': is_expired,
            'aws_request_id': lock_data.get('aws_request_id')
        }

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
    lock_manager = None
    browser_manager = None
    
    try:
        # Валідація конфігурації середовища
        try:
            validate_environment()
        except ValueError as config_error:
            logger.error(f"❌ [Replenish] Помилка конфігурації: {config_error}")
            return create_response('error', f'Configuration error: {config_error}')
        
        # Валідація вхідних даних
        validate_event(event)
        
        # Перевірка спеціальних команд
        if event.get('command') == 'force_unlock':
            logger.warning("🔓 [Replenish] Отримано команду примусового розблокування")
            temp_lock_manager = LambdaLockManager()
            if temp_lock_manager.force_unlock():
                return create_response('success', 'Lock forcefully removed')
            else:
                return create_response('error', 'Failed to force unlock')
        
        if event.get('command') == 'lock_status':
            logger.info("� [Replenish] Перевірка статусу блокування")
            temp_lock_manager = LambdaLockManager()
            status = temp_lock_manager.get_lock_status()
            return create_response('success', 'Lock status retrieved', **status)
        
        # Ініціалізуємо менеджер блокування
        lock_manager = LambdaLockManager()
        
        # Спробуємо отримати блокування
        lock_acquired, lock_message = lock_manager.acquire_lock()
        
        if not lock_acquired:
            logger.warning(f"🚫 [Replenish] Не вдалося отримати блокування: {lock_message}")
            return create_response('locked', lock_message)
        
        logger.info("�🚀 [Replenish] функція запущена з блокуванням")
        
    except ValueError as e:
        logger.error(f"❌ [Replenish] Невалідні вхідні дані: {e}")
        return create_response('error', f'Invalid input: {e}')
    
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
        # ВАЖЛИВО: звільняємо блокування в будь-якому випадку
        if lock_manager:
            lock_manager.release_lock()
        
        if browser_manager:
            browser_manager.cleanup()
            logger.info("🚪 [Replenish] Браузер закрито.")