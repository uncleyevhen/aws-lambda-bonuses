import os
import logging
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger(__name__)

class PromoService:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.getenv('SESSION_S3_BUCKET', 'lambda-promo-sessions')
        self.promo_codes_key = 'promo-codes/available_codes.json'
        self.used_codes_key = 'promo-codes/used_codes_count.json'
        self.batch_threshold = int(os.getenv('BATCH_THRESHOLD', '20'))  # Поріг для запуску поповнення
        self.min_codes_threshold = int(os.getenv('MIN_CODES_THRESHOLD', '3'))  # Мінімум кодів для запуску поповнення
        
    def add_used_code_count(self, amount: str):
        """
        Додає інформацію про використання промокоду для певної суми.
        Повертає True, якщо потрібно запустити поповнення.
        """
        try:
            # Спробуємо завантажити існуючі дані
            try:
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.used_codes_key)
                used_data = json.loads(response['Body'].read().decode('utf-8'))
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    # Файл не існує, створюємо новий
                    used_data = {}
                else:
                    raise e
            
            # Додаємо лічильник для цієї суми
            if amount not in used_data:
                used_data[amount] = 0
            
            used_data[amount] += 1
            
            # Зберігаємо оновлені дані
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.used_codes_key,
                Body=json.dumps(used_data, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"📝 Додано використання промокоду для суми {amount}. Всього використано: {used_data[amount]}")
            
            # Перевіряємо, чи потрібно запускати поповнення
            total_used = sum(used_data.values())
            should_trigger = total_used >= self.batch_threshold
            
            if should_trigger:
                logger.info(f"🚀 Поріг досягнуто ({total_used} >= {self.batch_threshold}). Потрібно запустити поповнення.")
            else:
                logger.info(f"⏳ Використано {total_used} промокодів з {self.batch_threshold}. Очікуємо...")
            
            return should_trigger
            
        except Exception as e:
            logger.error(f"❌ Помилка при записі використання промокоду: {e}")
            return False
    
    def get_and_clear_used_codes_count(self):
        """
        Отримує та очищає лічильник використаних промокодів. 
        Використовується функцією поповнення.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.used_codes_key)
            used_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Зберігаємо поточні дані
            current_used = used_data.copy()
            
            # Очищаємо лічильники
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.used_codes_key,
                Body=json.dumps({}, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"🗑️ Лічильники очищено. Оброблено: {current_used}")
            return current_used
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info("ℹ️ Файл з лічильниками не існує.")
                return {}
            else:
                logger.error(f"❌ Помилка при очищенні лічильників: {e}")
                return {}
        except Exception as e:
            logger.error(f"❌ Неочікувана помилка при роботі з лічильниками: {e}")
            return None

    def get_and_remove_code_from_s3(self, amount_key: str):
        """
        Швидко завантажує коди з S3, вибирає один, видаляє його зі списку 
        і відразу зберігає оновлений список.
        Повертає tuple: (promo_code, should_trigger_replenish)
        """
        try:
            logger.info(f"☁️ [Fast] Завантаження списку промокодів з s3://{self.s3_bucket}/{self.promo_codes_key}")
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.promo_codes_key)
            all_codes = json.loads(response['Body'].read().decode('utf-8'))
            
            available_for_amount = all_codes.get(amount_key, [])
            if not available_for_amount:
                logger.error(f"❌ Промокоди для суми '{amount_key}' закінчилися!")
                return None, False
            
            promo_code = available_for_amount.pop(0)
            remaining_count = len(available_for_amount)
            logger.info(f"✅ [Fast] Вибрано промокод: {promo_code}. Залишилось {remaining_count} кодів.")
            
            all_codes[amount_key] = available_for_amount
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.promo_codes_key,
                Body=json.dumps(all_codes, indent=2),
                ContentType='application/json'
            )
            logger.info("☁️ [Fast] Оновлений (зменшений) список промокодів збережено в S3.")
            
            # Перевіряємо, чи потрібно запускати поповнення через малу кількість кодів
            should_trigger_low_count = remaining_count <= self.min_codes_threshold
            if should_trigger_low_count:
                logger.info(f"⚠️ [Fast] Залишилось мало промокодів ({remaining_count} <= {self.min_codes_threshold}). Потрібно поповнення.")
            
            return promo_code, should_trigger_low_count

        except ClientError as e:
            if hasattr(e, 'response') and 'Error' in e.response and 'Code' in e.response['Error'] and e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"❌ Файл з промокодами не знайдено в S3: {self.promo_codes_key}")
            else:
                logger.error(f"❌ Помилка S3 при роботі з промокодами: {e}")
            return None, False
        except Exception as e:
            logger.error(f"❌ Неочікувана помилка при роботі з промокодами: {e}")
            return None, False
