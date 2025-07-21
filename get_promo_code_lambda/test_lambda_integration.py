#!/usr/bin/env python3
"""
Скрипт для тестування інтеграції get-promo-code лямбда функції з репленіш системою.

Цей скрипт тестує:
1. Отримання промокодів через API Gateway
2. Автоматичний запуск поповнення при досягненні порогів
3. Взаємодію між get-promo-code та replenish-promo-code функціями
4. Відновлення промокодів після виклику репленіш

ВИКОРИСТАННЯ:
python3 test_lambda_integration.py
"""

import json
import time
import requests
import boto3
import logging
from datetime import datetime

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LambdaIntegrationTester:
    def __init__(self):
        # AWS клієнти
        self.lambda_client = boto3.client('lambda', region_name='eu-north-1')
        self.s3_client = boto3.client('s3', region_name='eu-north-1')
        
        # Конфігурація (замініть на ваші реальні значення)
        self.api_endpoint = "https://k3pok2o5t1.execute-api.eu-north-1.amazonaws.com/get-code"
        self.s3_bucket = "lambda-promo-sessions"
        self.promo_codes_key = "promo-codes/available_codes.json"
        self.used_codes_key = "promo-codes/used_codes_count.json"
        self.get_function_name = "get-promo-code"
        self.replenish_function_name = "replenish-promo-code"
        
        # Тестові параметри
        self.test_amount = 50  # Тестова сума для промокодів
        self.batch_threshold = 3  # Знижений поріг для швидкого тестування
        self.min_codes_threshold = 2  # Мінімум кодів для запуску поповнення
        
    def check_api_endpoint(self):
        """Перевіряє, чи правильно налаштований API endpoint"""
        if "YOUR_API_ID" in self.api_endpoint:
            logger.error("❌ ПОМИЛКА: Потрібно замінити YOUR_API_ID на реальний API Gateway endpoint!")
            logger.info("🔧 Отримайте endpoint командою:")
            logger.info("aws apigatewayv2 get-apis --query 'Items[?Name==`promo-code-api`].ApiEndpoint' --output text")
            return False
        return True
    
    def setup_test_environment(self):
        """Підготовка тестового середовища"""
        logger.info("🔧 Підготовка тестового середовища...")
        
        # Створюємо початковий стан з мінімальною кількістю промокодів
        initial_codes = {
            str(self.test_amount): [
                f"BON{i:06d}" for i in range(1, 4)  # Тільки 3 коди для швидкого тестування
            ]
        }
        
        try:
            # Зберігаємо початкові промокоди
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.promo_codes_key,
                Body=json.dumps(initial_codes, indent=2),
                ContentType='application/json'
            )
            
            # Очищаємо лічильники використання
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.used_codes_key,
                Body=json.dumps({}, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Створено {len(initial_codes[str(self.test_amount)])} тестових промокодів для суми {self.test_amount} грн")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка підготовки тестового середовища: {e}")
            return False
    
    def get_s3_state(self):
        """Отримує поточний стан промокодів та лічильників з S3"""
        try:
            # Промокоди
            try:
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.promo_codes_key)
                promo_codes = json.loads(response['Body'].read().decode('utf-8'))
            except self.s3_client.exceptions.NoSuchKey:
                promo_codes = {}
            
            # Лічильники
            try:
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.used_codes_key)
                used_counts = json.loads(response['Body'].read().decode('utf-8'))
            except self.s3_client.exceptions.NoSuchKey:
                used_counts = {}
            
            return promo_codes, used_counts
            
        except Exception as e:
            logger.error(f"❌ Помилка отримання стану S3: {e}")
            return {}, {}
    
    def call_get_promo_api(self, amount):
        """Викликає API для отримання промокоду"""
        try:
            payload = {"amount": amount}
            response = requests.post(
                self.api_endpoint,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('promo_code'), True
            else:
                logger.error(f"❌ API помилка {response.status_code}: {response.text}")
                return None, False
                
        except Exception as e:
            logger.error(f"❌ Помилка виклику API: {e}")
            return None, False
    
    def check_lambda_logs(self, function_name, start_time):
        """Перевіряє логи лямбда функції"""
        try:
            # Використовуємо CloudWatch Logs (потрібні додаткові права)
            logs_client = boto3.client('logs', region_name='eu-north-1')
            log_group = f'/aws/lambda/{function_name}'
            
            response = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=int(start_time.timestamp() * 1000),
                filterPattern='[INFO] [Get]'  # Фільтруємо лише наші логи
            )
            
            events = response.get('events', [])
            if events:
                logger.info(f"📋 Знайдено {len(events)} логів для {function_name}")
                for event in events[-3:]:  # Показуємо останні 3
                    logger.info(f"  🔍 {event['message'].strip()}")
            
            return events
            
        except Exception as e:
            logger.warning(f"⚠️ Не вдалося отримати логи для {function_name}: {e}")
            return []
    
    def test_promo_code_retrieval(self):
        """Тестує отримання промокодів через API"""
        logger.info(f"🧪 ТЕСТ 1: Отримання промокодів для суми {self.test_amount} грн")
        
        promo_codes_before, used_counts_before = self.get_s3_state()
        codes_before = len(promo_codes_before.get(str(self.test_amount), []))
        
        logger.info(f"📊 Стан ПЕРЕД запитом: {codes_before} промокодів, використано: {used_counts_before}")
        
        # Викликаємо API
        start_time = datetime.now()
        promo_code, success = self.call_get_promo_api(self.test_amount)
        
        if success and promo_code:
            logger.info(f"✅ Отримано промокод: {promo_code}")
            
            # Перевіряємо стан після запиту
            time.sleep(2)  # Даємо час на обробку
            promo_codes_after, used_counts_after = self.get_s3_state()
            codes_after = len(promo_codes_after.get(str(self.test_amount), []))
            
            logger.info(f"📊 Стан ПІСЛЯ запиту: {codes_after} промокодів, використано: {used_counts_after}")
            
            # Перевіряємо логи
            self.check_lambda_logs(self.get_function_name, start_time)
            
            return promo_code, codes_before - codes_after == 1
        else:
            logger.error("❌ Не вдалося отримати промокод")
            return None, False
    
    def test_batch_trigger(self):
        """Тестує запуск поповнення при досягненні батч-порогу"""
        logger.info(f"🧪 ТЕСТ 2: Тестування батч-порогу ({self.batch_threshold} запитів)")
        
        successful_requests = 0
        promo_codes = []
        
        # Робимо кілька запитів для досягнення порогу
        for i in range(self.batch_threshold + 1):
            logger.info(f"📞 Запит {i+1}/{self.batch_threshold + 1}")
            
            start_time = datetime.now()
            promo_code, success = self.call_get_promo_api(self.test_amount)
            
            if success and promo_code:
                promo_codes.append(promo_code)
                successful_requests += 1
                logger.info(f"  ✅ Успішно: {promo_code}")
                
                # Перевіряємо, чи запустилося поповнення після досягнення порогу
                if successful_requests == self.batch_threshold:
                    logger.info("🚀 Досягнуто поріг батчу! Очікуємо запуск поповнення...")
                    time.sleep(5)  # Даємо час на запуск репленіш
                    
                    # Перевіряємо логи репленіш функції
                    replenish_logs = self.check_lambda_logs(self.replenish_function_name, start_time)
                    if replenish_logs:
                        logger.info("✅ Репленіш функція була викликана!")
                    else:
                        logger.warning("⚠️ Логи репленіш функції не знайдено (може знадобитися час)")
            else:
                logger.error(f"  ❌ Запит {i+1} не вдався")
            
            time.sleep(1)  # Пауза між запитами
        
        logger.info(f"📊 Результат батч-тесту: {successful_requests} успішних запитів з {self.batch_threshold + 1}")
        return successful_requests >= self.batch_threshold
    
    def test_low_codes_trigger(self):
        """Тестує запуск поповнення при малій кількості кодів"""
        logger.info(f"🧪 ТЕСТ 3: Тестування порогу малої кількості кодів ({self.min_codes_threshold})")
        
        # Спочатку зменшуємо кількість кодів до мінімуму
        promo_codes, _ = self.get_s3_state()
        current_codes = promo_codes.get(str(self.test_amount), [])
        
        if len(current_codes) > self.min_codes_threshold:
            # Залишаємо тільки min_codes_threshold + 1 кодів
            promo_codes[str(self.test_amount)] = current_codes[:self.min_codes_threshold + 1]
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.promo_codes_key,
                Body=json.dumps(promo_codes, indent=2),
                ContentType='application/json'
            )
            logger.info(f"🔧 Залишено {len(promo_codes[str(self.test_amount)])} кодів для тестування")
        
        # Робимо запит, який має спустити кількість кодів нижче порогу
        start_time = datetime.now()
        promo_code, success = self.call_get_promo_api(self.test_amount)
        
        if success:
            logger.info(f"✅ Отримано промокод: {promo_code}")
            logger.info("🚀 Має запуститися поповнення через малу кількість кодів...")
            
            time.sleep(5)  # Даємо час на запуск
            
            # Перевіряємо логи
            replenish_logs = self.check_lambda_logs(self.replenish_function_name, start_time)
            if replenish_logs:
                logger.info("✅ Репленіш функція була викликана через малу кількість кодів!")
                return True
            else:
                logger.warning("⚠️ Логи репленіш функції не знайдено")
                return False
        else:
            logger.error("❌ Не вдалося отримати промокод для тестування")
            return False
    
    def test_replenish_effectiveness(self):
        """Тестує ефективність поповнення промокодів"""
        logger.info("🧪 ТЕСТ 4: Перевірка ефективності поповнення")
        
        # Стан перед поповненням
        promo_codes_before, _ = self.get_s3_state()
        codes_before = len(promo_codes_before.get(str(self.test_amount), []))
        
        logger.info(f"📊 Кодів ПЕРЕД поповненням: {codes_before}")
        
        # Викликаємо репленіш функцію напряму
        try:
            logger.info("🚀 Запускаємо репленіш функцію напряму...")
            
            payload = {
                'trigger_source': 'manual_test',
                'trigger_reasons': ['testing_replenish_effectiveness']
            }
            
            response = self.lambda_client.invoke(
                FunctionName=self.replenish_function_name,
                InvocationType='RequestResponse',  # Синхронний виклик для тестування
                Payload=json.dumps(payload)
            )
            
            response_payload = json.loads(response['Payload'].read().decode('utf-8'))
            logger.info(f"📋 Відповідь репленіш функції: {response_payload}")
            
            # Чекаємо завершення поповнення
            logger.info("⏳ Очікуємо завершення поповнення (30 секунд)...")
            time.sleep(30)
            
            # Стан після поповнення
            promo_codes_after, _ = self.get_s3_state()
            codes_after = len(promo_codes_after.get(str(self.test_amount), []))
            
            logger.info(f"📊 Кодів ПІСЛЯ поповнення: {codes_after}")
            
            if codes_after > codes_before:
                logger.info(f"✅ Поповнення успішне! Додано {codes_after - codes_before} кодів")
                return True
            else:
                logger.warning("⚠️ Кількість кодів не збільшилася")
                return False
                
        except Exception as e:
            logger.error(f"❌ Помилка тестування репленіш: {e}")
            return False
    
    def run_full_test_suite(self):
        """Запускає повний набір тестів"""
        logger.info("🚀 ПОЧАТОК ПОВНОГО ТЕСТУВАННЯ ІНТЕГРАЦІЇ")
        logger.info("=" * 60)
        
        # Перевірка API endpoint
        if not self.check_api_endpoint():
            return False
        
        # Підготовка
        if not self.setup_test_environment():
            logger.error("❌ Не вдалося підготувати тестове середовище")
            return False
        
        results = {
            'basic_retrieval': False,
            'batch_trigger': False,
            'low_codes_trigger': False,
            'replenish_effectiveness': False
        }
        
        try:
            # Тест 1: Базове отримання промокодів
            _, results['basic_retrieval'] = self.test_promo_code_retrieval()
            
            # Тест 2: Батч-тригер
            results['batch_trigger'] = self.test_batch_trigger()
            
            # Тест 3: Тригер малої кількості кодів
            results['low_codes_trigger'] = self.test_low_codes_trigger()
            
            # Тест 4: Ефективність поповнення
            results['replenish_effectiveness'] = self.test_replenish_effectiveness()
            
        except Exception as e:
            logger.error(f"❌ Помилка під час тестування: {e}")
        
        # Підсумок
        logger.info("\n📊 ПІДСУМОК ТЕСТУВАННЯ:")
        logger.info("=" * 40)
        
        total_tests = len(results)
        passed_tests = sum(results.values())
        
        for test_name, passed in results.items():
            status = "✅ ПРОЙДЕНО" if passed else "❌ НЕ ПРОЙДЕНО"
            logger.info(f"  {test_name}: {status}")
        
        logger.info(f"\n🎯 Результат: {passed_tests}/{total_tests} тестів пройдено")
        
        if passed_tests == total_tests:
            logger.info("🎉 ВСІ ТЕСТИ ПРОЙДЕНО! Інтеграція працює коректно.")
        else:
            logger.warning("⚠️ Деякі тести не пройдено. Перевірте налаштування.")
        
        return passed_tests == total_tests

def main():
    """Головна функція для запуску тестів"""
    print("🧪 ТЕСТУВАЛЬНИК ІНТЕГРАЦІЇ GET-PROMO-CODE ↔ REPLENISH")
    print("=" * 60)
    
    tester = LambdaIntegrationTester()
    
    # Запускаємо повний набір тестів
    success = tester.run_full_test_suite()
    
    if success:
        print("\n🎉 Всі тести пройдено успішно!")
        exit(0)
    else:
        print("\n⚠️ Деякі тести не пройдено.")
        exit(1)

if __name__ == "__main__":
    main()
