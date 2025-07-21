#!/usr/bin/env python3
"""
Скрипт для додавання тестової суми 1002 в used_codes_count.json в S3
і запуску тесту швидкості створення 10 промокодів
"""
import json
import boto3
import os
import sys
from botocore.exceptions import ClientError

def add_test_amount_to_s3():
    """Додає суму 1002 з лічільником 10 до S3 файлу used_codes_count.json"""
    
    # Налаштування S3
    bucket = os.getenv('SESSION_S3_BUCKET', 'lambda-promo-sessions')
    key = os.getenv('USED_CODES_S3_KEY', 'promo-codes/used_codes_count.json')
    
    try:
        s3_client = boto3.client('s3')
        
        # Спробуємо завантажити існуючий файл
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            used_data = json.loads(response['Body'].read().decode('utf-8'))
            print(f"📦 Завантажено існуючі дані: {used_data}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("📦 Файл не існує, створюємо новий")
                used_data = {}
            else:
                raise
        
        # Додаємо тестову суму
        used_data['1002'] = 10  # 10 використаних промокодів суми 1002
        
        # Зберігаємо назад в S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(used_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"✅ Успішно оновлено used_codes_count.json:")
        print(f"   📊 Дані: {used_data}")
        print(f"   🪣 Bucket: {bucket}")
        print(f"   🔑 Key: {key}")
        
        return True
        
    except Exception as e:
        print(f"❌ Помилка роботи з S3: {e}")
        return False

def test_lambda_locally():
    """Тестує lambda функцію локально"""
    try:
        # Додаємо шлях до модулів
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        
        from lambda_function import replenish_promo_code_handler
        
        # Створюємо тестовий event
        test_event = {
            'test': True,
            'source': 'speed_test'
        }
        
        # Створюємо mock context
        class MockContext:
            def __init__(self):
                self.function_name = 'test-promo-replenish'
                self.function_version = '1'
                self.memory_limit_in_mb = '512'
                self.remaining_time_in_millis = lambda: 900000  # 15 хвилин
        
        context = MockContext()
        
        print("🚀 Запускаємо Lambda функцію локально...")
        result = replenish_promo_code_handler(test_event, context)
        
        print("📊 Результат:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        return result
        
    except Exception as e:
        print(f"❌ Помилка тестування Lambda: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Основна функція"""
    print("🧪 ТЕСТ ШВИДКОСТІ СТВОРЕННЯ ПРОМОКОДІВ")
    print("=" * 50)
    
    # Крок 1: Додаємо тестові дані в S3
    print("\n📦 Крок 1: Додавання тестової суми 1002 в S3...")
    if not add_test_amount_to_s3():
        print("❌ Не вдалося оновити S3, перевірте AWS credentials")
        return
    
    # Крок 2: Запускаємо Lambda функцію
    print("\n🚀 Крок 2: Запуск Lambda функції...")
    result = test_lambda_locally()
    
    if result:
        print("\n✅ Тест завершено!")
        if result.get('status') == 'success':
            print(f"🎉 Успішно оброблено суми: {result.get('processed_amounts', [])}")
            print(f"⏱️ Час виконання: {result.get('execution_time', 'н/д')}с")
        else:
            print(f"❌ Помилка: {result.get('message', 'Невідома помилка')}")
    else:
        print("❌ Тест не вдався")

if __name__ == '__main__':
    main()
