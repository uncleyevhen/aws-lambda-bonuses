#!/usr/bin/env python3
"""
Скрипт для додавання тестових використаних промокодів у S3
Це дозволить функції поповнення працювати довше для тестування блокування
"""

import boto3
import json
import sys
import time
from botocore.exceptions import ClientError

class TestUsedCodesManager:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket = 'lambda-promo-sessions'
        self.used_codes_key = 'promo-codes/used_codes_count.json'  # Правильний шлях як у Lambda функції
    
    def add_test_used_codes(self, test_amounts_with_counts):
        """
        Додає тестові лічільники використаних промокодів
        
        Args:
            test_amounts_with_counts: dict - суми і кількість використаних кодів
                Наприклад: {100: 15, 200: 20, 500: 25, 1000: 30}
        """
        try:
            # Спробуємо завантажити існуючі дані
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=self.used_codes_key)
                used_data = json.loads(response['Body'].read().decode('utf-8'))
                print(f"📄 Завантажено існуючі лічільники: {used_data}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    used_data = {}
                    print("📝 Створюємо новий файл лічільників")
                else:
                    raise e
            
            # Додаємо тестові дані
            total_added = 0
            for amount, count in test_amounts_with_counts.items():
                amount_str = str(amount)
                old_count = used_data.get(amount_str, 0)
                used_data[amount_str] = old_count + count
                total_added += count
                print(f"➕ Сума {amount} грн: {old_count} + {count} = {used_data[amount_str]} використаних кодів")
            
            # Додаємо метадані
            used_data['_test_metadata'] = {
                'test_created': time.time(),
                'test_total_added': total_added,
                'test_amounts': list(test_amounts_with_counts.keys())
            }
            
            # Зберігаємо оновлені дані
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.used_codes_key,
                Body=json.dumps(used_data, indent=2),
                ContentType='application/json'
            )
            
            print(f"\n✅ Тестові лічільники додано успішно!")
            print(f"📊 Загалом додано: {total_added} використаних промокодів")
            print(f"📈 Сум для поповнення: {len(test_amounts_with_counts)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Помилка додавання тестових лічільників: {e}")
            return False
    
    def get_current_state(self):
        """Показує поточний стан лічільників"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.used_codes_key)
            used_data = json.loads(response['Body'].read().decode('utf-8'))
            
            print("📊 Поточний стан лічільників використання:")
            print("=" * 50)
            
            total_used = 0
            amounts = []
            
            for key, value in used_data.items():
                if not key.startswith('_'):  # Пропускаємо метадані
                    amounts.append((int(key), value))
                    total_used += value
            
            amounts.sort()
            
            for amount, count in amounts:
                print(f"  💰 {amount} грн: {count} використано")
            
            print(f"\n📈 Загалом: {total_used} використаних промокодів")
            
            if '_test_metadata' in used_data:
                meta = used_data['_test_metadata']
                test_time = time.strftime('%Y-%m-%d %H:%M:%S', 
                                        time.localtime(meta.get('test_created', 0)))
                print(f"🧪 Тестові дані додано: {test_time}")
                print(f"🧪 Тестових кодів додано: {meta.get('test_total_added', 0)}")
            
            return used_data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("ℹ️ Файл лічільників не існує")
                return {}
            else:
                print(f"❌ Помилка отримання стану: {e}")
                return None
        except Exception as e:
            print(f"❌ Неочікувана помилка: {e}")
            return None
    
    def clear_all_counters(self):
        """Очищає всі лічільники"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.used_codes_key,
                Body=json.dumps({}, indent=2),
                ContentType='application/json'
            )
            print("✅ Всі лічільники очищено")
            return True
        except Exception as e:
            print(f"❌ Помилка очищення лічільників: {e}")
            return False

def main():
    """Головна функція"""
    manager = TestUsedCodesManager()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == '--status':
            manager.get_current_state()
            return
        
        elif command == '--clear':
            print("🗑️ Очищення всіх лічільників...")
            if manager.clear_all_counters():
                print("✅ Лічільники очищено успішно")
            return
        
        elif command == '--add-light':
            # Легкий тест - швидко виконається
            test_data = {
                150: 8,   # 8 використаних кодів для 150 грн
                350: 12   # 12 використаних кодів для 350 грн  
            }
            print("🧪 Додавання легких тестових лічільників...")
            manager.add_test_used_codes(test_data)
            return
            
        elif command == '--add-heavy':
            # Важкий тест - функція працюватиме довше
            test_data = {
                100: 25,   # 25 використаних кодів для 100 грн
                250: 30,   # 30 використаних кодів для 250 грн
                500: 35,   # 35 використаних кодів для 500 грн
                1000: 40   # 40 використаних кодів для 1000 грн
            }
            print("🧪 Додавання важких тестових лічільників (функція працюватиме довше)...")
            manager.add_test_used_codes(test_data)
            return
        
        elif command == '--help':
            print("Використання:")
            print("  python3 test_used_codes.py --status      # Показати поточний стан")
            print("  python3 test_used_codes.py --clear       # Очистити всі лічільники")
            print("  python3 test_used_codes.py --add-light   # Додати легкі тестові дані")
            print("  python3 test_used_codes.py --add-heavy   # Додати важкі тестові дані")
            print("  python3 test_used_codes.py --help        # Показати цю довідку")
            return
    
    # За замовчуванням показуємо статус
    print("📊 Поточний стан лічільників:")
    manager.get_current_state()
    print("\nДля допомоги: python3 test_used_codes.py --help")

if __name__ == "__main__":
    main()
