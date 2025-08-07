#!/usr/bin/env python3
"""
Скрипт для перевірки стану промокодів в S3.
Показує поточну кількість доступних промокодів та лічильники використання.
"""

import boto3
import json
import sys
from datetime import datetime

class S3PromoChecker:
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name='eu-north-1')
        self.bucket = 'lambda-promo-sessions'
        self.promo_codes_key = 'promo-codes/available_codes.json'
        self.used_codes_key = 'promo-codes/used_codes_count.json'
    
    def get_promo_codes_state(self):
        """Отримує стан промокодів з S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.promo_codes_key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data
        except self.s3_client.exceptions.NoSuchKey:
            print("❌ Файл з промокодами не знайдено в S3")
            return {}
        except Exception as e:
            print(f"❌ Помилка читання промокодів: {e}")
            return {}
    
    def get_used_codes_state(self):
        """Отримує лічильники використання з S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.used_codes_key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data
        except self.s3_client.exceptions.NoSuchKey:
            return {}
        except Exception as e:
            print(f"❌ Помилка читання лічильників: {e}")
            return {}
    
    def show_status(self):
        """Показує детальний статус промокодів"""
        print("📊 СТАН ПРОМОКОДІВ В S3")
        print("=" * 40)
        print(f"🕐 Час перевірки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"☁️ S3 Bucket: {self.bucket}")
        print()
        
        # Отримуємо дані
        promo_codes = self.get_promo_codes_state()
        used_counts = self.get_used_codes_state()
        
        if not promo_codes and not used_counts:
            print("❌ Немає даних в S3")
            return False
        
        # Показуємо доступні промокоди
        if promo_codes:
            print("💰 ДОСТУПНІ ПРОМОКОДИ:")
            print("-" * 25)
            total_codes = 0
            
            # Сортуємо суми
            amounts = []
            for amount_str in promo_codes.keys():
                if amount_str != '_metadata' and amount_str.isdigit():
                    amounts.append(int(amount_str))
            
            amounts.sort()
            
            for amount in amounts:
                amount_str = str(amount)
                codes_list = promo_codes.get(amount_str, [])
                count = len(codes_list)
                total_codes += count
                
                # Кольорове кодування по кількості
                if count == 0:
                    status = "❌"
                elif count <= 3:
                    status = "⚠️"
                elif count <= 5:
                    status = "🔶"
                else:
                    status = "✅"
                
                print(f"  {status} {amount} грн: {count} промокодів")
                
                # Показуємо кілька прикладів кодів
                if count > 0 and len(sys.argv) > 1 and sys.argv[1] == '--verbose':
                    examples = codes_list[:3]
                    examples_str = ', '.join(examples)
                    if count > 3:
                        examples_str += f" ... (+{count-3})"
                    print(f"    📝 Приклади: {examples_str}")
            
            print(f"\n📈 Загалом доступно: {total_codes} промокодів")
            
            # Метадані
            if '_metadata' in promo_codes:
                metadata = promo_codes['_metadata']
                last_updated = metadata.get('last_updated', 'невідомо')
                print(f"🔄 Останнє оновлення: {last_updated}")
        else:
            print("❌ Немає доступних промокодів")
        
        print()
        
        # Показуємо лічильники використання
        if used_counts:
            print("📊 ЛІЧИЛЬНИКИ ВИКОРИСТАННЯ:")
            print("-" * 30)
            total_used = 0
            
            # Сортуємо суми
            used_amounts = [int(k) for k in used_counts.keys() if k.isdigit()]
            used_amounts.sort()
            
            for amount in used_amounts:
                amount_str = str(amount)
                used_count = used_counts.get(amount_str, 0)
                total_used += used_count
                print(f"  📞 {amount} грн: {used_count} використано")
            
            print(f"\n📈 Загалом використано: {total_used} промокодів")
            
            # Попередження про пороги
            batch_threshold = 20  # Можна винести в конфігурацію
            if total_used >= batch_threshold:
                print(f"🚨 УВАГА: Досягнуто поріг батчу ({total_used} >= {batch_threshold})")
                print("   Має запуститися поповнення промокодів!")
        else:
            print("ℹ️ Лічильники використання порожні")
        
        return True
    
    def reset_used_counters(self):
        """Скидає лічильники використання (для тестування)"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.used_codes_key,
                Body=json.dumps({}, indent=2),
                ContentType='application/json'
            )
            print("✅ Лічильники використання скинуто")
            return True
        except Exception as e:
            print(f"❌ Помилка скидання лічильників: {e}")
            return False
    
    def add_test_codes(self, amount, count=5):
        """Додає тестові промокоди для вказаної суми"""
        try:
            promo_codes = self.get_promo_codes_state()
            
            amount_str = str(amount)
            if amount_str not in promo_codes:
                promo_codes[amount_str] = []
            
            # Генеруємо тестові коди
            existing_codes = set(promo_codes[amount_str])
            new_codes = []
            
            i = 1
            while len(new_codes) < count:
                test_code = f"BON{amount}{i:03d}TEST"
                if test_code not in existing_codes:
                    new_codes.append(test_code)
                    existing_codes.add(test_code)
                i += 1
            
            promo_codes[amount_str].extend(new_codes)
            
            # Додаємо метадані
            promo_codes['_metadata'] = {
                'last_updated': datetime.now().isoformat(),
                'test_mode': True
            }
            
            # Зберігаємо
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.promo_codes_key,
                Body=json.dumps(promo_codes, indent=2),
                ContentType='application/json'
            )
            
            print(f"✅ Додано {count} тестових промокодів для суми {amount} грн")
            print(f"📝 Нові коди: {', '.join(new_codes)}")
            return True
            
        except Exception as e:
            print(f"❌ Помилка додавання тестових кодів: {e}")
            return False

def main():
    """Головна функція"""
    checker = S3PromoChecker()
    
    # Парсимо аргументи командного рядка
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == '--help' or command == '-h':
            print("🔧 ВИКОРИСТАННЯ:")
            print("  python3 check_s3_state.py                 # Показати стан")
            print("  python3 check_s3_state.py --verbose       # Детальний стан")
            print("  python3 check_s3_state.py --reset         # Скинути лічильники")
            print("  python3 check_s3_state.py --add-test 50 3 # Додати 3 тестових коди для 50 грн")
            return
        
        elif command == '--reset':
            print("🔄 Скидання лічильників використання...")
            checker.reset_used_counters()
            return
        
        elif command == '--add-test':
            if len(sys.argv) >= 4:
                try:
                    amount = int(sys.argv[2])
                    count = int(sys.argv[3])
                    print(f"➕ Додавання {count} тестових кодів для суми {amount} грн...")
                    checker.add_test_codes(amount, count)
                except ValueError:
                    print("❌ Невірні аргументи. Використовуйте: --add-test <сума> <кількість>")
            else:
                print("❌ Недостатньо аргументів. Використовуйте: --add-test <сума> <кількість>")
            return
    
    # За замовчуванням показуємо стан
    success = checker.show_status()
    
    if not success:
        print("\n💡 ПОРАДИ:")
        print("  1. Перевірте, чи розгорнута replenish-promo-code функція")
        print("  2. Запустіть поповнення промокодів командою:")
        print("     aws lambda invoke --function-name replenish-promo-code /tmp/response.json")
        print("  3. Або додайте тестові коди:")
        print("     python3 check_s3_state.py --add-test 50 5")

if __name__ == "__main__":
    main()
