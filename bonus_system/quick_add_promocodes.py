#!/usr/bin/env python3
import json
import boto3
import random
import string

def generate_promo_code(amount):
    """Генерує промокод для заданої суми"""
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"BON{amount}{suffix}"

def add_promo_codes():
    """Додає промокоди для суми 200 грн"""
    s3 = boto3.client('s3', region_name='eu-north-1')
    bucket = 'lambda-promo-sessions'
    key = 'promo-codes/available_codes.json'
    
    # Скачуємо поточний файл
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read())
        print(f"Поточних промокодів для суми 200: {len(data.get('200', []))}")
    except Exception as e:
        print(f"Помилка при скачуванні файлу: {e}")
        return
    
    # Додаємо 10 нових промокодів для суми 200
    amount = "200"
    if amount not in data:
        data[amount] = []
    
    new_codes = []
    for i in range(10):
        code = generate_promo_code(amount)
        new_codes.append(code)
        data[amount].append(code)
    
    print(f"Додано {len(new_codes)} нових промокодів для суми 200:")
    for code in new_codes:
        print(f"  - {code}")
    
    # Завантажуємо оновлений файл
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Файл успішно оновлено. Тепер для суми 200: {len(data[amount])} промокодів")
    except Exception as e:
        print(f"❌ Помилка при завантаженні файлу: {e}")

if __name__ == "__main__":
    add_promo_codes()
