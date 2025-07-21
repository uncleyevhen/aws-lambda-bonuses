#!/usr/bin/env python3
"""
Швидкий тест API для get-promo-code лямбда функції.
Використовується для базової перевірки роботи API.
"""

import requests
import json
import sys

def test_api_quick():
    """Швидкий тест API"""
    
    # ЗАМІНІТЬ на ваш реальний API endpoint!
    api_endpoint = "https://YOUR_API_ID.execute-api.eu-north-1.amazonaws.com/get-code"
    
    if "YOUR_API_ID" in api_endpoint:
        print("❌ ПОМИЛКА: Потрібно замінити YOUR_API_ID на реальний API Gateway endpoint!")
        print("🔧 Отримайте endpoint командою:")
        print("aws apigatewayv2 get-apis --query 'Items[?Name==`promo-code-api`].ApiEndpoint' --output text")
        return False
    
    # Тестові дані
    test_cases = [
        {"amount": 50, "description": "Тест з сумою 50 грн"},
        {"amount": 100, "description": "Тест з сумою 100 грн"},
        {"amount": 150, "description": "Тест з сумою 150 грн"},
    ]
    
    print("🧪 ШВИДКИЙ ТЕСТ API GET-PROMO-CODE")
    print("=" * 40)
    
    success_count = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📞 Тест {i}: {test_case['description']}")
        
        try:
            response = requests.post(
                api_endpoint,
                json={"amount": test_case["amount"]},
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            print(f"  📊 Статус код: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                promo_code = data.get('promo_code')
                if promo_code:
                    print(f"  ✅ Успішно! Промокод: {promo_code}")
                    success_count += 1
                else:
                    print(f"  ❌ Відповідь без промокоду: {data}")
            else:
                print(f"  ❌ Помилка: {response.text}")
                
        except Exception as e:
            print(f"  ❌ Виняток: {e}")
    
    print(f"\n📊 Результат: {success_count}/{len(test_cases)} тестів успішно")
    return success_count == len(test_cases)

if __name__ == "__main__":
    success = test_api_quick()
    sys.exit(0 if success else 1)
