#!/usr/bin/env python3
"""
Скрипт для підрахунку всіх клієнтів в KeyCRM
"""

import requests
import json
import time

def count_all_clients():
    api_key = "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Проксі - ЗАМІНІТЬ НА ВАШІ НОВІ ДАНІ
    proxy_url = "http://YOUR_NEW_USERNAME:YOUR_NEW_PASSWORD@p.webshare.io:80"
    proxies = {
        'http': proxy_url,
        'https': proxy_url
    }
    
    page = 1
    total_buyers = 0
    buyers_with_orders = 0
    
    print("🔍 Підрахунок всіх клієнтів KeyCRM...")
    print("=" * 50)
    
    while True:
        try:
            response = requests.get(
                f"https://openapi.keycrm.app/v1/buyer?page={page}&limit=50&include=custom_fields",
                headers=headers,
                proxies=proxies,
                timeout=30
            )
            
            data = response.json()
            buyers = data.get('data', [])
            
            if not buyers:
                print(f"❌ Сторінка {page} порожня, завершуємо підрахунок")
                break
            
            # Підраховуємо клієнтів з замовленнями
            buyers_with_orders_on_page = sum(
                1 for buyer in buyers 
                if buyer.get('orders_sum') and float(buyer.get('orders_sum', 0)) > 0
            )
            
            total_buyers += len(buyers)
            buyers_with_orders += buyers_with_orders_on_page
            
            print(f"Сторінка {page}: {len(buyers)} клієнтів, {buyers_with_orders_on_page} з замовленнями")
            
            # Якщо отримали менше 50 записів, це остання сторінка
            if len(buyers) < 50:
                print(f"✅ Остання сторінка (отримано {len(buyers)} записів)")
                break
            
            page += 1
            
            # Безпечна межа
            if page > 200:  # Максимум 200 сторінок = 10000 клієнтів
                print(f"⚠️ Досягнуто межу в {page-1} сторінок")
                break
                
            time.sleep(0.3)  # Пауза між запитами
            
        except Exception as e:
            print(f"❌ Помилка на сторінці {page}: {e}")
            break
    
    print("=" * 50)
    print(f"📊 ПІДСУМКИ:")
    print(f"Всього клієнтів: {total_buyers}")
    print(f"Клієнтів з замовленнями: {buyers_with_orders}")
    print(f"Клієнтів без замовлень: {total_buyers - buyers_with_orders}")
    print(f"Відсоток з замовленнями: {buyers_with_orders/total_buyers*100:.1f}%")
    print(f"Сторінок оброблено: {page-1}")

if __name__ == "__main__":
    count_all_clients()
