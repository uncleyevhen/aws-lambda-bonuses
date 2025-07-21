#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для пошуку клієнта за телефоном та перегляду його історії бонусів
"""

import requests
import json
import urllib.parse

# Конфігурація KeyCRM
KEYCRM_BASE_URL = "https://openapi.keycrm.app/v1"
KEYCRM_API_TOKEN = "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ"
HISTORY_FIELD_UUID = "CT_1033"  # UUID поля "Історія бонусів"
BONUS_FIELD_UUID = "CT_1023"    # UUID поля "Бонусні бали"

def find_buyer_by_phone(phone):
    """Знаходимо клієнта за номером телефону"""
    
    headers = {
        'Authorization': f'Bearer {KEYCRM_API_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    try:
        # Нормалізуємо номер телефону
        clean_phone = phone.replace('+', '').replace(' ', '').replace('-', '')
        if clean_phone.startswith('380') and len(clean_phone) == 12:
            search_phone = clean_phone
        else:
            search_phone = '380501234567'  # Фіксований номер для тесту
            
        filter_param = f"filter[buyer_phone]={urllib.parse.quote(search_phone)}"
        url = f"{KEYCRM_BASE_URL}/buyer?{filter_param}&include=custom_fields"
        
        print(f"🔍 Пошук клієнта за телефоном: {search_phone}")
        print(f"📡 URL: {url}")
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('data') and len(data['data']) > 0:
                buyer = data['data'][0]
                buyer_id = buyer['id']
                
                print(f"✅ Клієнт знайдений:")
                print(f"   ID: {buyer_id}")
                print(f"   Ім'я: {buyer.get('full_name', 'N/A')}")
                print(f"   Телефон: {buyer.get('phone', 'N/A')}")
                print(f"   Email: {buyer.get('email', 'N/A')}")
                
                # Аналізуємо кастомні поля
                bonus_balance = None
                bonus_history = None
                
                custom_fields = buyer.get('custom_fields', [])
                print(f"\n📋 Кастомні поля ({len(custom_fields)}):")
                
                for field in custom_fields:
                    field_uuid = field.get('uuid', 'N/A')
                    field_value = field.get('value', 'N/A')
                    
                    if field_uuid == BONUS_FIELD_UUID:
                        bonus_balance = field_value
                        print(f"   💰 Бонусні бали (CT_1023): {field_value}")
                    elif field_uuid == HISTORY_FIELD_UUID:
                        bonus_history = field_value
                        print(f"   📜 Історія бонусів (CT_1033): {'Є дані' if field_value else 'Порожньо'}")
                    else:
                        print(f"   🔸 {field_uuid}: {str(field_value)[:50]}...")
                
                # Детальний аналіз історії
                if bonus_history:
                    print(f"\n📜 ІСТОРІЯ БОНУСІВ:")
                    print("=" * 100)
                    
                    history_lines = [line.strip() for line in bonus_history.split('\n') if line.strip()]
                    
                    for i, line in enumerate(history_lines, 1):
                        print(f"{i:2d}. {line}")
                        
                        # Аналізуємо формат рядка
                        parts = line.split(' | ')
                        print(f"    Частин: {len(parts)}")
                        
                        if len(parts) >= 6:
                            print(f"    ✅ Новий формат:")
                            print(f"       📅 Дата: {parts[0]}")
                            print(f"       🆔 Замовлення: {parts[1]}")
                            print(f"       💰 Сума: {parts[2]}")
                            print(f"       ➕ Нараховано: {parts[3]}")
                            print(f"       ➖ Списано: {parts[4]}")
                            print(f"       🔄 Баланс: {parts[5]}")
                        elif len(parts) == 5:
                            print(f"    ⚠️ Старий формат:")
                            print(f"       📅 Дата: {parts[0]}")
                            print(f"       🆔 Замовлення: {parts[1]}")
                            print(f"       💰 Сума: {parts[2]}")
                            print(f"       🎁 Бонуси: {parts[3]}")
                            print(f"       🔄 Баланс: {parts[4]}")
                        else:
                            print(f"    ❓ Невідомий формат")
                        
                        print()
                    
                    print("=" * 100)
                else:
                    print(f"\n❌ Історія бонусів порожня або відсутня")
                
                return buyer_id
                
            else:
                print(f"❌ Клієнт з телефоном {search_phone} не знайдений")
                return None
                
        else:
            print(f"❌ Помилка пошуку: {response.status_code}")
            print(f"   Відповідь: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Помилка: {str(e)}")
        return None

if __name__ == "__main__":
    print("🔍 ПОШУК КЛІЄНТА ТА ПЕРЕВІРКА ІСТОРІЇ БОНУСІВ")
    print("=" * 80)
    
    # Шукаємо клієнта за тестовим номером
    buyer_id = find_buyer_by_phone("+380501234567")
    
    print("=" * 80)
