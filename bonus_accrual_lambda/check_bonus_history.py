#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для перевірки історії бонусів клієнта в KeyCRM
"""

import requests
import json

# Конфігурація KeyCRM
KEYCRM_BASE_URL = "https://openapi.keycrm.app/v1"
KEYCRM_API_TOKEN = "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ"
BUYER_ID = 32477  # ID клієнта з тестів
HISTORY_FIELD_UUID = "CT_1033"  # UUID поля "Історія бонусів"

def get_buyer_info():
    """Отримуємо інформацію про клієнта та його історію бонусів"""
    
    headers = {
        'Authorization': f'Bearer {KEYCRM_API_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(f"{KEYCRM_BASE_URL}/buyer/{BUYER_ID}", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            print("🎯 Інформація про клієнта:")
            print(f"   Ім'я: {data.get('full_name', 'N/A')}")
            print(f"   Телефон: {data.get('phone', 'N/A')}")
            print(f"   Email: {data.get('email', 'N/A')}")
            
            # Знаходимо поле з бонусами
            custom_fields = data.get('custom_fields', [])
            
            bonus_balance = None
            bonus_history = None
            
            for field in custom_fields:
                if field.get('uuid') == 'CT_1023':  # Бонусні бали
                    bonus_balance = field.get('value', 'N/A')
                elif field.get('uuid') == HISTORY_FIELD_UUID:  # Історія бонусів
                    bonus_history = field.get('value', 'N/A')
            
            print(f"   Поточний баланс бонусів: {bonus_balance}")
            print()
            
            if bonus_history:
                print("📜 Історія бонусів:")
                print("-" * 80)
                
                # Розбиваємо історію на рядки
                history_lines = bonus_history.split('\n')
                for i, line in enumerate(history_lines, 1):
                    if line.strip():
                        print(f"{i:2d}. {line.strip()}")
                        
                print("-" * 80)
                print()
                
                # Аналізуємо формат
                print("🔍 Аналіз формату:")
                recent_entries = [line.strip() for line in history_lines[:3] if line.strip()]
                
                for i, entry in enumerate(recent_entries, 1):
                    print(f"\n{i}. {entry}")
                    
                    # Перевіряємо новий формат: дата | #orderId | сума₴ | +нараховано | -списано | баланс
                    parts = entry.split(' | ')
                    if len(parts) >= 6:
                        print(f"   ✅ Новий формат (6+ частин):")
                        print(f"   📅 Дата: {parts[0]}")
                        print(f"   🆔 Замовлення: {parts[1]}")
                        print(f"   💰 Сума: {parts[2]}")
                        print(f"   ➕ Нараховано: {parts[3]}")
                        print(f"   ➖ Списано: {parts[4]}")
                        print(f"   🔄 Баланс: {parts[5]}")
                    elif len(parts) == 5:
                        print(f"   ⚠️ Старий формат (5 частин):")
                        print(f"   📅 Дата: {parts[0]}")
                        print(f"   🆔 Замовлення: {parts[1]}")
                        print(f"   💰 Сума: {parts[2]}")
                        print(f"   🎁 Бонуси: {parts[3]}")
                        print(f"   🔄 Баланс: {parts[4]}")
                    else:
                        print(f"   ❓ Невідомий формат ({len(parts)} частин)")
                        
            else:
                print("❌ Історія бонусів не знайдена")
                
        else:
            print(f"❌ Помилка отримання даних: {response.status_code}")
            print(f"   Відповідь: {response.text}")
            
    except Exception as e:
        print(f"❌ Помилка: {str(e)}")

if __name__ == "__main__":
    print("🔍 ПЕРЕВІРКА ІСТОРІЇ БОНУСІВ В KEYCRM")
    print("=" * 60)
    get_buyer_info()
    print("=" * 60)
