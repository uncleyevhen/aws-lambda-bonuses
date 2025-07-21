#!/usr/bin/env python3
"""
Перевірка результатів оновлення історії бонусів
"""

import json
import urllib.request

# Конфігурація
KEYCRM_API_TOKEN = 'M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ'
KEYCRM_BASE_URL = 'https://openapi.keycrm.app/v1'

def make_keycrm_request(url: str):
    try:
        full_url = f"{KEYCRM_BASE_URL}{url}" if not url.startswith('http') else url
        
        req = urllib.request.Request(full_url)
        req.add_header('Authorization', f'Bearer {KEYCRM_API_TOKEN}')
        req.add_header('Accept', 'application/json')
        
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
            
    except Exception as e:
        print(f"❌ Помилка запиту: {str(e)}")
        return None

def check_client_data():
    """
    Перевірити дані тестового клієнта
    """
    print("🔍 Перевірка даних клієнта після тестування...")
    
    # ID клієнта з тестування
    buyer_id = 40382
    
    result = make_keycrm_request(f"/buyer/{buyer_id}?include=custom_fields")
    
    if result:
        print(f"👤 Клієнт: {result.get('full_name')}")
        print(f"📧 Email: {result.get('email')}")
        print(f"📱 Телефон: {result.get('phone')}")
        
        if result.get('custom_fields'):
            print("\n📋 Кастомні поля:")
            for field in result['custom_fields']:
                field_name = field.get('name', 'Невідомо')
                field_uuid = field.get('uuid')
                field_value = field.get('value')
                
                print(f"   🔹 {field_name} ({field_uuid}): {field_value}")
                
                # Особлива увага до історії бонусів
                if field_uuid == 'CT_1033':
                    print("   📜 Історія бонусів:")
                    if field_value:
                        history_lines = str(field_value).split('\n')
                        for i, line in enumerate(history_lines, 1):
                            if line.strip():
                                print(f"      {i}. {line.strip()}")
                    else:
                        print("      (порожня)")
        else:
            print("❌ Кастомні поля відсутні")
    else:
        print("❌ Не вдалося отримати дані клієнта")

if __name__ == "__main__":
    print("🔍 Перевірка результатів запису історії бонусів")
    print("=" * 50)
    check_client_data()
