#!/usr/bin/env python3
"""
Скрипт для дослідження структури даних API KeyCRM
"""

import requests
import json

def debug_api():
    """Дослідження структури даних API"""
    
    # Завантажуємо конфігурацію
    try:
        with open('config_newsletter.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("❌ Файл config_newsletter.json не знайдено!")
        return
    
    api_key = config['api_key']
    funnel_id = config.get('newsletter_funnel_id', 15)
    base_url = "https://openapi.keycrm.app/v1"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    print("🔍 ДОСЛІДЖЕННЯ СТРУКТУРИ ДАНИХ API")
    print("=" * 50)
    
    # Дослідження карток
    print(f"1. Дослідження карток з воронки {funnel_id} (з include contact.client)...")
    try:
        response = requests.get(f"{base_url}/pipelines/cards?pipeline_id={funnel_id}&page=1&limit=3&include=contact.client", headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            cards = data.get('data', [])
            print(f"   ✅ Отримано {len(cards)} карток для аналізу")
            
            for i, card in enumerate(cards, 1):
                print(f"\n   Картка {i}:")
                print(f"   {json.dumps(card, ensure_ascii=False, indent=6)}")
                
        else:
            print(f"   ❌ Помилка: {response.status_code}")
            print(f"   Відповідь: {response.text}")
    except Exception as e:
        print(f"   ❌ Помилка: {e}")
    
    # Дослідження клієнтів
    print(f"\n2. Дослідження структури клієнтів...")
    try:
        response = requests.get(f"{base_url}/buyer?page=1&limit=2", headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            buyers = data.get('data', [])
            print(f"   ✅ Отримано {len(buyers)} клієнтів для аналізу")
            
            for i, buyer in enumerate(buyers, 1):
                print(f"\n   Клієнт {i}:")
                print(f"   {json.dumps(buyer, ensure_ascii=False, indent=6)}")
                
        else:
            print(f"   ❌ Помилка: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Помилка: {e}")
    
    # Дослідження воронок
    print(f"\n3. Дослідження структури воронок...")
    try:
        response = requests.get(f"{base_url}/pipelines", headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            pipelines = data.get('data', [])
            print(f"   ✅ Отримано {len(pipelines)} воронок")
            
            for pipeline in pipelines:
                if pipeline.get('id') == funnel_id:
                    print(f"\n   Наша воронка (ID {funnel_id}):")
                    print(f"   {json.dumps(pipeline, ensure_ascii=False, indent=6)}")
                    break
                
        else:
            print(f"   ❌ Помилка: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Помилка: {e}")

if __name__ == "__main__":
    debug_api()
    
    # Додаткова перевірка зв'язку contact_id та buyer_id
    print(f"\n4. ДОДАТКОВА ПЕРЕВІРКА: зв'язок contact_id та buyer_id...")
    try:
        with open('config_newsletter.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        api_key = config['api_key']
        funnel_id = config.get('newsletter_funnel_id', 15)
        base_url = "https://openapi.keycrm.app/v1"
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Отримуємо кілька карток з contact_id та include contact.client
        response = requests.get(f"{base_url}/pipelines/cards?pipeline_id={funnel_id}&page=1&limit=5&include=contact.client", headers=headers, timeout=10)
        if response.status_code == 200:
            cards = response.json().get('data', [])
            contact_ids = [card.get('contact_id') for card in cards if card.get('contact_id')]
            
            print(f"   Знайдено contact_ids: {contact_ids[:3]}")
            
            # Аналізуємо структуру з include
            if cards:
                card = cards[0]
                contact = card.get('contact')
                if contact:
                    print(f"   ✅ Contact data знайдено:")
                    print(f"   Contact ID: {contact.get('id')}")
                    print(f"   Contact структура: {list(contact.keys())}")
                    
                    client = contact.get('client')
                    if client:
                        print(f"   ✅ Client data знайдено:")
                        print(f"   Client ID: {client.get('id')}")
                        print(f"   Client Name: {client.get('full_name', 'Невідомо')}")
                        print(f"   📝 Висновок: contact.client.id = buyer_id")
                    else:
                        print(f"   ⚠️ Client data не знайдено в contact")
                else:
                    print(f"   ⚠️ Contact data не знайдено")
            
            # Перевіряємо чи є ці ID серед buyer ID
            if contact_ids:
                test_contact_id = contact_ids[0]
                buyer_response = requests.get(f"{base_url}/buyer/{test_contact_id}", headers=headers, timeout=10)
                if buyer_response.status_code == 200:
                    buyer_data = buyer_response.json()
                    print(f"   ✅ Contact ID {test_contact_id} відповідає покупцю:")
                    print(f"   Ім'я: {buyer_data.get('full_name', 'Невідомо')}")
                    print(f"   ID: {buyer_data.get('id')}")
                    print("   📝 Висновок: contact_id = buyer_id")
                else:
                    print(f"   ❌ Contact ID {test_contact_id} не є ID покупця (статус: {buyer_response.status_code})")
    except Exception as e:
        print(f"   ❌ Помилка: {e}")
