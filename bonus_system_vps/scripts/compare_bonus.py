#!/usr/bin/env python3
"""
Скрипт для порівняння бонусів клієнта в KeyCRM та локальній БД.

Використання:
    python scripts/compare_bonus.py 380962629162
"""

import sys
import re
import requests
from typing import Optional

# Налаштування
KEYCRM_API_TOKEN = "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ"
KEYCRM_BASE_URL = "https://openapi.keycrm.app/v1"
VPS_API_URL = "https://api.safeyourlove.com"

# UUID кастомних полів KeyCRM
BONUS_FIELD_UUID = "CT_1023"
RESERVED_BONUS_FIELD_UUID = "CT_1034"
HISTORY_FIELD_UUID = "CT_1033"
BONUS_EXPIRY_FIELD_UUID = "CT_1024"


def normalize_phone(phone: str) -> Optional[str]:
    """Нормалізує телефон до формату 380XXXXXXXXX"""
    if not phone:
        return None
    
    clean = re.sub(r'[^\d]', '', str(phone))
    
    if clean.startswith('0') and len(clean) == 10:
        clean = '38' + clean
    elif clean.startswith('80') and len(clean) == 11:
        clean = '3' + clean
    elif len(clean) == 9:
        clean = '380' + clean
    
    if len(clean) != 12 or not clean.startswith('380'):
        return None
    
    return clean


def get_from_keycrm(phone: str) -> dict:
    """Отримує дані про бонуси з KeyCRM"""
    headers = {
        "Authorization": f"Bearer {KEYCRM_API_TOKEN}",
        "Accept": "application/json"
    }
    
    # Пошук за телефоном
    url = f"{KEYCRM_BASE_URL}/buyer?filter[buyer_phone]={phone}&include=custom_fields"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        buyers = data.get("data", [])
        
        if not buyers:
            return {"found": False, "message": "Покупця не знайдено в KeyCRM"}
        
        results = []
        for buyer in buyers:
            buyer_id = buyer.get("id")
            full_name = buyer.get("full_name", "")
            phones = buyer.get("phone", [])
            emails = buyer.get("email", [])
            
            bonus_balance = 0
            reserved_balance = 0
            bonus_expiry = None
            history = ""
            
            custom_fields = buyer.get("custom_fields", [])
            for field in custom_fields:
                uuid = field.get("uuid")
                value = field.get("value")
                
                if uuid == BONUS_FIELD_UUID:
                    try:
                        bonus_balance = int(float(value or 0))
                    except:
                        pass
                elif uuid == RESERVED_BONUS_FIELD_UUID:
                    try:
                        reserved_balance = int(float(value or 0))
                    except:
                        pass
                elif uuid == BONUS_EXPIRY_FIELD_UUID:
                    bonus_expiry = value
                elif uuid == HISTORY_FIELD_UUID:
                    history = value or ""
            
            results.append({
                "buyer_id": buyer_id,
                "full_name": full_name,
                "phones": phones,
                "emails": emails,
                "bonus_balance": bonus_balance,
                "reserved_balance": reserved_balance,
                "bonus_expiry": bonus_expiry,
                "history_preview": history[:200] if history else ""
            })
        
        return {"found": True, "buyers": results, "count": len(results)}
    
    except Exception as e:
        return {"found": False, "error": str(e)}


def get_from_vps_db(phone: str) -> dict:
    """Отримує дані про бонуси з VPS БД через API"""
    
    try:
        # Спочатку пробуємо API баланс
        url = f"{VPS_API_URL}/api/v1/balance/{phone}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "found": data.get("found", False),
                "bonus_balance": data.get("bonus_balance", 0),
                "reserved_balance": data.get("reserved_balance", 0),
                "bonus_expiry": data.get("bonus_expiry"),
                "actual_bonus_in_db": data.get("actual_bonus_in_db", data.get("bonus_balance", 0)),
                "expired": data.get("expired", False),
                "phone": data.get("phone", phone)
            }
        else:
            return {"found": False, "error": f"HTTP {response.status_code}: {response.text}"}
    
    except Exception as e:
        return {"found": False, "error": str(e)}


def main():
    if len(sys.argv) < 2:
        print("Використання: python scripts/compare_bonus.py <телефон>")
        sys.exit(1)
    
    phone = sys.argv[1]
    normalized = normalize_phone(phone)
    
    if not normalized:
        print(f"❌ Невалідний номер телефону: {phone}")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"🔍 ПЕРЕВІРКА БОНУСІВ ДЛЯ: {normalized}")
    print(f"{'='*60}\n")
    
    # 1. Дані з KeyCRM
    print("📋 KEYCRM:")
    print("-" * 40)
    keycrm_data = get_from_keycrm(normalized)
    
    if not keycrm_data.get("found"):
        print(f"   ❌ {keycrm_data.get('message', keycrm_data.get('error', 'Невідома помилка'))}")
    else:
        print(f"   Знайдено записів: {keycrm_data['count']}")
        total_bonus_crm = 0
        total_reserved_crm = 0
        
        for i, buyer in enumerate(keycrm_data['buyers'], 1):
            print(f"\n   📌 Запис #{i}:")
            print(f"      ID: {buyer['buyer_id']}")
            print(f"      Ім'я: {buyer['full_name']}")
            print(f"      Телефони: {buyer['phones']}")
            print(f"      Email: {buyer['emails']}")
            print(f"      💰 Бонуси: {buyer['bonus_balance']}")
            print(f"      🔒 Резерв: {buyer['reserved_balance']}")
            print(f"      📅 Термін: {buyer['bonus_expiry']}")
            
            total_bonus_crm += buyer['bonus_balance']
            total_reserved_crm += buyer['reserved_balance']
        
        if keycrm_data['count'] > 1:
            print(f"\n   ⚠️ УВАГА: Є {keycrm_data['count']} дублікатів!")
            print(f"   📊 Сума бонусів (всі записи): {total_bonus_crm}")
            print(f"   📊 Сума резерву (всі записи): {total_reserved_crm}")
    
    # 2. Дані з VPS БД
    print(f"\n\n💾 VPS БАЗА ДАНИХ:")
    print("-" * 40)
    vps_data = get_from_vps_db(normalized)
    
    if not vps_data.get("found"):
        if "error" in vps_data:
            print(f"   ❌ Помилка: {vps_data['error']}")
        else:
            print(f"   ℹ️ Клієнта не знайдено в БД")
    else:
        print(f"   💰 Бонуси: {vps_data['bonus_balance']}")
        print(f"   🔒 Резерв: {vps_data['reserved_balance']}")
        print(f"   📅 Термін: {vps_data['bonus_expiry']}")
        if vps_data.get('expired'):
            print(f"   ⚠️ ПРОСТРОЧЕНО!")
        if vps_data.get('actual_bonus_in_db') != vps_data.get('bonus_balance'):
            print(f"   🔍 Реальне значення в БД: {vps_data['actual_bonus_in_db']}")
    
    # 3. Порівняння
    print(f"\n\n📊 ПОРІВНЯННЯ:")
    print("-" * 40)
    
    if keycrm_data.get("found") and vps_data.get("found"):
        # Беремо максимальний бонус з KeyCRM (як в міграції)
        max_crm_bonus = max(b['bonus_balance'] for b in keycrm_data['buyers'])
        max_crm_reserved = max(b['reserved_balance'] for b in keycrm_data['buyers'])
        
        db_bonus = vps_data['bonus_balance']
        db_reserved = vps_data['reserved_balance']
        
        print(f"   KeyCRM (макс.): бонуси={max_crm_bonus}, резерв={max_crm_reserved}")
        print(f"   VPS БД:         бонуси={db_bonus}, резерв={db_reserved}")
        
        if max_crm_bonus != db_bonus or max_crm_reserved != db_reserved:
            print(f"\n   ❌ РОЗБІЖНІСТЬ ВИЯВЛЕНА!")
            if max_crm_bonus != db_bonus:
                diff = max_crm_bonus - db_bonus
                print(f"      Бонуси: різниця {'+' if diff > 0 else ''}{diff}")
            if max_crm_reserved != db_reserved:
                diff = max_crm_reserved - db_reserved
                print(f"      Резерв: різниця {'+' if diff > 0 else ''}{diff}")
        else:
            print(f"\n   ✅ Дані співпадають")
    elif keycrm_data.get("found") and not vps_data.get("found"):
        print(f"   ⚠️ Є в KeyCRM, але відсутній в БД!")
    elif not keycrm_data.get("found") and vps_data.get("found"):
        print(f"   ⚠️ Є в БД, але відсутній в KeyCRM!")
    else:
        print(f"   ℹ️ Клієнта немає ні в KeyCRM, ні в БД")
    
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
