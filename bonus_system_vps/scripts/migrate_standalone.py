#!/usr/bin/env python3
"""
Standalone скрипт міграції бонусів з KeyCRM в PostgreSQL на VPS.

Цей скрипт:
1. Експортує всіх клієнтів з KeyCRM з кастомними полями
2. Групує їх за телефоном (об'єднує дублікати)
3. Імпортує в PostgreSQL на VPS

Запуск:
    python scripts/migrate_standalone.py --dry-run  # Тест без запису
    python scripts/migrate_standalone.py            # Реальна міграція
    
    # Для VPS (через SSH tunnel або напряму до БД):
    python scripts/migrate_standalone.py --database-url "postgresql://bonus:PASSWORD@77.42.76.72:5432/bonus_db"
"""

import os
import re
import sys
import json
import argparse
import logging
from datetime import datetime
from collections import defaultdict
from typing import Optional, List, Dict, Any, Tuple

import requests

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('migration.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# ==================== Нормалізація телефону ====================

def normalize_phone(phone: str) -> Optional[str]:
    """
    Нормалізує телефонний номер до формату 380XXXXXXXXX.
    """
    if not phone:
        return None
    
    clean_phone = re.sub(r'[^\d]', '', str(phone))
    
    if not clean_phone:
        return None
    
    if clean_phone.startswith('0') and len(clean_phone) == 10:
        clean_phone = '38' + clean_phone
    elif clean_phone.startswith('380') and len(clean_phone) == 12:
        pass
    elif clean_phone.startswith('80') and len(clean_phone) == 11:
        clean_phone = '3' + clean_phone
    elif len(clean_phone) == 9:
        clean_phone = '380' + clean_phone
    
    if len(clean_phone) != 12 or not clean_phone.startswith('380'):
        return None
    
    return clean_phone


# ==================== KeyCRM Migrator ====================

class KeyCRMMigrator:
    """Клас для міграції даних з KeyCRM"""
    
    # UUID кастомних полів KeyCRM
    BONUS_FIELD_UUID = "CT_1023"
    RESERVED_BONUS_FIELD_UUID = "CT_1034"
    HISTORY_FIELD_UUID = "CT_1033"
    BONUS_EXPIRY_FIELD_UUID = "CT_1024"
    
    def __init__(self, api_token: str, database_url: str, dry_run: bool = True):
        self.api_token = api_token
        self.base_url = "https://openapi.keycrm.app/v1"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json"
        }
        self.dry_run = dry_run
        self.database_url = database_url
        
        # Статистика
        self.stats = {
            "total_buyers": 0,
            "unique_phones": 0,
            "duplicates_merged": 0,
            "clients_created": 0,
            "clients_with_bonus": 0,
            "total_bonus_amount": 0,
            "errors": 0
        }
    
    def fetch_all_buyers(self) -> list:
        """Завантажує всіх покупців з KeyCRM з rate limiting"""
        logger.info("📥 Завантаження покупців з KeyCRM...")
        logger.info("   (Ліміт API: 60 запитів/хв, затримка 1.1 сек між запитами)")
        
        import time
        
        all_buyers = []
        page = 1
        limit = 50
        rate_limit_delay = 1.1  # 60 req/min = 1 req/sec + buffer
        
        while True:
            try:
                url = f"{self.base_url}/buyer?page={page}&limit={limit}&include=custom_fields"
                response = requests.get(url, headers=self.headers, timeout=30)
                
                # Handle rate limiting
                if response.status_code == 429:
                    logger.warning(f"⏳ Rate limit на сторінці {page}, чекаємо 60 сек...")
                    time.sleep(60)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                buyers = data.get("data", [])
                if not buyers:
                    break
                
                all_buyers.extend(buyers)
                
                # Progress logging
                if page % 20 == 0:
                    last_page = data.get("last_page", 1)
                    progress = (page / last_page) * 100
                    logger.info(f"  📊 Сторінка {page}/{last_page} ({progress:.1f}%): {len(all_buyers)} покупців")
                
                # Перевіряємо чи є наступна сторінка
                last_page = data.get("last_page", 1)
                if page >= last_page:
                    break
                
                page += 1
                
                # Rate limiting delay
                time.sleep(rate_limit_delay)
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning(f"⏳ Rate limit (429), чекаємо 60 сек...")
                    time.sleep(60)
                    continue
                logger.error(f"❌ HTTP помилка на сторінці {page}: {e}")
                if page > 1:
                    break
                raise
            except Exception as e:
                logger.error(f"❌ Помилка на сторінці {page}: {e}")
                if page > 1:
                    break
                raise
        
        self.stats["total_buyers"] = len(all_buyers)
        logger.info(f"✅ Завантажено {len(all_buyers)} покупців")
        return all_buyers
    
    def extract_bonus_data(self, buyer: dict) -> dict:
        """Витягує дані бонусів з кастомних полів"""
        bonus_balance = 0
        reserved_balance = 0
        history = ""
        bonus_expiry = None
        
        custom_fields = buyer.get("custom_fields", [])
        for field in custom_fields:
            uuid = field.get("uuid")
            value = field.get("value")
            
            if uuid == self.BONUS_FIELD_UUID:
                try:
                    bonus_balance = int(float(value or 0))
                except (ValueError, TypeError):
                    pass
            elif uuid == self.RESERVED_BONUS_FIELD_UUID:
                try:
                    reserved_balance = int(float(value or 0))
                except (ValueError, TypeError):
                    pass
            elif uuid == self.HISTORY_FIELD_UUID:
                history = value or ""
            elif uuid == self.BONUS_EXPIRY_FIELD_UUID:
                if value:
                    try:
                        bonus_expiry = datetime.strptime(value, "%Y-%m-%d").date()
                    except ValueError:
                        try:
                            bonus_expiry = datetime.strptime(value[:10], "%Y-%m-%d").date()
                        except ValueError:
                            pass
        
        return {
            "bonus_balance": bonus_balance,
            "reserved_balance": reserved_balance,
            "history": history,
            "bonus_expiry": bonus_expiry
        }
    
    def group_by_phone(self, buyers: list) -> dict:
        """Групує покупців за нормалізованим телефоном"""
        logger.info("🔄 Групування покупців за телефоном...")
        
        phone_groups = defaultdict(list)
        skipped = 0
        
        for buyer in buyers:
            phones = buyer.get("phone", [])
            if not phones:
                skipped += 1
                continue
            
            phone = phones[0] if isinstance(phones, list) else phones
            normalized = normalize_phone(phone)
            
            if not normalized:
                skipped += 1
                continue
            
            phone_groups[normalized].append(buyer)
        
        self.stats["unique_phones"] = len(phone_groups)
        self.stats["duplicates_merged"] = self.stats["total_buyers"] - len(phone_groups) - skipped
        
        logger.info(f"✅ Унікальних телефонів: {len(phone_groups)}")
        logger.info(f"✅ Об'єднано дублікатів: {self.stats['duplicates_merged']}")
        logger.info(f"⚠️ Пропущено (без телефону): {skipped}")
        
        return phone_groups
    
    def merge_duplicates(self, buyers: list) -> dict:
        """Об'єднує дані з дублікатів"""
        merged = {
            "bonus_balance": 0,
            "reserved_balance": 0,
            "keycrm_ids": [],
            "emails": set(),
            "names": set(),
            "history": "",
            "bonus_expiry": None
        }
        
        for buyer in buyers:
            buyer_id = buyer.get("id")
            if buyer_id:
                merged["keycrm_ids"].append(buyer_id)
            
            emails = buyer.get("email", [])
            if emails:
                if isinstance(emails, list):
                    merged["emails"].update(e.lower() for e in emails if e)
                else:
                    merged["emails"].add(emails.lower())
            
            name = buyer.get("full_name")
            if name:
                merged["names"].add(name)
            
            bonus_data = self.extract_bonus_data(buyer)
            if bonus_data["bonus_balance"] > merged["bonus_balance"]:
                merged["bonus_balance"] = bonus_data["bonus_balance"]
            if bonus_data["reserved_balance"] > merged["reserved_balance"]:
                merged["reserved_balance"] = bonus_data["reserved_balance"]
            
            if len(bonus_data["history"]) > len(merged["history"]):
                merged["history"] = bonus_data["history"]
            
            if bonus_data["bonus_expiry"]:
                if not merged["bonus_expiry"] or bonus_data["bonus_expiry"] > merged["bonus_expiry"]:
                    merged["bonus_expiry"] = bonus_data["bonus_expiry"]
        
        merged["emails"] = list(merged["emails"])
        merged["names"] = list(merged["names"])
        
        return merged
    
    def save_to_json(self, clients_data: List[Dict], filename: str = "migration_data.json"):
        """Зберігає дані в JSON файл для перевірки"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(clients_data, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"💾 Дані збережено в {filename}")
    
    def migrate_via_api(self, clients_data: List[Dict], api_url: str):
        """Мігрує дані через API VPS"""
        import time
        
        logger.info(f"🌐 Міграція через API: {api_url}")
        
        # Відправляємо батчами по 100 клієнтів
        batch_size = 100
        total_batches = (len(clients_data) + batch_size - 1) // batch_size
        
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        total_errors = 0
        
        for i in range(0, len(clients_data), batch_size):
            batch = clients_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            # Форматуємо дані для API
            api_clients = []
            for client in batch:
                api_client = {
                    "phone": client["phone"],
                    "bonus_balance": client["bonus_balance"],
                    "reserved_balance": client["reserved_balance"],
                    "bonus_expiry": str(client["bonus_expiry"]) if client["bonus_expiry"] else None,
                    "keycrm_ids": client["keycrm_ids"],
                    "emails": client["emails"],
                    "names": client["names"]
                }
                api_clients.append(api_client)
            
            payload = {
                "clients": api_clients,
                "overwrite": True
            }
            
            try:
                response = requests.post(
                    f"{api_url}/migration/clients/import",
                    json=payload,
                    timeout=120,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    total_inserted += result.get("inserted", 0)
                    total_updated += result.get("updated", 0)
                    total_skipped += result.get("skipped", 0)
                    total_errors += result.get("errors", 0)
                    
                    if batch_num % 10 == 0 or batch_num == total_batches:
                        logger.info(f"  📦 Батч {batch_num}/{total_batches}: inserted={result.get('inserted')}, updated={result.get('updated')}")
                else:
                    logger.error(f"❌ Батч {batch_num}: HTTP {response.status_code} - {response.text[:200]}")
                    total_errors += len(batch)
                
                # Невелика затримка між батчами
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Батч {batch_num}: {e}")
                total_errors += len(batch)
        
        self.stats["clients_created"] = total_inserted + total_updated
        logger.info(f"✅ Міграція завершена: inserted={total_inserted}, updated={total_updated}, skipped={total_skipped}, errors={total_errors}")
        return total_errors == 0
    
    def migrate_to_postgres(self, clients_data: List[Dict]):
        """Мігрує дані в PostgreSQL (пряме підключення)"""
        try:
            import psycopg2
        except ImportError:
            logger.error("❌ psycopg2 не встановлено. Встановіть: pip install psycopg2-binary")
            return False
        
        logger.info(f"🔌 Підключення до PostgreSQL...")
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'clients'
                );
            """)
            if not cursor.fetchone()[0]:
                logger.error("❌ Таблиця 'clients' не існує")
                return False
            
            inserted = 0
            updated = 0
            
            for client in clients_data:
                try:
                    cursor.execute(
                        "SELECT id, bonus_balance, reserved_balance FROM clients WHERE phone = %s",
                        (client['phone'],)
                    )
                    existing = cursor.fetchone()
                    
                    if existing:
                        existing_id, existing_bonus, existing_reserved = existing
                        if client['bonus_balance'] > existing_bonus or client['reserved_balance'] > existing_reserved:
                            cursor.execute("""
                                UPDATE clients SET 
                                    bonus_balance = GREATEST(bonus_balance, %s),
                                    reserved_balance = GREATEST(reserved_balance, %s),
                                    bonus_expiry = COALESCE(%s, bonus_expiry),
                                    keycrm_ids = %s,
                                    emails = %s,
                                    names = %s,
                                    updated_at = NOW()
                                WHERE phone = %s
                            """, (
                                client['bonus_balance'],
                                client['reserved_balance'],
                                client['bonus_expiry'],
                                client['keycrm_ids'],
                                client['emails'],
                                client['names'],
                                client['phone']
                            ))
                            updated += 1
                    else:
                        cursor.execute("""
                            INSERT INTO clients (phone, bonus_balance, reserved_balance, bonus_expiry, keycrm_ids, emails, names)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            client['phone'],
                            client['bonus_balance'],
                            client['reserved_balance'],
                            client['bonus_expiry'],
                            client['keycrm_ids'],
                            client['emails'],
                            client['names']
                        ))
                        inserted += 1
                    
                except Exception as e:
                    logger.error(f"❌ Помилка для {client['phone']}: {e}")
                    self.stats["errors"] += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.stats["clients_created"] = inserted
            logger.info(f"✅ Вставлено: {inserted}, оновлено: {updated}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка підключення до БД: {e}")
            return False
    
    def migrate(self):
        """Виконує повну міграцію"""
        logger.info("🚀 Початок міграції...")
        logger.info(f"   Режим: {'DRY RUN (тестовий)' if self.dry_run else 'PRODUCTION'}")
        
        # 1. Завантажуємо покупців
        buyers = self.fetch_all_buyers()
        if not buyers:
            logger.error("❌ Не вдалося завантажити покупців")
            return
        
        # 2. Групуємо за телефоном
        phone_groups = self.group_by_phone(buyers)
        
        # 3. Готуємо дані для міграції
        logger.info("📝 Підготовка даних...")
        
        clients_to_create = []
        
        for phone, group_buyers in phone_groups.items():
            try:
                merged_data = self.merge_duplicates(group_buyers)
                
                # Пропускаємо клієнтів без бонусів
                if merged_data["bonus_balance"] == 0 and merged_data["reserved_balance"] == 0:
                    continue
                
                client = {
                    "phone": phone,
                    "bonus_balance": merged_data["bonus_balance"],
                    "reserved_balance": merged_data["reserved_balance"],
                    "bonus_expiry": merged_data["bonus_expiry"],
                    "keycrm_ids": merged_data["keycrm_ids"],
                    "emails": merged_data["emails"],
                    "names": merged_data["names"]
                }
                clients_to_create.append(client)
                
                self.stats["clients_with_bonus"] += 1
                self.stats["total_bonus_amount"] += merged_data["bonus_balance"]
                
                if len(group_buyers) > 1:
                    logger.debug(f"  📦 {phone}: об'єднано {len(group_buyers)} дублікатів")
                
            except Exception as e:
                logger.error(f"❌ Помилка для телефону {phone}: {e}")
                self.stats["errors"] += 1
        
        logger.info(f"✅ Підготовлено {len(clients_to_create)} клієнтів з бонусами")
        
        # 4. Зберігаємо в JSON для перевірки
        self.save_to_json(clients_to_create, "migration_data.json")
        
        # 5. Мігруємо в БД або через API
        if not self.dry_run:
            # Визначаємо чи це API URL чи database URL
            if self.database_url.startswith("http"):
                success = self.migrate_via_api(clients_to_create, self.database_url)
            else:
                success = self.migrate_to_postgres(clients_to_create)
            
            if not success:
                logger.error("❌ Міграція не вдалася")
        else:
            logger.info(f"🔍 DRY RUN: було б створено {len(clients_to_create)} клієнтів")
            logger.info(f"   Дані збережено в migration_data.json для перевірки")
        
        # 6. Статистика
        self.print_stats()
    
    def print_stats(self):
        """Виводить статистику міграції"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 СТАТИСТИКА МІГРАЦІЇ:")
        logger.info("=" * 60)
        logger.info(f"  Всього покупців в KeyCRM: {self.stats['total_buyers']}")
        logger.info(f"  Унікальних телефонів: {self.stats['unique_phones']}")
        logger.info(f"  Об'єднано дублікатів: {self.stats['duplicates_merged']}")
        logger.info(f"  Клієнтів з бонусами: {self.stats['clients_with_bonus']}")
        logger.info(f"  Загальна сума бонусів: {self.stats['total_bonus_amount']}")
        logger.info(f"  Створено/оновлено в БД: {self.stats['clients_created']}")
        logger.info(f"  Помилок: {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Міграція бонусів з KeyCRM в PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади:
  # Dry-run (тест без запису)
  python migrate_standalone.py --dry-run

  # Міграція через API (рекомендовано)
  python migrate_standalone.py --api-url https://api.safeyourlove.com
  
  # Міграція через пряме підключення до БД
  python migrate_standalone.py --database-url postgresql://bonus:pass@localhost:5432/bonus_db
        """
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Тестовий запуск без запису в БД"
    )
    parser.add_argument(
        "--api-token",
        default=os.getenv("KEYCRM_API_TOKEN", "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ"),
        help="API токен KeyCRM"
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("VPS_API_URL", "https://api.safeyourlove.com"),
        help="URL API VPS для міграції через HTTP"
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="URL бази даних PostgreSQL (якщо не вказано - використовується --api-url)"
    )
    
    args = parser.parse_args()
    
    # Визначаємо цільовий URL (API або database)
    target_url = args.database_url if args.database_url else args.api_url
    
    if not args.api_token:
        logger.error("❌ Не вказано API токен KeyCRM")
        sys.exit(1)
    
    logger.info(f"🎯 Ціль міграції: {target_url}")
    
    migrator = KeyCRMMigrator(
        api_token=args.api_token,
        database_url=target_url,
        dry_run=args.dry_run
    )
    
    migrator.migrate()


if __name__ == "__main__":
    main()
