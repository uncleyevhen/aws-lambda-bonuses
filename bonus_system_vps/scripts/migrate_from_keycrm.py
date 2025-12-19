#!/usr/bin/env python3
"""
Скрипт міграції даних з KeyCRM в PostgreSQL.

Цей скрипт:
1. Експортує всіх клієнтів з KeyCRM
2. Групує їх за телефоном (об'єднує дублікати)
3. Імпортує в PostgreSQL

Запуск:
    python scripts/migrate_from_keycrm.py --dry-run  # Тест без запису
    python scripts/migrate_from_keycrm.py            # Реальна міграція
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from collections import defaultdict

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Додаємо шлях до app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.client import Client
from app.models.transaction import BonusTransaction, TransactionType
from app.utils.phone import normalize_phone
from app.database import Base

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        
        # Підключення до БД
        self.engine = create_engine(database_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Статистика
        self.stats = {
            "total_buyers": 0,
            "unique_phones": 0,
            "duplicates_merged": 0,
            "clients_created": 0,
            "errors": 0
        }
    
    def fetch_all_buyers(self) -> list:
        """Завантажує всіх покупців з KeyCRM"""
        logger.info("📥 Завантаження покупців з KeyCRM...")
        
        all_buyers = []
        page = 1
        limit = 50
        
        while True:
            try:
                url = f"{self.base_url}/buyer?page={page}&limit={limit}&include=custom_fields"
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                buyers = data.get("data", [])
                if not buyers:
                    break
                
                all_buyers.extend(buyers)
                logger.info(f"  Сторінка {page}: {len(buyers)} покупців (всього: {len(all_buyers)})")
                
                # Перевіряємо чи є наступна сторінка
                if page >= data.get("last_page", 1):
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"❌ Помилка на сторінці {page}: {e}")
                break
        
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
                    bonus_balance = int(value or 0)
                except (ValueError, TypeError):
                    pass
            elif uuid == self.RESERVED_BONUS_FIELD_UUID:
                try:
                    reserved_balance = int(value or 0)
                except (ValueError, TypeError):
                    pass
            elif uuid == self.HISTORY_FIELD_UUID:
                history = value or ""
            elif uuid == self.BONUS_EXPIRY_FIELD_UUID:
                if value:
                    try:
                        bonus_expiry = datetime.strptime(value, "%Y-%m-%d").date()
                    except ValueError:
                        pass
        
        return {
            "bonus_balance": bonus_balance,
            "reserved_balance": reserved_balance,
            "history": history,
            "bonus_expiry": bonus_expiry
        }
    
    def group_by_phone(self, buyers: list) -> dict:
        """
        Групує покупців за нормалізованим телефоном.
        Об'єднує дублікати.
        """
        logger.info("🔄 Групування покупців за телефоном...")
        
        phone_groups = defaultdict(list)
        skipped = 0
        
        for buyer in buyers:
            # Отримуємо телефон
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
        """
        Об'єднує дані з дублікатів.
        
        Логіка:
        - Бонуси: максимальне значення серед дублікатів
        - keycrm_ids: всі ID
        - emails, names: всі унікальні
        - Історія: найдовша
        """
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
            
            # Emails
            emails = buyer.get("email", [])
            if emails:
                if isinstance(emails, list):
                    merged["emails"].update(e.lower() for e in emails if e)
                else:
                    merged["emails"].add(emails.lower())
            
            # Names
            name = buyer.get("full_name")
            if name:
                merged["names"].add(name)
            
            # Бонуси - беремо максимум
            bonus_data = self.extract_bonus_data(buyer)
            if bonus_data["bonus_balance"] > merged["bonus_balance"]:
                merged["bonus_balance"] = bonus_data["bonus_balance"]
            if bonus_data["reserved_balance"] > merged["reserved_balance"]:
                merged["reserved_balance"] = bonus_data["reserved_balance"]
            
            # Історія - беремо найдовшу
            if len(bonus_data["history"]) > len(merged["history"]):
                merged["history"] = bonus_data["history"]
            
            # Дата закінчення - беремо найпізнішу
            if bonus_data["bonus_expiry"]:
                if not merged["bonus_expiry"] or bonus_data["bonus_expiry"] > merged["bonus_expiry"]:
                    merged["bonus_expiry"] = bonus_data["bonus_expiry"]
        
        merged["emails"] = list(merged["emails"])
        merged["names"] = list(merged["names"])
        
        return merged
    
    def create_client(self, phone: str, data: dict) -> Client:
        """Створює клієнта в PostgreSQL"""
        client = Client(
            phone=phone,
            bonus_balance=data["bonus_balance"],
            reserved_balance=data["reserved_balance"],
            bonus_expiry=data["bonus_expiry"],
            emails=data["emails"],
            names=data["names"],
            keycrm_ids=data["keycrm_ids"]
        )
        return client
    
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
        
        # 3. Створюємо таблиці якщо їх немає
        if not self.dry_run:
            Base.metadata.create_all(self.engine)
            logger.info("✅ Таблиці БД створено/перевірено")
        
        # 4. Мігруємо кожну групу
        logger.info("📝 Міграція клієнтів...")
        
        clients_to_create = []
        
        for phone, group_buyers in phone_groups.items():
            try:
                # Об'єднуємо дані
                merged_data = self.merge_duplicates(group_buyers)
                
                # Пропускаємо клієнтів без бонусів
                if merged_data["bonus_balance"] == 0 and merged_data["reserved_balance"] == 0:
                    continue
                
                # Створюємо клієнта
                client = self.create_client(phone, merged_data)
                clients_to_create.append(client)
                
                if len(group_buyers) > 1:
                    logger.info(f"  📦 {phone}: об'єднано {len(group_buyers)} дублікатів, "
                               f"бонуси={merged_data['bonus_balance']}, "
                               f"резерв={merged_data['reserved_balance']}")
                
            except Exception as e:
                logger.error(f"❌ Помилка для телефону {phone}: {e}")
                self.stats["errors"] += 1
        
        # 5. Зберігаємо в БД
        if not self.dry_run:
            try:
                self.session.add_all(clients_to_create)
                self.session.commit()
                self.stats["clients_created"] = len(clients_to_create)
                logger.info(f"✅ Створено {len(clients_to_create)} клієнтів")
            except Exception as e:
                logger.error(f"❌ Помилка збереження: {e}")
                self.session.rollback()
                self.stats["errors"] += 1
        else:
            self.stats["clients_created"] = len(clients_to_create)
            logger.info(f"🔍 DRY RUN: було б створено {len(clients_to_create)} клієнтів")
        
        # 6. Виводимо статистику
        self.print_stats()
    
    def print_stats(self):
        """Виводить статистику міграції"""
        logger.info("\n" + "=" * 50)
        logger.info("📊 СТАТИСТИКА МІГРАЦІЇ:")
        logger.info("=" * 50)
        logger.info(f"  Всього покупців в KeyCRM: {self.stats['total_buyers']}")
        logger.info(f"  Унікальних телефонів: {self.stats['unique_phones']}")
        logger.info(f"  Об'єднано дублікатів: {self.stats['duplicates_merged']}")
        logger.info(f"  Створено клієнтів: {self.stats['clients_created']}")
        logger.info(f"  Помилок: {self.stats['errors']}")
        logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Міграція даних з KeyCRM в PostgreSQL")
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Тестовий запуск без запису в БД"
    )
    parser.add_argument(
        "--api-token",
        default=os.getenv("KEYCRM_API_TOKEN"),
        help="API токен KeyCRM"
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "postgresql://bonus:bonuspass123@localhost:5432/bonus_db"),
        help="URL бази даних PostgreSQL"
    )
    
    args = parser.parse_args()
    
    if not args.api_token:
        logger.error("❌ Не вказано API токен KeyCRM (--api-token або KEYCRM_API_TOKEN)")
        sys.exit(1)
    
    migrator = KeyCRMMigrator(
        api_token=args.api_token,
        database_url=args.database_url,
        dry_run=args.dry_run
    )
    
    migrator.migrate()


if __name__ == "__main__":
    main()

