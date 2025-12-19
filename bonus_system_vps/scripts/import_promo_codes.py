#!/usr/bin/env python3
"""
Скрипт імпорту промокодів в PostgreSQL.

Може імпортувати з:
1. Локального JSON файлу
2. AWS S3 (потребує boto3 та AWS credentials)

Формат JSON:
{
    "100": ["CODE1", "CODE2", ...],
    "200": ["CODE3", "CODE4", ...],
    ...
}

Запуск:
    # З JSON файлу
    python scripts/import_promo_codes.py --file promo_codes.json
    
    # З S3
    python scripts/import_promo_codes.py --s3-bucket lambda-promo-sessions --s3-key promo-codes/available_codes.json
    
    # Dry-run (тільки перевірка)
    python scripts/import_promo_codes.py --file promo_codes.json --dry-run
"""

import os
import sys
import json
import argparse
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Додаємо шлях до app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.promo_code import PromoCode
from app.database import Base

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_from_json(file_path: str) -> dict:
    """Завантажує промокоди з JSON файлу"""
    logger.info(f"📂 Читання файлу: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


def load_from_s3(bucket: str, key: str) -> dict:
    """Завантажує промокоди з S3"""
    try:
        import boto3
    except ImportError:
        logger.error("❌ boto3 не встановлено. Встановіть: pip install boto3")
        sys.exit(1)
    
    logger.info(f"☁️ Завантаження з S3: s3://{bucket}/{key}")
    
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    return data


def import_promo_codes(codes_data: dict, database_url: str, dry_run: bool = False):
    """Імпортує промокоди в БД"""
    
    # Підключення до БД
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Створюємо таблицю якщо не існує
    if not dry_run:
        Base.metadata.create_all(engine)
    
    stats = {
        "total": 0,
        "added": 0,
        "duplicates": 0,
        "by_amount": {}
    }
    
    for amount_str, codes in codes_data.items():
        try:
            amount = int(amount_str)
        except ValueError:
            logger.warning(f"⚠️ Пропускаємо невалідну суму: {amount_str}")
            continue
        
        amount_added = 0
        amount_duplicates = 0
        
        for code in codes:
            stats["total"] += 1
            
            # Перевіряємо чи код вже існує
            existing = session.query(PromoCode).filter(PromoCode.code == code).first()
            if existing:
                amount_duplicates += 1
                stats["duplicates"] += 1
                continue
            
            if not dry_run:
                promo = PromoCode(
                    code=code,
                    amount=amount,
                    is_used=False
                )
                session.add(promo)
            
            amount_added += 1
            stats["added"] += 1
        
        stats["by_amount"][amount] = {
            "added": amount_added,
            "duplicates": amount_duplicates,
            "total_in_file": len(codes)
        }
        
        logger.info(f"  💰 Сума {amount}: додано {amount_added}, дублікатів {amount_duplicates}")
    
    if not dry_run:
        try:
            session.commit()
            logger.info("✅ Дані збережено в БД")
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Помилка збереження: {e}")
            raise
    else:
        logger.info("🔍 DRY RUN: дані не збережено")
    
    session.close()
    return stats


def main():
    parser = argparse.ArgumentParser(description="Імпорт промокодів в PostgreSQL")
    
    # Джерело даних
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--file", 
        help="Шлях до JSON файлу з промокодами"
    )
    source_group.add_argument(
        "--s3-bucket",
        help="S3 bucket з промокодами"
    )
    
    parser.add_argument(
        "--s3-key",
        default="promo-codes/available_codes.json",
        help="S3 key (шлях до файлу в bucket)"
    )
    
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "postgresql://bonus:bonuspass123@localhost:5432/bonus_db"),
        help="URL бази даних PostgreSQL"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Тестовий запуск без запису в БД"
    )
    
    args = parser.parse_args()
    
    logger.info("🚀 Початок імпорту промокодів")
    logger.info(f"   Режим: {'DRY RUN (тестовий)' if args.dry_run else 'PRODUCTION'}")
    
    # Завантажуємо дані
    if args.file:
        codes_data = load_from_json(args.file)
    else:
        codes_data = load_from_s3(args.s3_bucket, args.s3_key)
    
    # Показуємо статистику
    total_codes = sum(len(codes) for codes in codes_data.values())
    logger.info(f"📊 Знайдено промокодів: {total_codes}")
    for amount, codes in codes_data.items():
        logger.info(f"   💰 Сума {amount}: {len(codes)} кодів")
    
    # Імпортуємо
    stats = import_promo_codes(codes_data, args.database_url, args.dry_run)
    
    # Підсумок
    logger.info("\n" + "=" * 50)
    logger.info("📊 ПІДСУМОК ІМПОРТУ:")
    logger.info("=" * 50)
    logger.info(f"  Всього в файлі: {stats['total']}")
    logger.info(f"  Додано: {stats['added']}")
    logger.info(f"  Дублікатів (пропущено): {stats['duplicates']}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
