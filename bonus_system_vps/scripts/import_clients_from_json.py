#!/usr/bin/env python3
"""
Скрипт імпорту клієнтів з JSON файлу в VPS через API.

Читає готовий migration_data.json і відправляє дані батчами на API.

Запуск:
    python scripts/import_clients_from_json.py --file migration_data.json
    python scripts/import_clients_from_json.py --file migration_data.json --dry-run
    python scripts/import_clients_from_json.py --file migration_data.json --api-url http://77.42.76.72
"""

import os
import sys
import json
import argparse
import logging
import time
from typing import List, Dict, Any

import requests

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('import_clients.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def load_clients_from_json(file_path: str) -> List[Dict[str, Any]]:
    """Завантажує клієнтів з JSON файлу"""
    logger.info(f"📂 Читання файлу: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        clients = json.load(f)
    
    logger.info(f"✅ Завантажено {len(clients)} клієнтів")
    return clients


def chunks(lst: List, n: int):
    """Розбиває список на батчі по n елементів"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def import_clients_via_api(
    clients: List[Dict[str, Any]], 
    api_url: str, 
    batch_size: int = 100,
    dry_run: bool = False
) -> Dict[str, int]:
    """Імпортує клієнтів через API VPS"""
    
    stats = {
        "total": len(clients),
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "batches_sent": 0
    }
    
    total_batches = (len(clients) + batch_size - 1) // batch_size
    logger.info(f"📦 Всього батчів: {total_batches} (по {batch_size} клієнтів)")
    
    if dry_run:
        logger.info("🔍 DRY RUN: Дані не будуть відправлені")
        return stats
    
    for i, batch in enumerate(chunks(clients, batch_size), 1):
        # Форматуємо дані для API
        api_clients = []
        for client in batch:
            api_client = {
                "phone": client["phone"],
                "bonus_balance": client.get("bonus_balance", 0),
                "reserved_balance": client.get("reserved_balance", 0),
                "bonus_expiry": str(client["bonus_expiry"]) if client.get("bonus_expiry") else None,
                "keycrm_ids": client.get("keycrm_ids", []),
                "emails": client.get("emails", []),
                "names": client.get("names", [])
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
                stats["inserted"] += result.get("inserted", 0)
                stats["updated"] += result.get("updated", 0)
                stats["skipped"] += result.get("skipped", 0)
                stats["errors"] += result.get("errors", 0)
                stats["batches_sent"] += 1
                
                # Прогрес кожні 10 батчів або на останньому
                if i % 10 == 0 or i == total_batches:
                    progress = (i / total_batches) * 100
                    logger.info(
                        f"  📊 Батч {i}/{total_batches} ({progress:.1f}%): "
                        f"inserted={stats['inserted']}, updated={stats['updated']}, "
                        f"skipped={stats['skipped']}, errors={stats['errors']}"
                    )
            else:
                logger.error(f"❌ Батч {i}: HTTP {response.status_code} - {response.text[:200]}")
                stats["errors"] += len(batch)
            
            # Невелика затримка між батчами
            time.sleep(0.3)
            
        except requests.exceptions.Timeout:
            logger.error(f"❌ Батч {i}: Timeout")
            stats["errors"] += len(batch)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ Батч {i}: Connection error - {e}")
            stats["errors"] += len(batch)
        except Exception as e:
            logger.error(f"❌ Батч {i}: {e}")
            stats["errors"] += len(batch)
    
    return stats


def print_stats(stats: Dict[str, int]):
    """Виводить статистику імпорту"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 СТАТИСТИКА ІМПОРТУ:")
    logger.info("=" * 60)
    logger.info(f"  Всього клієнтів у файлі: {stats['total']}")
    logger.info(f"  Відправлено батчів: {stats['batches_sent']}")
    logger.info(f"  Вставлено (нових): {stats['inserted']}")
    logger.info(f"  Оновлено: {stats['updated']}")
    logger.info(f"  Пропущено: {stats['skipped']}")
    logger.info(f"  Помилок: {stats['errors']}")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Імпорт клієнтів з JSON в VPS через API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади:
  # Імпорт з локального JSON
  python scripts/import_clients_from_json.py --file migration_data.json
  
  # Dry-run (тест без відправки)
  python scripts/import_clients_from_json.py --file migration_data.json --dry-run
  
  # Вказати API URL
  python scripts/import_clients_from_json.py --file migration_data.json --api-url http://77.42.76.72
        """
    )
    
    parser.add_argument(
        "--file", "-f",
        required=True,
        help="Шлях до JSON файлу з клієнтами (migration_data.json)"
    )
    
    parser.add_argument(
        "--api-url",
        default=os.getenv("VPS_API_URL", "http://77.42.76.72"),
        help="URL API VPS (default: http://77.42.76.72)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Розмір батчу (default: 100)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Тестовий запуск без відправки даних"
    )
    
    args = parser.parse_args()
    
    logger.info("🚀 Початок імпорту клієнтів")
    logger.info(f"   Файл: {args.file}")
    logger.info(f"   API URL: {args.api_url}")
    logger.info(f"   Batch size: {args.batch_size}")
    logger.info(f"   Режим: {'DRY RUN' if args.dry_run else 'PRODUCTION'}")
    
    # Перевіряємо з'єднання з API
    if not args.dry_run:
        try:
            response = requests.get(f"{args.api_url}/health", timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ API доступний: {response.json()}")
            else:
                logger.error(f"❌ API повернув статус {response.status_code}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Не вдалося підключитися до API: {e}")
            sys.exit(1)
    
    # Завантажуємо клієнтів
    clients = load_clients_from_json(args.file)
    
    if not clients:
        logger.error("❌ Файл порожній або не містить клієнтів")
        sys.exit(1)
    
    # Показуємо приклад даних
    sample = clients[0]
    logger.info(f"📋 Приклад даних: phone={sample.get('phone')}, "
                f"bonus={sample.get('bonus_balance')}, "
                f"reserved={sample.get('reserved_balance')}")
    
    # Фільтруємо клієнтів з бонусами (опціонально можна прибрати)
    clients_with_bonus = [c for c in clients if c.get('bonus_balance', 0) > 0 or c.get('reserved_balance', 0) > 0]
    logger.info(f"📊 Клієнтів з бонусами: {len(clients_with_bonus)} з {len(clients)}")
    
    # Імпортуємо
    stats = import_clients_via_api(
        clients_with_bonus,
        args.api_url,
        args.batch_size,
        args.dry_run
    )
    
    # Статистика
    print_stats(stats)
    
    if stats["errors"] == 0:
        logger.info("✅ Імпорт завершено успішно!")
    else:
        logger.warning(f"⚠️ Імпорт завершено з {stats['errors']} помилками")


if __name__ == "__main__":
    main()
