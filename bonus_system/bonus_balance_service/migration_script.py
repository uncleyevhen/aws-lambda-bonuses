"""
Скрипт міграції існуючих балансів бонусів з KeyCRM в DynamoDB
Виконує дедуплікацію клієнтів по номеру телефону та об'єднує їх історії
"""
import json
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Set
import time

# Додаємо шляхи до необхідних модулів
current_dir = os.path.dirname(os.path.abspath(__file__))
bonus_operations_path = os.path.join(current_dir, '..', 'bonus_operations')
sys.path.append(bonus_operations_path)
sys.path.append(current_dir)

# Імпортуємо необхідні модулі
from keycrm_client import KeyCRMClient
from bonus_balance_manager import BonusBalanceManager
from sync_service import BonusSyncService

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BonusMigrationService:
    """
    Сервіс міграції бонусів з KeyCRM в DynamoDB
    
    Логіка міграції:
    1. Отримуємо всіх клієнтів з KeyCRM
    2. Групуємо їх по номерах телефонів (дедуплікація)
    3. Об'єднуємо баланси та історії дублікатів
    4. Зберігаємо консолідовані дані в DynamoDB
    5. Синхронізуємо назад в KeyCRM для всіх дублікатів
    """
    
    def __init__(self, dry_run: bool = False):
        self.keycrm = KeyCRMClient()
        self.balance_manager = BonusBalanceManager()
        self.sync_service = BonusSyncService()
        self.dry_run = dry_run
        
        self.stats = {
            'total_keycrm_clients': 0,
            'unique_phone_numbers': 0,
            'duplicates_found': 0,
            'migrated_successfully': 0,
            'migration_errors': 0,
            'consolidated_bonuses': 0
        }
        
    def normalize_phone(self, phone: str) -> str:
        """Нормалізація номера телефону"""
        if not phone:
            return ''
            
        # Очищаємо від всіх символів крім цифр
        clean_phone = ''.join(filter(str.isdigit, phone))
        
        # Приводимо до стандартного формату +380...
        if clean_phone.startswith('0') and len(clean_phone) == 10:
            return '+38' + clean_phone
        elif clean_phone.startswith('380') and len(clean_phone) == 12:
            return '+' + clean_phone
        elif clean_phone.startswith('38') and len(clean_phone) == 11:
            return '+3' + clean_phone
        elif len(clean_phone) == 9:
            return '+380' + clean_phone
        
        return '+' + clean_phone if clean_phone else ''
    
    def get_all_keycrm_clients(self) -> List[Dict]:
        """Отримання всіх клієнтів з KeyCRM з бонусами"""
        try:
            all_clients = []
            page = 1
            per_page = 100
            
            logger.info("Починаємо отримання клієнтів з KeyCRM...")
            
            while True:
                url = f"{self.keycrm.base_url}/buyers?page={page}&per_page={per_page}&include=custom_fields"
                result = self.keycrm.make_request(url)
                
                if not result['success']:
                    logger.error(f"Помилка отримання клієнтів (сторінка {page}): {result.get('error')}")
                    break
                
                clients = result['data'].get('data', [])
                if not clients:
                    break
                
                # Фільтруємо клієнтів з бонусами або номерами телефонів
                for client in clients:
                    phone_numbers = client.get('phone', [])
                    has_bonus_fields = False
                    
                    if client.get('custom_fields'):
                        for field in client['custom_fields']:
                            if field.get('uuid') in ['CT_1023', 'CT_1034', 'CT_1033']:  # Бонусні поля
                                if field.get('value'):
                                    has_bonus_fields = True
                                    break
                    
                    if phone_numbers and phone_numbers[0] and (has_bonus_fields or True):  # Включаємо всіх з телефонами
                        all_clients.append(client)
                
                logger.info(f"Отримано сторінку {page}, клієнтів на сторінці: {len(clients)}, всього зібрано: {len(all_clients)}")
                page += 1
                
                # Пауза між запитами
                time.sleep(0.1)
            
            logger.info(f"Загалом отримано {len(all_clients)} клієнтів з KeyCRM")
            self.stats['total_keycrm_clients'] = len(all_clients)
            return all_clients
            
        except Exception as e:
            logger.error(f"Помилка отримання клієнтів з KeyCRM: {str(e)}")
            return []
    
    def group_clients_by_phone(self, clients: List[Dict]) -> Dict[str, List[Dict]]:
        """Групування клієнтів по номерах телефонів"""
        phone_groups = {}
        
        for client in clients:
            phone_numbers = client.get('phone', [])
            if not phone_numbers or not phone_numbers[0]:
                continue
                
            normalized_phone = self.normalize_phone(phone_numbers[0])
            if not normalized_phone:
                continue
            
            if normalized_phone not in phone_groups:
                phone_groups[normalized_phone] = []
            
            phone_groups[normalized_phone].append(client)
        
        # Підраховуємо статистику
        self.stats['unique_phone_numbers'] = len(phone_groups)
        
        duplicates = 0
        for phone, group in phone_groups.items():
            if len(group) > 1:
                duplicates += len(group) - 1
        
        self.stats['duplicates_found'] = duplicates
        
        logger.info(f"Згруповано клієнтів: {len(phone_groups)} унікальних номерів, {duplicates} дублікатів")
        return phone_groups
    
    def consolidate_client_data(self, clients: List[Dict]) -> Dict:
        """Консолідація даних дублікатів клієнтів"""
        consolidated = {
            'active_bonus': 0,
            'reserved_bonus': 0,
            'total_earned': 0,
            'total_used': 0,
            'transaction_history': [],
            'client_names': [],
            'client_ids': [],
            'emails': set()
        }
        
        for client in clients:
            # Зберігаємо інформацію про клієнта
            consolidated['client_ids'].append(str(client.get('id', '')))
            consolidated['client_names'].append(client.get('full_name', ''))
            
            if client.get('email') and client['email'][0]:
                consolidated['emails'].add(client['email'][0])
            
            # Обробляємо кастомні поля з бонусами
            if client.get('custom_fields'):
                for field in client['custom_fields']:
                    uuid = field.get('uuid')
                    value = field.get('value')
                    
                    if uuid == 'CT_1023' and value:  # Активні бонуси
                        consolidated['active_bonus'] += int(value or 0)
                    elif uuid == 'CT_1034' and value:  # Зарезервовані бонуси
                        consolidated['reserved_bonus'] += int(value or 0)
                    elif uuid == 'CT_1033' and value:  # Історія бонусів
                        # Парсимо історію та додаємо до загальної
                        history_entries = self.parse_keycrm_history(value, client.get('id'))
                        consolidated['transaction_history'].extend(history_entries)
        
        # Сортуємо історію по даті
        consolidated['transaction_history'].sort(key=lambda x: x.get('timestamp', ''))
        
        # Конвертуємо emails в список
        consolidated['emails'] = list(consolidated['emails'])
        
        return consolidated
    
    def parse_keycrm_history(self, history_text: str, client_id: str) -> List[Dict]:
        """Парсинг історії бонусів з текстового поля KeyCRM"""
        entries = []
        
        if not history_text:
            return entries
        
        lines = history_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or not line.startswith('['):
                continue
            
            try:
                # Спробуємо розпарсити запис формату [дата] опис
                if ']' in line:
                    date_part = line[1:line.index(']')]
                    description = line[line.index(']') + 1:].strip()
                    
                    # Спробуємо витягнути суму з опису
                    amount = 0
                    transaction_type = 'unknown'
                    
                    if '+' in description:
                        transaction_type = 'earned'
                        # Знаходимо число після +
                        import re
                        match = re.search(r'\+(\d+)', description)
                        if match:
                            amount = int(match.group(1))
                    elif '-' in description:
                        if 'резерв' in description.lower() or 'зарезерв' in description.lower():
                            transaction_type = 'reserved'
                        else:
                            transaction_type = 'used'
                        # Знаходимо число після -
                        match = re.search(r'-(\d+)', description)
                        if match:
                            amount = -int(match.group(1))
                    
                    # Створюємо запис транзакції
                    entry = {
                        'timestamp': self.parse_date_from_keycrm(date_part),
                        'type': transaction_type,
                        'amount': amount,
                        'description': description,
                        'source_client_id': str(client_id),
                        'migrated_from_keycrm': True
                    }
                    
                    entries.append(entry)
                    
            except Exception as e:
                logger.warning(f"Не вдалося розпарсити запис історії: {line}, помилка: {str(e)}")
                continue
        
        return entries
    
    def parse_date_from_keycrm(self, date_str: str) -> str:
        """Конвертація дати з KeyCRM в ISO формат"""
        try:
            # Спробуємо різні формати дат
            formats = ['%d.%m.%Y %H:%M', '%d.%m.%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.isoformat() + 'Z'
                except ValueError:
                    continue
            
            # Якщо нічого не вийшло, повертаємо поточну дату
            return datetime.utcnow().isoformat() + 'Z'
            
        except Exception:
            return datetime.utcnow().isoformat() + 'Z'
    
    def migrate_phone_group(self, phone_number: str, clients: List[Dict]) -> bool:
        """Міграція групи клієнтів з одним номером телефону"""
        try:
            logger.info(f"Міграція групи {phone_number}: {len(clients)} клієнтів")
            
            # Консолідуємо дані всіх дублікатів
            consolidated = self.consolidate_client_data(clients)
            
            # Логуємо консолідацію
            total_bonus = consolidated['active_bonus'] + consolidated['reserved_bonus']
            logger.info(f"Консолідовано для {phone_number}: активні={consolidated['active_bonus']}, "
                       f"зарезервовані={consolidated['reserved_bonus']}, історія={len(consolidated['transaction_history'])} записів")
            
            if self.dry_run:
                logger.info(f"DRY RUN: Пропускаємо збереження для {phone_number}")
                return True
            
            # Перевіряємо, чи вже існує запис в DynamoDB
            existing = self.balance_manager.get_balance(phone_number)
            
            if existing['success'] and existing['data']['active_bonus'] > 0:
                logger.warning(f"Запис для {phone_number} вже існує в DynamoDB з балансом {existing['data']['active_bonus']}. Пропускаємо.")
                return True
            
            # Зберігаємо консолідовані дані в DynamoDB
            if consolidated['active_bonus'] > 0 or consolidated['reserved_bonus'] > 0 or consolidated['transaction_history']:
                # Додаємо початковий баланс
                if consolidated['active_bonus'] > 0:
                    transaction = {
                        'type': 'migration',
                        'amount': consolidated['active_bonus'],
                        'description': f"🔄 Міграція з KeyCRM: +{consolidated['active_bonus']} активних бонусів",
                        'client_ids': consolidated['client_ids'],
                        'client_names': consolidated['client_names']
                    }
                    
                    result = self.balance_manager.update_balance(
                        phone_number,
                        active_bonus_delta=consolidated['active_bonus'],
                        transaction=transaction
                    )
                    
                    if not result['success']:
                        logger.error(f"Помилка збереження активних бонусів для {phone_number}: {result.get('error')}")
                        return False
                
                # Додаємо зарезервовані бонуси
                if consolidated['reserved_bonus'] > 0:
                    transaction = {
                        'type': 'migration',
                        'amount': consolidated['reserved_bonus'],
                        'description': f"🔄 Міграція з KeyCRM: +{consolidated['reserved_bonus']} зарезервованих бонусів"
                    }
                    
                    result = self.balance_manager.update_balance(
                        phone_number,
                        reserved_bonus_delta=consolidated['reserved_bonus'],
                        transaction=transaction
                    )
                    
                    if not result['success']:
                        logger.error(f"Помилка збереження зарезервованих бонусів для {phone_number}: {result.get('error')}")
                        return False
                
                # Додаємо історичні записи
                for history_entry in consolidated['transaction_history']:
                    self.balance_manager.add_transaction(
                        phone_number,
                        history_entry['type'],
                        history_entry['amount'],
                        description=history_entry['description']
                    )
            
            # Синхронізуємо назад в KeyCRM
            sync_result = self.sync_service.sync_to_all_client_duplicates(phone_number)
            
            if not sync_result['success']:
                logger.warning(f"Помилка синхронізації назад в KeyCRM для {phone_number}: {sync_result.get('error')}")
            else:
                logger.info(f"Синхронізовано назад в {sync_result.get('total_synced', 0)} клієнтів KeyCRM")
            
            self.stats['migrated_successfully'] += 1
            self.stats['consolidated_bonuses'] += total_bonus
            
            return True
            
        except Exception as e:
            logger.error(f"Помилка міграції групи {phone_number}: {str(e)}")
            self.stats['migration_errors'] += 1
            return False
    
    def run_migration(self):
        """Запуск повної міграції"""
        logger.info(f"Починаємо міграцію бонусів з KeyCRM в DynamoDB (DRY RUN: {self.dry_run})")
        
        start_time = time.time()
        
        try:
            # 1. Отримуємо всіх клієнтів з KeyCRM
            all_clients = self.get_all_keycrm_clients()
            
            if not all_clients:
                logger.error("Не вдалося отримати клієнтів з KeyCRM")
                return
            
            # 2. Групуємо клієнтів по телефонах
            phone_groups = self.group_clients_by_phone(all_clients)
            
            # 3. Мігруємо кожну групу
            processed = 0
            for phone_number, clients in phone_groups.items():
                success = self.migrate_phone_group(phone_number, clients)
                processed += 1
                
                if processed % 10 == 0:
                    logger.info(f"Оброблено {processed}/{len(phone_groups)} номерів")
                
                # Пауза між міграціями
                time.sleep(0.1)
            
            # 4. Виводимо статистику
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info("=== СТАТИСТИКА МІГРАЦІЇ ===")
            logger.info(f"Клієнтів в KeyCRM: {self.stats['total_keycrm_clients']}")
            logger.info(f"Унікальних номерів: {self.stats['unique_phone_numbers']}")
            logger.info(f"Знайдено дублікатів: {self.stats['duplicates_found']}")
            logger.info(f"Успішно мігровано: {self.stats['migrated_successfully']}")
            logger.info(f"Помилок міграції: {self.stats['migration_errors']}")
            logger.info(f"Всього консолідовано бонусів: {self.stats['consolidated_bonuses']}")
            logger.info(f"Час виконання: {duration:.2f} секунд")
            
        except Exception as e:
            logger.error(f"Критична помилка міграції: {str(e)}")


def main():
    """Головна функція запуску міграції"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Міграція бонусів з KeyCRM в DynamoDB')
    parser.add_argument('--dry-run', action='store_true', help='Тестовий запуск без збереження')
    parser.add_argument('--phone', type=str, help='Мігрувати конкретний номер телефону')
    
    args = parser.parse_args()
    
    # Створюємо сервіс міграції
    migration_service = BonusMigrationService(dry_run=args.dry_run)
    
    if args.phone:
        # Міграція конкретного номера
        logger.info(f"Міграція конкретного номера: {args.phone}")
        # Тут можна додати логіку для міграції одного номера
    else:
        # Повна міграція
        migration_service.run_migration()


if __name__ == "__main__":
    main()
