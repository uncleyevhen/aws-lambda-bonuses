#!/usr/bin/env python3
"""
СМАРТ генератор промокодів з фільтрацією та аналізом існуючих кодів (ПАРАЛЕЛЬНИЙ РЕЖИМ).

ОСНОВНІ ФУНКЦІЇ:
1. manage_promo_codes() - ГОЛОВНА ФУНКЦІЯ для паралельного управління промокодами
2. worker_process() - робочий процес для обробки діапазону сум
3. smart_promo_management_worker() - логіка обробки для кожного процесу
4. apply_amount_filter() / apply_code_filter() - фільтрація в адмін-панелі  
5. get_all_bon_codes_with_pagination() - збір всіх BON кодів з таблиці з підтримкою пагінації
6. generate_promo_codes() / create_promo_codes() - генерація та створення кодів
7. delete_specific_promo_codes() - видалення конкретних промокодів
8. upload_to_s3() / download_from_s3() - синхронізація з S3

ПАРАЛЕЛЬНИЙ РЕЖИМ:
- Автоматично розділяє діапазон сум між процесами
- Кількість процесів налаштовується через CONFIG['parallel_processes']
- Кожен процес обробляє свій діапазон незалежно
- Результати збираються та записуються в S3

ЗАПУСК:
- HEADLESS режим: PLAYWRIGHT_HEADED=false python3 promo_generator.py  
- HEADED режим (за замовчуванням): python3 promo_generator/promo_generator.py

НАЛАШТУВАННЯ ПАРАЛЕЛЬНОСТІ:
- Через змінну середовища: PROMO_PARALLEL_PROCESSES=5
- Або в CONFIG['parallel_processes']

ОНОВЛЕННЯ v3.0:
- Видалено не паралельний режим
- Залишено тільки паралельну обробку
- Спрощено архітектуру
"""

from typing import Any, Dict, List, Optional, Set
import boto3
import json
import random
import string
import logging
import time
import os
import sys
import re
import datetime
import multiprocessing
from multiprocessing import Process, Queue
from dotenv import load_dotenv

# Додаємо шлях до shared модулів для імпорту
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

# Імпорти для браузера та логіну (з обробкою помилок)
try:
    from shared.browser_manager import create_browser_manager
    from shared.promo_service import PromoService
    BROWSER_MODULES_AVAILABLE = True
except ImportError as e:
    # Створюємо logger, якщо він ще не ініціалізований
    logger = logging.getLogger(__name__)
    logger.warning(f"⚠️ Не вдалося імпортувати браузерні модулі: {e}")
    create_browser_manager = None
    PromoService = None
    BROWSER_MODULES_AVAILABLE = False

# Завантажуємо змінні середовища з .env файлу
load_dotenv()

# Ініціалізуємо глобальний logger
logger = logging.getLogger(__name__)

# --- Конфігурація паралельного режиму ---
CONFIG = {
    's3_bucket': 'lambda-promo-sessions',
    's3_key': 'promo-codes/available_codes.json',
    'region': 'eu-north-1',
    'target_codes_per_amount': 10,
    'start_amount': 1,      # Початкова сума для обробки
    'end_amount': 2000,        # Кінцева сума для обробки
    'sort_order': 'asc',  # Порядок сортування: 'asc' (зростання) або 'desc' (спадання)
    'sync_s3': True,     # Синхронізація з S3
    'auto_delete_excess': False,  # Автоматичне видалення зайвих промокодів
    'delete_existing_before_add': False,  # Видалення всіх існуючих промокодів перед додаванням нових
    'verbose_logging': True,     # Детальне логування
    'quick_mode': True,  # Швидкий режим - мінімум логів для оптимальних випадків
    'parallel_processes': 3,    # Кількість паралельних процесів
    'process_timeout': 600,  # Таймаут для процесу в секундах (10 хвилин)
}

# Налаштування кількості процесів через змінні середовища
def get_processes_count():
    """Отримує кількість процесів із змінних середовища або конфігурації.
    Мінімум 2 процеси для паралельного режиму."""
    env_processes = os.getenv('PROMO_PARALLEL_PROCESSES')
    if env_processes:
        try:
            return max(2, int(env_processes))  # Мінімум 2 процеси
        except ValueError:
            pass
    return max(2, CONFIG.get('parallel_processes', 5))  # Мінімум 2 процеси

# Оновлюємо конфігурацію
CONFIG['parallel_processes'] = get_processes_count()

# --- Функції для паралельної роботи ---

def split_range_for_processes(start_amount, end_amount, num_processes):
    """
    Розділяє діапазон сум на частини для паралельних процесів.
    
    Args:
        start_amount: початкова сума
        end_amount: кінцева сума  
        num_processes: кількість процесів
        
    Returns:
        list: список кортежів (start, end) для кожного процесу
    """
    logger.info(f"🔍 Розділення діапазону {start_amount}-{end_amount} на {num_processes} процесів")
    
    total_range = end_amount - start_amount + 1
    chunk_size = total_range // num_processes
    remainder = total_range % num_processes
    
    ranges = []
    current_start = start_amount
    
    logger.info(f"📊 Загальний діапазон: {total_range}, розмір блоку: {chunk_size}, остача: {remainder}")
    
    for i in range(num_processes):
        # Додаємо по одному до розміру chunks, якщо є остача
        current_chunk_size = chunk_size + (1 if i < remainder else 0)
        current_end = current_start + current_chunk_size - 1
        
        # Обмежуємо кінець діапазону
        current_end = min(current_end, end_amount)
        
        ranges.append((current_start, current_end))
        logger.debug(f"  🧩 Процес {i+1}: діапазон {current_start}-{current_end} ({current_chunk_size} сум)")
        
        current_start = current_end + 1
        
        # Якщо досягли кінця, зупиняємось
        if current_start > end_amount:
            break
    
    logger.info(f"✅ Розділення завершено. Створено {len(ranges)} діапазонів")
    return ranges

def worker_process(process_id, start_amount, end_amount, result_queue, config_override=None):
    """
    Робочий процес для обробки діапазону сум.
    
    Args:
        process_id: ідентифікатор процесу
        start_amount: початкова сума для обробки
        end_amount: кінцева сума для обробки
        result_queue: черга для передачі результатів
        config_override: перевизначення конфігурації
    """
    try:
        # Налаштовуємо логування для процесу
        process_logger = logging.getLogger(f'worker_{process_id}')
        process_logger.setLevel(logging.INFO)
        
        # Створюємо файл логу для процесу
        log_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        log_filename = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            'logs', 
            f'worker_{process_id}_{log_timestamp}.log'
        )
        
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        process_logger.addHandler(file_handler)
        process_logger.propagate = False
        
        process_logger.info(f"🚀 Процес {process_id} запущено для діапазону {start_amount}-{end_amount}")
        
        # Застосовуємо перевизначення конфігурації
        local_config = CONFIG.copy()
        if config_override:
            local_config.update(config_override)
        
        local_config['start_amount'] = start_amount
        local_config['end_amount'] = end_amount
        
        # Запускаємо обробку для цього діапазону
        result = smart_promo_management_worker(process_id, local_config, process_logger)
        
        # Відправляємо результат в чергу
        result_data = {
            'process_id': process_id,
            'start_amount': start_amount,
            'end_amount': end_amount,
            'success': result.get('success', False),
            'codes_data': result.get('codes_data', {}),
            'operations': result.get('operations', {'created': 0, 'deleted': 0, 'unchanged': 0}),
            'log_file': log_filename
        }
        
        result_queue.put(result_data)
        process_logger.info(f"✅ Процес {process_id} завершено успішно")
        
    except Exception as e:
        # Створюємо process_logger, якщо він не був створений
        process_logger = logging.getLogger(f'worker_{process_id}')
        
        error_result = {
            'process_id': process_id,
            'start_amount': start_amount,
            'end_amount': end_amount,
            'success': False,
            'error': str(e),
            'codes_data': {},
            'operations': {'created': 0, 'deleted': 0, 'unchanged': 0}
        }
        result_queue.put(error_result)
        
        process_logger.error(f"❌ Помилка в процесі {process_id}: {e}")

def smart_promo_management_worker(process_id, config, process_logger):
    """
    Робоча функція для одного процесу управління промокодами.
    
    Args:
        process_id: ідентифікатор процесу
        config: конфігурація для процесу
        process_logger: логгер для процесу
        
    Returns:
        dict: результати роботи процесу
    """
    # Встановлюємо глобальний logger для сумісності з функціями, що його використовують
    global logger
    logger = process_logger
    
    process_logger.info(f"🔧 Робочий процес {process_id} розпочав роботу")
    
    # Імпорти
    import sys
    import os
    
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, parent_dir)
    sys.path.insert(0, os.path.join(parent_dir, 'replenish_promo_code_lambda'))
    
    # Додаємо поточну папку до sys.path для доступу до функцій promo_generator
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    
    try:
        from shared.browser_manager import create_browser_manager
        from shared.promo_service import PromoService
        from promo_smart import PromoSmartManager
        
        # Імпортуємо функції з поточного модуля безпосередньо
        # Оскільки ми знаходимося в робочому процесі, функції вже визначені в цьому файлі
        global set_table_rows_per_page, apply_bon_filter_once, apply_amount_range_filter
        global clear_all_filters, sort_table_by_amount, get_all_bon_codes_with_pagination
        global create_promo_codes, delete_specific_promo_codes, check_existing_bon_codes_for_amount, generate_bon_code
        
        # Перевіряємо, що всі функції доступні
        required_functions = [
            'set_table_rows_per_page', 'apply_bon_filter_once', 'apply_amount_range_filter',
            'clear_all_filters', 'sort_table_by_amount', 'get_all_bon_codes_with_pagination',
            'create_promo_codes', 'delete_specific_promo_codes', 'check_existing_bon_codes_for_amount', 'generate_bon_code'
        ]
        
        missing_functions = []
        for func_name in required_functions:
            if func_name not in globals():
                missing_functions.append(func_name)
        
        if missing_functions:
            raise ImportError(f"Не знайдено функції: {', '.join(missing_functions)}")
        
        process_logger.info(f"✅ Процес {process_id}: Всі модулі та функції імпортовано успішно")
        
    except ImportError as e:
        process_logger.error(f"❌ Не вдалося імпортувати необхідні модулі: {e}")
        return {'success': False, 'error': f'Import error: {e}'}
    
    # Додаємо затримку для уникнення конфліктів між процесами
    time.sleep(process_id * 2)  # Кожен процес стартує з затримкою
    
    browser_manager = create_browser_manager()
    
    try:
        # Ініціалізація браузера
        process_logger.info(f"🚀 Процес {process_id}: Ініціалізація браузера...")
        page = browser_manager.initialize()
        promo_service = PromoService(page)
        
        # Логін
        process_logger.info(f"🔑 Процес {process_id}: Логін в адмін-панель...")
        if not promo_service.login():
            process_logger.error(f"❌ Процес {process_id}: Не вдалося увійти в адмін-панель")
            return {'success': False, 'error': 'Login failed'}
        
        process_logger.info(f"✅ Процес {process_id}: Успішний логін!")
        promo_smart_manager = PromoSmartManager(promo_service)
        
        # Словник для результатів
        codes_data = {}
        total_operations = {'created': 0, 'deleted': 0, 'unchanged': 0}
        
        # Отримуємо iframe
        iframe = promo_service._get_iframe()
        if not iframe:
            process_logger.error(f"❌ Процес {process_id}: Не вдалося отримати iframe")
            return {'success': False, 'error': 'iframe not found'}
        
        # Застосовуємо фільтри та отримуємо дані
        process_logger.info(f"🔍 Процес {process_id}: Отримання промокодів для діапазону {config['start_amount']}-{config['end_amount']}...")
        
        # Встановлюємо максимальну кількість рядків на сторінці
        set_table_rows_per_page(iframe, 160)
        
        # Застосовуємо BON фільтр один раз для всього діапазону
        if not apply_bon_filter_once(iframe):
            process_logger.error(f"❌ Процес {process_id}: Не вдалося застосувати BON фільтр")
            return {'success': False, 'error': 'BON filter failed'}
        
        # Застосовуємо фільтр по діапазону сум
        if not apply_amount_range_filter(iframe, config['start_amount'], config['end_amount']):
            process_logger.warning(f"⚠️ Процес {process_id}: Не вдалося застосувати фільтр діапазону")
        
        # Сортуємо таблицю по розміру знижки для кращої організації даних
        process_logger.info(f"📊 Застосовуємо сортування по розміру знижки ({config['sort_order']})...")
        sort_success = sort_table_by_amount(iframe, config['sort_order'])
        if not sort_success:
            process_logger.warning("⚠️ Не вдалося відсортувати таблицю по розміру знижки, продовжуємо без сортування")
        else:
            sort_label = "зростання" if config['sort_order'] == "asc" else "спадання"
            process_logger.info(f"✅ Таблицю успішно відсортовано по розміру знижки ({sort_label})")

        # Якщо активна опція видалення всіх існуючих промокодів перед додаванням нових
        if config.get('delete_existing_before_add', False):
            process_logger.info(f"🗑️ Процес {process_id}: Видалення всіх існуючих промокодів перед додаванням нових...")
            
            # Отримуємо page з promo_service
            page = promo_service.page if hasattr(promo_service, 'page') else None
            
            # Видаляємо всі промокоди в діапазоні
            deleted_count = delete_all_promo_codes_in_range(iframe, config['start_amount'], config['end_amount'], process_logger, page)
            total_operations['deleted'] += deleted_count
            
            # Очищуємо словник кодів, оскільки всі були видалені
            all_codes_by_amount = {}
            for amount in range(config['start_amount'], config['end_amount'] + 1):
                all_codes_by_amount[amount] = []

        # Отримуємо коди для діапазону
        all_codes_by_amount = get_all_bon_codes_with_pagination(iframe)
        
        if not all_codes_by_amount:
            process_logger.info(f"ℹ️ Процес {process_id}: Не знайдено промокодів у діапазоні")
            all_codes_by_amount = {}
            for amount in range(config['start_amount'], config['end_amount'] + 1):
                all_codes_by_amount[amount] = []
        
        # Аналіз та обробка кожної суми
        for amount in range(config['start_amount'], config['end_amount'] + 1):
            code_objects = all_codes_by_amount.get(amount, [])
            
            # Розділяємо коди на активні та неактивні
            active_codes = [obj['code'] for obj in code_objects if obj['status'] == 'active']
            inactive_codes = [obj['code'] for obj in code_objects if obj['status'] == 'inactive']
            
            current_count_active = len(active_codes)
            current_count_inactive = len(inactive_codes)
            target_count = config['target_codes_per_amount']
            
            process_logger.info(f"📊 Процес {process_id}: Аналіз суми {amount} грн - {current_count_active} активних + {current_count_inactive} неактивних = {len(code_objects)} всього (цільова к-ть: {target_count})")
            
            # КРОК 1: ЗАВЖДИ видаляємо неактивні коди (незалежно від auto_delete)
            if inactive_codes:
                process_logger.info(f"🗑️ Процес {process_id}: Видаляємо {len(inactive_codes)} неактивних кодів для суми {amount} грн")
                
                # Застосовуємо фільтр по конкретній сумі для видалення неактивних
                # BON фільтр вже застосований глобально, тільки змінюємо діапазон сум
                if apply_amount_range_filter(iframe, amount, amount):
                    page = promo_service.page if hasattr(promo_service, 'page') else None
                    delete_result = delete_specific_promo_codes(iframe, inactive_codes, page)
                    deleted_inactive_count = delete_result.get('deleted', 0) if isinstance(delete_result, dict) else delete_result
                    total_operations['deleted'] += deleted_inactive_count
                    
                    process_logger.info(f"✅ Процес {process_id}: Видалено {deleted_inactive_count} неактивних кодів з {len(inactive_codes)}")
                    
                else:
                    process_logger.warning(f"⚠️ Процес {process_id}: Не вдалося застосувати фільтр для видалення неактивних кодів суми {amount} грн")
            
            # КРОК 2: Аналізуємо активні коди
            if current_count_active < target_count:
                # Потрібно створити коди
                needed = target_count - current_count_active
                process_logger.info(f"➕ Процес {process_id}: Для {amount} грн потрібно створити {needed} кодів")
                
                # Генеруємо нові коди
                new_codes = []
                for _ in range(needed):
                    # Перевіряємо, чи вже існують промокоди для цієї суми
                    existing_bon_codes = check_existing_bon_codes_for_amount(amount, active_codes + new_codes)
                    
                    # Якщо вже є достатньо існуючих кодів, не генеруємо нові
                    if len(existing_bon_codes) >= target_count:
                        process_logger.info(f"ℹ️ Процес {process_id}: Вже існує достатньо промокодів для суми {amount} грн")
                        break
                    
                    # Генеруємо новий код
                    new_code = generate_bon_code(amount)
                    new_codes.append(new_code)
                
                # Створюємо коди
                created_count = create_promo_codes(promo_service, new_codes, amount)
                total_operations['created'] += created_count
                
                # Оновлюємо дані для результату
                for code in new_codes[:created_count]:
                    active_codes.append(code)
                    
            elif current_count_active > target_count and config.get('auto_delete_excess', False):
                # Потрібно видалити зайві коди
                excess = current_count_active - target_count
                process_logger.info(f"➖ Процес {process_id}: Для {amount} грн потрібно видалити {excess} зайвих кодів")
                
                codes_to_delete = active_codes[-excess:]  # Видаляємо останні
                
                # ВАЖЛИВО: Перед видаленням застосовуємо фільтр по конкретній сумі
                # щоб коди були видимі в таблиці
                process_logger.info(f"🔍 Застосовуємо фільтр по сумі {amount} грн перед видаленням...")
                
                # BON фільтр вже застосований, тільки змінюємо діапазон сум
                if apply_amount_range_filter(iframe, amount, amount):
                    # Отримуємо page з promo_service
                    page = promo_service.page if hasattr(promo_service, 'page') else None
                    delete_result = delete_specific_promo_codes(iframe, codes_to_delete, page)
                    deleted_count = delete_result.get('deleted', 0) if isinstance(delete_result, dict) else delete_result
                    total_operations['deleted'] += deleted_count
                    
                    # Оновлюємо дані
                    active_codes = active_codes[:-deleted_count] if deleted_count > 0 else active_codes
                    
                    process_logger.info(f"✅ Видалено {deleted_count} з {excess} кодів для {amount} грн")
                else:
                    process_logger.warning(f"⚠️ Не вдалося застосувати фільтр для суми {amount} грн, пропускаємо видалення")
                
            else:
                total_operations['unchanged'] += 1
                
            # Зберігаємо результат для цієї суми
            codes_data[amount] = active_codes
        
        process_logger.info(f"✅ Процес {process_id}: Обробка завершена")
        
        return {
            'success': True,
            'codes_data': codes_data,
            'operations': total_operations
        }
        
    except Exception as e:
        process_logger.error(f"❌ Процес {process_id}: Помилка в робочій функції: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        try:
            if 'browser_manager' in locals() and browser_manager:
                if hasattr(browser_manager, 'cleanup'):
                    browser_manager.cleanup()
                elif hasattr(browser_manager, 'browser') and browser_manager.browser:
                    browser_manager.cleanup()
                elif hasattr(browser_manager, 'close'):
                    browser_manager.cleanup()
        except Exception as cleanup_error:
            process_logger.warning(f"⚠️ Помилка при закритті браузера: {cleanup_error}")

def check_existing_bon_codes_for_amount(amount, existing_codes):
    """
    Перевіряє, чи існують промокоди з заданою сумою, що починаються з 'BON'.
    
    Args:
        amount: сума промокоду
        existing_codes: список існуючих кодів
        
    Returns:
        list: список існуючих промокодів з заданою сумою
    """
    logger.debug(f"🔍 Перевірка наявності існуючих BON кодів для суми {amount} грн")
    bon_codes_for_amount = []
    for code in existing_codes:
        # Перевіряємо, чи код починається з 'BON' і чи містить правильну суму
        # if code.startswith('BON') and str(amount) in code:
        bon_codes_for_amount.append(code)
    
    logger.debug(f"✅ Знайдено {len(bon_codes_for_amount)} існуючих BON кодів для суми {amount} грн")
    return bon_codes_for_amount

def generate_bon_code(amount):
    """
    Генерує новий BON код для заданої суми.
    
    Args:
        amount: сума промокоду
        
    Returns:
        str: новий BON код
    """
    logger.debug(f"🔄 Генерація нового BON коду для суми {amount} грн")
    suffix = generate_random_string(5)
    new_code = f"BON{amount}{suffix}"
    logger.debug(f"✅ Згенеровано новий BON код: {new_code}")
    return new_code

def download_from_s3():
    """Завантажує існуючі дані з S3."""
    try:
        import boto3
        s3 = boto3.client('s3', region_name=CONFIG['region'])
        bucket = CONFIG['s3_bucket']
        key = CONFIG['s3_key']
        
        logger.info(f"📥 Завантаження існуючих промокодів з S3: s3://{bucket}/{key}")
        
        response = s3.get_object(Bucket=bucket, Key=key)
        existing_data = json.loads(response['Body'].read().decode('utf-8'))
        
        logger.info("✅ Існуючі промокоди успішно завантажено з S3")
        return existing_data
        
    except Exception as e:
        if 'NoSuchKey' in str(e):
            logger.info("ℹ️ Файл промокодів не існує в S3, створюємо новий")
            return {}
        else:
            logger.error(f"❌ Помилка при завантаженні з S3: {e}")
            logger.warning("Продовжуємо з порожньою базою промокодів")
            return {}

def save_to_local_file(data, filename=None):
    """Зберігає дані в локальний JSON файл."""
    try:
        if filename is None:
            # Створюємо ім'я файлу з timestamp
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            filename = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f'promo_codes_{timestamp}.json'
            )
        
        logger.info(f"💾 Збереження промокодів в локальний файл: {filename}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Промокоди збережено в файл: {filename}")
        return filename

    except Exception as e:
        logger.error(f"❌ Помилка при збереженні в файл: {e}")
        return None

def upload_to_s3(data, save_local=True):
    """Завантажує дані в S3 та опціонально зберігає локально."""
    # Спочатку зберігаємо локально (якщо потрібно)
    local_file = None
    if save_local:
        local_file = save_to_local_file(data)
    
    # Потім завантажуємо в S3
    try:
        import boto3
        s3 = boto3.client('s3', region_name=CONFIG['region'])
        bucket = CONFIG['s3_bucket']
        key = CONFIG['s3_key']
        
        logger.info(f"☁️ Завантаження промокодів в S3: s3://{bucket}/{key}")
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        
        logger.info("✅ Промокоди успішно завантажено в S3")
        if local_file:
            logger.info(f"📁 Також збережено локально: {local_file}")
        return True

    except Exception as e:
        logger.error(f"❌ Помилка при завантаженні в S3: {e}")
        logger.error("Перевірте ваші AWS креданшали та налаштування")
        if local_file:
            logger.info(f"✅ Але промокоди збережено локально: {local_file}")
        return False

def generate_random_string(length):
    """Генерує випадковий рядок із великих літер та цифр."""
    import random
    import string
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def manage_promo_codes():
    """
    🎯 ГОЛОВНА ФУНКЦІЯ УПРАВЛІННЯ ПРОМОКОДАМИ (ПАРАЛЕЛЬНИЙ РЕЖИМ)
    
    Запускає декілька процесів для обробки різних діапазонів сум,
    а потім збирає результати та записує їх в S3.
    """
    logger.info("🚀 ПОЧАТОК ПАРАЛЕЛЬНОГО УПРАВЛІННЯ ПРОМОКОДАМИ")
    logger.info("=" * 60)
    
    start_amount = CONFIG['start_amount']
    end_amount = CONFIG['end_amount']
    num_processes = CONFIG['parallel_processes']
    
    logger.info(f"⚙️ Конфігурація паралельної обробки:")
    logger.info(f"  📊 Діапазон сум: {start_amount}-{end_amount}")
    logger.info(f"  🔄 Кількість процесів: {num_processes}")
    logger.info(f"  ⏱️ Таймаут процесу: {CONFIG['process_timeout']} сек")
    
    # Розділяємо діапазон на частини
    ranges = split_range_for_processes(start_amount, end_amount, num_processes)
    logger.info(f"📋 Діапазони для процесів:")
    for i, (start, end) in enumerate(ranges):
        logger.info(f"  Процес {i+1}: {start}-{end} ({end-start+1} сум)")
    
    # Створюємо чергу для результатів
    result_queue = Queue()
    processes = []
    
    # Запускаємо процеси
    logger.info("🚀 Запуск паралельних процесів...")
    for i, (start, end) in enumerate(ranges):
        process_id = i + 1
        p = Process(
            target=worker_process,
            args=(process_id, start, end, result_queue)
        )
        p.start()
        processes.append(p)
        logger.info(f"  ✅ Процес {process_id} запущено")
    
    # Збираємо результати
    logger.info("⏳ Очікування завершення процесів...")
    results = []
    
    # Чекаємо результати від всіх процесів
    for i in range(len(processes)):
        try:
            result = result_queue.get(timeout=CONFIG['process_timeout'])
            results.append(result)
            logger.info(f"📦 Отримано результат від процесу {result['process_id']}")
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні результату процесу: {e}")
    
    # Завершуємо всі процеси
    for i, p in enumerate(processes):
        p.join(timeout=10)  # Даємо 10 секунд на завершення
        if p.is_alive():
            logger.warning(f"⚠️ Процес {i+1} не завершився, завершуємо примусово")
            p.terminate()
            p.join()
    
    # Аналізуємо результати
    logger.info("📊 АНАЛІЗ РЕЗУЛЬТАТІВ:")
    all_codes_data = {}
    total_operations = {'created': 0, 'deleted': 0, 'unchanged': 0}
    successful_processes = 0
    
    for result in results:
        process_id = result['process_id']
        if result['success']:
            successful_processes += 1
            logger.info(f"  ✅ Процес {process_id}: Успішно ({result['start_amount']}-{result['end_amount']})")
            
            # Збираємо дані промокодів
            all_codes_data.update(result['codes_data'])
            
            # Додаємо операції
            for op, count in result['operations'].items():
                total_operations[op] += count
        else:
            logger.error(f"  ❌ Процес {process_id}: Помилка - {result.get('error', 'Unknown error')}")
    
    logger.info(f"📈 Підсумок операцій:")
    logger.info(f"  ➕ Створено: {total_operations['created']}")
    logger.info(f"  ➖ Видалено: {total_operations['deleted']}")
    logger.info(f"  ✅ Успішних процесів: {successful_processes}/{len(processes)}")
    
    # Записуємо результати в S3 (тільки якщо є успішні процеси)
    if successful_processes > 0 and CONFIG['sync_s3']:
        logger.info("☁️ Запис результатів в S3...")
        
        # Завантажуємо існуючі дані з S3
        logger.info("📥 Завантажуємо існуючі промокоди з S3...")
        existing_s3_data = download_from_s3()
        
        # Якщо існуючих даних немає, створюємо порожній словник
        if not existing_s3_data:
            existing_s3_data = {}
        
        # Видаляємо метадані з існуючих даних, якщо вони є
        if '_metadata' in existing_s3_data:
            existing_metadata = existing_s3_data.pop('_metadata')
            logger.info(f"📋 Знайдено існуючі метадані: останнє оновлення {existing_metadata.get('last_updated', 'невідоме')}")
        
        # Готуємо дані для S3 - об'єднуємо існуючі з новими
        s3_data = existing_s3_data.copy()  # Копіюємо всі існуючі дані
        
        # Оновлюємо тільки ті діапазони, які були успішно оброблені
        logger.info("🔄 Об'єднуємо нові результати з існуючими даними...")
        updated_ranges = []
        for amount, codes in all_codes_data.items():
            s3_data[str(amount)] = codes
            updated_ranges.append(amount)
        
        if updated_ranges:
            min_updated = min(updated_ranges)
            max_updated = max(updated_ranges)
            logger.info(f"📊 Оновлено промокоди для діапазону: {min_updated}-{max_updated}")
            logger.info(f"📋 Всього оновлено сум: {len(updated_ranges)}")
        
        # Підрахуємо загальну статистику
        total_amounts_in_s3 = len([k for k in s3_data.keys() if k != '_metadata'])
        logger.info(f"📈 Загальна кількість сум з промокодами в S3: {total_amounts_in_s3}")
        
        # Додаємо метадані
        s3_data['_metadata'] = {
            'last_updated': datetime.datetime.now().isoformat(),
            'total_processes': len(processes),
            'successful_processes': successful_processes,
            'updated_ranges': updated_ranges,
            'operations': total_operations,
            'config': {
                'target_codes_per_amount': CONFIG['target_codes_per_amount'],
                'parallel_processes': CONFIG['parallel_processes']
            }
        }
        
        if upload_to_s3(s3_data):
            logger.info("✅ Результати успішно записано в S3")
        else:
            logger.error("❌ Помилка при записі в S3")
    
    logger.info("🏁 ПАРАЛЕЛЬНЕ УПРАВЛІННЯ ПРОМОКОДАМИ ЗАВЕРШЕНО")
    return successful_processes == len(processes)

if __name__ == "__main__":
    # Налаштування логування
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_filename = os.path.join(log_dir, f'promo_generator_{log_timestamp}.log')

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    logger.propagate = False
    logger.info(f"🔍 Логування налаштовано. Логи зберігаються у: {log_filename}")

    # Запуск головної функції
    try:
        # На macOS, 'spawn' є безпечнішим методом, особливо при роботі з GUI/браузерами
        multiprocessing.set_start_method('spawn', force=True)
        logger.info("🔧 Встановлено метод запуску multiprocessing: spawn (для macOS)")

        success = manage_promo_codes()
        if success:
            logger.info("✅ Управління промокодами завершено успішно.")
            sys.exit(0)
        else:
            logger.error("❌ Управління промокодами завершено з помилками.")
            sys.exit(1)
    except Exception as e:
        logger.critical(f"💥 Критична помилка в головному процесі: {e}", exc_info=True)
        sys.exit(1)

def apply_amount_filter(iframe, amount):
    """
    💰 Застосовує фільтр по сумі промокодів в адмін-панелі.
    
    ВАЖЛИВО: Спочатку очищає існуючий фільтр по сумі, а потім застосовує новий.
    
    Args:
        iframe: iframe адмін-панелі
        amount (int): сума для фільтрації
        
    Returns:
        bool: успішність застосування фільтра
    """
    try:
        logger.info(f"💰 Застосовуємо фільтр по сумі: {amount}")
        
        return apply_amount_range_filter(iframe, amount, amount)
        
    except Exception as e:
        logger.error(f"❌ Помилка при застосуванні фільтра: {e}")
        return False

def apply_amount_range_filter(iframe, start_amount, end_amount):
    """
    💰 Застосовує фільтр по діапазону сум промокодів в адмін-панелі.
    
    Args:
        iframe: iframe адмін-панелі
        start_amount (int): мінімальна сума (включно)
        end_amount (int): максимальна сума (включно)
        
    Returns:
        bool: успішність застосування фільтра
    """
    try:
        logger.info(f"💰 Застосовуємо фільтр по діапазону сум: {start_amount}-{end_amount} грн")
        
        # Використовуємо JavaScript для активації фільтра по сумі
        filter_js = f"""
        (function() {{
            try {{
                // Знаходимо заголовок "Розмір знижки" (колонка 4778)
                const amountHeader = document.querySelector('#header_id_4778');
                if (!amountHeader) {{
                    return {{ success: false, error: "Не знайдено заголовок 'Розмір знижки'" }};
                }}
                
                // Активуємо фільтр без використання hover
                const event = new MouseEvent('mouseenter', {{
                    view: window,
                    bubbles: true,
                    cancelable: true
                }});
                amountHeader.dispatchEvent(event);
                
                // Даємо час для появи блоку фільтрації
                setTimeout(() => {{
                    const filterBlock = document.querySelector('#sortingBlock_4778');
                    if (!filterBlock) {{
                        return {{ success: false, error: "Не знайдено блок фільтрації" }};
                    }}
                    
                    // Активуємо блок фільтрації
                    filterBlock.click();
                    
                    // Знаходимо поля введення
                    const fromField = filterBlock.querySelector('input[name="text1"][placeholder="від"]');
                    const toField = filterBlock.querySelector('input[name="text2"][placeholder="до"]');
                    
                    if (!fromField || !toField) {{
                        return {{ success: false, error: "Поля 'від' і 'до' не знайдено" }};
                    }}
                    
                    // Заповнюємо поля
                    fromField.value = '{start_amount}';
                    toField.value = '{end_amount}';
                    
                    // Тригеримо події введення
                    fromField.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    toField.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    
                    // Натискаємо Enter для застосування фільтра
                    const enterEvent = new KeyboardEvent('keydown', {{
                        key: 'Enter',
                        code: 'Enter',
                        keyCode: 13,
                        bubbles: true
                    }});
                    toField.dispatchEvent(enterEvent);
                    
                    return {{ success: true }};
                }}, 300);
                
                return {{ success: true }};
            }} catch (error) {{
                return {{ success: false, error: error.message }};
            }}
        }})();
        """
        
        # Виконуємо JavaScript
        result = iframe.evaluate(filter_js)
        
        if isinstance(result, dict) and not result.get('success', False):
            error_msg = result.get('error', 'Невідома помилка')
            logger.error(f"❌ Помилка при застосуванні фільтра діапазону: {error_msg}")
            return False
        
        # Очікуємо завершення фільтрації
        logger.info("⏳ Очікуємо завершення фільтрації по діапазону сум...")
        time.sleep(1.5)  # Збільшую час очікування для діапазону

        logger.info(f"✅ Фільтр по діапазону сум {start_amount}-{end_amount} успішно застосовано")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка при застосуванні фільтра діапазону: {e}")
        return False

def apply_code_filter(iframe, search_term="BON"):
    """
    🔍 Застосовує фільтр по коду промокоду в адмін-панелі.
    
    Знаходить заголовок "Код", активує пошук і фільтрує по заданому терміну
    (зазвичай "BON" для пошуку наших промокодів).
    
    Args:
        iframe: iframe адмін-панелі  
        search_term (str): термін для пошуку (за замовчуванням "BON")
        
    Returns:
        bool: успішність застосування фільтра
    """
    try:
        logger.info(f"🔍 Застосовуємо фільтр по коду: '{search_term}'...")
        
        # Використовуємо JavaScript для активації фільтра по коду
        filter_js = f"""
        (function() {{
            try {{
                // Знаходимо заголовок "Код" (колонка 4776)
                const codeHeader = document.querySelector('#header_id_4776');
                if (!codeHeader) {{
                    return {{ success: false, error: "Не знайдено заголовок 'Код'" }};
                }}
                
                // Активуємо фільтр без використання hover
                const event = new MouseEvent('mouseenter', {{
                    view: window,
                    bubbles: true,
                    cancelable: true
                }});
                codeHeader.dispatchEvent(event);
                
                // Даємо час для появи блоку фільтрації
                setTimeout(() => {{
                    const filterBlock = document.querySelector('#sortingBlock_4776');
                    if (!filterBlock) {{
                        return {{ success: false, error: "Не знайдено блок фільтрації для коду" }};
                    }}
                    
                    // Активуємо блок фільтрації
                    filterBlock.click();
                    
                    // Знаходимо поле пошуку
                    const searchField = filterBlock.querySelector('input[placeholder="пошук..."]');
                    if (!searchField) {{
                        return {{ success: false, error: "Поле пошуку не знайдено" }};
                    }}
                    
                    // Заповнюємо поле пошуку
                    searchField.value = '{search_term}';
                    
                    // Тригеримо події введення
                    searchField.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    
                    // Натискаємо Enter для застосування фільтра
                    const enterEvent = new KeyboardEvent('keydown', {{
                        key: 'Enter',
                        code: 'Enter',
                        keyCode: 13,
                        bubbles: true
                    }});
                    searchField.dispatchEvent(enterEvent);
                    
                    return {{ success: true }};
                }}, 300);
                
                return {{ success: true }};
            }} catch (error) {{
                return {{ success: false, error: error.message }};
            }}
        }})();
        """
        
        # Виконуємо JavaScript
        result = iframe.evaluate(filter_js)
        
        if isinstance(result, dict) and not result.get('success', False):
            error_msg = result.get('error', 'Невідома помилка')
            logger.error(f"❌ Помилка при застосуванні фільтра по коду: {error_msg}")
            return False
        
        # Власне очікування завершення фільтрації
        logger.info("⏳ Очікуємо завершення фільтрації по коду...")
        try:
            # Чекаємо на лоадер, якщо він з'явиться
            loader = iframe.locator('#datagrid-loader')
            try:
                loader.wait_for(state='visible', timeout=1000)  # Зменшую з 2000 на 1000
                logger.debug("Лоадер з'явився, чекаємо його зникнення...")
                loader.wait_for(state='hidden', timeout=5000)  # Зменшую з 10000 на 5000
                logger.debug("✅ Лоадер зник після фільтрації по коду")
            except Exception:
                logger.debug("Лоадер не з'явився, використовуємо networkidle")
                iframe.page.wait_for_load_state('networkidle', timeout=3000)  # Зменшую з 5000 на 3000
        except Exception as e:
            logger.warning(f"⚠️ Помилка очікування після фільтрації по коду: {e}")
        
        logger.info(f"✅ Фільтр по коду '{search_term}' успішно застосовано")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка при застосуванні фільтра по коду: {e}")
        return False


def apply_bon_filter_once(iframe):
    """
    Застосовує фільтр по коду 'BON' один раз для всіх сум.
    
    ВАЖЛИВО: Цей фільтр застосовується ОДИН РАЗ перед початком обробки всіх сум.
    Після застосування цього фільтра, виконується повна пагінація для отримання
    всіх BON промокодів з усіх сторінок.
    """
    try:
        logger.info("🔍 Застосовуємо фільтр по коду 'BON' (один раз для всіх сторінок)...")
        
        # Спочатку скидаємо всі фільтри
        clear_all_filters(iframe)
        
        # Застосовуємо фільтр по BON
        success = apply_code_filter(iframe, "BON")
        
        if success:
            logger.info("✅ Фільтр BON успішно застосовано, тепер можна збирати дані з усіх сторінок")
        else:
            logger.error("❌ Не вдалося застосувати фільтр BON")
            
        return success
    except Exception as e:
        logger.error(f"❌ Помилка при застосуванні BON фільтра: {e}")
        return False

def clear_all_filters(iframe):
    """
    Скидає всі активні фільтри в таблиці.
    """
    try:
        logger.info("🧹 Скидаємо всі фільтри...")
        
        # Пробуємо знайти кнопку скидання всіх фільтрів
        reset_all_button = iframe.locator('#reset_datagrid')
        if reset_all_button.count() > 0:
            logger.info("🔄 Знайдено кнопку скидання фільтрів, натискаємо...")
            reset_all_button.click()
            time.sleep(1)
            return True
            
        # Альтернативний варіант - перезавантажити сторінку
        logger.info("🔄 Перезавантажуємо таблицю для скидання фільтрів...")
        iframe.evaluate('document.location.reload()')
        time.sleep(2)
        
        # Чекаємо завершення завантаження
        try:
            loader = iframe.locator('#datagrid-loader')
            loader.wait_for(state='hidden', timeout=5000)
        except Exception:
            iframe.page.wait_for_load_state('networkidle', timeout=3000)
            
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ Помилка скидання фільтрів: {e}")
        return False

def get_codes_from_current_page(
    iframe, 
    all_codes_set: Optional[Set[str]] = None, 
    duplicates_info: Optional[Dict[str, List[int]]] = None, 
    sort_order: str = "asc"
) -> Dict[int, List[Dict[str, Any]]]:
    """
    Оптимізована функція для збору всіх BON кодів з поточної сторінки таблиці.
    Використовує evaluate_all для мінімізації запитів до браузера.
    Зберігає всю логіку оригіналу: відстеження дублікатів, сортування.

    Args:
        iframe: Playwright iframe об'єкт.
        all_codes_set: (опціонально) множина для відстеження унікальних кодів.
        duplicates_info: (опціонально) словник для збору інформації про дублікати.
        sort_order: Порядок сортування (для логування).

    Returns:
        Dict[int, List[Dict[str, Any]]]: Словник, де ключ - сума, 
        значення - список об'єктів {'code': str, 'status': str, 'amount': int}.
        Неактивні коди додаються на початок списку, активні - в кінець.
    """
    
    # Ініціалізуємо локальні структури даних, якщо вони не передані
    if all_codes_set is None:
        all_codes_set_local = set()
        using_external_set = False
    else:
        all_codes_set_local = all_codes_set
        using_external_set = True
        
    if duplicates_info is None:
        duplicates_info_local = {}
        using_external_duplicates = False
    else:
        duplicates_info_local = duplicates_info
        using_external_duplicates = True

    codes_by_amount = {}
    page_codes_count = 0
    
    try:
        logger.debug("🚀 Починаємо оптимізований збір даних з таблиці...")
        
        # --- Основна оптимізація: отримуємо всі дані за один запит ---
        # Селектор для всіх видимих рядків тіла таблиці, що містять дані
        rows_selector = 'table#datagrid tbody tr:visible:not(.no-data)'

        # Використовуємо evaluate_all для отримання даних з усіх рядків одразу
        rows_data = iframe.locator(rows_selector).evaluate_all("""
        rows => rows.map(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            
            // Мінімальна кількість комірок для валідного рядка
            if (cells.length < 5) { 
                return null; 
            }
            
            // Індекси комірок (можуть відрізнятися, потрібно адаптувати):
            // 0 - ID, 1 - Назва, 2 - Статус, 3 - Код, 4 - Сума
            const statusTextRaw = cells[2]?.innerText?.trim() || '';
            const codeTextRaw = cells[3]?.innerText?.trim() || '';
            const amountTextRaw = cells[4]?.innerText?.trim() || '';
            
            return {
                statusText: statusTextRaw,
                codeText: codeTextRaw,
                amountText: amountTextRaw
            };
        }).filter(Boolean) // Видаляємо null значення (невалідні рядки)
        """)
        
        logger.debug(f"📥 Отримано дані з {len(rows_data)} потенційних рядків таблиці.")

        # --- Обробляємо отримані дані в Python ---
        for i, row_data in enumerate(rows_data):
            try:
                status_text_raw = row_data['statusText']
                code_raw = row_data['codeText']
                amount_text_raw = row_data['amountText']

                # --- Обробка коду ---
                if not code_raw:
                    logger.debug(f"Пропущено рядок {i+1}: порожній код.")
                    continue
                
                code = code_raw.strip()
                
                # Додаткове логування для діагностики формату кодів
                logger.debug(f"Аналіз коду: '{code}' (початок: '{code[:10]}'...)")
                
                # Перевіряємо, чи код починається з 'BON' або має інший формат
                if not code.startswith('BON'):
                    logger.info(f"⚠️ Код не починається з 'BON': '{code}'")
                    # Продовжуємо обробку, але позначаємо це в логах
                    # Можливо, це існуючі коди з іншим форматом

                # --- Обробка статусу ---
                status_text_normalized = status_text_raw.strip().lower()
                
                # Розширена логіка розпізнавання статусів
                active_keywords = ['так', 'yes', 'активний', 'active', 'активен', 'true', '1', 'включен', 'enabled']
                inactive_keywords = ['ні', 'no', 'неактивний', 'inactive', 'неактивен', 'false', '0', 'выключен', 'disabled', 'вимкнено', 'вимкнений']
                
                is_active = None
                
                # Спочатку перевіряємо точні збіги
                if status_text_normalized in active_keywords:
                    is_active = True
                elif status_text_normalized in inactive_keywords:
                    is_active = False
                else:
                    # Якщо точного збігу немає, шукаємо часткові збіги
                    for keyword in active_keywords:
                        if keyword in status_text_normalized:
                            is_active = True
                            break
                    
                    if is_active is None:
                        for keyword in inactive_keywords:
                            if keyword in status_text_normalized:
                                is_active = False
                                break
                
                # Якщо все ще не визначили статус, логуємо для діагностики
                if is_active is None:
                    logger.warning(f"⚠️ Не вдалося розпізнати статус для коду {code}: '{status_text_raw}' -> '{status_text_normalized}'")
                    # За замовчуванням вважаємо неактивним для безпеки
                    is_active = False
                
                status_str = 'active' if is_active else 'inactive'
                
                # Додаткове логування для діагностики
                if not is_active:
                    logger.info(f"🔍 Знайдено НЕАКТИВНИЙ код: {code} (статус: '{status_text_raw}')")
                else:
                    logger.debug(f"🔍 Знайдено активний код: {code} (статус: '{status_text_raw}')")

                # --- Обробка суми ---
                amount_match = re.search(r'(\d+)', amount_text_raw)
                if not amount_match:
                    logger.debug(f" ⚠️ НЕ РОЗПІЗНАНО СУМУ: для коду {code} (колонка: '{amount_text_raw}')")
                    continue
                
                amount = int(amount_match.group(1))

                # --- Логіка відстеження дублікатів (як в оригіналі) ---
                is_duplicate = False
                if code in all_codes_set_local:
                    is_duplicate = True
                    if code not in duplicates_info_local:
                        # Знаходимо оригінальну суму цього коду з вже зібраних даних
                        original_amount = None
                        for check_amount, check_code_objects in codes_by_amount.items():
                            # Шукаємо код серед об'єктів
                            if any(obj['code'] == code for obj in check_code_objects):
                                original_amount = check_amount
                                break
                        
                        if original_amount is not None:
                            duplicates_info_local[code] = [original_amount]
                        else:
                            # Якщо не знайшли (теоретично можливо при одночасному доступі),
                            # додаємо поточну суму як першу
                            duplicates_info_local[code] = []

                    # Додаємо поточну суму до списку сум для цього коду
                    if amount not in duplicates_info_local[code]:
                        duplicates_info_local[code].append(amount)
                    
                    # Логування дубліката
                    if len(set(duplicates_info_local[code])) > 1:
                        logger.error(f"❌ КРИТИЧНИЙ ДУБЛІКАТ: {code} має РІЗНІ СУМИ: {sorted(list(set(duplicates_info_local[code])))} грн! Поточна: {amount} грн")

                # --- Додаємо код до множини унікальних (якщо використовуємо локальну) ---
                if not using_external_set:
                    all_codes_set_local.add(code)

                # --- Додаємо код до основного словника з сортуванням ---
                if amount not in codes_by_amount:
                    codes_by_amount[amount] = []
                
                code_obj = {
                    'code': code,
                    'status': status_str,
                    'amount': amount
                }

                # Сортуємо: неактивні коди першими, потім активні
                # (як в оригіналі: insert(0) для неактивних, append для активних)
                if not is_active:
                    codes_by_amount[amount].insert(0, code_obj) # Неактивні на початок
                else:
                    codes_by_amount[amount].append(code_obj) # Активні в кінець

                page_codes_count += 1

            except (ValueError, IndexError, KeyError) as e:
                logger.debug(f"Помилка обробки рядка {i}: {e}")
                continue # Продовжуємо з наступним рядком

        # Детальна статистика по статусах
        total_active = 0
        total_inactive = 0
        for amount_codes in codes_by_amount.values():
            for code_obj in amount_codes:
                if code_obj['status'] == 'active':
                    total_active += 1
                else:
                    total_inactive += 1

        logger.info(f"✅ Сторінку оброблено. Знайдено {page_codes_count} BON кодів:")
        logger.info(f"   🟢 Активних: {total_active}")
        logger.info(f"   🔴 Неактивних: {total_inactive}")
        
        # Повертаємо коди, знайдені на цій конкретній сторінці
        return codes_by_amount

    except Exception as e:
        logger.error(f"❌ Критична помилка в get_all_bon_codes_from_table_optimized: {e}", exc_info=True)
        # У випадку критичної помилки повертаємо порожній результат
        return {}
    
def get_all_bon_codes_with_pagination(iframe) -> Dict[int, List[Dict[str, Any]]]:
    """
    Отримує всі BON коди з поточної таблиці (після застосування фільтрів),
    обробляючи всі сторінки пагінації.
    Використовує оптимізовану функцію збору даних з однієї сторінки.
    Зберігає всю логіку оригіналу: пагінація, очікування, обробка помилок.

    Args:
        iframe: Елемент iframe таблиці промокодів

    Returns:
        dict: {сума: [список_об'єктів_кодів]} - коди зі статусами
    """
    # Ініціалізуємо codes_by_amount на початку для уникнення помилок
    codes_by_amount = {}
    duplicates_info = {} # Ініціалізуємо тут, щоб була доступна в finally

    try:
        logger.info("📋 ПОЧАТОК збору всіх BON кодів з відфільтрованої таблиці (з усіх сторінок)...")

        # Перевіряємо поточні налаштування таблиці
        pager_text = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
        logger.info(f"📄 Інформація про пагінацію: {pager_text}")

        # Зменшена пауза для стабілізації в HEADED режимі
        headed_mode = os.getenv('PLAYWRIGHT_HEADED', 'True').lower() in ['true', '1', 'yes']
        if headed_mode:
            logger.info("🖥️ HEADED режим: пауза 0.5 сек для стабілізації...")
            time.sleep(0.5) # Зменшую з 2 сек до 0.5 сек

        # Використовуємо Set для запобігання дублікатів
        all_codes_set: Set[str] = set() # Для відстеження всіх унікальних кодів
        current_page = 1
        has_next_page = True
        total_processed_rows = 0

        while has_next_page:
            logger.info(f"📄 Обробка сторінки {current_page}...")

            # --- Виклик ОПТИМІЗОВАНОЇ функції для поточної сторінки ---
            page_codes = get_codes_from_current_page(
                iframe, 
                all_codes_set=all_codes_set, 
                duplicates_info=duplicates_info, 
                sort_order="asc" # або передавати ззовні, якщо потрібно
            )
            page_codes_count = sum(len(codes) for codes in page_codes.values())
            total_processed_rows += page_codes_count

            # --- Об'єднуємо результати з поточної сторінки в загальний словник ---
            for amount, code_objects in page_codes.items():
                if amount not in codes_by_amount:
                    codes_by_amount[amount] = []
                # Додаємо коди, зберігаючи їхню відсортовану черговість (неактивні спочатку)
                codes_by_amount[amount].extend(code_objects)

            # --- Логування стану сторінки ---
            logger.info(f" 💰 Сторінка {current_page}: оброблено {page_codes_count} кодів")
            for amount, codes in page_codes.items():
                count = len(codes)
                active_count = sum(1 for obj in codes if obj['status'] == 'active')
                inactive_count = count - active_count
                logger.info(f" 💰 {amount} грн: {active_count} активних + {inactive_count} неактивних = {count} всього")

            logger.info(f"📝 Загальна кількість унікальних кодів після сторінки {current_page}: {len(all_codes_set)}")

            # --- Перевіряємо наявність наступної сторінки ---
            # Оновлений код для знаходження стрілки "вправо" для пагінації
            next_page_button = iframe.locator('.datagrid-pager .fl-l.r.active').first
            has_next_page = next_page_button.count() > 0

            # Виводимо інформацію про пагінацію
            if has_next_page:
                logger.info(f"📑 Є наступна сторінка після {current_page}")
            else:
                logger.info(f"📑 Це остання сторінка ({current_page})")

            if has_next_page:
                # --- Перед переходом зберігаємо інформацію про поточну сторінку для порівняння ---
                previous_page_info = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
                logger.info(f"📑 Поточний стан пагінації перед переходом: {previous_page_info}")

                # Для статистики рахуємо кількість кодів до переходу
                previous_all_codes_count = len(all_codes_set)
                logger.info(f"📊 Кількість унікальних кодів перед переходом: {previous_all_codes_count}")

                # --- Клік на кнопку наступної сторінки ---
                try:
                    next_page_button.click()
                    logger.info("➡️ Клік по кнопці 'наступна сторінка'...")

                    # --- Очікування завантаження нової сторінки ---
                    try:
                        # Очікуємо, поки loader зникне
                        loader = iframe.locator('#datagrid-loader')
                        loader.wait_for(state='hidden', timeout=5000)
                        logger.info("✅ Лоадер прихований, сторінка завантажена")
                    except Exception:
                        logger.info("📊 Лоадер не знайдено, використовуємо альтернативний метод очікування")
                        # Альтернативний метод - чекаємо на стабілізацію мережевої активності
                        iframe.page.wait_for_load_state('networkidle', timeout=3000)

                    # --- Перевіряємо, що сторінка дійсно змінилася ---
                    new_page_info = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
                    logger.info(f"📑 Поточний стан пагінації після переходу: {new_page_info}")

                    # Порівнюємо інформацію про сторінку до і після переходу
                    if previous_page_info == new_page_info:
                        logger.warning(f"⚠️ ПРОБЛЕМА ПАГІНАЦІЇ! Інформація про сторінку не змінилася після переходу!")
                        # Спробуємо ще раз
                        logger.info(f"🔄 Спроба повторного кліку на кнопку наступної сторінки...")
                        if next_page_button.count() > 0:
                            next_page_button.click()
                            time.sleep(2) # Збільшимо затримку для надійності
                        else:
                            logger.error(f"❌ Кнопка наступної сторінки зникла при повторній спробі!")
                            break # Виходимо з циклу, якщо кнопка зникла

                except Exception as click_error:
                    logger.error(f"❌ Помилка при переході на наступну сторінку: {click_error}")
                    break # Виходимо з циклу в разі критичної помилки

                current_page += 1
            # --- Кінець блоку переходу на наступну сторінку ---

        # --- Підсумок збору ---
        logger.info("✅ ЗБІР ВСІХ СТОРІНОК ЗАВЕРШЕНО!")
        logger.info(f"📊 ПІДСУМОК: Загалом знайдено {len(all_codes_set)} унікальних BON промокодів")
        total_codes = sum(len(codes) for codes in codes_by_amount.values())
        logger.info(f"📊 ПІДСУМОК: Загалом оброблено {total_codes} BON промокодів (включаючи активні та неактивні)")
        logger.info(f"📊 ПІДСУМОК: Загалом оброблено {total_processed_rows} рядків")
        logger.info(f"📚 ПІДСУМОК: Загалом оброблено {current_page} сторінок пагінації")

        # Детальна статистика по активних/неактивних кодах
        total_active_final = 0
        total_inactive_final = 0
        amounts_with_codes = []
        
        for amount, code_objects in codes_by_amount.items():
            active_count = sum(1 for obj in code_objects if obj['status'] == 'active')
            inactive_count = len(code_objects) - active_count
            total_active_final += active_count
            total_inactive_final += inactive_count
            
            if code_objects:  # Якщо є коди для цієї суми
                amounts_with_codes.append(amount)
                logger.info(f"💰 Сума {amount} грн: {active_count} активних + {inactive_count} неактивних = {len(code_objects)} всього")
        
        logger.info(f"📊 ФІНАЛЬНА СТАТИСТИКА:")
        logger.info(f"   🟢 Активних промокодів: {total_active_final}")
        logger.info(f"   🔴 Неактивних промокодів: {total_inactive_final}")
        logger.info(f"   💰 Сум з промокодами: {len(amounts_with_codes)}")
        
        if amounts_with_codes:
            logger.info(f"   💰 Діапазон сум: {min(amounts_with_codes)}-{max(amounts_with_codes)} грн")

        # --- ВАЛІДАЦІЯ ДУБЛІКАТІВ ---
        if duplicates_info:
            logger.info(f"🔍 Виявлено {len(duplicates_info)} унікальних кодів з дублікатами!")
            for code, amounts in duplicates_info.items():
                if len(set(amounts)) > 1: # Перевірка на справжній дублікат
                    logger.error(f"❌ КРИТИЧНИЙ ДУБЛІКАТ: {code} має РІЗНІ СУМИ: {sorted(list(set(amounts)))} грн!")
                else:
                    # Можливо, це просто повторна зустріч того ж коду-суми
                    logger.debug(f"🔍 Повторний код (можливо дублікат): {code} для суми {amounts[0]} грн")
            logger.info("✅ ОБРОБКА ДУБЛІКАТІВ ЗАВЕРШЕНА!")
        else:
            logger.info("✅ Дублікатів під час збору не виявлено")

        return codes_by_amount

    except Exception as e:
        logger.error(f"❌ Помилка при зборі BON кодів: {e}")
        return {}
    finally:
        # Виконуємо ВАЛІДАЦІЮ ДУБЛІКАТІВ НАВІТЬ ЯКЩО БУЛА ПОМИЛКА
        # (якщо codes_by_amount не порожній)
        try:
            if codes_by_amount: # Тепер codes_by_amount завжди визначений
                logger.info("🔍 Перевірка дублікатів після збору всіх кодів...")
                # Передаємо інформацію про дублікати, якщо вона є
                if duplicates_info:
                    logger.debug(f"📊 Передаємо інформацію про {len(duplicates_info)} дублікатів до функції валідації")
                # --- Виклик функції валідації (якщо вона є) ---
                # from ... import validate_duplicates_after_collection
                # codes_by_amount = validate_duplicates_after_collection(codes_by_amount, duplicates_info, iframe)
                # logger.debug("🔍 Функція validate_duplicates_after_collection() завершена")
        except Exception as validation_error:
            logger.error(f"❌ Помилка при валідації дублікатів: {validation_error}")
        # Завжди повертаємо codes_by_amount (навіть якщо порожній)
        # return codes_by_amount # Не повертаємо тут, тому що return буде в try блоці

# Створюємо псевдонім для зворотної сумісності
get_all_bon_codes_from_table = get_all_bon_codes_with_pagination

def create_promo_codes(promo_service, codes_list, amount):
    """
    Створює промокоди в адмін-панелі для конкретної суми.
    Має механізм відновлення у випадку зависання.
    
    Args:
        promo_service: Сервіс роботи з промокодами
        codes_list (list): Список промокодів для створення
        amount (int): Сума промокоду
        
    Returns:
        int: Кількість успішно створених промокодів
    """
    logger.info(f"🏗️ Створення {len(codes_list)} промокодів для суми {amount} грн...")
    
    created_count = 0
    total_codes = len(codes_list)
    
    for i, code in enumerate(codes_list, 1):
        logger.info(f"🎯 ({i}/{total_codes}) Створюємо код: {code}")
        
        try:
            # Встановлюємо таймаут для операції створення
            # Playwright TimeoutError буде перехоплено, якщо операція займе більше 60 секунд
            success = promo_service.create_promo_code(code, amount)
            
            if success:
                created_count += 1
                logger.info(f"  ✅ Код {code} створено успішно!")
            else:
                logger.warning(f"  ❌ Не вдалося створити {code}")

        except Exception as e: # Перехоплюємо будь-яку помилку, включаючи TimeoutError
            logger.error(f"🚨 Помилка або зависання при створенні коду {code}: {e}")
            logger.info("🔄 Спроба оновити сторінку для відновлення...")
            try:
                promo_service.page.reload(wait_until='domcontentloaded', timeout=30000)
                logger.info("✅ Сторінку успішно оновлено. Пропускаємо поточний код і продовжуємо.")
                # Після оновлення сторінки, можливо, знадобиться повторна навігація
                # до розділу промокодів, але PromoService повинен це обробити.
                # Продовжуємо з наступним кодом.
                continue
            except Exception as reload_e:
                logger.error(f"❌ Не вдалося оновити сторінку після зависання: {reload_e}")
                logger.error("🛑 Перериваємо створення промокодів для поточної суми.")
                break # Виходимо з циклу для цієї суми
    
    success_rate = (created_count / total_codes) * 100 if total_codes > 0 else 0
    logger.info(f"📊 Результат створення: {created_count}/{total_codes} ({success_rate:.1f}%)")
    
    return created_count

def set_table_rows_per_page(iframe, rows_count=160):
    """
    Налаштовує максимальну кількість рядків на сторінці для швидшого збору даних.
    
    Args:
        iframe: iframe адмін-панелі
        rows_count (int): Бажана кількість рядків (за замовчуванням 160)
        
    Returns:
        bool: Успішність налаштування
    """
    try:
        logger.info(f"⚙️ Налаштування кількості рядків на сторінці: {rows_count}...")
        
        # Знаходимо кнопку налаштувань (три крапки)
        settings_button = iframe.locator('.button.settings')
        if not settings_button.count():
            logger.warning("⚠️ Не знайдено кнопку налаштувань")
            return False
        
        # Клікаємо на кнопку налаштувань
        settings_button.click()
        time.sleep(0.5)
        
        # Шукаємо випадаючий список з кількістю рядків
        rows_select = iframe.locator('#datagrid-perpage-select')
        if not rows_select.count():
            logger.warning("⚠️ Не знайдено селект з кількістю рядків")
            settings_button.click()  # закриваємо налаштування
            return False
        
        # Вибираємо максимальну кількість рядків
        rows_select.select_option(value=str(rows_count))
        time.sleep(1.0)  # Чекаємо, поки таблиця перезавантажиться
        
        # Перевіряємо, чи застосувалися зміни
        try:
            # Чекаємо на лоадер, якщо він з'явиться
            loader = iframe.locator('#datagrid-loader')
            try:
                loader.wait_for(state='hidden', timeout=5000)
            except Exception:
                # Якщо лоадер не з'явився, чекаємо завершення мережевої активності
                iframe.page.wait_for_load_state('networkidle', timeout=3000)
        except Exception as e:
            logger.warning(f"⚠️ Помилка очікування після зміни кількості рядків: {e}")
        
        logger.info(f"✅ Кількість рядків на сторінці встановлено: {rows_count}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка при налаштуванні кількості рядків: {e}")
        return False

def sort_table_by_amount(iframe, sort_order="asc"):
    """
    Сортує таблицю в адмін-панелі по розміру знижки (кліком на заголовок колонки).
    Це допоможе покращити структуру даних та зменшити ймовірність дублікатів.
    
    Args:
        iframe: iframe адмін-панелі
        sort_order: порядок сортування ("asc" для зростання, "desc" для спадання)
        
    Returns:
        bool: успішність операції сортування
    """
    try:
        logger.info("🔄 Сортування таблиці по розміру знижки...")
        
        # Зберігаємо поточний стан пагінації для порівняння
        pager_text = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
        logger.info(f"📄 Поточний стан пагінації перед сортуванням: {pager_text}")
        
        # 1. Знаходимо заголовок "Розмір знижки" (колонка 4778)
        amount_header = iframe.locator('#header_id_4778')
        if not amount_header.count():
            logger.error("❌ Не знайдено заголовок 'Розмір знижки' для сортування")
            return False

        logger.info("🖱️ Клікаємо на заголовок 'Розмір знижки' для сортування...")
        
        
        # 2. Клікаємо на заголовок для сортування
        # Для налаштування порядку сортування (зростання/спадання):
        # - для зростання — один клік (якщо ще не встановлено)
        # - для спадання — два кліки (один для зростання, другий для спадання)
        
        # Спочатку перевіряємо поточний стан сортування
        sort_indicator = amount_header.locator('.sort-indicator')
        current_sort_state = "none"  # none, asc, desc
        
        if sort_indicator.count() > 0:
            sort_class = sort_indicator.get_attribute('class') or ""
            if 'sort-up' in sort_class:
                current_sort_state = "asc"
            elif 'sort-down' in sort_class:
                current_sort_state = "desc"
        
        logger.info(f"� Поточний стан сортування: {current_sort_state}, потрібний: {sort_order}")
        
        # Визначаємо кількість кліків потрібних для досягнення бажаного порядку
        clicks_needed = 0
        if sort_order == "asc":
            if current_sort_state == "none":
                clicks_needed = 1  # none -> asc
            elif current_sort_state == "desc":
                clicks_needed = 2  # desc -> none -> asc
            # якщо вже asc - нічого не робимо
        elif sort_order == "desc":
            if current_sort_state == "none":
                clicks_needed = 2  # none -> asc -> desc
            elif current_sort_state == "asc":
                clicks_needed = 1  # asc -> desc
            # якщо вже desc - нічого не робимо
        
        logger.info(f"🖱️ Потрібно виконати {clicks_needed} кліків для встановлення сортування {sort_order}")
        
        # Виконуємо потрібну кількість кліків
        for click_num in range(clicks_needed):

            # Зберігаємо початковий стан першого рядка для перевірки змін
            try:
                initial_first_row = iframe.locator('table#datagrid tbody tr').first
                initial_row_text = ""
                if initial_first_row.count() > 0:
                    initial_row_text = initial_first_row.inner_text()
            except Exception:
                initial_row_text = ""
            logger.info(f"👆 Клік {click_num + 1}/{clicks_needed} на заголовок...")
            amount_header.click()
            time.sleep(0.2)  # Коротка пауза між кліками
        
            # 3. Чекаємо на оновлення таблиці після сортування
            logger.info("⏳ Очікуємо завершення сортування...")
            
            # Спочатку чекаємо появу лоадера, якщо він є
            try:
                loader = iframe.locator('#datagrid-loader')
                if loader.count() > 0:
                    logger.info("🔄 Чекаємо появу лоадера...")
                    loader.wait_for(state='visible', timeout=2000)
                    logger.info("🔄 Лоадер з'явився, чекаємо його зникнення...")
                    loader.wait_for(state='hidden', timeout=8000)
                    logger.info("✅ Лоадер зник - сортування завершено")
                else:
                    logger.info("📊 Лоадер не знайдено, використовуємо альтернативний метод очікування")
                    # Альтернативний метод - чекаємо на стабілізацію мережевої активності
                    iframe.page.wait_for_load_state('networkidle', timeout=5000)
                    logger.info("✅ Мережева активність стабілізувалась")
            except Exception as loader_error:
                logger.info(f"⚠️ Не вдалося відстежити лоадер: {loader_error}")
                
                # Резервний метод - перевірка змін вмісту таблиці
                logger.info("🔄 Використовуємо резервний метод очікування...")
                sorting_completed = False
                max_attempts = 30  # Збільшуємо кількість спроб (3 секунди)
                
                for attempt in range(max_attempts):
                    try:
                        time.sleep(0.1)  # Коротка пауза між перевірками
                        
                        # Перевіряємо, чи змінився вміст першого рядка (індикатор сортування)
                        current_first_row = iframe.locator('table#datagrid tbody tr').first
                        if current_first_row.count() > 0:
                            current_row_text = current_first_row.inner_text()
                            
                            # Якщо вміст змінився або минуло достатньо часу
                            if current_row_text != initial_row_text or attempt >= 15:
                                # Додаткова перевірка на стабільність (три рази підряд однаковий результат)
                                time.sleep(0.3)
                                stable_check1 = current_first_row.inner_text() if current_first_row.count() > 0 else ""
                                time.sleep(0.2)
                                stable_check2 = current_first_row.inner_text() if current_first_row.count() > 0 else ""
                                
                                if stable_check1 == stable_check2 == current_row_text:
                                    sorting_completed = True
                                    logger.info(f"✅ Сортування завершено після {attempt + 1} спроб ({(attempt + 1) * 0.1:.1f} сек)")
                                    break
                                    
                    except Exception as wait_error:
                        logger.debug(f"Помилка при перевірці стану таблиці (спроба {attempt + 1}): {wait_error}")
                        continue
                
                if not sorting_completed:
                    logger.warning("⚠️ Не вдалося точно визначити завершення сортування, додаємо фінальну паузу")
                    # Фінальна пауза для надійності
                    time.sleep(1.5)  # Збільшуємо час очікування
        
        # Перевіряємо стан таблиці після сортування
        new_pager_text = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
        logger.info(f"📄 Стан пагінації після сортування: {new_pager_text}")
        
        # Перевіряємо, чи змінився стан пагінації
        if pager_text == new_pager_text:
            logger.info("✅ Стан пагінації не змінився, сортування не вплинуло на розбивку сторінок")
        else:
            logger.info("✅ Стан пагінації змінився після сортування, дані перегруповані")
        
        # Перевірка напряму сортування (за зростанням чи за спаданням)
        sort_indicator = amount_header.locator('.sort-indicator')
        if sort_indicator.count() > 0:
            sort_class = sort_indicator.get_attribute('class')
            actual_sort_state = "none"
            
            if 'sort-down' in sort_class:
                actual_sort_state = "desc"
                logger.info("📉 Встановлено сортування за спаданням (від більших сум до менших)")
            elif 'sort-up' in sort_class:
                actual_sort_state = "asc"
                logger.info("📈 Встановлено сортування за зростанням (від менших сум до більших)")
            else:
                logger.info("🔄 Індикатор сортування знайдено, але напрям не визначено")
            
            # Перевіряємо, чи досягнуто потрібний порядок сортування
            if actual_sort_state == sort_order:
                logger.info(f"✅ Сортування встановлено правильно: {sort_order}")
            else:
                logger.warning(f"⚠️ Очікувалось {sort_order}, але встановлено {actual_sort_state}")
        else:
            logger.warning("⚠️ Індикатор сортування не знайдено")
        
        logger.info("✅ Сортування по розміру знижки виконано успішно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка при сортуванні таблиці по розміру знижки: {e}")
        return False

def delete_all_promo_codes_on_page(iframe, page=None):
    """
    Видаляє всі промокоди на поточній сторінці, використовуючи головний чекбокс для вибору.
    
    Args:
        iframe: iframe адмін-панелі
        page: основна сторінка (для обробки діалогів підтвердження)
        
    Returns:
        dict: результат операції з кількістю видалених кодів
    """
    try:
        logger.info("🗑️ Видалення всіх промокодів на сторінці...")
        
        # Перевіряємо стан таблиці
        try:
            all_rows = iframe.locator('table#datagrid tbody tr')
            total_rows = all_rows.count()
            logger.info(f"📊 Всього рядків в таблиці: {total_rows}")
            
            if total_rows == 0:
                logger.warning("⚠️ Таблиця порожня - неможливо видалити жодного промокоду")
                return {"selected": 0, "deleted": 0, "success": False}
                
        except Exception as e:
            logger.warning(f"⚠️ Помилка при перевірці таблиці: {e}")
        
        # Вибираємо всі промокоди через головний чекбокс
        # Використовуємо більш конкретний селектор, оскільки є два елементи з id="datagridMasterCheckBox"
        master_checkbox = iframe.locator('#header_id_10005 #datagridMasterCheckBox')
        if master_checkbox.count() > 0:
            try:
                # Клікаємо на головний чекбокс для вибору всіх
                master_checkbox.click()
                time.sleep(0.5)  # Даємо час для оновлення інтерфейсу
                
                # Перевіряємо, чи всі чекбокси вибрані
                selected_count = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
                logger.info(f"✅ Вибрано {selected_count} промокодів через головний чекбокс")
                
                if selected_count == 0:
                    logger.warning("❌ Не вдалося вибрати жодного промокоду")
                    return {"selected": 0, "deleted": 0, "success": False}
                    
            except Exception as e:
                logger.error(f"❌ Помилка при виборі всіх промокодів: {e}")
                return {"selected": 0, "deleted": 0, "success": False}
        else:
            logger.error("❌ Не знайдено головний чекбокс для вибору всіх промокодів")
            return {"selected": 0, "deleted": 0, "success": False}
        
        # Видаляємо вибрані промокоди
        # Перевіряємо режим браузера для вибору оптимальної стратегії
        headed_mode = os.getenv('PLAYWRIGHT_HEADED', 'true').lower() in ['true', '1', 'yes']
        
        if headed_mode:
            logger.info("🖥️ HEADED режим: використовуємо стандартну функцію видалення")
            delete_success = delete_selected_codes(iframe, page)
        else:
            logger.info("👻 HEADLESS режим: використовуємо оптимізовану функцію видалення")
            delete_success = delete_selected_codes_headless_optimized(iframe, page)
        
        if delete_success:
            logger.info(f"✅ Успішно видалено {selected_count} промокодів!")
            return {"selected": selected_count, "deleted": selected_count, "success": True}
        else:
            logger.error("❌ Помилка при видаленні вибраних промокодів")
            # Рахуємо, скільки реально було видалено
            final_checked_count = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
            actually_deleted = selected_count - final_checked_count
            return {"selected": selected_count, "deleted": actually_deleted, "success": False}
        
    except Exception as e:
        logger.error(f"❌ Критична помилка при видаленні всіх промокодів на сторінці: {e}")
        return {"selected": 0, "deleted": 0, "success": False, "error": str(e)}

def delete_all_promo_codes_in_range(iframe, start_amount, end_amount, process_logger, page=None):
    """
    Видаляє всі промокоди в заданому діапазоні сум з урахуванням пагінації.
    Після видалення кодів з першої сторінки, інші коди автоматично переходять на першу сторінку.
    
    Args:
        iframe: iframe адмін-панелі
        start_amount (int): початкова сума діапазону
        end_amount (int): кінцева сума діапазону
        process_logger: логгер процесу
        page: основна сторінка (для обробки діалогів підтвердження)
        
    Returns:
        int: кількість видалених промокодів
    """
    total_deleted = 0
    
    try:
        process_logger.info(f"🗑️ Видалення всіх промокодів в діапазоні {start_amount}-{end_amount} грн")
        
        # Продовжуємо видаляти, поки на сторінці є промокоди
        iteration = 0
        max_iterations = 1000  # Захист від нескінченного циклу
        
        while iteration < max_iterations:
            iteration += 1
            
            # Отримуємо коди з поточної сторінки
            page_codes = get_codes_from_current_page(iframe)
            
            # Збираємо всі коди для видалення
            codes_to_delete = []
            for amount, code_objects in page_codes.items():
                for obj in code_objects:
                    codes_to_delete.append(obj['code'])
            
            if not codes_to_delete:
                process_logger.info("✅ Всі промокоди в діапазоні видалено")
                break
            
            process_logger.info(f"🗑️ Ітерація {iteration}: знайдено {len(codes_to_delete)} промокодів для видалення")
            
            # Видаляємо промокоди
            # Використовуємо оптимізований метод видалення всіх кодів одразу
            delete_result = delete_all_promo_codes_on_page(iframe, page)
            deleted_count = delete_result.get('deleted', 0) if isinstance(delete_result, dict) else delete_result
            total_deleted += deleted_count
            
            process_logger.info(f"✅ Ітерація {iteration}: видалено {deleted_count} промокодів")
            
            # Чекаємо трохи, щоб сторінка оновилася
            time.sleep(15)  # Збільшено затримку для уникнення зависання
        
        if iteration >= max_iterations:
            process_logger.warning("⚠️ Досягнуто максимальну кількість ітерацій видалення")
            
    except Exception as e:
        process_logger.error(f"❌ Помилка при видаленні промокодів в діапазоні: {e}")
    
    process_logger.info(f"📊 Загалом видалено {total_deleted} промокодів в діапазоні {start_amount}-{end_amount} грн")
    return total_deleted

def validate_duplicates_after_collection(codes_by_amount, duplicates_info=None, iframe=None):
    """
    Перевіряє та обробляє дублікати промокодів після збору всіх кодів з усіх сторінок.
    Для кожного дублікату проводить фільтрацію та переписує коди в основний масив.
    
    Args:
        codes_by_amount: словник з кодами по сумам
        duplicates_info: інформація про дублікати, зібрана під час збору (опціонально)
        promo_service: готовий об'єкт PromoService для роботи з адмін-панеллю
        iframe: готовий iframe для роботи з таблицею
    """
    logger.debug("🔍 Запуск функції validate_duplicates_after_collection()")
    
    if not duplicates_info:
        logger.info("✅ Інформація про дублікаті не передана - перевіряємо тільки фінальний набір")
        # Звичайна перевірка дублікатів у фінальному наборі
        return _validate_final_duplicates(codes_by_amount)
    
    logger.info(f"📊 Отримано інформацію про {len(duplicates_info)} дублікатів під час збору")
    
    # Обробляємо кожен дублікат
    duplicate_amounts_to_process = set()
    for code, amounts in duplicates_info.items():
        logger.info(f"  🔍 Дублікат: {code} для сум: {amounts}")
        for amount in amounts:
            duplicate_amounts_to_process.add(amount)
    
    if duplicate_amounts_to_process:
        logger.info(f"🔄 Потрібно переобробити коди для сум: {sorted(duplicate_amounts_to_process)}")
        
        # Перевіряємо наявність необхідних параметрів
        if not iframe:
            logger.error("❌ Для переобробки дублікатів потрібні promo_service та iframe")
            logger.info("🔄 Виконуємо тільки фінальну перевірку...")
            return _validate_final_duplicates(codes_by_amount)
        
        try:
            # Переобробляємо коди для кожної суми з дублікатами
            for amount in sorted(duplicate_amounts_to_process):
                logger.info(f"� Переобробляємо коди для суми {amount} грн через дублікати...")
                
                # Застосовуємо фільтр по сумі
                if not apply_amount_filter(iframe, amount):
                    logger.warning(f"⚠️ Не вдалося застосувати фільтр для суми {amount} грн")
                    continue
                
                # Збираємо коди заново для цієї суми
                fresh_codes_dict = get_all_bon_codes_from_table(iframe)
                
                if fresh_codes_dict and amount in fresh_codes_dict:
                    fresh_code_objects = fresh_codes_dict[amount]
                    logger.info(f"✅ Зібрано {len(fresh_code_objects)} свіжих кодів для суми {amount} грн")
                    # Переписуємо коди в основний масив
                    codes_by_amount[amount] = fresh_code_objects
                    fresh_codes_list = [obj['code'] for obj in fresh_code_objects]
                    logger.info(f"📝 Коди для суми {amount} грн оновлено: {fresh_codes_list}")
                elif fresh_codes_dict:
                    logger.warning(f"⚠️ Не знайдено кодів для суми {amount} грн в результаті")
                    codes_by_amount[amount] = []
                else:
                    logger.warning(f"⚠️ Не вдалося зібрати свіжі коди для суми {amount} грн")
            
            logger.info("✅ Переобробка дублікатів завершена")
            
        except Exception as e:
            logger.error(f"❌ Помилка при переобробці дублікатів: {e}")
    
    # Фінальна перевірка на дублікати
    logger.info("🔍 Фінальна перевірка на дублікати після переобробки...")
    final_result = _validate_final_duplicates(codes_by_amount)
    
    logger.debug("🔍 Функція validate_duplicates_after_collection() завершена")
    return final_result


def _validate_final_duplicates(codes_by_amount):
    """
    Допоміжна функція для перевірки дублікатів у фінальному наборі кодів.
    """
    # Збираємо всі коди в один список для аналізу
    all_codes_list = []
    for amount, code_objects in codes_by_amount.items():
        for obj in code_objects:
            all_codes_list.append((obj['code'], amount, obj['status']))
    
    logger.info(f"📊 Загалом зібрано {len(all_codes_list)} кодів для аналізу дублікатів")
    
    # Перевіряємо на дублікати
    code_counts = {}
    code_amounts = {}  # Зберігаємо суми для кожного коду
    
    for code, amount, status in all_codes_list:
        if code in code_counts:
            code_counts[code] += 1
        else:
            code_counts[code] = 1
            code_amounts[code] = []
        code_amounts[code].append((amount, status))
    
    # Знаходимо дублікати
    duplicates = {code: count for code, count in code_counts.items() if count > 1}
    
    if duplicates:
        logger.warning(f"⚠️ ЗНАЙДЕНО {len(duplicates)} ДУБЛІКАТІВ ПРОМОКОДІВ У ФІНАЛЬНОМУ НАБОРІ!")
        
        # Аналізуємо кожен дублікат
        for code, count in duplicates.items():
            amounts_statuses = code_amounts[code]
            amounts = set(amt for amt, stat in amounts_statuses)
            logger.warning(f"  🔄 Код {code} зустрічався {count} разів для сум: {sorted(amounts)}")
            
            if len(amounts) > 1:
                logger.error(f"  ❌ КРИТИЧНА ПОМИЛКА: Код {code} має різні суми: {sorted(amounts)}")
            else:
                logger.info(f"  ✅ Код {code} має однакову суму: {list(amounts)[0]} грн")
        
        # Очищаємо дублікати - залишаємо лише один екземпляр кожного коду
        logger.info("🧹 Очищаємо дублікати...")
        cleaned_codes_by_amount = {}
        
        for amount, code_objects in codes_by_amount.items():
            # Збираємо унікальні коди, зберігаючи статус
            seen_codes = set()
            cleaned_objects = []
            
            for obj in code_objects:
                code = obj['code']
                if code not in seen_codes:
                    seen_codes.add(code)
                    cleaned_objects.append(obj)
            
            cleaned_codes_by_amount[amount] = cleaned_objects
            
            removed_count = len(code_objects) - len(cleaned_objects)
            if removed_count > 0:
                logger.info(f"  🧹 Для суми {amount} грн: видалено {removed_count} дублікатів, залишилось {len(cleaned_objects)} унікальних кодів")
        
        # Перевіряємо результат
        total_after = sum(len(codes) for codes in cleaned_codes_by_amount.values())
        total_before = sum(len(codes) for codes in codes_by_amount.values())
        logger.info(f"✅ Очищення завершено: {total_before} → {total_after} кодів (видалено {total_before - total_after} дублікатів)")
        
        return cleaned_codes_by_amount
    else:
        logger.info("✅ Дублікатів не знайдено у фінальному наборі")
        return codes_by_amount


def select_specific_promo_codes_optimized(iframe, codes_to_select: List[str]) -> bool:
    try:
        # 1. Отримати всі коди з їхніми індексами (рядками) за один запит
        rows_data = iframe.locator('table#datagrid tbody tr:visible:not(.no-data)').evaluate_all("""
        rows => rows.map((row, index) => {
            const codeCell = row.querySelector('td:nth-child(4)'); // 4-та колонка - код
            return {
                code: codeCell ? codeCell.innerText.trim() : null,
                rowIndex: index
            };
        })
        """)
        
        # 2. Створити множину кодів для швидкого пошуку
        codes_to_select_set = set(codes_to_select)
        
        # 3. Знайти індекси рядків, які потрібно вибрати
        indices_to_select = [data['rowIndex'] for data in rows_data if data['code'] in codes_to_select_set]
        
        if not indices_to_select:
            logger.warning("⚠️ Жодного коду зі списку не знайдено на поточній сторінці.")
            return False
        
        logger.info(f"🔍 Знайдено {len(indices_to_select)} кодів для вибору.")
        
        # 4. Виконати вибірку чекбоксів за індексами одним JS-запитом
        # Припускаємо, що чекбокс є першим елементом td:nth-child(1) input[type='checkbox']
        iframe.evaluate("""(indices) => {
            const tableBody = document.querySelector('table#datagrid tbody');
            const rows = tableBody.querySelectorAll('tr:visible:not(.no-data)');
            indices.forEach(index => {
                if (index >= 0 && index < rows.length) {
                    const checkbox = rows[index].querySelector('td:nth-child(1) input[type="checkbox"]');
                    if (checkbox && !checkbox.checked) {
                        checkbox.click();
                    }
                }
            });
        }""", indices_to_select)
        
        logger.info(f"✅ Успішно вибрано {len(indices_to_select)} чекбоксів.")
        return True
    
    except Exception as e:
        logger.error(f"❌ Помилка при оптимізованому виборі кодів: {e}")
        return False

def select_specific_promo_codes(iframe, promo_codes, page=None):
    """
    Вибирає конкретні промокоди через чекбокси.
    Використовує .click() для тригеру DOM подій.
    
    Args:
        iframe: iframe адмін-панелі
        promo_codes: список промокодів для вибору
        page: основна сторінка
        
    Returns:
        int: кількість вибраних промокодів
    """
    selected_count = 0
    codes_to_find = set(promo_codes)
    codes_not_found = []
    
    logger.info(f"☑️ Вибираємо {len(codes_to_find)} промокодів...")

    if not page:
        logger.error("❌ Об'єкт 'page' не передано")
        return 0

    # Спочатку перевіряємо, скільки рядків взагалі є в таблиці
    try:
        all_rows = iframe.locator('table#datagrid tbody tr')
        total_rows = all_rows.count()
        logger.info(f"📊 Всього рядків в таблиці: {total_rows}")
        
        if total_rows == 0:
            logger.warning("⚠️ Таблиця порожня - неможливо вибрати жодного промокоду")
            return 0
    except Exception as e:
        logger.warning(f"⚠️ Помилка при перевірці таблиці: {e}")

    for code_to_find in codes_to_find:
        try:
            # Шукаємо рядок з промокодом
            row_locator = iframe.locator(f'//tr[td[4][normalize-space(.)="{code_to_find}"]]')
            
            if row_locator.count() == 0:
                codes_not_found.append(code_to_find)
                logger.warning(f"⚠️ Не знайдено рядок для: {code_to_find}")
                continue

            row = row_locator
            logger.debug(f"  🎯 Знайдено рядок для: {code_to_find}")

            # Шукаємо чекбокс
            checkbox = row.locator('input[type="checkbox"].datagrid-check-control')
            if checkbox.count() == 0:
                checkbox = row.locator('input[type="checkbox"]')

            if checkbox.count() > 0:
                if not checkbox.is_checked():
                    try:
                        # Використовуємо .click() для тригеру DOM подій
                        checkbox.click()
                        time.sleep(0.05)  # Зменшую очікування з 0.1 на 0.05 сек
                        
                        if checkbox.is_checked():
                            logger.debug(f"  ✅ Вибрано: {code_to_find}")
                            selected_count += 1
                        else:
                            # Fallback через JavaScript
                            checkbox.evaluate('element => element.checked = true')
                            if checkbox.is_checked():
                                selected_count += 1
                                
                    except Exception as e:
                        logger.warning(f"  ❌ Помилка при виборі {code_to_find}: {e}")
                        # Fallback через JavaScript
                        try:
                            checkbox.evaluate('element => element.checked = true')
                            if checkbox.is_checked():
                                selected_count += 1
                        except Exception:
                            pass
                else:
                    logger.debug(f"  ✓ Вже вибраний: {code_to_find}")
                    selected_count += 1
            else:
                logger.warning(f"  ⚠️ Чекбокс не знайдено для: {code_to_find}")

        except Exception as e:
            logger.warning(f"❌ Помилка при обробці {code_to_find}: {e}")
            continue
            
    # Фінальна перевірка
    time.sleep(0.1)  # Зменшую очікування з 0.2 на 0.1 сек
    final_checked_count = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
    
    # Логуємо результат
    logger.info(f"✅ Вибрано {final_checked_count} промокодів")
    
    if codes_not_found:
        logger.warning(f"⚠️ Не знайдено в таблиці {len(codes_not_found)} кодів з {len(codes_to_find)}")
        logger.info(f"📋 Коди не знайдені: {', '.join(codes_not_found[:10])}")  # Показуємо тільки перші 10

    return final_checked_count

def delete_selected_codes(iframe, page=None):
    """
    Натискає кнопку видалення та обробляє діалоги підтвердження.
    Ця функція передбачає, що чекбокси ВЖЕ вибрані.
    
    Args:
        iframe: iframe адмін-панелі
        page: основна сторінка (обов'язково для обробки діалогів)
        
    Returns:
        bool: успішність операції
    """
    if not page:
        logger.error("❌ Об'єкт 'page' не передано, неможливо обробити діалоги підтвердження.")
        return False

    try:
        # Перевіряємо, чи є що видаляти
        selected_count = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
        if selected_count == 0:
            logger.warning("⚠️ Немає вибраних промокодів для видалення")
            return False
            
        logger.info(f"🗑️ Видаляємо {selected_count} вибраних промокодів...")

        # Налаштовуємо обробник стандартного діалогу
        dialog_handled = False
        def handle_dialog(dialog):
            nonlocal dialog_handled
            logger.info(f"💬 Отримано діалог: {dialog.message}")
            dialog.accept()
            dialog_handled = True
            logger.info("✅ Діалог підтверджено")
        
        page.once('dialog', handle_dialog)

        # Викликаємо видалення через JavaScript
        function_exists = iframe.evaluate('typeof removeSelectedGrids === "function"')
        
        if function_exists:
            logger.info("�️ Викликаємо removeSelectedGrids()...")
            iframe.evaluate('removeSelectedGrids()')
        else:
            logger.warning("⚠️ Функція removeSelectedGrids не знайдена!")
            return False
        
        # Очікуємо появи модального вікна
        time.sleep(0.5)
        
        # Шукаємо кнопку підтвердження
        confirm_selectors = [
            '.confirm-modal__button--ok',
            'button:has-text("Підтвердити")',
            '#dialog-window .confirm-modal__button--ok'
        ]
        
        button_found = False
        for selector in confirm_selectors:
            confirm_buttons = iframe.locator(selector)
            if confirm_buttons.count() > 0:
                logger.info(f"✅ Знайдено кнопку підтвердження: {selector}")
                confirm_buttons.nth(0).click()
                logger.info("🎯 Кнопку 'Підтвердити' натиснуто!")
                button_found = True
                break
        
        if not button_found:
            logger.info("⌨️ Кнопка не знайдена, пробуємо Enter...")
            iframe.press('Enter')
        
        # Очікуємо завершення операції
        time.sleep(1)

        # Чекаємо оновлення таблиці
        try:
            iframe.locator('#datagrid-loader').wait_for(state='hidden', timeout=10000)
        except Exception:
            pass  # Якщо лоадер не з'явився, продовжуємо
        
        # Перевіряємо результат
        remaining_selected = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
        
        if remaining_selected == 0:
            logger.info("✅ Всі вибрані промокоди успішно видалено!")
            return True
        else:
            logger.warning(f"⚠️ Залишилося {remaining_selected} невидалених промокодів")
            return False
            
    except Exception as e:
        logger.error(f"❌ Помилка при видаленні: {e}")
        return False

def delete_specific_promo_codes(iframe, promo_codes, page=None):
    """
    Видаляє конкретні промокоди: вибирає їх через чекбокси і видаляє одним пакетом.
    
    Args:
        iframe: iframe адмін-панелі
        promo_codes: список промокодів для видалення
        page: основна сторінка (опціонально, але потрібна для модальних вікон)
        
    Returns:
        dict: результат операції з кількістю видалених кодів
    """
    try:
        logger.info(f"🎯 Видалення {len(promo_codes)} конкретних промокодів...")
        
        if not promo_codes:
            logger.warning("⚠️ Список промокодів порожній")
            return {"selected": 0, "deleted": 0, "success": False}
        
        # Перевіряємо стан таблиці
        try:
            all_rows = iframe.locator('table#datagrid tbody tr')
            total_rows = all_rows.count()
            logger.info(f"📊 Всього рядків в таблиці: {total_rows}")
            
            if total_rows == 0:
                logger.warning("⚠️ Таблиця порожня - неможливо видалити жодного промокоду")
                return {"selected": 0, "deleted": 0, "success": False}
                
        except Exception as e:
            logger.warning(f"⚠️ Помилка при перевірці таблиці: {e}")
        
        # 1. Вибираємо промокоди через чекбокси
        selected_count = select_specific_promo_codes(iframe, promo_codes, page)
        
        if selected_count == 0:
            logger.warning("❌ Не вдалося вибрати жодного промокоду")
            logger.info("💡 Можливо, ці промокоди вже були видалені або їх немає в поточній таблиці")
            return {"selected": 0, "deleted": 0, "success": False}
        
        logger.info(f"✅ Вибрано {selected_count} промокодів з {len(promo_codes)} запланованих")
        
        # Невелика пауза перед видаленням, щоб дати UI оновитись
        time.sleep(0.5)

        # 2. Видаляємо вибрані промокоди
        # Перевіряємо режим браузера для вибору оптимальної стратегії
        headed_mode = os.getenv('PLAYWRIGHT_HEADED', 'true').lower() in ['true', '1', 'yes']
        
        if headed_mode:
            logger.info("🖥️ HEADED режим: використовуємо стандартну функцію видалення")
            delete_success = delete_selected_codes(iframe, page)
        else:
            logger.info("👻 HEADLESS режим: використовуємо оптимізовану функцію видалення")
            delete_success = delete_selected_codes_headless_optimized(iframe, page)
        
        if delete_success:
            logger.info(f"✅ Успішно видалено {selected_count} промокодів!")
            return {"selected": selected_count, "deleted": selected_count, "success": True}
        else:
            logger.error("❌ Помилка при видаленні вибраних промокодів")
            # Рахуємо, скільки реально було видалено
            final_checked_count = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
            actually_deleted = selected_count - final_checked_count
            return {"selected": selected_count, "deleted": actually_deleted, "success": False}
        
    except Exception as e:
        logger.error(f"❌ Критична помилка при видаленні конкретних промокодів: {e}")
        return {"selected": 0, "deleted": 0, "success": False, "error": str(e)}


def delete_selected_codes_headless_optimized(iframe, page=None):
    """
    Покращена функція видалення для headless режиму.
    Натискає кнопку видалення та обробляє діалоги підтвердження.
    Ця функція передбачає, що чекбокси ВЖЕ вибрані.
    
    Args:
        iframe: iframe адмін-панелі
        page: основна сторінка (обов'язково для обробки діалогів)
        
    Returns:
        bool: успішність операції
    """
    if not page:
        logger.error("❌ Об'єкт 'page' не передано, неможливо обробити діалоги підтвердження.")
        return False

    try:
        # Перевіряємо, чи є що видаляти
        selected_count = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
        if selected_count == 0:
            logger.warning("⚠️ Немає вибраних промокодів для видалення")
            return False
            
        logger.info(f"🗑️ [HEADLESS] Видаляємо {selected_count} вибраних промокодів...")

        # Налаштовуємо обробник стандартного діалогу (для headless режиму)
        dialog_handled = False
        def handle_dialog(dialog):
            nonlocal dialog_handled
            logger.info(f"💬 [HEADLESS] Отримано діалог: {dialog.message}")
            dialog.accept()
            dialog_handled = True
            logger.info("✅ [HEADLESS] Діалог підтверджено")
        
        page.on('dialog', handle_dialog)

        # Перевіряємо доступність функції видалення
        function_exists = iframe.evaluate('typeof removeSelectedGrids === "function"')
        
        if function_exists:
            logger.info("🔧 [HEADLESS] Викликаємо removeSelectedGrids()...")
            iframe.evaluate('removeSelectedGrids()')
        else:
            logger.warning("⚠️ [HEADLESS] Функція removeSelectedGrids не знайдена, шукаємо альтернативні способи...")
            
            # Альтернативний спосіб: пошук кнопки видалення
            delete_button_selectors = [
                'button[onclick*="removeSelectedGrids"]',
                'input[onclick*="removeSelectedGrids"]',
                'a[onclick*="removeSelectedGrids"]',
                '.btn-delete',
                '#delete-selected',
                'button:has-text("Видалити")',
                'input[value*="Видалити"]',
                '[title*="Видалити"]'
            ]
            
            button_found = False
            for selector in delete_button_selectors:
                buttons = iframe.locator(selector)
                if buttons.count() > 0:
                    logger.info(f"✅ [HEADLESS] Знайдено кнопку видалення: {selector}")
                    buttons.first.click()
                    button_found = True
                    break
            
            if not button_found:
                logger.info("⌨️ [HEADLESS] Кнопка підтвердження не знайдена, пробуємо Enter...")
                iframe.press('Enter')
        
        # Очікуємо появи діалогу або модального вікна (збільшую час для headless)
        logger.info("⏳ [HEADLESS] Очікуємо появи діалогу підтвердження...")
        time.sleep(1.5)  # Збільшую час для headless режиму
        
        # Перевіряємо, чи був оброблений стандартний діалог
        if dialog_handled:
            logger.info("✅ [HEADLESS] Стандартний діалог оброблено, операція має завершитись")
        else:
            # Шукаємо модальне вікно підтвердження
            logger.info("🔍 [HEADLESS] Стандартний діалог не з'явився, шукаємо модальне вікно...")
            
            confirm_selectors = [
                '.confirm-modal__button--ok',
                'button:has-text("Підтвердити")',
                'button:has-text("Так")',
                'button:has-text("OK")',
                '#dialog-window .confirm-modal__button--ok',
                '.modal-footer button.btn-primary',
                '.ui-dialog-buttonset button:first-child',
                'button[onclick*="confirm"]',
                '.dialog-confirm-button'
            ]
            
            button_found = False
            for selector in confirm_selectors:
                confirm_buttons = iframe.locator(selector)
                if confirm_buttons.count() > 0:
                    logger.info(f"✅ [HEADLESS] Знайдено кнопку підтвердження: {selector}")
                    confirm_buttons.first.click()
                    logger.info("🎯 [HEADLESS] Кнопку підтвердження натиснуто!")
                    button_found = True
                    time.sleep(0.5)
                    break
            
            if not button_found:
                logger.info("⌨️ [HEADLESS] Кнопка підтвердження не знайдена, пробуємо Enter та JavaScript...")
                
                # Спроба 1: Enter на iframe
                try:
                    iframe.press('Enter')
                    time.sleep(0.5)
                except Exception:
                    pass
                
                # Спроба 2: JavaScript підтвердження
                try:
                    iframe.evaluate("""
                        // Пробуємо підтвердити через різні способи
                        if (typeof confirmDelete === 'function') {
                            confirmDelete();
                        } else if (typeof confirm !== 'undefined') {
                            // Якщо є глобальний confirm, перевизначаємо його
                            window.confirm = function() { return true; };
                        }
                        
                        // Імітуємо натискання Enter на документі
                        const event = new KeyboardEvent('keydown', {
                            key: 'Enter',
                            code: 'Enter',
                            keyCode: 13,
                            bubbles: true
                        });
                        document.dispatchEvent(event);
                        
                        // Спробуємо тригернути подію на формі
                        const forms = document.querySelectorAll('form');
                        if (forms.length > 0) {
                            forms[0].dispatchEvent(new Event('submit', {bubbles: true}));
                        }
                    """)
                    logger.info("🔧 [HEADLESS] Виконано JavaScript підтвердження")
                except Exception as js_error:
                    logger.warning(f"⚠️ [HEADLESS] JavaScript підтвердження не вдалося: {js_error}")
        
        # Очікуємо завершення операції (збільшуючи час для headless)
        logger.info("⏳ [HEADLESS] Очікуємо завершення операції видалення...")
        time.sleep(3.0)  # Збільшую час для headless режиму

        # Чекаємо оновлення таблиці
        logger.info("🔄 [HEADLESS] Перевіряємо оновлення таблиці...")
        try:
            # Спочатку чекаємо появи лоадера
            loader = iframe.locator('#datagrid-loader')
            try:
                loader.wait_for(state='visible', timeout=3000)
                logger.info("⏳ [HEADLESS] Лоадер з'явився, чекаємо його зникнення...")
                loader.wait_for(state='hidden', timeout=15000)
                logger.info("✅ [HEADLESS] Лоадер зник, таблиця оновлена")
            except Exception:
                logger.info("⏳ [HEADLESS] Лоадер не з'явився, чекаємо стабілізації мережі...")
                iframe.page.wait_for_load_state('networkidle', timeout=8000)
        except Exception as wait_error:
            logger.warning(f"⚠️ [HEADLESS] Помилка очікування оновлення таблиці: {wait_error}")
        
        # Додаткова пауза для стабілізації
        time.sleep(1.0)
        
        # Фінальна перевірка результату
        remaining_selected = iframe.locator('table#datagrid tbody input[type="checkbox"]:checked').count()
        total_rows_now = iframe.locator('table#datagrid tbody tr').count()
        
        logger.info(f"📊 [HEADLESS] Поточна кількість рядків в таблиці: {total_rows_now}")
        logger.info(f"📊 [HEADLESS] Залишилося вибраних чекбоксів: {remaining_selected}")
        
        if remaining_selected == 0:
            logger.info("✅ [HEADLESS] Всі вибрані промокоди успішно видалено!")
            return True
        else:
            # Додаткова перевірка: можливо, чекбокси просто скинулися, але коди видалилися
            if remaining_selected < selected_count:
                deleted_count = selected_count - remaining_selected
                logger.info(f"✅ [HEADLESS] Частково успішно: видалено {deleted_count} з {selected_count} промокодів")
                return True
            else:
                logger.warning(f"⚠️ [HEADLESS] Операція видалення не завершилася успішно")
                return False
        
    except Exception as e:
        logger.error(f"❌ [HEADLESS] Помилка при видаленні: {e}")
        return False

def main():
    """
    Основна функція для запуску генератора промокодів у паралельному режимі.
    """
    logger.info("🔄 Запуск у ПАРАЛЕЛЬНОМУ режимі")
    return manage_promo_codes()

# === ПСЕВДОНІМИ ДЛЯ ЗВОРОТНОЇ СУМІСНОСТІ ===
create_codes_for_amount = create_promo_codes
set_max_rows_per_page = set_table_rows_per_page
sort_table_by_discount_amount = sort_table_by_amount

if __name__ == "__main__":
    # Налаштування логування
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_filename = os.path.join(log_dir, f'promo_generator_{log_timestamp}.log')

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    logger.propagate = False
    logger.info(f"🔍 Логування налаштовано. Логи зберігаються у: {log_filename}")

    # Запуск головної функції
    try:
        # Встановлюємо метод запуску для сумісності
        # ВАЖЛИВО: Це має бути зроблено до створення будь-яких процесів
        if sys.platform.startswith('darwin'):
             # На macOS, 'spawn' є безпечнішим методом, особливо при роботі з GUI/браузерами
             multiprocessing.set_start_method('spawn', force=True)
             logger.info("🔧 Встановлено метод запуску multiprocessing: spawn (для macOS)")
        elif sys.platform.startswith('linux'):
             # На Linux, 'fork' є стандартним і швидким методом
             multiprocessing.set_start_method('fork', force=True)
             logger.info("🔧 Встановлено метод запуску multiprocessing: fork (для Linux)")

        success = manage_promo_codes()
        if success:
            logger.info("✅ Управління промокодами завершено успішно.")
            sys.exit(0)
        else:
            logger.error("❌ Управління промокодами завершено з помилками.")
            sys.exit(1)
    except Exception as e:
        logger.critical(f"💥 Критична помилка в головному процесі: {e}", exc_info=True)
        sys.exit(1)
