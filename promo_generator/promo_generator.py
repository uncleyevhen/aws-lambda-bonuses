#!/usr/bin/env python3
"""
СМАРТ генератор промокодів з фільтрацією та аналізом існуючих кодів.

ОСНОВНІ ФУНКЦІЇ:
1. smart_promo_management_main() - ГОЛОВНА ФУНКЦІЯ для автоматичного управління промокодами
2. apply_amount_filter_improved() / apply_code_filter_improved() - фільтрація в адмін-панелі  
3. get_all_bon_codes_from_table() - збір всіх BON кодів з таблиці з підтримкою пагінації
4. generate_codes_for_amount() / create_codes_for_amount() - генерація та створення кодів
5. delete_specific_promo_codes() - видалення конкретних промокодів
6. upload_to_s3() / download_from_s3() - синхронізація з S3

ЗАПУСК:
- HEADLESS режим: PLAYWRIGHT_HEADED=false python3 promo_generator.py  
- HEADED режим (за замовчуванням): python3 promo_generator/promo_generator.py

ОНОВЛЕННЯ v2.5:
- Видалено невикористовувані функції для оптимізації коду
- Залишено лише активно використовувані функції
- Покращено читабельність і зменшено розмір файлу
"""

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
import importlib
import importlib.util
import inspect
from multiprocessing import Process, Queue, Manager
import threading
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Додаємо шлях до replenish_promo_code_lambda для імпорту
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(parent_dir, 'replenish_promo_code_lambda'))

# Імпорти для браузера та логіну (з обробкою помилок)
try:
    from browser_manager import create_browser_manager
    from promo_logic import PromoService
    BROWSER_MODULES_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"⚠️ Не вдалося імпортувати браузерні модулі: {e}")
    create_browser_manager = None
    PromoService = None
    BROWSER_MODULES_AVAILABLE = False

# Завантажуємо змінні середовища з .env файлу
load_dotenv()

# --- Основна конфігурація ---
CONFIG = {
    's3_bucket': 'lambda-promo-sessions',
    's3_key': 'promo-codes/available_codes.json',
    'region': 'eu-north-1',
    'target_codes_per_amount': 10,
    'start_amount': 1,
    'end_amount': 1000,  # Діапазон до 100
    'sort_order': 'asc',  # Порядок сортування: 'asc' (зростання) або 'desc' (спадання)
    'sync_s3': True,
    'auto_delete_excess': True,  # Увімкнемо видалення для тестування
    'verbose_logging': False,  # Додано для контролю детального логування
    'quick_mode': True,  # Швидкий режим - мінімум логів для оптимальних випадків
    'parallel_processes': 10,  # Встановлюємо 2 процеси для тестування паралельності
    'process_timeout': 600,  # Таймаут для процесу в секундах (10 хвилин)
}

# Налаштування кількості процесів через змінні середовища
def get_processes_count():
    """Отримує кількість процесів із змінних середовища або конфігурації."""
    env_processes = os.getenv('PROMO_PARALLEL_PROCESSES')
    if env_processes:
        try:
            return max(1, int(env_processes))
        except ValueError:
            pass
    return CONFIG.get('parallel_processes', 1)

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
    total_range = end_amount - start_amount + 1
    chunk_size = total_range // num_processes
    remainder = total_range % num_processes
    
    ranges = []
    current_start = start_amount
    
    for i in range(num_processes):
        # Додаємо по одному до розміру chunks, якщо є остача
        current_chunk_size = chunk_size + (1 if i < remainder else 0)
        current_end = current_start + current_chunk_size - 1
        
        # Обмежуємо кінець діапазону
        current_end = min(current_end, end_amount)
        
        ranges.append((current_start, current_end))
        current_start = current_end + 1
        
        # Якщо досягли кінця, зупиняємось
        if current_start > end_amount:
            break
    
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
        
        if 'process_logger' in locals():
            process_logger.error(f"❌ Помилка в процесі {process_id}: {e}")
        else:
            print(f"❌ Помилка в процесі {process_id}: {e}")

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
        from replenish_promo_code_lambda.browser_manager import create_browser_manager
        from replenish_promo_code_lambda.promo_logic import PromoService
        from promo_smart import PromoSmartManager
        
        # Імпортуємо необхідні функції безпосередньо з глобального простору імен
        # Оскільки ми вже в модулі promo_generator, просто отримуємо посилання на функції
        import inspect
        current_module = inspect.getmodule(inspect.currentframe())
        
        # Імпортуємо функції через importlib
        import importlib.util
        import types
        
        # Отримуємо модуль прямо з файлу
        spec = importlib.util.spec_from_file_location("promo_generator", __file__)
        if spec and spec.loader:
            promo_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(promo_module)
            
            # Імпортуємо всі необхідні функції
            set_max_rows_per_page = getattr(promo_module, 'set_max_rows_per_page', None)
            apply_bon_filter_once = getattr(promo_module, 'apply_bon_filter_once', None)
            apply_amount_range_filter = getattr(promo_module, 'apply_amount_range_filter', None)
            clear_all_filters = getattr(promo_module, 'clear_all_filters', None)
            sort_table_by_discount_amount = getattr(promo_module, 'sort_table_by_discount_amount', None)
            get_all_bon_codes_from_table = getattr(promo_module, 'get_all_bon_codes_from_table', None)
            create_codes_for_amount = getattr(promo_module, 'create_codes_for_amount', None)
            delete_specific_promo_codes = getattr(promo_module, 'delete_specific_promo_codes', None)
            select_specific_promo_codes = getattr(promo_module, 'select_specific_promo_codes', None)
            delete_selected_codes = getattr(promo_module, 'delete_selected_codes', None)
            delete_selected_codes_headless_optimized = getattr(promo_module, 'delete_selected_codes_headless_optimized', None)
            generate_random_bon_code = getattr(promo_module, 'generate_random_bon_code', None)
        else:
            process_logger.error("❌ Не вдалося отримати spec для модуля")
            return {'success': False, 'error': 'Module spec error'}
        
        # Перевіряємо, що всі функції знайдені
        missing_functions = []
        for func_name, func in [
            ('set_max_rows_per_page', set_max_rows_per_page),
            ('apply_bon_filter_once', apply_bon_filter_once),
            ('apply_amount_range_filter', apply_amount_range_filter),
            ('clear_all_filters', clear_all_filters),
            ('sort_table_by_discount_amount', sort_table_by_discount_amount),
            ('get_all_bon_codes_from_table', get_all_bon_codes_from_table),
            ('create_codes_for_amount', create_codes_for_amount),
            ('delete_specific_promo_codes', delete_specific_promo_codes),
            ('select_specific_promo_codes', select_specific_promo_codes),
            ('delete_selected_codes', delete_selected_codes),
            ('delete_selected_codes_headless_optimized', delete_selected_codes_headless_optimized),
            ('generate_random_bon_code', generate_random_bon_code),
        ]:
            if func is None:
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
        set_max_rows_per_page(iframe, 160)
        
        # Застосовуємо BON фільтр
        if not apply_bon_filter_once(iframe):
            process_logger.error(f"❌ Процес {process_id}: Не вдалося застосувати BON фільтр")
            return {'success': False, 'error': 'BON filter failed'}
        
        # Застосовуємо фільтр по діапазону сум
        if not apply_amount_range_filter(iframe, config['start_amount'], config['end_amount']):
            process_logger.warning(f"⚠️ Процес {process_id}: Не вдалося застосувати фільтр діапазону")
        
        # Сортуємо таблицю по розміру знижки для кращої організації даних
        process_logger.info(f"📊 Застосовуємо сортування по розміру знижки ({config['sort_order']})...")
        sort_success = sort_table_by_discount_amount(iframe, config['sort_order'])
        if not sort_success:
            process_logger.warning("⚠️ Не вдалося відсортувати таблицю по розміру знижки, продовжуємо без сортування")
        else:
            sort_label = "зростання" if config['sort_order'] == "asc" else "спадання"
            process_logger.info(f"✅ Таблицю успішно відсортовано по розміру знижки ({sort_label})")

        # Отримуємо коди для діапазону
        all_codes_by_amount = get_all_bon_codes_from_table(iframe)
        
        if not all_codes_by_amount:
            process_logger.info(f"ℹ️ Процес {process_id}: Не знайдено промокодів у діапазоні")
            all_codes_by_amount = {}
            for amount in range(config['start_amount'], config['end_amount'] + 1):
                all_codes_by_amount[amount] = []
        
        # Аналіз та обробка кожної суми
        for amount in range(config['start_amount'], config['end_amount'] + 1):
            code_objects = all_codes_by_amount.get(amount, [])
            active_codes = [obj['code'] for obj in code_objects if obj['status'] == 'active']
            
            current_count = len(active_codes)
            target_count = config['target_codes_per_amount']
            
            process_logger.info(f"📊 Процес {process_id}: Аналіз суми {amount} грн - поточно {current_count}, потрібно {target_count}")
            
            if current_count < target_count:
                # Потрібно створити коди
                needed = target_count - current_count
                process_logger.info(f"➕ Процес {process_id}: Для {amount} грн потрібно створити {needed} кодів")
                
                # Генеруємо нові коди
                new_codes = []
                for _ in range(needed):
                    new_code = generate_random_bon_code(active_codes + new_codes)
                    new_codes.append(new_code)
                
                # Створюємо коди
                created_count = create_codes_for_amount(promo_service, new_codes, amount)
                total_operations['created'] += created_count
                
                # Оновлюємо дані для результату
                for code in new_codes[:created_count]:
                    active_codes.append(code)
                    
            elif current_count > target_count and config.get('auto_delete_excess', False):
                # Потрібно видалити зайві коди
                excess = current_count - target_count
                process_logger.info(f"➖ Процес {process_id}: Для {amount} грн потрібно видалити {excess} зайвих кодів")
                
                codes_to_delete = active_codes[-excess:]  # Видаляємо останні
                
                # ВАЖЛИВО: Перед видаленням застосовуємо фільтр по конкретній сумі
                # щоб коди були видимі в таблиці
                process_logger.info(f"🔍 Застосовуємо фільтр по сумі {amount} грн перед видаленням...")
                
                # Очищаємо попередні фільтри
                clear_all_filters(iframe)
                
                # Застосовуємо BON фільтр
                apply_bon_filter_once(iframe)
                
                # Застосовуємо фільтр по конкретній сумі
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
                
                # Відновлюємо фільтр по діапазону для подальшої роботи
                apply_bon_filter_once(iframe)
                apply_amount_range_filter(iframe, config['start_amount'], config['end_amount'])
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
                if hasattr(browser_manager, 'close'):
                    browser_manager.close()
                elif hasattr(browser_manager, 'cleanup'):
                    browser_manager.cleanup()
                elif hasattr(browser_manager, 'browser') and browser_manager.browser:
                    browser_manager.browser.close()
        except Exception as cleanup_error:
            process_logger.warning(f"⚠️ Помилка при закритті браузера: {cleanup_error}")

def generate_random_bon_code(existing_codes):
    """
    Генерує унікальний BON код, який не існує в списку існуючих.
    
    Args:
        existing_codes: список існуючих кодів
        
    Returns:
        str: новий унікальний BON код
    """
    while True:
        # Генеруємо код формату BON + 6 символів
        suffix = generate_random_string(6)
        new_code = f"BON{suffix}"
        
        if new_code not in existing_codes:
            return new_code

def parallel_promo_management():
    """
    Основна функція паралельного управління промокодами.
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
        
        # Готуємо дані для S3
        s3_data = {}
        for amount, codes in all_codes_data.items():
            s3_data[str(amount)] = codes
        
        # Додаємо метадані
        s3_data['_metadata'] = {
            'last_updated': datetime.datetime.now().isoformat(),
            'total_processes': len(processes),
            'successful_processes': successful_processes,
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

# Налаштування логування
import datetime

# Створюємо папку для логів, якщо вона не існує
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Формуємо ім'я файлу логу з датою та часом
log_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_filename = os.path.join(log_dir, f'promo_generator_{log_timestamp}.log')

# Налаштовуємо логування одночасно у файл та консоль
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Налаштування форматування логів
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Налаштовуємо виведення у консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)

# Налаштовуємо виведення у файл
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)

# Вимикаємо поширення логів, щоб уникнути дублювання від basicConfig
logger.propagate = False

logger.info(f"🔍 Логування налаштовано. Логи зберігаються у: {log_filename}")

def generate_random_string(length):
    """Генерує випадковий рядок із великих літер та цифр."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def apply_amount_filter_improved(iframe, amount):
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
        
        # 1. Знаходимо заголовок "Розмір знижки" (колонка 4778)
        amount_header = iframe.locator('#header_id_4778')
        if not amount_header.count():
            logger.error("❌ Не знайдено заголовок 'Розмір знижки'")
            return False
        
        logger.info("🖱️ Наводимо мишку на заголовок 'Розмір знижки'...")
        amount_header.hover()
        time.sleep(0.3)  # Коротке очікування для появи фільтра
        
        # 2. Перевіряємо чи з'явився блок фільтрації
        filter_block = iframe.locator('#sortingBlock_4778')
        if not filter_block.count():
            logger.error("❌ Не знайдено блок фільтрації")
            return False
        
        # 3. Чекаємо щоб поля стали видимими
        from_field = iframe.locator('#sortingBlock_4778 input[name="text1"][placeholder="від"]')
        to_field = iframe.locator('#sortingBlock_4778 input[name="text2"][placeholder="до"]')
        
        # Додатково клікаємо на блок фільтрації щоб активувати поля
        filter_block.click()
        time.sleep(0.3)
        
        if not from_field.count() or not to_field.count():
            logger.error("❌ Поля 'від' і 'до' не знайдено")
            return False
        
        # 4. Застосовуємо фільтр по діапазону
        logger.info(f"📝 Застосовуємо фільтр діапазону: від {start_amount} до {end_amount}...")
        
        from_field.click()
        from_field.fill(str(start_amount))
        time.sleep(0.3)
        
        to_field.click()
        to_field.fill(str(end_amount))
        time.sleep(0.3)
        
        # 5. Натискаємо Enter для застосування фільтра
        logger.info("⌨️ Натискаємо Enter для застосування фільтра діапазону...")
        to_field.press('Enter')
        
        # Власне очікування завершення фільтрації
        logger.info("⏳ Очікуємо завершення фільтрації по діапазону сум...")
        time.sleep(1.0)  # Збільшую час очікування для діапазону

        logger.info(f"✅ Фільтр по діапазону сум {start_amount}-{end_amount} успішно застосовано")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка при застосуванні фільтра діапазону: {e}")
        return False

def apply_code_filter_improved(iframe, search_term="BON"):
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
        
        # 1. Знаходимо заголовок "Код" (колона 4776)
        code_header = iframe.locator('#header_id_4776')
        if not code_header.count():
            logger.error("❌ Не знайдено заголовок 'Код'")
            return False
        
        logger.info("🖱️ Наводимо мишку на заголовок 'Код'...")
        code_header.hover()
        time.sleep(0.2)  # Зменшую очікування для появи фільтра з 0.5 на 0.2
        
        # 2. Перевіряємо чи з'явився блок фільтрації
        filter_block = iframe.locator('#sortingBlock_4776')
        if not filter_block.count():
            logger.error("❌ Не знайдено блок фільтрації для коду")
            return False
        
        # 3. Знаходимо поле пошуку
        search_field = iframe.locator('#sortingBlock_4776 input[placeholder="пошук..."]')
        
        # Додатково клікаємо на блок фільтрації щоб активувати поле
        filter_block.click()
        time.sleep(0.3)  # Зменшую очікування з 1 на 0.3
        
        if not search_field.count():
            logger.error("❌ Поле пошуку не знайдено")
            return False
        
        logger.info(f"📝 Заповнюємо поле пошуку: '{search_term}'")
        
        # 4. Очищаємо та заповнюємо поле пошуку
        search_field.click()
        search_field.clear()
        search_field.fill(search_term)
        time.sleep(0.2)  # Зменшую очікування з 0.5 на 0.2
        
        # 5. Натискаємо Enter для застосування фільтра
        logger.info("⌨️ Натискаємо Enter для застосування фільтра...")
        search_field.press('Enter')
        
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
        success = apply_code_filter_improved(iframe, "BON")
        
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

def get_all_bon_codes_from_table(iframe):
    """
    Отримує всі **АКТИВНІ** BON коди з поточної таблиці (після застосування фільтрів).
    Обробляє всі сторінки пагінації.
    
    Args:
        iframe: Елемент iframe таблиці промокодів
    
    Returns:
        dict: {сума: [список_об'єктів_кодів]} - коди зі статусами
    """
    # Ініціалізуємо codes_by_amount на початку для уникнення помилок
    codes_by_amount = {}
    duplicates_info = {}  # Ініціалізуємо тут, щоб була доступна в finally
    
    try:
        logger.info("📋 ПОЧАТОК збору всіх BON кодів з відфільтрованої таблиці (з усіх сторінок)...")
        
        # Перевіряємо поточні налаштування таблиці
        pager_text = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
        logger.info(f"📄 Інформація про пагінацію: {pager_text}")
        
        # Зменшена пауза для стабілізації в HEADED режимі
        headed_mode = os.getenv('PLAYWRIGHT_HEADED', 'True').lower() in ['true', '1', 'yes']
        if headed_mode:
            logger.info("🖥️ HEADED режим: пауза 0.5 сек для стабілізації...")
            time.sleep(0.5)  # Зменшую з 2 сек до 0.5 сек

        # Використовуємо Set для запобігання дублікатів
        codes_by_amount = {}
        all_codes_set = set()  # Для відстеження всіх унікальних кодів
        duplicates_info = {}  # Зберігаємо інформацію про дублікати: {код: [список_сум]}
        current_page = 1
        has_next_page = True
        total_processed_rows = 0
        
        while has_next_page:
            logger.info(f"📄 Обробка сторінки {current_page}...")
            
            # Отримуємо всі рядки на поточній сторінці
            all_rows = iframe.locator('table#datagrid tbody tr')
            rows_count = all_rows.count()
            
            logger.info(f"📋 Аналізуємо {rows_count} рядків на сторінці {current_page}...")
            
            if rows_count == 0:
                logger.warning(f"⚠️ На сторінці {current_page} немає рядків! Можливо кінець даних.")
                break
            
            # Лічильник для цієї сторінки
            page_codes_count = 0
            
            # Обробляємо рядки на поточній сторінці
            for i in range(rows_count):
                try:
                    row = all_rows.nth(i)
                    
                    # Перевіряємо, чи це валідний рядок з даними
                    cells = row.locator('td')
                    cell_count = cells.count()
                    if not row.is_visible() or cell_count < 5:
                        continue
                    
                    # Код знаходиться в 4-й колонці (індекс 3)
                    code_cell = cells.nth(3)
                    code = code_cell.inner_text().strip()
                    
                    if not code or not code.startswith('BON'):
                        continue
                    
                    # Розмір знижки знаходиться в 5-й колонці (індекс 4)
                    amount_cell = cells.nth(4)
                    amount_text = amount_cell.inner_text().strip()
                    
                    # Парсимо суму з тексту (може бути "168 грн" або просто "168")
                    amount_match = re.search(r'(\d+)', amount_text)
                    if not amount_match:
                        logger.debug(f"  ⚠️ НЕ РОЗПІЗНАНО СУМУ: для коду {code} (колонка: '{amount_text}') - сторінка {current_page}, рядок {i+1}")
                        continue
                    
                    amount = int(amount_match.group(1))
                    
                    # Перевіряємо, чи це дублікат
                    if code in all_codes_set:
                        # ЗБЕРІГАЄМО ІНФОРМАЦІЮ ПРО ДУБЛІКАТ
                        if code not in duplicates_info:
                            # Знаходимо оригінальну суму цього коду
                            original_amount = None
                            for check_amount, check_code_objects in codes_by_amount.items():
                                check_codes = [obj['code'] for obj in check_code_objects]
                                if code in check_codes:
                                    original_amount = check_amount
                                    break
                            
                            if original_amount:
                                duplicates_info[code] = [original_amount]
                        
                        # Додаємо поточну суму до списку сум для цього коду
                        if amount not in duplicates_info[code]:
                            duplicates_info[code].append(amount)
                        
                        # Логуємо дублікат тільки для критичних помилок
                        if len(set(duplicates_info[code])) > 1:
                            logger.error(f"  ❌ КРИТИЧНИЙ ДУБЛІКАТ: {code} має РІЗНІ СУМИ: {sorted(duplicates_info[code])} грн! Поточна: {amount} грн")
                        
                        continue
                    
                    # Додаємо код до загального списку
                    if code not in all_codes_set:
                        all_codes_set.add(code)
                    
                    # Отримуємо статус промокоду (колонка 3, індекс 2)
                    status_cell = cells.nth(2)
                    status_text = status_cell.inner_text().strip().lower()
                    is_active = status_text in ['так', 'yes', 'активний', 'active']
                    
                    # Додаємо код до основного масиву
                    if amount not in codes_by_amount:
                        codes_by_amount[amount] = []
                    
                    # Зберігаємо код як об'єкт зі статусом
                    code_obj = {
                        'code': code,
                        'status': 'active' if is_active else 'inactive',
                        'amount': amount
                    }
                    
                    # Сортуємо: неактивні коди першими, потім активні
                    if not is_active:
                        codes_by_amount[amount].insert(0, code_obj)  # Неактивні на початок
                    else:
                        codes_by_amount[amount].append(code_obj)     # Активні в кінець

                    page_codes_count += 1
                    
                except Exception as row_error:
                    logger.debug(f"Помилка обробки рядка {i}: {row_error}")
                    continue
                    
            logger.info(f"✅ Додано {page_codes_count} кодів з поточної сторінки {current_page}")
            total_processed_rows += page_codes_count
            
            # Виводимо стислу статистику по кодах на поточній сторінці
            if page_codes_count > 0:
                logger.info(f"📊 Сторінка {current_page}: {page_codes_count} кодів")
                # Показуємо розподіл тільки якщо є коди і включене детальне логування
                if CONFIG.get('verbose_logging', False):
                    for amount in sorted(codes_by_amount.keys()):
                        count = len(codes_by_amount[amount])
                        if count > 0:
                            active_count = sum(1 for obj in codes_by_amount[amount] if obj['status'] == 'active')
                            inactive_count = count - active_count
                            logger.info(f"  💰 {amount} грн: {active_count} активних + {inactive_count} неактивних = {count} всього")
            
            # Логуємо загальну кількість унікальних кодів
            logger.info(f"📝 Загальна кількість унікальних кодів після сторінки {current_page}: {len(all_codes_set)}")
            
            # Перевіряємо наявність наступної сторінки
            # Оновлений код для знаходження стрілки "вправо" для пагінації
            next_page_button = iframe.locator('.datagrid-pager .fl-l.r.active').first
            has_next_page = has_next_page & next_page_button.count() > 0
            
            # Виводимо інформацію про пагінацію
            if has_next_page:
                logger.info(f"📑 Є наступна сторінка після {current_page}")
            else:
                logger.info(f"📑 Це остання сторінка ({current_page})")
            
            if has_next_page:
                # Перед переходом зберігаємо інформацію про поточну сторінку для порівняння
                previous_page_info = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
                logger.info(f"📑 Поточний стан пагінації перед переходом: {previous_page_info}")
                
                # Для статистики рахуємо кількість кодів до переходу
                previous_all_codes_count = len(all_codes_set)
                logger.info(f"📊 Кількість унікальних кодів перед переходом: {previous_all_codes_count}")
                
                # Зберігаємо останній код з поточної сторінки для перевірки
                last_processed_codes = list(all_codes_set)[-10:] if all_codes_set else []
                logger.info(f"🔍 Останні 10 оброблених кодів: {last_processed_codes}")
                
                logger.info(f"📑 ПЕРЕХІД: з сторінки {current_page} на сторінку {current_page + 1}...")
                
                try:
                    # Явно очікуємо, щоб кнопка була клікабельною
                    next_page_button.wait_for(state='visible', timeout=3000)
                    
                    # Клікаємо на кнопку переходу на наступну сторінку
                    logger.info(f"🖱️ Клікаємо на кнопку наступної сторінки...")
                    next_page_button.click()
                    time.sleep(0.5)  # Коротка пауза для початку переходу
                    
                    # Оновлюємо лічильник сторінок
                    current_page += 1
                    logger.info(f"📄 Перейшли до сторінки {current_page}")
                    
                    # Почекаємо явно на індикатор завантаження, якщо він є
                    logger.info(f"⏳ Чекаємо на завершення завантаження нової сторінки...")
                    
                    # Спочатку перевіримо наявність лоадера
                    loader = iframe.locator('#datagrid-loader')
                    if loader.count() > 0:
                        try:
                            loader.wait_for(state='visible', timeout=1000)  # Спочатку чекаємо, щоб лоадер став видимим
                            logger.info(f"🔄 Лоадер з'явився, чекаємо його зникнення...")
                            loader.wait_for(state='hidden', timeout=5000)  # Потім чекаємо, щоб він зник
                            logger.info(f"✅ Лоадер зник, сторінка завантажилась")
                        except Exception as loader_error:
                            logger.info(f"⚠️ Не вдалося відстежити лоадер: {loader_error}")
                            # Якщо не вдалося відстежити лоадер, чекаємо стабілізації мережевої активності
                            #iframe.page.wait_for_load_state('networkidle', timeout=3000)
                    else:
                        # Якщо лоадера немає, просто почекаємо
                        logger.info(f"⏱️ Лоадер не знайдено, чекаємо 1.5 секунди...")
                        time.sleep(1.5)  # Даємо час на завантаження нової сторінки
                    
                    # Перевіряємо, що сторінка дійсно змінилася
                    new_page_info = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
                    logger.info(f"📑 Поточний стан пагінації після переходу: {new_page_info}")
                    
                    # Порівнюємо інформацію про сторінку до і після переходу
                    if previous_page_info == new_page_info:
                        logger.warning(f"⚠️ ПРОБЛЕМА ПАГІНАЦІЇ! Інформація про сторінку не змінилася після переходу!")
                        
                        # Спробуємо ще раз
                        logger.info(f"🔄 Спроба повторного кліку на кнопку наступної сторінки...")
                        next_page_button = iframe.locator('.datagrid-pager .fl-l.r.active').first
                        if next_page_button.count() > 0:
                            next_page_button.click()
                            time.sleep(2)  # Збільшимо затримку для надійності
                        else:
                            logger.error(f"❌ Кнопка наступної сторінки зникла при повторній спробі!")
                    
                except Exception as click_error:
                    logger.error(f"❌ Помилка при переході на наступну сторінку: {click_error}")
                    break
            else:
                logger.info(f"📑 Досягнуто останньої сторінки ({current_page}).")
        
        # ОБРОБКА ДУБЛІКАТІВ ПІСЛЯ ЗБОРУ ВСІХ КОДІВ
        if duplicates_info:
            logger.warning(f"🔄 ЗНАЙДЕНО {len(duplicates_info)} ДУБЛІКАТІВ ПІД ЧАС ЗБОРУ!")
            logger.info("🔧 ПОЧИНАЄМО ОБРОБКУ ДУБЛІКАТІВ...")
            
            for code, amounts in duplicates_info.items():
                logger.info(f"🔍 Обробляємо дублікат: {code} для сум: {amounts}")
                
                if len(amounts) == 1:
                    # Дублікат з однаковою сумою - нічого не робимо
                    logger.info(f"  ✅ Код {code} має однакову суму {amounts[0]} грн - дублікат вже відфільтрований")
                else:
                    # Дублікат з різними сумами - потрібно додати до всіх сум
                    logger.warning(f"  ⚠️ Код {code} має різні суми: {amounts} грн")
                    
                    # Знаходимо, в якій сумі код вже є
                    current_amount = None
                    for amount in amounts:
                        if amount in codes_by_amount:
                            code_objects = codes_by_amount[amount]
                            codes_list = [obj['code'] for obj in code_objects]
                            if code in codes_list:
                                current_amount = amount
                                break
                    
                    if current_amount:
                        logger.info(f"  📍 Код {code} наразі знаходиться в сумі {current_amount} грн")
                        
                        # Додаємо код до всіх інших сум з цього дубліката
                        for amount in amounts:
                            if amount != current_amount:
                                if amount not in codes_by_amount:
                                    codes_by_amount[amount] = []
                                
                                # Перевіряємо чи код вже є
                                codes_list = [obj['code'] for obj in codes_by_amount[amount]]
                                if code not in codes_list:
                                    # Додаємо як неактивний дублікат
                                    duplicate_obj = {
                                        'code': code,
                                        'status': 'inactive',  # Дублікати завжди неактивні
                                        'amount': amount
                                    }
                                    codes_by_amount[amount].append(duplicate_obj)
                                    logger.info(f"  ➕ ДОДАНО: {code} до суми {amount} грн (дублікат)")
                                else:
                                    logger.info(f"  ✅ Код {code} вже є в сумі {amount} грн")
                    else:
                        logger.error(f"  ❌ КРИТИЧНА ПОМИЛКА: Код {code} не знайдено в жодній сумі!")
            
            logger.info("✅ ОБРОБКА ДУБЛІКАТІВ ЗАВЕРШЕНА!")
        else:
            logger.info("✅ Дублікатів під час збору не виявлено")
        
        # Перевіряємо, чи всі коди унікальні та правильно підраховані
        total_codes = sum(len(codes) for codes in codes_by_amount.values())
        logger.info(f"📊 ПІДСУМОК: Загалом знайдено {total_codes} BON промокодів (включаючи активні та неактивні)")
        logger.info(f"📊 ПІДСУМОК: Загалом оброблено {total_processed_rows} рядків")
        logger.info(f"📚 ПІДСУМОК: Загалом оброблено {current_page} сторінок пагінації")
        logger.info(f"🔍 ПІДСУМОК: Кількість унікальних кодів у all_codes_set: {len(all_codes_set)}")
        
        # Рахуємо активні та неактивні промокоди окремо
        active_count = 0
        inactive_count = 0
        for amount, code_objects in codes_by_amount.items():
            for obj in code_objects:
                if obj['status'] == 'active':
                    active_count += 1
                else:
                    inactive_count += 1
        
        logger.info(f"📊 СТАТИСТИКА ЗА СТАТУСАМИ: {active_count} активних + {inactive_count} неактивних = {total_codes} всього")
        
        # Звіряємо розмір all_codes_set з сумою кодів для всіх сум
        if total_codes != len(all_codes_set):
            logger.warning(f"⚠️ НЕСУМІСНІСТЬ ДАНИХ: Кількість унікальних кодів ({len(all_codes_set)}) відрізняється від суми кодів по сумам ({total_codes})!")
            
            # Детальна перевірка дублікатів
            logger.info(f"🔍 Виконуємо перевірку на можливі дублікати промокодів...")
            # Збираємо всі коди в один список
            all_codes_list = []
            for amount, code_objects in codes_by_amount.items():
                for obj in code_objects:
                    all_codes_list.append(obj['code'])
            
            # Перевіряємо на дублікати
            code_counts = {}
            for code in all_codes_list:
                if code in code_counts:
                    code_counts[code] += 1
                else:
                    code_counts[code] = 1
            
            # Виводимо інформацію про дублікати
            duplicates = {code: count for code, count in code_counts.items() if count > 1}
            if duplicates:
                logger.error(f"❌ ЗНАЙДЕНО ДУБЛІКАТИ В ДАНИХ: {len(duplicates)} кодів дублюються!")
                for code, count in duplicates.items():
                    logger.error(f"  ❌ Код {code} зустрічається {count} разів")
                    # Знаходимо, в яких сумах зустрічається цей код
                    amounts = []
                    for amount, code_objects in codes_by_amount.items():
                        codes_list = [obj['code'] for obj in code_objects]
                        if code in codes_list:
                            amounts.append(amount)
                    logger.error(f"  ❌ Код {code} зустрічається для сум: {amounts}")
            else:
                logger.info(f"✅ Дублікатів кодів не знайдено, але є невідповідність між all_codes_set і сумою кодів по сумам")
        
        # Показуємо стислий результат по сумам тільки один раз
        if CONFIG.get('verbose_logging', False):
            logger.info(f"📊 ПІДСУМОК ПО СУМАМ:")
            for amount, code_objects in sorted(codes_by_amount.items()):
                active_codes = [obj for obj in code_objects if obj['status'] == 'active']
                inactive_codes = [obj for obj in code_objects if obj['status'] == 'inactive']
                
                logger.info(f"  💰 {amount} грн: {len(active_codes)} активних + {len(inactive_codes)} неактивних = {len(code_objects)} всього")
        
        # Не повертаємо тут, тому що return буде в finally блоці
        
    except Exception as e:
        logger.error(f"❌ Помилка при зборі BON кодів: {e}")
        return {}
    finally:
                # Виконуємо ВАЛІДАЦІЮ ДУБЛІКАТІВ НАВІТЬ ЯКЩО БУЛА ПОМИЛКА
        # (якщо codes_by_amount не порожній)
        try:
            if codes_by_amount:  # Тепер codes_by_amount завжди визначений
                logger.info("🔍 Перевірка дублікатів після збору всіх кодів...")
                
                # Передаємо інформацію про дублікати, якщо вона є
                if duplicates_info:
                    logger.debug(f"📊 Передаємо інформацію про {len(duplicates_info)} дублікатів до функції валідації")
                    codes_by_amount = validate_duplicates_after_collection(codes_by_amount, duplicates_info, iframe)
                else:
                    codes_by_amount = validate_duplicates_after_collection(codes_by_amount)
                
                # Перераховуємо загальну кількість після видалення дублікатів
                total_codes_after = sum(len(codes) for codes in codes_by_amount.values())
                logger.info(f"📊 Кількість кодів після видалення дублікатів: {total_codes_after}")
                
                # Виводимо фінальний підсумок тільки якщо включене детальне логування
                if CONFIG.get('verbose_logging', False):
                    logger.info(f"📊 ФІНАЛЬНИЙ ПІДСУМОК ПО СУМАМ:")
                    for amount, codes in sorted(codes_by_amount.items()):
                        active_count = sum(1 for obj in codes if obj['status'] == 'active')
                        inactive_count = len(codes) - active_count
                        logger.info(f"  💰 {amount} грн: {active_count} активних + {inactive_count} неактивних = {len(codes)} всього")
                    
        except Exception as validation_error:
            logger.error(f"❌ Помилка при валідації дублікатів: {validation_error}")
        
        # Завжди повертаємо codes_by_amount (навіть якщо порожній)
        return codes_by_amount



def smart_promo_management_main():
    """
    🎯 ГОЛОВНА ФУНКЦІЯ СМАРТ УПРАВЛІННЯ ПРОМОКОДАМИ
    
    Автоматично виконує повний цикл управління промокодами:
    1. Застосовує фільтр BON та отримує ВСІ промокоди одразу з усіх сторінок пагінації
    2. В пам'яті аналізує промокоди для кожної суми
    3. Спочатку додає всі недостатні промокоди для всіх сум 
    4. Потім видаляє всі надлишкові промокоди для всіх сум
    5. Синхронізує фінальний стан з S3
    
    Використовує оптимізацію: завантажує всі коди одразу і обробляє в пам'яті.
    
    Returns:
        bool: успішність виконання всіх операцій
    """
    logger.info("🎯 ПОЧАТОК СМАРТ УПРАВЛІННЯ ПРОМОКОДАМИ")
    logger.info("=" * 60)
    
    # Імпорти
    import sys
    import os
    
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, parent_dir)
    
    from replenish_promo_code_lambda.browser_manager import create_browser_manager
    from replenish_promo_code_lambda.promo_logic import PromoService
    from promo_smart import PromoSmartManager
    
    # Показуємо конфігурацію
    target_count = CONFIG['target_codes_per_amount']
    start_amount = CONFIG['start_amount']
    end_amount = CONFIG['end_amount']
    sort_order = CONFIG['sort_order']
    auto_delete = CONFIG['auto_delete_excess']
    
    logger.info(f"⚙️ Конфігурація:")
    logger.info(f"  🎯 Цільова кількість кодів на суму: {target_count}")
    logger.info(f"  💰 Діапазон сум: {start_amount}-{end_amount} грн")
    logger.info(f"  📊 Порядок сортування: {sort_order} ({'зростання' if sort_order == 'asc' else 'спадання'})")
    logger.info(f"  🗑️ Автовидалення зайвих: {auto_delete}")
    
    # Показуємо режим браузера
    headed_mode = os.getenv('PLAYWRIGHT_HEADED', 'true').lower() in ['true', '1', 'yes']
    if headed_mode:
        logger.info("🖥️ HEADED режим: Ви побачите весь процес у браузері!")
    else:
        logger.info("👻 HEADLESS режим: Обробка у фоні")
    
    browser_manager = create_browser_manager()
    
    try:
        # Ініціалізація браузера
        logger.info("\n🚀 Ініціалізація браузера...")
        page = browser_manager.initialize()
        promo_service = PromoService(page)
        
        # Логін
        logger.info("🔑 Логін в адмін-панель...")
        if not promo_service.login():
            logger.error("❌ Не вдалося увійти в адмін-панель")
            return False
        
        logger.info("✅ Успішний логін!")
        promo_smart_manager = PromoSmartManager(promo_service)
        
        # Словник для зберігання фінального стану промокодів для S3
        final_s3_state = {}
        # Лічильники для підсумкового звіту
        total_operations = {'created': 0, 'deleted': 0, 'unchanged': 0}
        
        # ЕТАП 1: Застосовуємо BON фільтр і отримуємо ВСІ промокоди з усіх сторінок пагінації
        logger.info("\n🔍 ЕТАП 1: Отримання всіх BON промокодів з адмін-панелі...")
        iframe = promo_service._get_iframe()
        if not iframe:
            logger.error("❌ Не вдалося отримати iframe")
            return False
        
        # Спочатку встановлюємо максимальну кількість рядків на сторінці
        max_rows_set = set_max_rows_per_page(iframe, 160)
        if not max_rows_set:
            logger.warning("⚠️ Не вдалося встановити максимальну кількість рядків, продовжуємо з поточними налаштуваннями")
        
        # Застосовуємо BON фільтр один раз
        bon_filter_success = apply_bon_filter_once(iframe)
        if not bon_filter_success:
            logger.error("❌ Не вдалося застосувати BON фільтр")
            return False
            
        # Сортуємо таблицю по розміру знижки для кращої організації даних
        logger.info(f"📊 Застосовуємо сортування по розміру знижки ({sort_order})...")
        sort_success = sort_table_by_discount_amount(iframe, sort_order)
        if not sort_success:
            logger.warning("⚠️ Не вдалося відсортувати таблицю по розміру знижки, продовжуємо без сортування")
        else:
            sort_label = "зростання" if sort_order == "asc" else "спадання"
            logger.info(f"✅ Таблицю успішно відсортовано по розміру знижки ({sort_label})")
        
        # Застосовуємо фільтр по діапазону сум через адмінку
        logger.info(f"🎯 Застосовуємо фільтр по діапазону сум: {start_amount}-{end_amount} грн...")
        range_filter_success = None; #apply_amount_range_filter(iframe, start_amount, end_amount)
        if not range_filter_success:
            logger.warning("⚠️ Не вдалося застосувати фільтр діапазону, збираємо всі коди")
        else:
            logger.info("✅ Фільтр діапазону успішно застосовано")
        
        # Отримуємо всі коди з усіх сторінок пагінації (вже відфільтровані по діапазону)
        logger.info("🔍 ПОЧАТОК ЗБОРУ ВСІХ ПРОМОКОДІВ...")
        all_codes_by_amount = get_all_bon_codes_from_table(iframe)
        logger.info("✅ ЗАВЕРШЕНО ЗБІР ВСІХ ПРОМОКОДІВ")
        
        # Додаткова перевірка цілісності даних (тільки якщо включене детальне логування)
        if all_codes_by_amount and CONFIG.get('verbose_logging', False):
            # Загальна кількість кодів для всіх сум
            total_codes = sum(len(codes) for codes in all_codes_by_amount.values())
            # Кількість унікальних кодів - витягуємо коди з об'єктів
            all_unique_codes = set()
            for code_objects in all_codes_by_amount.values():
                for obj in code_objects:
                    all_unique_codes.add(obj['code'])
            
            logger.info(f"📊 Контрольна перевірка: Знайдено {total_codes} промокодів, з них {len(all_unique_codes)} унікальних")
            
            if total_codes != len(all_unique_codes):
                logger.warning(f"⚠️ ВИЯВЛЕНО ПРОБЛЕМУ: Кількість унікальних кодів ({len(all_unique_codes)}) відрізняється від загальної кількості ({total_codes})!")
        elif all_codes_by_amount:
            # Швидка перевірка без детального логування
            total_codes = sum(len(codes) for codes in all_codes_by_amount.values())
            logger.info(f"📊 Контрольна перевірка: Знайдено {total_codes} промокодів")
        
        if not all_codes_by_amount:
            logger.warning("⚠️ Не знайдено жодного BON промокоду в системі")
            # Створимо порожні списки для сум в діапазоні
            for amount in range(start_amount, end_amount + 1):
                all_codes_by_amount[amount] = []
        
        # ЕТАП 2: Аналіз потреб (створення/видалення) для кожної суми
        logger.info("\n📊 ЕТАП 2: Аналіз потреб для кожної суми...")
        
        # Підготуємо структури даних для обробки
        codes_to_create_by_amount = {}  # {amount: [codes_to_create]}
        codes_to_delete_by_amount = {}  # {amount: [codes_to_delete]}
        
        # Виводимо загальну статистику перед аналізом (тільки якщо включене детальне логування)
        if CONFIG.get('verbose_logging', False):
            total_bon_codes = sum(len(codes) for codes in all_codes_by_amount.values())
            total_amounts = len(all_codes_by_amount)
            logger.info(f"📊 Загальна статистика: {total_bon_codes} промокодів для {total_amounts} різних сум")
            
            # Підрахунок промокодів по сумах (тільки активних)
            sums_with_codes = []
            for amount, code_objects in all_codes_by_amount.items():
                active_codes = [obj for obj in code_objects if obj['status'] == 'active']
                if active_codes:
                    sums_with_codes.append(amount)
            if sums_with_codes:
                min_sum = min(sums_with_codes) if sums_with_codes else 0
                max_sum = max(sums_with_codes) if sums_with_codes else 0
                logger.info(f"📊 Діапазон наявних сум: від {min_sum} грн до {max_sum} грн")
        
        for amount in range(start_amount, end_amount + 1):
            # Отримуємо коди для цієї суми з новою структурою
            code_objects = all_codes_by_amount.get(amount, [])
            
            # Фільтруємо тільки активні промокоди для підрахунку
            active_codes = [obj['code'] for obj in code_objects if obj['status'] == 'active']
            inactive_codes = [obj['code'] for obj in code_objects if obj['status'] == 'inactive']
            
            current_count_active = len(active_codes)
            current_count_inactive = len(inactive_codes)
            current_count_total = len(code_objects)
            
            # Пропускаємо суми, які більші за end_amount, але могли потрапити через неточне сортування
            if amount > end_amount:
                logger.info(f"💰 Пропускаємо суму {amount} грн, оскільки вона більша за максимальну ({end_amount} грн)")
                continue

            logger.info(f"💰 Аналіз суми {amount} грн: {current_count_active} активних + {current_count_inactive} неактивних = {current_count_total} (цільова к-ть: {target_count})")
            
            # Видаляємо всі неактивні промокоди
            if inactive_codes:
                logger.info(f"�️ Додаємо {len(inactive_codes)} неактивних кодів до списку на видалення")
                if amount not in codes_to_delete_by_amount:
                    codes_to_delete_by_amount[amount] = []
                codes_to_delete_by_amount[amount].extend(inactive_codes)
            
            # Аналізуємо активні промокоди
            if current_count_active == target_count:
                logger.info("✅ Кількість активних промокодів оптимальна - створення не потрібно")
                total_operations['unchanged'] += 1
                
            elif current_count_active < target_count:
                # Потрібно додати коди
                needed_count = target_count - current_count_active
                logger.info(f"📈 Потрібно ДОДАТИ {needed_count} активних промокодів")
                
                # Генеруємо нові коди для цієї суми
                new_codes = generate_codes_for_amount(amount, needed_count, active_codes)
                if new_codes:
                    codes_to_create_by_amount[amount] = new_codes
                    
            elif current_count_active > target_count and auto_delete:
                # Потрібно видалити зайві активні коди
                excess_count = current_count_active - target_count
                logger.info(f"📉 Потрібно ВИДАЛИТИ {excess_count} зайвих активних промокодів")
                
                # Додаємо коди для видалення (беремо перші зайві)
                codes_to_delete = active_codes[:excess_count]
                if amount not in codes_to_delete_by_amount:
                    codes_to_delete_by_amount[amount] = []
                codes_to_delete_by_amount[amount].extend(codes_to_delete)
                
            elif current_count_active > target_count and not auto_delete:
                excess_count = current_count_active - target_count
                logger.warning(f"⚠️ Зайві {excess_count} активних промокодів (автовидалення відключено)")
            
            # Зберігаємо тільки активні коди для S3
            final_s3_state[str(amount)] = active_codes.copy()
        
        # ЕТАП 3: Створення всіх нових промокодів для всіх сум
        total_to_create = sum(len(codes) for codes in codes_to_create_by_amount.values())
        if total_to_create > 0:
            logger.info("\n➕ ЕТАП 3: Створення всіх нових промокодів...")
            logger.info(f"📝 Всього потрібно створити: {total_to_create} промокодів для {len(codes_to_create_by_amount)} сум")
            
            for amount, new_codes in sorted(codes_to_create_by_amount.items()):
                logger.info(f"\n💰 Створення промокодів для суми {amount} грн ({len(new_codes)} шт.)...")
                created_count = create_codes_for_amount(promo_service, new_codes, amount)
                total_operations['created'] += created_count
                
                if created_count > 0:
                    # Оновлюємо стан в пам'яті
                    successfully_created = new_codes[:created_count]
                    final_s3_state[str(amount)].extend(successfully_created)
                    logger.info(f"✅ Створено {created_count}/{len(new_codes)} промокодів")
                else:
                    logger.warning(f"⚠️ Не вдалося створити жодного промокоду для суми {amount} грн")
        else:
            logger.info("\n➕ ЕТАП 3: Створення промокодів - пропускаємо (немає потреби)")
        
        # ЕТАП 4: Видалення надлишкових промокодів для всіх сум
        total_to_delete = sum(len(codes) for codes in codes_to_delete_by_amount.values())
        if total_to_delete > 0 and auto_delete:
            logger.info("\n➖ ЕТАП 4: Видалення надлишкових промокодів...")
            logger.info(f"🗑️ Всього потрібно видалити: {total_to_delete} промокодів для {len(codes_to_delete_by_amount)} сум")
            
            # Оновлюємо iframe для видалення
            iframe = promo_service._get_iframe()
            if not iframe:
                logger.error("❌ Не вдалося отримати iframe для видалення")
            else:
                # Перезастосовуємо BON фільтр для видалення
                bon_filter_success = apply_bon_filter_once(iframe)
                if not bon_filter_success:
                    logger.error("❌ Не вдалося застосувати BON фільтр перед видаленням")
                else:
                    for amount, codes_to_delete in sorted(codes_to_delete_by_amount.items()):
                        logger.info(f"\n💰 Видалення промокодів для суми {amount} грн ({len(codes_to_delete)} шт.)...")
                        
                        # Застосовуємо фільтр для цієї суми
                        filter_success = apply_amount_filter_improved(iframe, amount)
                        if filter_success:
                            # Спочатку перевіряємо, які коди дійсно є в таблиці
                            logger.info("🔍 Перевірка наявності кодів в таблиці перед видаленням...")
                            
                            # Оновлюємо дані з таблиці
                            time.sleep(0.5)
                            fresh_codes_dict = get_all_bon_codes_from_table(iframe)
                            
                            if fresh_codes_dict and amount in fresh_codes_dict:
                                code_objects = fresh_codes_dict[amount]
                                # Отримуємо всі коди (активні та неактивні) для фільтрації
                                actual_codes_for_amount = [obj['code'] for obj in code_objects]
                                logger.info(f"🔄 Знайдено {len(actual_codes_for_amount)} актуальних кодів в таблиці")
                            else:
                                actual_codes_for_amount = []
                                logger.warning(f"⚠️ Не знайдено кодів для суми {amount} в актуальній таблиці")
                            
                            # Фільтруємо коди для видалення - тільки ті, що є в таблиці
                            codes_to_delete_filtered = [code for code in codes_to_delete if code in actual_codes_for_amount]
                            
                            if not codes_to_delete_filtered:
                                logger.warning("⚠️ Жодного коду для видалення не знайдено в поточній таблиці")
                                logger.info("💡 Можливо, коди вже були видалені раніше або кеш застарів")
                                
                                # Оновлюємо final_s3_state актуальними даними
                                final_s3_state[str(amount)] = actual_codes_for_amount
                                logger.info(f"📊 Стан оновлено актуальними даними: {len(actual_codes_for_amount)} кодів")
                            else:
                                logger.info(f"🎯 Знайдено {len(codes_to_delete_filtered)} кодів для видалення з {len(codes_to_delete)} запланованих")
                                
                                # Отримуємо page з promo_service для обробки діалогів
                                page = promo_service.page
                                deletion_result = delete_specific_promo_codes(iframe, codes_to_delete_filtered, page)
                                
                                if deletion_result.get('success', False):
                                    deleted_count = deletion_result.get('deleted', 0)
                                    total_operations['deleted'] += deleted_count
                                    
                                    # Оновлюємо стан в пам'яті
                                    successfully_deleted_set = set(codes_to_delete_filtered[:deleted_count])
                                    final_s3_state[str(amount)] = [code for code in actual_codes_for_amount 
                                                                 if code not in successfully_deleted_set]
                                    
                                    logger.info(f"✅ Видалено {deleted_count}/{len(codes_to_delete_filtered)} промокодів")
                                else:
                                    logger.error(f"❌ Помилка видалення промокодів для суми {amount} грн")
                                    # Все одно оновлюємо стан актуальними даними
                                    final_s3_state[str(amount)] = actual_codes_for_amount
                        else:
                            logger.error(f"❌ Не вдалося застосувати фільтр для видалення промокодів суми {amount} грн")
        elif total_to_delete > 0 and not auto_delete:
            logger.info("\n➖ ЕТАП 4: Видалення промокодів - пропускаємо (автовидалення відключено)")
        else:
            logger.info("\n➖ ЕТАП 4: Видалення промокодів - пропускаємо (немає потреби)")

        # Підсумок операцій
        logger.info(f"\n🎉 ПІДСУМОК ОПЕРАЦІЙ")
        logger.info("=" * 40)
        logger.info(f"➕ Створено: {total_operations['created']} промокодів")
        logger.info(f"➖ Видалено: {total_operations['deleted']} промокодів")
        logger.info(f"⏹️ Без змін: {total_operations['unchanged']} сум")
        
        # Синхронізація з S3 (завжди, якщо включена)
        changes_made = total_operations['created'] > 0 or total_operations['deleted'] > 0
        if CONFIG['sync_s3']:
            logger.info("\n🔄 СИНХРОНІЗАЦІЯ ФІНАЛЬНОГО СТАНУ З S3")
            logger.info("-" * 30)
            
            if changes_made:
                logger.info("📝 Були зміни - оновлюємо S3 з новими даними")
            else:
                logger.info("📝 Змін не було, але перевіряємо та оновлюємо S3 актуальними даними")
            
            # Завантажуємо поточний стан з S3, щоб не втратити дані по іншим сумам
            full_s3_state = download_from_s3()
            
            # Оновлюємо тільки ті суми, які ми обробляли
            full_s3_state.update(final_s3_state)

            sync_success = upload_to_s3(full_s3_state)
            if sync_success:
                logger.info("✅ Синхронізація з S3 завершена успішно")
            else:
                logger.error("❌ Помилка синхронізації з S3")
        else:
            logger.info("\n🔄 СИНХРОНІЗАЦІЯ З S3 - відключена в конфігурації")
        
        logger.info("\n🎯 СМАРТ УПРАВЛІННЯ ЗАВЕРШЕНО УСПІШНО!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Критична помилка в смарт управлінні: {e}")
        return False
        
    finally:
        if browser_manager:
            logger.info("🧹 Закриття браузера...")
            browser_manager.cleanup()

def generate_codes_for_amount(amount, count, existing_codes):
    """
    Генерує нові промокоди для конкретної суми.
    
    Args:
        amount (int): Сума промокоду
        count (int): Кількість кодів для генерації
        existing_codes (list): Існуючі коди для уникнення дублікатів
        
    Returns:
        list: Список згенерованих промокодів
    """
    logger.info(f"🎲 Генерація {count} нових промокодів для суми {amount} грн...")
    
    amount_str = str(amount)
    prefix = f'BON{amount_str}'
    random_part_length = max(3, 7 - len(amount_str))
    
    # Створюємо set з існуючих кодів для швидкої перевірки
    existing_codes_set = set(existing_codes) if existing_codes else set()
    
    new_codes = set()
    attempts = 0
    max_attempts = count * 100  # Збільшена кількість спроб
    
    while len(new_codes) < count and attempts < max_attempts:
        new_code = prefix + generate_random_string(random_part_length)
        
        # Перевіряємо на дублікати
        if new_code not in existing_codes_set and new_code not in new_codes:
            new_codes.add(new_code)
        
        attempts += 1
    
    result = list(new_codes)
    logger.info(f"✅ Згенеровано {len(result)} унікальних промокодів")
    
    if result:
        logger.debug(f"Приклади нових кодів: {result[:3]}{'...' if len(result) > 3 else ''}")
    
    return result


def create_codes_for_amount(promo_service, codes_list, amount):
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

def upload_to_s3(data):
    """Завантажує дані в S3."""
    try:
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
        return True

    except Exception as e:
        logger.error(f"❌ Помилка при завантаженні в S3: {e}")
        logger.error("Перевірте ваші AWS креданшали та налаштування")
        return False

def set_max_rows_per_page(iframe, rows_count=160):
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

def sort_table_by_discount_amount(iframe, sort_order="asc"):
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
                if not apply_amount_filter_improved(iframe, amount):
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


def download_from_s3():
    """Завантажує поточні промокоди з S3."""
    try:
        import boto3
        s3 = boto3.client('s3', region_name=CONFIG['region'])
        bucket = CONFIG['s3_bucket']
        key = CONFIG['s3_key']
        
        logger.info(f"☁️ Завантажуємо поточні промокоди з S3: s3://{bucket}/{key}")
        
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        logger.info(f"✅ Завантажено промокоди з S3")
        return data
        
    except Exception as e:
        logger.warning(f"⚠️ Не вдалося завантажити з S3: {e}")
        logger.info("📝 Повертаємо порожню структуру")
        return {}



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
                logger.error("❌ [HEADLESS] Не вдалося знайти способ видалення")
                return False
        
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
    Основна функція для запуску генератора промокодів.
    Тепер підтримує паралельний режим роботи.
    """
    # Перевіряємо чи потрібно запускати в паралельному режимі
    parallel_mode = CONFIG.get('parallel_processes', 1) > 1
    
    if parallel_mode:
        logger.info("🔄 Запуск у ПАРАЛЕЛЬНОМУ режимі")
        return parallel_promo_management()
    else:
        logger.info("📝 Запуск у ЗВИЧАЙНОМУ режимі")
        return smart_promo_management_main()

if __name__ == "__main__":
    # Налаштування для multiprocessing на macOS
    multiprocessing.set_start_method('spawn', force=True)
    main()
