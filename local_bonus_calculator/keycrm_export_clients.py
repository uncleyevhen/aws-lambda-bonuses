#!/usr/bin/env python3
"""
Багатопроцесорний скрипт для експорту даних клієнтів з KeyCRM у CSV файл
Витягує ім'я, прізвище, емейл, телефон, загальну суму замовлень
"""

import requests
import json
import time
import logging
from datetime import datetime, timezone
import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp
import math
import os
import sys
import threading

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('client_export_multiprocess.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KeyCRMClientExporter:
    def __init__(self, api_key, proxy_config=None):
        self.api_key = api_key
        self.base_url = "https://openapi.keycrm.app/v1"
        
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Конфігурація проксі
        self.proxy_config = proxy_config
        self.proxy_enabled = proxy_config is not None
        
        if self.proxy_enabled and proxy_config:
            logger.info(f"🌐 Увімкнено проксі: {proxy_config.get('host', 'unknown')}:{proxy_config.get('port', 'unknown')}")
            self.rate_limit_delay = 0.1  # 600 запитів/хвилину
        else:
            logger.info("⚠️ Проксі не налаштовано")
            self.rate_limit_delay = 0.3  # 200 запитів/хвилину
    
    def make_request(self, url, max_retries=5):
        """Виконання запиту з обробкою помилок та підтримкою проксі"""
        full_url = f"{self.base_url}{url}"
        
        for attempt in range(max_retries):
            response = None
            session = requests.Session()
            
            try:
                # Налаштовуємо проксі якщо увімкнено
                if self.proxy_enabled and self.proxy_config:
                    proxy_url = f"http://{self.proxy_config['username']}:{self.proxy_config['password']}@{self.proxy_config['host']}:{self.proxy_config['port']}"
                    session.proxies = {
                        'http': proxy_url,
                        'https': proxy_url
                    }
                
                # Виконуємо запит
                response = session.get(full_url, headers=self.headers, timeout=20)
                
                if response is None:
                    raise ValueError("Не вдалося отримати відповідь від сервера")
                
                response.raise_for_status()
                
                # Мінімальна пауза
                time.sleep(self.rate_limit_delay)
                
                return response.json()
                
            except (requests.exceptions.ProxyError, 
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout) as proxy_error:
                
                if attempt < max_retries - 1:
                    # Для проксі помилок робимо довшу паузу
                    wait_time = min(2 ** attempt, 10)  # Експоненціальна затримка до 10 сек
                    logger.warning(f"Проксі помилка {attempt + 1}/{max_retries} для {full_url}: {proxy_error}")
                    logger.info(f"Чекаємо {wait_time} секунд перед повторною спробою...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Проксі помилка після {max_retries} спроб для {full_url}: {proxy_error}")
                    raise
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 1 + attempt  # Лінійна затримка для інших помилок
                    logger.warning(f"Спроба {attempt + 1}/{max_retries} невдала для {full_url}: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Помилка запиту до {full_url} після {max_retries} спроб: {e}")
                    raise
            finally:
                session.close()
    
    def get_total_pages(self, limit=50):
        """Отримання загальної кількості сторінок"""
        try:
            response = self.make_request(f"/buyer?page=1&limit={limit}")
            
            if response is None:
                logger.error("Не вдалося отримати відповідь від API")
                return 0, 0
            
            total_records = response.get('total', 0)
            total_pages = response.get('last_page', 0)
            
            logger.info(f"📊 Загальна кількість клієнтів: {total_records}")
            logger.info(f"📄 Загальна кількість сторінок (limit={limit}): {total_pages}")
            
            return total_pages, total_records
            
        except Exception as e:
            logger.error(f"Помилка отримання загальної кількості сторінок: {e}")
            return 0, 0

def extract_client_data(buyer):
    """Витягує потрібні дані клієнта"""
    buyer_id = buyer.get('id', '')
    full_name = buyer.get('full_name', '')
    
    # Розділяємо ім'я та прізвище
    name_parts = full_name.split(' ', 1) if full_name else ['', '']
    first_name = name_parts[0] if len(name_parts) > 0 else ''
    last_name = name_parts[1] if len(name_parts) > 1 else ''
    
    # Отримуємо емейли та телефони
    emails = buyer.get('email', [])
    phones = buyer.get('phone', [])
    
    # Конвертуємо в рядки
    email_str = ', '.join(emails) if isinstance(emails, list) else str(emails) if emails else ''
    phone_str = ', '.join(phones) if isinstance(phones, list) else str(phones) if phones else ''
    
    # Отримуємо фінансові дані
    orders_sum = float(buyer.get('orders_sum', 0) or 0)
    orders_count = int(buyer.get('orders_count', 0) or 0)
    
    # Дата створення
    created_at = buyer.get('created_at', '')
    
    return {
        'id': buyer_id,
        'first_name': first_name,
        'last_name': last_name,
        'full_name': full_name,
        'email': email_str,
        'phone': phone_str,
        'orders_sum': orders_sum,
        'orders_count': orders_count,
        'created_at': created_at
    }

def progress_monitor(global_counter, lock, total_records, start_time, stop_event):
    """Моніторинг загального прогресу"""
    while not stop_event.is_set():
        try:
            time.sleep(30)  # Оновлення кожні 30 секунд
            if stop_event.is_set():
                break
                
            with lock:
                current_count = global_counter.value
            
            if current_count > 0:
                progress = (current_count / total_records) * 100 if total_records > 0 else 0
                elapsed = (datetime.now() - start_time).total_seconds()
                speed = current_count / elapsed if elapsed > 0 else 0
                
                if total_records > current_count:
                    remaining = total_records - current_count
                    eta_seconds = remaining / speed if speed > 0 else 0
                    eta_minutes = eta_seconds / 60
                    
                    logger.info(f"⏱️  ПОТОЧНИЙ ПРОГРЕС: {current_count}/{total_records} ({progress:.1f}%) | Швидкість: {speed:.1f} кл/сек | ETA: ~{eta_minutes:.1f} хв")
                else:
                    logger.info(f"⏱️  ПОТОЧНИЙ ПРОГРЕС: {current_count}/{total_records} ({progress:.1f}%) | Швидкість: {speed:.1f} кл/сек")
                    
        except Exception as e:
            logger.error(f"Помилка в моніторингу прогресу: {e}")
            break

def process_page_range_export(args):
    """
    Функція для обробки діапазону сторінок в окремому процесі (експорт)
    """
    start_page, end_page, api_key, proxy_config, process_id, total_records, global_counter, lock = args
    
    # Налаштовуємо логування для процесу
    logger = logging.getLogger(f"Process-{process_id}")
    
    # Створюємо експортер для цього процесу
    exporter = KeyCRMClientExporter(api_key, proxy_config)
    
    logger.info(f"� Процес {process_id}: старт експорту сторінок {start_page}-{end_page} (всього {end_page - start_page + 1} сторінок)")
    
    exported_clients = []
    total_processed = 0
    
    # Локальний лічильник клієнтів цього процесу
    local_client_count = 0
    
    try:
        for page in range(start_page, end_page + 1):
            try:
                # Показуємо прогрес для кожної сторінки
                pages_done = page - start_page + 1
                pages_total = end_page - start_page + 1
                if pages_done % 10 == 1:  # Показуємо кожну 10-ту сторінку
                    progress_percent = (pages_done / pages_total) * 100
                    logger.info(f"📄 P{process_id}: експортуємо сторінку {page} ({pages_done}/{pages_total}, {progress_percent:.1f}%)")
                
                # Завантажуємо сторінку клієнтів
                response = exporter.make_request(f"/buyer?page={page}&limit=50")
                
                if response is None:
                    logger.warning(f"❌ Не вдалося отримати дані для сторінки {page} в процесі {process_id}")
                    continue
                
                buyers = response.get('data', [])
                
                if not buyers:
                    logger.info(f"❌ Сторінка {page} порожня в процесі {process_id}")
                    continue
                
                # Логуємо кількість клієнтів на сторінці якщо вона відрізняється від очікуваної
                if len(buyers) != 50:
                    logger.info(f"📄 P{process_id}: сторінка {page} містить {len(buyers)} клієнтів (очікувалось 50)")
                
                # Обробляємо клієнтів
                for buyer in buyers:
                    local_client_count += 1
                    
                    # Оновлюємо глобальний лічильник
                    with lock:
                        global_counter.value += 1
                        current_global_count = global_counter.value
                    
                    client_data = extract_client_data(buyer)
                    exported_clients.append(client_data)
                    
                    # Логуємо кожного 1000-го клієнта ГЛОБАЛЬНО
                    if current_global_count % 1000 == 0:
                        global_progress = (current_global_count / total_records) * 100 if total_records > 0 else 0
                        logger.info(f"🌍 ЗАГАЛЬНИЙ ПРОГРЕС: {current_global_count}/{total_records} клієнтів ({global_progress:.1f}%) [P{process_id}: {local_client_count} локально]")
                
                total_processed += len(buyers)
                
                # Логуємо прогрес кожні 10 сторінок
                if page % 10 == 0:
                    pages_done = page - start_page + 1
                    pages_total = end_page - start_page + 1
                    progress_percent = (pages_done / pages_total) * 100
                    
                    # Отримуємо поточний глобальний прогрес
                    with lock:
                        current_global_count = global_counter.value
                    global_progress = (current_global_count / total_records) * 100 if total_records > 0 else 0
                    
                    logger.info(f"📊 P{process_id}: сторінка {page}/{end_page} ({progress_percent:.1f}%), експортовано {local_client_count} клієнтів в цьому процесі. Загальний прогрес: {current_global_count}/{total_records} ({global_progress:.1f}%)")
                
            except Exception as e:
                logger.error(f"❌ P{process_id}: Помилка при обробці сторінки {page}: {e}")
                
                # Якщо це проксі помилка, спробуємо без проксі
                if "ProxyError" in str(e) or "Connection refused" in str(e):
                    logger.warning(f"🔄 P{process_id}: Проксі недоступний, спробуємо без проксі для сторінки {page}")
                    try:
                        # Тимчасово відключаємо проксі
                        exporter.proxy_enabled = False
                        exporter.rate_limit_delay = 0.3
                        
                        # Повторюємо запит без проксі
                        response = exporter.make_request(f"/buyer?page={page}&limit=50")
                        
                        if response and response.get('data'):
                            logger.info(f"✅ P{process_id}: Успішно завантажили сторінку {page} без проксі")
                            buyers = response.get('data', [])
                            for buyer in buyers:
                                local_client_count += 1
                                
                                # Оновлюємо глобальний лічильник
                                with lock:
                                    global_counter.value += 1
                                    current_global_count = global_counter.value
                                
                                client_data = extract_client_data(buyer)
                                exported_clients.append(client_data)
                            total_processed += len(buyers)
                        
                        # Відновлюємо проксі
                        exporter.proxy_enabled = proxy_config is not None
                        exporter.rate_limit_delay = 0.3
                        
                    except Exception as fallback_error:
                        logger.error(f"❌ P{process_id}: Помилка навіть без проксі для сторінки {page}: {fallback_error}")
                
                continue
    
    except Exception as e:
        logger.error(f"❌ P{process_id}: Критична помилка: {e}")
    
    pages_processed = end_page - start_page + 1
    
    # Отримуємо фінальне значення глобального лічильника
    with lock:
        final_global_count = global_counter.value
    
    logger.info(f"✅ Процес {process_id} завершено: {pages_processed} сторінок, {len(exported_clients)} клієнтів з цього процесу. Загальний прогрес: {final_global_count}/{total_records}")
    
    return {
        'process_id': process_id,
        'start_page': start_page,
        'end_page': end_page,
        'exported_clients': exported_clients,
        'total_processed': total_processed
    }

def export_all_clients_multiprocess(exporter, num_processes=10, test_mode=False):
    """
    Багатопроцесорний експорт всіх клієнтів
    """
    logger.info("=" * 80)
    logger.info("📤 БАГАТОПРОЦЕСОРНИЙ ЕКСПОРТ КЛІЄНТІВ З KEYCRM")
    logger.info("=" * 80)
    
    try:
        # Отримуємо загальну кількість сторінок
        total_pages, total_records = exporter.get_total_pages(limit=50)
        
        if total_pages == 0:
            logger.error("❌ Не вдалося отримати кількість сторінок")
            return []
        
        # В тестовому режимі обробляємо тільки перші 5 сторінок
        if test_mode:
            total_pages = min(5, total_pages)
            logger.info(f"🧪 ТЕСТОВИЙ РЕЖИМ: експортуємо тільки {total_pages} сторінок")
        
        logger.info(f"📊 Загальна кількість сторінок: {total_pages}")
        logger.info(f"🔄 Кількість процесів: {num_processes}")
        
        # Розподіляємо сторінки між процесами
        pages_per_process = math.ceil(total_pages / num_processes)
        process_ranges = []
        
        for i in range(num_processes):
            start_page = i * pages_per_process + 1
            end_page = min((i + 1) * pages_per_process, total_pages)
            
            if start_page <= total_pages:
                process_ranges.append((start_page, end_page, i + 1))
        
        logger.info("📋 Розподіл сторінок між процесами:")
        for start_page, end_page, process_id in process_ranges:
            logger.info(f"   Процес {process_id}: сторінки {start_page}-{end_page} ({end_page-start_page+1} сторінок)")
        
        # Готуємо аргументи для процесів з спільним лічильником
        manager = mp.Manager()
        global_counter = manager.Value('i', 0)  # Спільний лічильник клієнтів
        lock = manager.Lock()  # Блокування для синхронізації
        
        process_args = [
            (start_page, end_page, exporter.api_key, exporter.proxy_config, process_id, total_records, global_counter, lock)
            for start_page, end_page, process_id in process_ranges
        ]
        
        # Запускаємо багатопроцесорний експорт
        start_time = datetime.now()
        last_progress_time = start_time
        
        logger.info(f"🚀 Запускаємо {len(process_args)} процесів...")
        
        # Запускаємо монітор прогресу
        stop_monitor = threading.Event()
        monitor_thread = threading.Thread(
            target=progress_monitor, 
            args=(global_counter, lock, total_records, start_time, stop_monitor)
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        
        all_clients = []
        total_exported = 0
        completed_processes = 0
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            # Подаємо завдання
            future_to_process = {
                executor.submit(process_page_range_export, args): args[4]  # process_id
                for args in process_args
            }
            
            # Збираємо результати
            for future in as_completed(future_to_process):
                process_id = future_to_process[future]
                
                try:
                    result = future.result()
                    
                    # Об'єднуємо результати
                    all_clients.extend(result['exported_clients'])
                    total_exported += result['total_processed']
                    completed_processes += 1
                    
                    # Показуємо загальний прогрес
                    overall_progress = (completed_processes / len(process_args)) * 100
                    total_clients_so_far = len(all_clients)
                    client_progress = (total_clients_so_far / total_records) * 100 if total_records > 0 else 0
                    logger.info(f"✅ Процес {result['process_id']} завершив роботу (+{len(result['exported_clients'])} клієнтів). Процесів: {completed_processes}/{len(process_args)} ({overall_progress:.1f}%), клієнтів: {total_clients_so_far}/{total_records} ({client_progress:.1f}%)")
                    
                except Exception as e:
                    completed_processes += 1
                    overall_progress = (completed_processes / len(process_args)) * 100
                    logger.error(f"❌ Процес {process_id} завершився з помилкою: {e}. Прогрес: {completed_processes}/{len(process_args)} ({overall_progress:.1f}%)")
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Зупиняємо монітор прогресу
        stop_monitor.set()
        monitor_thread.join(timeout=1)
        
        # Фінальна статистика
        logger.info("=" * 80)
        logger.info("РЕЗУЛЬТАТИ БАГАТОПРОЦЕСОРНОГО ЕКСПОРТУ:")
        logger.info(f"Процесів запущено: {len(process_args)}")
        logger.info(f"Сторінок оброблено: {total_pages}")
        logger.info(f"Клієнтів експортовано: {len(all_clients)}")
        logger.info(f"Час виконання: {execution_time:.1f} секунд")
        if len(all_clients) > 0:
            logger.info(f"Швидкість: {len(all_clients)/execution_time:.1f} клієнтів/сек")
        logger.info("=" * 80)
        
        return all_clients
        
    except Exception as e:
        logger.error(f"❌ Критична помилка багатопроцесорного експорту: {e}")
        return []

def save_clients_to_csv(clients_data, filename=None):
    """Збереження даних клієнтів у CSV файл"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"keycrm_clients_export_{timestamp}.csv"
    
    # Переконуємося що файл має розширення .csv
    if not filename.endswith('.csv'):
        filename += '.csv'
    
    try:
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # Заголовки
            writer.writerow([
                'ID клієнта',
                'Ім\'я',
                'Прізвище',
                'Повне ім\'я',
                'Email',
                'Телефон',
                'Загальна сума замовлень (₴)',
                'Кількість замовлень',
                'Дата створення'
            ])
            
            # Сортуємо за загальною сумою замовлень (від найбільшої до найменшої)
            sorted_clients = sorted(
                clients_data, 
                key=lambda x: x.get('orders_sum', 0), 
                reverse=True
            )
            
            # Записуємо дані
            for client in sorted_clients:
                writer.writerow([
                    client.get('id', ''),
                    client.get('first_name', ''),
                    client.get('last_name', ''),
                    client.get('full_name', ''),
                    client.get('email', ''),
                    client.get('phone', ''),
                    f"{client.get('orders_sum', 0):.2f}",
                    client.get('orders_count', 0),
                    client.get('created_at', '')
                ])
        
        logger.info(f"📄 CSV файл збережено: {filename}")
        logger.info(f"📊 Експортовано {len(clients_data)} клієнтів")
        
        # Показуємо статистику
        if clients_data:
            total_sum = sum(client.get('orders_sum', 0) for client in clients_data)
            total_orders = sum(client.get('orders_count', 0) for client in clients_data)
            clients_with_orders = len([c for c in clients_data if c.get('orders_sum', 0) > 0])
            
            logger.info(f"💰 Загальна сума всіх замовлень: {total_sum:,.2f}₴")
            logger.info(f"📦 Загальна кількість замовлень: {total_orders:,}")
            logger.info(f"🛍️ Клієнтів з замовленнями: {clients_with_orders}/{len(clients_data)}")
            
            if clients_with_orders > 0:
                avg_order_value = total_sum / total_orders if total_orders > 0 else 0
                avg_sum_per_client = total_sum / clients_with_orders
                logger.info(f"💵 Середня вартість замовлення: {avg_order_value:.2f}₴")
                logger.info(f"👤 Середня сума на клієнта: {avg_sum_per_client:.2f}₴")
        
        return filename
        
    except Exception as e:
        logger.error(f"❌ Помилка збереження CSV файлу: {e}")
        return None

def load_config():
    """Завантаження конфігурації"""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        return {
            "api_key": "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ",
            "proxy": {
                "enabled": True,
                "host": "p.webshare.io",
                "port": 80,
                "username": "oxfwmgn-UA-rotate",
                "password": "drci27S4eayj"
            }
        }

def main():
    """Головна функція"""
    print("📤 ЕКСПОРТ КЛІЄНТІВ З KEYCRM У CSV")
    print("=" * 80)
    
    # Завантажуємо конфігурацію
    config = load_config()
    
    # Налаштовуємо проксі
    proxy_config = None
    if config.get('proxy', {}).get('enabled', False):
        proxy_config = config['proxy']
        print(f"🌐 Проксі: {proxy_config['host']}:{proxy_config['port']}")
    
    # Визначаємо кількість процесів
    if proxy_config:
        num_processes = min(mp.cpu_count(), 5)  # Максимум 5 процесів з проксі
    else:
        num_processes = min(mp.cpu_count(), 2)  # Максимум 2 процеси без проксі
    print(f"🔄 Кількість процесів: {num_processes}")
    
    # Створюємо експортер
    exporter = KeyCRMClientExporter(
        api_key=config['api_key'],
        proxy_config=proxy_config
    )
    
    try:
        start_time = datetime.now()
        
        # Виконуємо багатопроцесорний експорт
        clients_data = export_all_clients_multiprocess(
            exporter=exporter,
            num_processes=num_processes,
            test_mode=False  # Всі клієнти
        )
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if clients_data:
            # Зберігаємо у CSV файл
            csv_filename = save_clients_to_csv(clients_data)
            
            if csv_filename:
                print(f"\n✅ Експорт завершено успішно!")
                print(f"📄 Файл: {csv_filename}")
                print(f"👥 Експортовано клієнтів: {len(clients_data)}")
                print(f"⏱️  Час виконання: {execution_time:.1f} секунд")
                print(f"🚀 Швидкість: {len(clients_data)/execution_time:.1f} клієнтів/сек")
                
                # Показуємо топ-10 клієнтів за сумою замовлень
                top_clients = sorted(
                    [c for c in clients_data if c.get('orders_sum', 0) > 0],
                    key=lambda x: x.get('orders_sum', 0),
                    reverse=True
                )[:10]
                
                if top_clients:
                    print(f"\n🏆 ТОП-{len(top_clients)} КЛІЄНТІВ ЗА СУМОЮ ЗАМОВЛЕНЬ:")
                    print("-" * 80)
                    for i, client in enumerate(top_clients, 1):
                        name = client.get('full_name', 'Невідомо')
                        orders_sum = client.get('orders_sum', 0)
                        orders_count = client.get('orders_count', 0)
                        print(f"{i:2d}. {name:<30} {orders_sum:>10.2f}₴ ({orders_count} замовлень)")
            else:
                print("\n❌ Помилка збереження файлу")
        else:
            print("\n❌ Не вдалося експортувати дані клієнтів")
        
    except KeyboardInterrupt:
        print("\n❌ Операцію перервано користувачем")
    except Exception as e:
        logger.error(f"Критична помилка: {e}")
        print(f"\n❌ Помилка: {e}")

if __name__ == "__main__":
    # Налаштування для multiprocessing
    mp.set_start_method('spawn', force=True)
    main()
