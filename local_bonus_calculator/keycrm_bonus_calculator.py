#!/usr/bin/env python3
"""
Багатопроцесорний скрипт для розрахунку бонусів клієнтам KeyCRM
Використовує multiprocessing для паралельної обробки сторінок
"""

import requests
import json
import time
from decimal import Decimal
import logging
from datetime import datetime, timezone
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp
import math
import os
import sys

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bonus_calculation_multiprocess.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KeyCRMBonusCalculator:
    def __init__(self, api_key, bonus_percentage=7.0, proxy_config=None):
        self.api_key = api_key
        self.base_url = "https://openapi.keycrm.app/v1"
        self.bonus_percentage = bonus_percentage
        
        # UUID кастомних полів KeyCRM
        self.BONUS_FIELD_UUID = "CT_1023"  # Поле "Бонусні бали"
        self.HISTORY_FIELD_UUID = "CT_1033"  # Поле "Історія бонусів"
        self.BONUS_EXPIRY_FIELD_UUID = "CT_1024"  # Поле "Дата закінчення бонусів"
        
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
            self.rate_limit_delay = 0.3  # 200 запитів/хвилину = 3.33/сек = 0.2сек затримка
        else:
            logger.info("⚠️ Проксі не налаштовано")
            self.rate_limit_delay = 0.3  # 200 запитів/хвилину = 3.33/сек = 0.3сек затримка
    
    def make_request(self, url, method="GET", data=None, max_retries=5):
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
                if method == "GET":
                    response = session.get(full_url, headers=self.headers, timeout=20)
                elif method == "PUT":
                    response = session.put(full_url, headers=self.headers, json=data, timeout=20)
                else:
                    raise ValueError(f"Непідтримуваний метод: {method}")
                
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
    
    def get_current_bonuses(self, buyer):
        """Отримання поточних бонусів з кастомних полів за UUID"""
        custom_fields = buyer.get('custom_fields', [])
        
        for field in custom_fields:
            if field.get('uuid') == self.BONUS_FIELD_UUID:
                try:
                    return int(field.get('value', 0) or 0)
                except (ValueError, TypeError):
                    return 0
        
        return 0
    
    def get_current_expiry_date(self, buyer):
        """Отримання поточної дати закінчення бонусів з кастомних полів за UUID"""
        custom_fields = buyer.get('custom_fields', [])
        
        for field in custom_fields:
            if field.get('uuid') == self.BONUS_EXPIRY_FIELD_UUID:
                return field.get('value', '')
        
        return ''

def process_page_range(args):
    """
    Функція для обробки діапазону сторінок в окремому процесі
    """
    start_page, end_page, api_key, bonus_percentage, proxy_config, dry_run, process_id, total_records, search_config = args
    
    # Налаштовуємо логування для процесу
    logger = logging.getLogger(f"Process-{process_id}")
    
    # Створюємо калькулятор для цього процесу
    calculator = KeyCRMBonusCalculator(api_key, bonus_percentage, proxy_config)
    
    # Конфігурація пошуку
    search_mode = search_config.get('enabled', False)
    only_search = search_config.get('only_search', False)
    min_bonus_amount = search_config.get('min_bonus_amount', 3000)
    
    if search_mode and only_search:
        logger.info(f"🔍 Процес {process_id}: РЕЖИМ ПОШУКУ - шукаємо клієнтів з бонусами ≥ {min_bonus_amount}")
    elif search_mode:
        logger.info(f"🔍 Процес {process_id}: ЗМІШАНИЙ РЕЖИМ - пошук клієнтів з бонусами ≥ {min_bonus_amount} + нарахування")
    else:
        logger.info(f"🚀 Процес {process_id}: обробляємо сторінки {start_page}-{end_page} (всього {end_page - start_page + 1} сторінок)")
    
    processed_customers = {}
    successful_updates = 0
    failed_updates = 0
    total_processed = 0
    
    # Глобальний лічильник клієнтів (наприклад, процес 1 починає з 0, процес 2 з ~12000, тощо)
    estimated_clients_per_page = 50  # Максимум клієнтів на сторінку
    global_client_start_index = (start_page - 1) * estimated_clients_per_page
    
    # Розраховуємо дату закінчення бонусів (до кінця літа 2025)
    expiry_date = datetime(2025, 8, 31, 23, 59, 59, tzinfo=timezone.utc)
    expiry_date_str = expiry_date.strftime('%Y-%m-%d')
    
    try:
        # Лічильник глобального прогресу
        current_global_client_index = global_client_start_index
        
        for page in range(start_page, end_page + 1):
            try:
                # Показуємо прогрес для кожної сторінки
                pages_done = page - start_page + 1
                pages_total = end_page - start_page + 1
                if pages_done % 5 == 1:  # Показуємо кожну 5-ту сторінку
                    progress_percent = (pages_done / pages_total) * 100
                    logger.info(f"📄 P{process_id}: обробляємо сторінку {page} ({pages_done}/{pages_total}, {progress_percent:.1f}%)")
                
                # Завантажуємо сторінку клієнтів
                response = calculator.make_request(
                    f"/buyer?page={page}&limit=50&include=custom_fields"
                )
                
                if response is None:
                    logger.warning(f"❌ Не вдалося отримати дані для сторінки {page} в процесі {process_id}")
                    continue
                
                buyers = response.get('data', [])
                
                if not buyers:
                    logger.info(f"❌ Сторінка {page} порожня в процесі {process_id}")
                    continue
                
                # Фільтруємо тільки покупців з замовленнями
                buyers_with_orders = [
                    buyer for buyer in buyers 
                    if buyer.get('orders_sum') and float(buyer.get('orders_sum', 0)) > 0
                ]
                
                page_updated = 0
                
                # Обробляємо клієнтів багатопоточно в межах процесу
                def update_client(buyer, local_client_index, total_clients_on_page):
                    nonlocal page_updated, current_global_client_index
                    
                    current_global_client_index += 1
                    buyer_id = buyer.get('id')
                    orders_sum = Decimal(str(buyer.get('orders_sum', 0)))
                    
                    # Показуємо глобальний прогрес відносно всіх клієнтів у системі
                    global_progress = (current_global_client_index / total_records) * 100
                    
                    # Перевіряємо поточні бонуси
                    current_bonuses = calculator.get_current_bonuses(buyer)
                    
                    # Якщо режим пошуку включений
                    if search_mode:
                        if current_bonuses >= min_bonus_amount:
                            logger.info(f"� P{process_id}: ЗНАЙДЕНО! {buyer.get('full_name', 'Невідомо')} має {current_bonuses} бонусів (≥{min_bonus_amount}) - клієнт {current_global_client_index}/{total_records}")
                            
                            # Зберігаємо дані про знайденого клієнта
                            processed_customers[str(buyer_id)] = {
                                'buyer_info': {
                                    'id': buyer_id,
                                    'full_name': buyer.get('full_name', ''),
                                    'email': buyer.get('email', []),
                                    'phone': buyer.get('phone', [])
                                },
                                'orders_count': buyer.get('orders_count', 0),
                                'total_amount': float(orders_sum),
                                'current_bonuses': current_bonuses,
                                'current_expiry_date': calculator.get_current_expiry_date(buyer),
                                'bonus_amount': 0,  # В режимі пошуку не рахуємо нові бонуси
                                'bonus_amount_int': 0,
                                'search_result': True
                            }
                            
                            # Якщо тільки пошук - пропускаємо нарахування
                            if only_search:
                                page_updated += 1
                                return True
                        else:
                            # Клієнт не відповідає критеріям пошуку
                            if current_global_client_index % 100 == 0:  # Логуємо кожного 100-го
                                logger.info(f"👤 P{process_id}: клієнт {current_global_client_index}/{total_records} ({global_progress:.1f}%) - {buyer.get('full_name', 'Невідомо')} ({current_bonuses} бонусів)")
                            
                            # Якщо тільки пошук і клієнт не підходить - пропускаємо
                            if only_search:
                                return False
                    else:
                        # Стандартний режим - показуємо прогрес
                        logger.info(f"�👤 P{process_id}: клієнт {current_global_client_index}/{total_records} ({global_progress:.1f}% загального прогресу) - {buyer.get('full_name', 'Невідомо')}")
                    
                    # Розраховуємо бонуси (тільки якщо не режим "тільки пошук")
                    if not (search_mode and only_search):
                        bonus_amount = (orders_sum * Decimal(str(calculator.bonus_percentage))) / Decimal('100')
                        bonus_amount_int = int(bonus_amount)
                        
                        if bonus_amount_int <= 0:
                            logger.info(f"❌ P{process_id}: {buyer.get('full_name', 'Невідомо')}: бонуси ≤ 0 ({bonus_amount_int}), пропускаємо - клієнт {current_global_client_index}/{total_records}")
                            return False
                        
                        # Перевіряємо поточну дату закінчення
                        current_expiry_date = calculator.get_current_expiry_date(buyer)
                        expected_expiry_date = "2025-08-31"  # Очікувана дата
                        
                        # Зберігаємо дані для звіту
                        if str(buyer_id) not in processed_customers:  # Якщо ще не додано в режимі пошуку
                            processed_customers[str(buyer_id)] = {
                                'buyer_info': {
                                    'id': buyer_id,
                                    'full_name': buyer.get('full_name', ''),
                                    'email': buyer.get('email', []),
                                    'phone': buyer.get('phone', [])
                                },
                                'orders_count': buyer.get('orders_count', 0),
                                'total_amount': float(orders_sum),
                                'bonus_amount': float(bonus_amount),
                                'bonus_amount_int': bonus_amount_int,
                                'current_bonuses': current_bonuses,
                                'current_expiry_date': current_expiry_date,
                                'search_result': False
                            }

                        # Пропускаємо, ТІЛЬКИ якщо бонуси і дата вже правильні
                        # ВАЖЛИВО: завжди оновлюємо, якщо розрахований бонус відрізняється від поточного
                        if current_bonuses == bonus_amount_int and current_expiry_date == expected_expiry_date:
                            logger.info(f"⏭️ P{process_id}: {buyer.get('full_name', 'Невідомо')}: вже має правильно {bonus_amount_int} бонусів до {current_expiry_date}, пропускаємо - клієнт {current_global_client_index}/{total_records}")
                            return False
                        
                        # Логуємо що потрібно оновити
                        if current_bonuses != bonus_amount_int:
                            logger.info(f"🔄 P{process_id}: {buyer.get('full_name', 'Невідомо')}: потрібно оновити бонуси {current_bonuses} → {bonus_amount_int} (сума: {float(orders_sum):.0f}₴ × {calculator.bonus_percentage}%)")
                        if current_expiry_date != expected_expiry_date:
                            logger.info(f"📅 P{process_id}: {buyer.get('full_name', 'Невідомо')}: потрібно оновити дату {current_expiry_date} → {expected_expiry_date}")

                        if not dry_run:
                            try:
                                # Визначаємо причину оновлення для логування
                                update_reason = []
                                if current_bonuses != bonus_amount_int:
                                    update_reason.append(f"бонуси {current_bonuses}→{bonus_amount_int}")
                                if current_expiry_date != expected_expiry_date:
                                    update_reason.append(f"дата {current_expiry_date}→{expected_expiry_date}")
                                
                                reason_text = ", ".join(update_reason)
                                
                                # Формуємо історію бонусів
                                date_str = datetime.now(timezone.utc).strftime('%d.%m')
                                expiry_date_short = "31.08.25"  # Кінець літа 2025
                                transaction_entry = f"🔄 {date_str} | Початкові бонуси | {float(orders_sum):.0f}₴ | +{bonus_amount_int} | -{current_bonuses} | {current_bonuses}→{bonus_amount_int} | до {expiry_date_short}"
                                
                                # Оновлюємо кастомні поля
                                update_data = {
                                    "custom_fields": [
                                        {"uuid": calculator.BONUS_FIELD_UUID, "value": str(bonus_amount_int)},
                                        {"uuid": calculator.BONUS_EXPIRY_FIELD_UUID, "value": expiry_date_str},
                                        {"uuid": calculator.HISTORY_FIELD_UUID, "value": transaction_entry}
                                    ]
                                }
                                
                                # Виконуємо PUT запит
                                update_response = calculator.make_request(f"/buyer/{buyer_id}", "PUT", update_data)
                                
                                if update_response:
                                    logger.info(f"✅ P{process_id}: {buyer.get('full_name', 'Невідомо')}: {bonus_amount_int} бонусів ({reason_text}) - клієнт {current_global_client_index}/{total_records}")
                                    page_updated += 1
                                    return True
                                else:
                                    logger.error(f"❌ P{process_id}: Помилка оновлення {buyer.get('full_name', 'Невідомо')} - клієнт {current_global_client_index}/{total_records}")
                                    return False
                                    
                            except Exception as e:
                                logger.error(f"❌ P{process_id}: Помилка оновлення {buyer.get('full_name', 'Невідомо')}: {e}")
                                return False
                        else:
                            page_updated += 1
                            return True
                    
                    # Якщо дійшли сюди в режимі "тільки пошук" - значить клієнт оброблений
                    return True
                
                # Багатопоточна обробка клієнтів на сторінці (обмежуємо для 200 запитів/хвилину)
                max_workers = 2  # Один потік на процес для дотримання rate limit
                total_clients_on_page = len(buyers_with_orders)
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(update_client, buyer, idx + 1, total_clients_on_page) 
                        for idx, buyer in enumerate(buyers_with_orders)
                    ]
                    
                    for future in as_completed(futures):
                        try:
                            success = future.result()
                            if success:
                                successful_updates += 1
                            else:
                                failed_updates += 1
                        except Exception as e:
                            failed_updates += 1
                            logger.error(f"❌ P{process_id}: Помилка в потоці: {e}")
                
                total_processed += len(buyers_with_orders)
                
                if page % 5 == 0:  # Логуємо прогрес кожні 5 сторінок
                    pages_done = page - start_page + 1
                    pages_total = end_page - start_page + 1
                    pages_remaining = pages_total - pages_done
                    progress_percent = (pages_done / pages_total) * 100
                    global_progress = (current_global_client_index / total_records) * 100 if total_records > 0 else 0
                    clients_remaining = total_records - current_global_client_index
                    logger.info(f"📊 P{process_id}: сторінка {page}/{end_page} ({progress_percent:.1f}%), оброблено {current_global_client_index}/{total_records} клієнтів ({global_progress:.1f}%), залишилось ~{clients_remaining} клієнтів")
                
            except Exception as e:
                logger.error(f"❌ P{process_id}: Помилка при обробці сторінки {page}: {e}")
                
                # Якщо це проксі помилка, можемо спробувати відключити проксі для цього процесу
                if "ProxyError" in str(e) or "Connection refused" in str(e):
                    logger.warning(f"🔄 P{process_id}: Проксі недоступний, спробуємо без проксі для сторінки {page}")
                    try:
                        # Тимчасово відключаємо проксі для цього запиту
                        calculator.proxy_enabled = False
                        calculator.rate_limit_delay = 0.3  # Зберігаємо обмеження 200 запитів/хвилину
                        
                        # Повторюємо запит без проксі
                        response = calculator.make_request(
                            f"/buyer?page={page}&limit=50&include=custom_fields"
                        )
                        
                        if response and response.get('data'):
                            logger.info(f"✅ P{process_id}: Успішно завантажили сторінку {page} без проксі")
                        
                        # Відновлюємо проксі для наступних запитів
                        calculator.proxy_enabled = proxy_config is not None
                        calculator.rate_limit_delay = 0.3  # Зберігаємо обмеження 200 запитів/хвилину
                        
                    except Exception as fallback_error:
                        logger.error(f"❌ P{process_id}: Помилка навіть без проксі для сторінки {page}: {fallback_error}")
                
                continue
    
    except Exception as e:
        logger.error(f"❌ P{process_id}: Критична помилка: {e}")
    
    pages_processed = end_page - start_page + 1
    logger.info(f"✅ Процес {process_id} завершено: {pages_processed} сторінок, {successful_updates} успішних, {failed_updates} помилок, {total_processed} клієнтів")
    
    return {
        'process_id': process_id,
        'start_page': start_page,
        'end_page': end_page,
        'processed_customers': processed_customers,
        'successful_updates': successful_updates,
        'failed_updates': failed_updates,
        'total_processed': total_processed
    }

def process_all_customers_multiprocess(calculator, num_processes=10, dry_run=True, test_mode=False, search_config=None):
    """
    Багатопроцесорна обробка всіх клієнтів
    """
    if search_config and search_config.get('enabled') and search_config.get('only_search'):
        logger.info("=" * 80)
        logger.info("� БАГАТОПРОЦЕСОРНИЙ ПОШУК КЛІЄНТІВ З ВЕЛИКИМИ БОНУСАМИ")
        logger.info("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info("�🚀 БАГАТОПРОЦЕСОРНЕ НАРАХУВАННЯ ПОЧАТКОВИХ БОНУСІВ")
        logger.info("=" * 80)
    
    try:
        # Отримуємо загальну кількість сторінок
        total_pages, total_records = calculator.get_total_pages(limit=50)
        
        if total_pages == 0:
            logger.error("❌ Не вдалося отримати кількість сторінок")
            return {}
        
        # В тестовому режимі обробляємо тільки перші 5 сторінок
        if test_mode:
            total_pages = min(5, total_pages)
            logger.info(f"🧪 ТЕСТОВИЙ РЕЖИМ: обробляємо тільки {total_pages} сторінок")
        
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
        
        # Готуємо аргументи для процесів
        process_args = [
            (start_page, end_page, calculator.api_key, calculator.bonus_percentage, 
             calculator.proxy_config, dry_run, process_id, total_records, search_config or {})
            for start_page, end_page, process_id in process_ranges
        ]
        
        # Запускаємо багатопроцесорну обробку
        start_time = datetime.now()
        
        logger.info(f"🚀 Запускаємо {len(process_args)} процесів...")
        
        all_results = {}
        total_successful = 0
        total_failed = 0
        total_customers = 0
        completed_processes = 0
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            # Подаємо завдання
            future_to_process = {
                executor.submit(process_page_range, args): args[6]  # process_id
                for args in process_args
            }
            
            # Збираємо результати
            for future in as_completed(future_to_process):
                process_id = future_to_process[future]
                
                try:
                    result = future.result()
                    
                    # Об'єднуємо результати
                    all_results.update(result['processed_customers'])
                    total_successful += result['successful_updates']
                    total_failed += result['failed_updates']
                    total_customers += result['total_processed']
                    completed_processes += 1
                    
                    # Показуємо загальний прогрес
                    overall_progress = (completed_processes / len(process_args)) * 100
                    logger.info(f"✅ Процес {result['process_id']} завершив роботу. Загальний прогрес: {completed_processes}/{len(process_args)} ({overall_progress:.1f}%)")
                    
                except Exception as e:
                    completed_processes += 1
                    overall_progress = (completed_processes / len(process_args)) * 100
                    logger.error(f"❌ Процес {process_id} завершився з помилкою: {e}. Прогрес: {completed_processes}/{len(process_args)} ({overall_progress:.1f}%)")
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Фінальна статистика
        logger.info("=" * 80)
        logger.info("РЕЗУЛЬТАТИ БАГАТОПРОЦЕСОРНОЇ ОБРОБКИ:")
        logger.info(f"Процесів запущено: {len(process_args)}")
        logger.info(f"Сторінок оброблено: {total_pages}")
        logger.info(f"Клієнтів з замовленнями: {total_customers}")
        
        if not dry_run:
            logger.info(f"Успішно оновлено: {total_successful}")
            logger.info(f"Помилок: {total_failed}")
            success_rate = (total_successful / (total_successful + total_failed) * 100) if (total_successful + total_failed) > 0 else 0
            logger.info(f"Відсоток успіху: {success_rate:.1f}%")
        
        logger.info(f"Час виконання: {execution_time:.1f} секунд")
        if total_customers > 0:
            logger.info(f"Швидкість: {total_customers/execution_time:.1f} клієнтів/сек")
        logger.info("=" * 80)
        
        return all_results
        
    except Exception as e:
        logger.error(f"❌ Критична помилка багатопроцесорної обробки: {e}")
        return {}

def generate_optimized_report(customer_bonuses, filename=None, search_config=None):
    """Генерація звіту"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if search_config and search_config.get('enabled') and search_config.get('only_search'):
            filename = f"keycrm_high_bonus_clients_{timestamp}"
        else:
            filename = f"keycrm_bonus_report_multiprocess_{timestamp}"
    
    # Сортуємо за бонусами
    if search_config and search_config.get('enabled') and search_config.get('only_search'):
        # В режимі пошуку сортуємо за поточними бонусами
        sorted_customers = sorted(
            customer_bonuses.items(),
            key=lambda x: x[1].get('current_bonuses', 0),
            reverse=True
        )
        report_method = 'multiprocess_search'
    else:
        # В звичайному режимі сортуємо за нарахованими бонусами
        sorted_customers = sorted(
            customer_bonuses.items(),
            key=lambda x: x[1]['bonus_amount'],
            reverse=True
        )
        report_method = 'multiprocess_optimized'
    
    # Детальний JSON звіт
    if search_config and search_config.get('enabled') and search_config.get('only_search'):
        # Рахуємо статистику для пошуку
        search_results = [data for data in customer_bonuses.values() if data.get('search_result', False)]
        
        report_data = {
            'generation_date': datetime.now().isoformat(),
            'method': report_method,
            'search_criteria': {
                'min_bonus_amount': search_config.get('min_bonus_amount', 3000),
                'search_only': True
            },
            'summary': {
                'total_customers_checked': len(customer_bonuses),
                'customers_found': len(search_results),
                'total_bonuses_found': sum(data.get('current_bonuses', 0) for data in search_results),
                'average_bonus_found': (sum(data.get('current_bonuses', 0) for data in search_results) / len(search_results)) if search_results else 0,
                'total_orders_value': sum(data.get('total_amount', 0) for data in search_results),
                'total_orders_count': sum(data.get('orders_count', 0) for data in search_results)
            },
            'customers': dict(sorted_customers)
        }
    else:
        # Стандартний звіт
        report_data = {
            'generation_date': datetime.now().isoformat(),
            'method': report_method,
            'summary': {
                'total_customers': len(customer_bonuses),
                'total_amount': sum(data['total_amount'] for data in customer_bonuses.values()),
                'total_new_bonus': sum(data['bonus_amount'] for data in customer_bonuses.values()),
                'total_current_bonus': sum(data['current_bonuses'] for data in customer_bonuses.values()),
                'total_orders': sum(data['orders_count'] for data in customer_bonuses.values()),
                'average_bonus': sum(data['bonus_amount'] for data in customer_bonuses.values()) / len(customer_bonuses) if customer_bonuses else 0,
                'average_order_value': sum(data['total_amount'] for data in customer_bonuses.values()) / sum(data['orders_count'] for data in customer_bonuses.values()) if sum(data['orders_count'] for data in customer_bonuses.values()) > 0 else 0
            },
            'customers': dict(sorted_customers)
        }
    
    with open(f"{filename}.json", 'w', encoding='utf-8') as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)
    
    # CSV звіт
    with open(f"{filename}.csv", 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        if search_config and search_config.get('enabled') and search_config.get('only_search'):
            # Заголовки для режиму пошуку
            writer.writerow([
                'ID клієнта', 'Ім\'я', 'Email', 'Телефон', 
                'Кількість замовлень', 'Загальна сума', 'Поточні бонуси',
                'Дата закінчення бонусів'
            ])
            
            for buyer_id, data in sorted_customers:
                if data.get('search_result', False):  # Тільки знайдені клієнти
                    buyer_info = data['buyer_info']
                    email = ', '.join(buyer_info.get('email', []))
                    phone = ', '.join(buyer_info.get('phone', []))
                    
                    writer.writerow([
                        buyer_id,
                        buyer_info.get('full_name', ''),
                        email,
                        phone,
                        data['orders_count'],
                        f"{data['total_amount']:.2f}",
                        data['current_bonuses'],
                        data.get('current_expiry_date', '')
                    ])
        else:
            # Стандартні заголовки
            writer.writerow([
                'ID клієнта', 'Ім\'я', 'Email', 'Телефон', 
                'Кількість замовлень', 'Загальна сума', 'Поточні бонуси',
                'Нові бонуси', 'Зміна бонусів'
            ])
            
            for buyer_id, data in sorted_customers:
                buyer_info = data['buyer_info']
                email = ', '.join(buyer_info.get('email', []))
                phone = ', '.join(buyer_info.get('phone', []))
                
                writer.writerow([
                    buyer_id,
                    buyer_info.get('full_name', ''),
                    email,
                    phone,
                    data['orders_count'],
                    f"{data['total_amount']:.2f}",
                    data['current_bonuses'],
                    data['bonus_amount_int'],
                    data.get('bonus_change', data['bonus_amount_int'])
                ])
    
    logger.info(f"Звіти збережено: {filename}.json та {filename}.csv")

def load_config():
    """Завантаження конфігурації"""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # Додаємо search_mode якщо його немає
        if 'search_mode' not in config:
            config['search_mode'] = {
                "enabled": False,
                "only_search": False,
                "min_bonus_amount": 3000
            }
            
        return config
    except FileNotFoundError:
        return {
            "api_key": "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ",
            "bonus_percentage": 7.0,
            "description": "Багатопроцесорна конфігурація",
            "search_mode": {
                "enabled": False,
                "only_search": False,
                "min_bonus_amount": 3000
            },
            "proxy": {
                "enabled": False,
                "host": "p.webshare.io",
                "port": 80,
                "username": "YOUR_NEW_USERNAME_HERE",
                "password": "YOUR_NEW_PASSWORD_HERE"
            }
        }

def main():
    """Головна функція"""
    # Завантажуємо конфігурацію
    config = load_config()
    
    # Перевіряємо режим пошуку
    search_config = config.get('search_mode', {})
    search_enabled = search_config.get('enabled', False)
    only_search = search_config.get('only_search', False)
    min_bonus_amount = search_config.get('min_bonus_amount', 3000)
    
    if search_enabled and only_search:
        print("🔍 ПОШУК КЛІЄНТІВ З ВЕЛИКИМИ БОНУСАМИ")
        print(f"(Шукаємо клієнтів з бонусами ≥ {min_bonus_amount})")
    elif search_enabled:
        print("🔍 ЗМІШАНИЙ РЕЖИМ: ПОШУК + НАРАХУВАННЯ БОНУСІВ")
        print(f"(Шукаємо клієнтів з бонусами ≥ {min_bonus_amount} + нараховуємо бонуси)")
    else:
        print("🚀 БАГАТОПРОЦЕСОРНИЙ КАЛЬКУЛЯТОР БОНУСІВ ДЛЯ KEYCRM")
        print("(Паралельна обробка всіх клієнтів)")
    print("=" * 80)
    
    # Налаштовуємо проксі
    proxy_config = None
    if config.get('proxy', {}).get('enabled', False):
        proxy_config = config['proxy']
        print(f"🌐 Проксі: {proxy_config['host']}:{proxy_config['port']}")
    
    if search_enabled and only_search:
        print(f"🔍 Відсоток нарахування: не застосовується (режим пошуку)")
        print(f"🎯 Мінімальна кількість бонусів для пошуку: {min_bonus_amount}")
    elif search_enabled:
        print(f"📊 Відсоток нарахування: {config['bonus_percentage']}%")
        print(f"🎯 Мінімальна кількість бонусів для пошуку: {min_bonus_amount}")
    else:
        print(f"📊 Відсоток нарахування: {config['bonus_percentage']}%")
    
    # Визначаємо кількість процесів (обмежуємо для 200 запитів/хвилину)
    if proxy_config:
        num_processes = min(mp.cpu_count(), 5)  # Максимум 5 процесів з проксі
    else:
        num_processes = min(mp.cpu_count(), 2)  # Максимум 2 процеси без проксі
    print(f"🔄 Кількість процесів: {num_processes}")
    
    # Створюємо калькулятор
    calculator = KeyCRMBonusCalculator(
        api_key=config['api_key'],
        bonus_percentage=config['bonus_percentage'],
        proxy_config=proxy_config
    )
    
    try:
        start_time = datetime.now()
        
        # Виконуємо багатопроцесорну обробку
        customer_bonuses = process_all_customers_multiprocess(
            calculator=calculator,
            num_processes=num_processes,
            dry_run=False if not (search_enabled and only_search) else True,  # В режимі пошуку не записуємо
            test_mode=False,  # Всі клієнти
            search_config=search_config
        )
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if customer_bonuses:
            # Генеруємо звіт
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if search_enabled and only_search:
                filename = f"keycrm_high_bonus_clients_{timestamp}"
            else:
                filename = f"keycrm_bonus_report_multiprocess_{timestamp}"
            generate_optimized_report(customer_bonuses, filename, search_config)
            
            if search_enabled and only_search:
                # Показуємо результати пошуку
                search_results = [
                    (buyer_id, data) for buyer_id, data in customer_bonuses.items()
                    if data.get('search_result', False)
                ]
                
                print(f"\n💎 ЗНАЙДЕНО {len(search_results)} КЛІЄНТІВ З БОНУСАМИ ≥ {min_bonus_amount}:")
                print("-" * 80)
                
                # Сортуємо за кількістю бонусів
                search_results.sort(key=lambda x: x[1]['current_bonuses'], reverse=True)
                
                for i, (buyer_id, data) in enumerate(search_results, 1):
                    buyer_name = data['buyer_info'].get('full_name', 'Невідомо')
                    current_bonuses = data['current_bonuses']
                    orders_sum = data['total_amount']
                    print(f"{i:2d}. {buyer_name:<25} Поточні бонуси: {current_bonuses:>5} | Замовлень на: {orders_sum:>8.0f}₴")
            else:
                # Показуємо топ-10
                sorted_customers = sorted(
                    customer_bonuses.items(),
                    key=lambda x: x[1]['bonus_amount'],
                    reverse=True
                )
                
                print(f"\n🏆 ТОП-{min(10, len(sorted_customers))} КЛІЄНТІВ ЗА НАРАХОВАНИМИ БОНУСАМИ:")
                print("-" * 80)
                for i, (buyer_id, data) in enumerate(sorted_customers[:10], 1):
                    buyer_name = data['buyer_info'].get('full_name', 'Невідомо')
                    new = data['bonus_amount_int']
                    orders_sum = data['total_amount']
                    print(f"{i:2d}. {buyer_name:<25} Замовлень на: {orders_sum:>8.0f}₴ → {new:>4} бонусів")
        
        if search_enabled and only_search:
            print(f"\n✅ Пошук клієнтів завершено!")
        else:
            print(f"\n✅ Багатопроцесорне нарахування завершено!")
        print(f"⏱️  Час виконання: {execution_time:.1f} секунд")
        if customer_bonuses:
            if search_enabled and only_search:
                search_results_count = len([data for data in customer_bonuses.values() if data.get('search_result', False)])
                print(f"🔍 Знайдено клієнтів з великими бонусами: {search_results_count}")
                print(f"🚀 Швидкість пошуку: {len(customer_bonuses)/execution_time:.1f} клієнтів/сек")
            else:
                print(f"🚀 Швидкість: {len(customer_bonuses)/execution_time:.1f} клієнтів/сек")
        
    except KeyboardInterrupt:
        print("\n❌ Операцію перервано користувачем")
    except Exception as e:
        logger.error(f"Критична помилка: {e}")
        print(f"\n❌ Помилка: {e}")

if __name__ == "__main__":
    # Налаштування для multiprocessing
    mp.set_start_method('spawn', force=True)
    main()
