#!/usr/bin/env python3
"""
Скрипт для управління лідами в воронці "Розсилка повідомлень" KeyCRM
Додає нових клієнтів до воронки розсилки для подальшої автоматизації
"""

import requests
import json
import time
from datetime import datetime
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import math
import csv

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('newsletter_leads_management.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class KeyCRMNewsletterManager:
    def __init__(self, api_key, proxy_config=None, auto_delete_duplicates=True, duplicate_status_id=257):
        self.api_key = api_key
        self.base_url = "https://openapi.keycrm.app/v1"
        self.newsletter_funnel_id = 15  # Воронка "Розсилка повідомлень"
        self.auto_delete_duplicates = auto_delete_duplicates
        self.duplicate_status_id = duplicate_status_id  # Статус для переміщення дублікатів
        
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        # Конфігурація проксі
        self.proxy_config = proxy_config
        self.proxy_enabled = proxy_config is not None
        
        if self.proxy_enabled and proxy_config:
            logger.info(f"🌐 Увімкнено проксі: {proxy_config.get('host', 'unknown')}:{proxy_config.get('port', 'unknown')}")
            self.rate_limit_delay = 0.3  # Затримка між запитами
        else:
            logger.info("Proxy not configured")
            self.rate_limit_delay = 0.3
    
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
                elif method == "POST":
                    response = session.post(
                        full_url, 
                        headers=self.headers, 
                        json=data, 
                        timeout=20
                    )
                elif method == "PUT":
                    response = session.put(
                        full_url, 
                        headers=self.headers, 
                        json=data, 
                        timeout=20
                    )
                elif method == "DELETE":
                    response = session.delete(full_url, headers=self.headers, timeout=20)
                else:
                    raise ValueError(f"Непідтримуваний метод: {method}")
                
                if response is None:
                    raise ValueError("Не вдалося отримати відповідь від сервера")
                
                response.raise_for_status()
                
                # Мінімальна пауза
                time.sleep(self.rate_limit_delay)
                
                # Для DELETE запитів може бути порожня відповідь
                if method == "DELETE":
                    if response.status_code == 204:  # No Content - успішне видалення
                        return {"success": True, "message": "Card deleted successfully"}
                    try:
                        return response.json()
                    except:
                        return {"success": True, "status_code": response.status_code}
                else:
                    return response.json()
                
            except (requests.exceptions.ProxyError, 
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout) as proxy_error:
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Експоненційна затримка
                    logger.warning(f"⚠️ Проксі помилка (спроба {attempt + 1}/{max_retries}): {proxy_error}. Чекаємо {wait_time}с...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ Проксі помилка після {max_retries} спроб: {proxy_error}")
                    raise
                    
            except requests.exceptions.HTTPError as http_error:
                if response and response.status_code == 429:  # Rate limit
                    wait_time = 60  # Чекаємо хвилину
                    logger.warning(f"⚠️ Rate limit досягнуто. Чекаємо {wait_time}с...")
                    time.sleep(wait_time)
                    continue
                elif response and response.status_code in [500, 502, 503, 504]:  # Server errors
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"⚠️ Помилка сервера {response.status_code} (спроба {attempt + 1}/{max_retries}). Чекаємо {wait_time}с...")
                        time.sleep(wait_time)
                        continue
                
                logger.error(f"HTTP error: {http_error}")
                if response:
                    logger.error(f"Server response: {response.text}")
                    logger.error(f"Response status: {response.status_code}")
                    logger.error(f"Response headers: {dict(response.headers)}")
                raise
                
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 1
                    logger.warning(f"Request error (attempt {attempt + 1}/{max_retries}): {e}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error after {max_retries} attempts: {e}")
                    raise
            finally:
                session.close()
    
    def get_pipeline_cards(self, pipeline_id, page=1, limit=50):
        """Отримання карток з конкретної воронки"""
        try:
            url = f"/pipelines/cards?page={page}&limit={limit}&filter[pipeline_id]={pipeline_id}&include=contact.client"
            response = self.make_request(url)
            return response
        except Exception as e:
            logger.error(f"Помилка отримання карток з воронки {pipeline_id}: {e}")
            return None
    
    def get_all_pipeline_cards(self, pipeline_id, max_pages=None):
        """Отримання всіх карток з воронки"""
        logger.info(f"🔍 Отримуємо всі картки з воронки {pipeline_id}...")
        if max_pages:
            logger.info(f"🧪 Тестовий режим: обмежуємо до {max_pages} сторінок")
            
        all_cards = {}
        page = 1
        
        while True:
            try:
                # Перевіряємо обмеження по сторінках у тестовому режимі
                if max_pages and page > max_pages:
                    logger.info(f"🧪 Досягнуто ліміт тестового режиму: {max_pages} сторінок")
                    break
                    
                response = self.get_pipeline_cards(pipeline_id, page)
                if not response or not response.get('data'):
                    break
                
                cards = response.get('data', [])
                cards_with_client_id = 0
                cards_without_client_id = 0
                duplicates_found = 0
                
                for card in cards:
                    contact = card.get('contact')
                    if contact and contact.get('client_id'):
                        client_id = contact.get('client_id')
                        
                        # Перевіряємо чи це дублікат
                        if client_id in all_cards:
                            duplicates_found += 1
                            existing_card = all_cards[client_id]
                            
                            # Порівнюємо дати створення, щоб визначити яку картку видалити
                            existing_created = existing_card.get('created_at', '')
                            new_created = card.get('created_at', '')
                            
                            logger.warning(f"🔄 ДУБЛІКАТ client_id {client_id}:")
                            logger.warning(f"   ├─ Існуюча картка: ID {existing_card.get('id')}, title: '{existing_card.get('title', 'N/A')}', created: {existing_created}")
                            logger.warning(f"   └─ Нова картка: ID {card.get('id')}, title: '{card.get('title', 'N/A')}', created: {new_created}")
                            
                            # Визначаємо яку картку переміщувати в статус 257 (залишаємо новішу)
                            if new_created > existing_created:
                                # Нова картка новіша - переміщуємо стару в статус 257
                                card_to_move = existing_card
                                card_to_keep = card
                                logger.warning(f"   └─ �️ ПЕРЕМІЩУЄМО СТАРУ картку ID {existing_card.get('id')} в статус 257")
                            else:
                                # Існуюча картка новіша або дати однакові - переміщуємо нову в статус 257
                                card_to_move = card
                                card_to_keep = existing_card
                                logger.warning(f"   └─ �️ ПЕРЕМІЩУЄМО НОВУ картку ID {card.get('id')} в статус 257")
                            
                            # Спробуємо перемістити дублікат в статус 257
                            move_result = self.delete_pipeline_card(card_to_move.get('id'))
                            if move_result:
                                logger.info(f"   ✅ Картку ID {card_to_move.get('id')} успішно переміщено в статус 257")
                                # Оновлюємо картку що залишається
                                all_cards[client_id] = card_to_keep
                            else:
                                logger.error(f"   ❌ Не вдалося перемістити картку ID {card_to_move.get('id')} в статус 257")
                                # В разі помилки переміщення, залишаємо ту що була
                                if client_id not in all_cards:
                                    all_cards[client_id] = existing_card
                        else:
                            # Нова унікальна картка
                            all_cards[client_id] = card
                        
                        cards_with_client_id += 1
                    else:
                        cards_without_client_id += 1
                        # Логуємо детально картки без client_id
                        card_id = card.get('id', 'unknown')
                        card_title = card.get('title', 'без назви')
                        contact_info = "немає контакта" if not contact else f"контакт без client_id (contact_id: {contact.get('id', 'unknown')})"
                        
                        logger.warning(f"⚠️ КАРТКА БЕЗ CLIENT_ID:")
                        logger.warning(f"   ├─ Card ID: {card_id}")
                        logger.warning(f"   ├─ Title: '{card_title}'")
                        logger.warning(f"   ├─ Contact: {contact_info}")
                        logger.warning(f"   └─ Created: {card.get('created_at', 'unknown')}")
                
                logger.info(f"📄 Сторінка {page}: отримано {len(cards)} карток загалом")
                logger.info(f"   ├─ З client_id: {cards_with_client_id}, без client_id: {cards_without_client_id}")
                logger.info(f"   ├─ Дублікатів знайдено: {duplicates_found}")
                logger.info(f"   └─ Унікальних клієнтів накопичено: {len(all_cards)}")
                
                # Перевіряємо чи є ще сторінки
                if page >= response.get('last_page', 1):
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Помилка обробки сторінки {page}: {e}")
                break
        
        logger.info(f"✅ Загалом отримано {len(all_cards)} унікальних карток з воронки {pipeline_id}")
        
        return all_cards

    def get_all_pipeline_cards_multiprocess(self, pipeline_id, max_pages=None, num_processes=3):
        """Мультипроцесорне отримання всіх карток з воронки"""
        logger.info(f"🔍 Мультипроцесорне отримання карток з воронки {pipeline_id}...")
        
        # Спочатку отримуємо першу сторінку щоб дізнатися загальну кількість
        first_response = self.get_pipeline_cards(pipeline_id, 1)
        if not first_response:
            logger.error("Не вдалося отримати першу сторінку карток")
            return {}
        
        total_pages = first_response.get('last_page', 1)
        total_records = first_response.get('total', 0)
        current_page = first_response.get('current_page', 1)
        per_page = first_response.get('per_page', 50)
        
        logger.info(f"📊 API інформація про воронку {pipeline_id}:")
        logger.info(f"   └─ Загальна кількість карток: {total_records}")
        logger.info(f"   └─ Карток на сторінці: {per_page}")
        logger.info(f"   └─ Загальна кількість сторінок: {total_pages}")
        logger.info(f"   └─ Поточна сторінка: {current_page}")
        
        # Аналізуємо першу сторінку
        first_page_cards = first_response.get('data', [])
        cards_with_client_id = sum(1 for card in first_page_cards if card.get('contact', {}).get('client_id'))
        cards_without_client_id = len(first_page_cards) - cards_with_client_id
        
        logger.info(f"📄 Перша сторінка: {len(first_page_cards)} карток")
        logger.info(f"   └─ З client_id: {cards_with_client_id}")
        logger.info(f"   └─ Без client_id: {cards_without_client_id}")
        
        # Застосовуємо обмеження якщо є
        if max_pages:
            total_pages = min(max_pages, total_pages)
            logger.info(f"🧪 Тестовий режим: обмежуємо до {total_pages} сторінок")
        
        logger.info(f"📊 Буде оброблено сторінок: {total_pages}")
        
        if total_pages <= 1:
            # Якщо тільки одна сторінка, повертаємо результат першого запиту
            all_cards = {}
            for card in first_response.get('data', []):
                contact = card.get('contact')
                if contact and contact.get('client_id'):
                    client_id = contact.get('client_id')
                    all_cards[client_id] = card
            return all_cards
        
        # Розподіляємо сторінки між процесами
        pages_per_process = math.ceil(total_pages / num_processes)
        process_ranges = []
        
        for i in range(num_processes):
            start_page = i * pages_per_process + 1
            end_page = min((i + 1) * pages_per_process, total_pages)
            
            if start_page <= total_pages:
                process_ranges.append((start_page, end_page, i + 1))
        
        logger.info(f"📋 Розподіл сторінок карток між {len(process_ranges)} процесами:")
        for start_page, end_page, process_id in process_ranges:
            logger.info(f"   Процес {process_id}: сторінки {start_page}-{end_page} ({end_page-start_page+1} сторінок)")
        
        # Готуємо аргументи для процесів
        process_args = [
            (start_page, end_page, self.api_key, self.proxy_config, 
             process_id, pipeline_id)
            for start_page, end_page, process_id in process_ranges
        ]
        
        # Запускаємо мультипроцесорне отримання карток
        start_time = datetime.now()
        all_cards = {}
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            # Подаємо завдання
            future_to_process = {
                executor.submit(get_pipeline_cards_range, args): args[4]  # process_id
                for args in process_args
            }
            
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Додаткова статистика та обробка дублікатів між процесами
        total_duplicates = 0
        total_without_client_id = 0
        cards_to_delete = []  # Картки для видалення (дублікати між процесами)
        
        for future in as_completed(future_to_process):
            process_id = future_to_process[future]
            
            try:
                result = future.result()
                cards_from_process = result.get('cards', {})
                
                # Перевіряємо дублікати між процесами
                for client_id, new_card in cards_from_process.items():
                    if client_id in all_cards:
                        # Знайдено дублікат між процесами
                        existing_card = all_cards[client_id]
                        total_duplicates += 1
                        
                        # Порівнюємо дати створення
                        existing_created = existing_card.get('created_at', '')
                        new_created = new_card.get('created_at', '')
                        
                        logger.warning(f"🔄 ДУБЛІКАТ МІЖ ПРОЦЕСАМИ client_id {client_id}:")
                        logger.warning(f"   ├─ Існуюча картка: ID {existing_card.get('id')}, created: {existing_created}")
                        logger.warning(f"   └─ Нова картка: ID {new_card.get('id')}, created: {new_created}")
                        
                        # Визначаємо яку картку видалити (залишаємо новішу)
                        if new_created > existing_created:
                            # Нова картка новіша - видаляємо стару
                            cards_to_delete.append(existing_card.get('id'))
                            all_cards[client_id] = new_card
                            logger.warning(f"   └─ 🗑️ ПОЗНАЧЕНО для видалення стару картку ID {existing_card.get('id')}")
                        else:
                            # Існуюча картка новіша - видаляємо нову
                            cards_to_delete.append(new_card.get('id'))
                            logger.warning(f"   └─ 🗑️ ПОЗНАЧЕНО для видалення нову картку ID {new_card.get('id')}")
                    else:
                        # Унікальна картка
                        all_cards[client_id] = new_card
                
                logger.info(f"✅ Процес {process_id} завершено: отримано {len(cards_from_process)} карток")
                
            except Exception as e:
                logger.error(f"❌ Процес {process_id} завершився з помилкою: {e}")
        
        # Видаляємо дублікати якщо увімкнено автовидалення
        if self.auto_delete_duplicates and cards_to_delete:
            logger.info(f"🗑️ Видаляємо {len(cards_to_delete)} дублікатів між процесами...")
            deleted_count = 0
            
            for card_id in cards_to_delete:
                try:
                    result = self.delete_pipeline_card(card_id)
                    if result:
                        deleted_count += 1
                        logger.info(f"   ✅ Картку ID {card_id} переміщено в статус {self.duplicate_status_id}")
                    else:
                        logger.error(f"   ❌ Не вдалося перемістити картку ID {card_id}")
                except Exception as e:
                    logger.error(f"   ❌ Помилка видалення картки ID {card_id}: {e}")
            
            logger.info(f"🗑️ Видалено {deleted_count}/{len(cards_to_delete)} дублікатів")
        
        logger.info(f"✅ Мультипроцесорне отримання карток завершено:")
        logger.info(f"   Загалом отримано: {len(all_cards)} унікальних карток")
        logger.info(f"   Дублікатів між процесами: {total_duplicates}")
        logger.info(f"   Час виконання: {execution_time:.1f} секунд")
        
        return all_cards
    
    def get_total_customers_pages(self, limit=50):
        """Отримання загальної кількості сторінок клієнтів"""
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
    
    def get_customers_page(self, page, limit=50):
        """Отримання клієнтів з конкретної сторінки"""
        try:
            url = f"/buyer?page={page}&limit={limit}"
            response = self.make_request(url)
            return response.get('data', []) if response else []
        except Exception as e:
            logger.error(f"Помилка отримання клієнтів зі сторінки {page}: {e}")
            return []
    
    def find_or_create_contact_for_buyer(self, buyer_id, buyer_info=None):
        """Знаходження або створення контакта для покупця"""
        try:
            # Спочатку спробуємо знайти існуючий контакт з client_id = buyer_id
            # Це складно зробити через API, тому спробуємо створити контакт
            # і KeyCRM сам зробить зв'язок якщо покупець вже існує
            
            if not buyer_info:
                # Отримуємо інформацію про покупця
                buyer_response = self.make_request(f"/buyer/{buyer_id}")
                if not buyer_response:
                    return None
                buyer_info = buyer_response
            
            # Створюємо дані для контакта на основі покупця
            contact_data = {
                "client_id": buyer_id,
                "full_name": buyer_info.get('full_name', ''),
            }
            
            # Додаємо email якщо є
            emails = buyer_info.get('email', [])
            if emails and len(emails) > 0:
                contact_data["email"] = emails[0]
            
            # Додаємо телефон якщо є
            phones = buyer_info.get('phone', [])
            if phones and len(phones) > 0:
                contact_data["phone"] = phones[0]
            
            # Спробуємо різні варіанти ендпоінтів для створення контакта
            contact_endpoints = ["/contacts", "/contact", "/buyer/contact", "/crm/contact"]
            
            for endpoint in contact_endpoints:
                try:
                    logger.debug(f"Пробуємо створити контакт через {endpoint}")
                    response = self.make_request(endpoint, method="POST", data=contact_data)
                    if response:
                        logger.info(f"✅ Контакт створено через {endpoint}")
                        return response
                except Exception as e:
                    logger.debug(f"Помилка з ендпоінтом {endpoint}: {e}")
                    continue
            
            logger.error(f"Всі ендпоінти контактів недоступні для покупця {buyer_id}")
            return None
            
        except Exception as e:
            logger.error(f"Помилка створення контакта для покупця {buyer_id}: {e}")
            return None

    def move_card_to_status(self, card_id, status_id):
        """Переміщення картки в інший статус"""
        try:
            card_data = {"status_id": status_id}
            response = self.make_request(f"/pipelines/cards/{card_id}", method="PUT", data=card_data)
            return response
        except Exception as e:
            logger.error(f"Помилка переміщення картки {card_id} в статус {status_id}: {e}")
            return None

    def delete_pipeline_card(self, card_id):
        """Видалення картки з воронки (переміщення в статус дублікату)"""
        try:
            # Замість видалення переміщуємо в статус дублікату
            response = self.move_card_to_status(card_id, self.duplicate_status_id)
            if response:
                logger.info(f"✅ Картку ID {card_id} переміщено в статус {self.duplicate_status_id} (дублікат)")
                return {"success": True, "message": f"Card moved to status {self.duplicate_status_id}"}
            return None
        except Exception as e:
            logger.error(f"Помилка переміщення картки {card_id} в статус {self.duplicate_status_id}: {e}")
            return None

    def create_pipeline_card(self, buyer_id, pipeline_id, status_id=241, buyer_info=None):
        """Створення нової картки в воронці"""
        try:
            # Згідно з API документацією, потрібно передавати контакт як об'єкт
            buyer_name = buyer_info.get('full_name', f'Client {buyer_id}') if buyer_info else f'Client {buyer_id}'
            
            # Створюємо картку згідно з API документацією
            card_data = {
                "title": f"{buyer_name}",
                "pipeline_id": pipeline_id,
                "contact": {
                    "full_name": buyer_name
                }
            }
            
            # Додаємо client_id якщо це не тестовий ID
            if not str(buyer_id).startswith('test_'):
                card_data["contact"]["client_id"] = buyer_id
            
            # Додаємо статус (завжди)
            card_data["status_id"] = status_id
            
            # Додаємо додаткову інформацію про контакт якщо є
            if buyer_info:
                if buyer_info.get('email'):
                    # email може бути списком, беремо перший
                    email = buyer_info['email']
                    if isinstance(email, list) and len(email) > 0:
                        card_data["contact"]["email"] = email[0]
                    elif isinstance(email, str):
                        card_data["contact"]["email"] = email
                        
                if buyer_info.get('phone'):
                    # phone може бути списком, беремо перший
                    phone = buyer_info['phone']
                    if isinstance(phone, list) and len(phone) > 0:
                        card_data["contact"]["phone"] = phone[0]
                    elif isinstance(phone, str):
                        card_data["contact"]["phone"] = phone
            
            # Створюємо картку
            logger.info(f"Sending card data: {json.dumps(card_data, ensure_ascii=False, indent=2)}")
            response = self.make_request("/pipelines/cards", method="POST", data=card_data)
            
            if response:
                logger.info(f"Card created successfully for client {buyer_id}")
                return response
            else:
                logger.warning(f"Failed to create card for client {buyer_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating card for client {buyer_id}: {e}")
            return None

    def _create_card_with_contact(self, buyer_id, pipeline_id, status_id=None, buyer_info=None):
        """Резервний метод створення картки через контакт"""
        try:
            # Спочатку знаходимо або створюємо контакт для покупця
            contact = self.find_or_create_contact_for_buyer(buyer_id, buyer_info)
            if not contact:
                logger.error(f"Не вдалося створити контакт для покупця {buyer_id}")
                return None
            
            contact_id = contact.get('id')
            if not contact_id:
                logger.error(f"Контакт створено, але не отримано contact_id для покупця {buyer_id}")
                return None
            
            # Тепер створюємо картку
            card_data = {
                "contact_id": contact_id,
                "pipeline_id": pipeline_id
            }
            
            if status_id:
                card_data["status_id"] = status_id
                
            # Додаємо назву картки
            buyer_name = buyer_info.get('full_name', f'Клієнт {buyer_id}') if buyer_info else f'Клієнт {buyer_id}'
            card_data["title"] = f"Розсилка - {buyer_name}"
            
            response = self.make_request("/pipelines/cards", method="POST", data=card_data)
            return response
        except Exception as e:
            logger.error(f"Помилка резервного створення картки для клієнта {buyer_id}: {e}")
            return None


def get_pipeline_cards_range(args):
    """
    Функція для отримання карток з діапазону сторінок в окремому процесі
    """
    start_page, end_page, api_key, proxy_config, process_id, pipeline_id = args
    
    # Налаштовуємо логування для процесу
    logger = logging.getLogger(f"Cards-Process-{process_id}")
    
    # Створюємо менеджер для цього процесу (без видалення дублікатів у процесах)
    manager = KeyCRMNewsletterManager(api_key, proxy_config, auto_delete_duplicates=False)
    
    logger.info(f"🚀 Процес {process_id}: отримуємо картки зі сторінок {start_page}-{end_page}")
    
    cards_collected = {}
    total_cards_processed = 0
    duplicates_found = 0  # Додаємо лічильник дублікатів в процесі
    
    try:
        for page in range(start_page, end_page + 1):
            try:
                response = manager.get_pipeline_cards(pipeline_id, page)
                
                if not response or not response.get('data'):
                    logger.info(f"CP{process_id}: Сторінка {page} порожня або помилка")
                    continue
                
                cards = response.get('data', [])
                if not cards:
                    logger.info(f"CP{process_id}: Досягнуто кінця списку на сторінці {page}")
                    break
                
                cards_with_client_id = 0
                cards_without_client_id = 0
                page_duplicates = 0  # Дублікати на поточній сторінці
                
                # Обробляємо картки
                for card in cards:
                    contact = card.get('contact')
                    if contact and contact.get('client_id'):
                        client_id = contact.get('client_id')
                        
                        # Перевіряємо чи це дублікат в межах процесу
                        if client_id in cards_collected:
                            page_duplicates += 1
                            duplicates_found += 1  # Загальний лічильник для процесу
                            existing_card = cards_collected[client_id]
                            
                            # Порівнюємо дати створення
                            existing_created = existing_card.get('created_at', '')
                            new_created = card.get('created_at', '')
                            
                            logger.warning(f"🔄 CP{process_id} ДУБЛІКАТ client_id {client_id}:")
                            logger.warning(f"   ├─ Існуюча картка: ID {existing_card.get('id')}, title: '{existing_card.get('title', 'N/A')}', created: {existing_created}")
                            logger.warning(f"   └─ Нова картка: ID {card.get('id')}, title: '{card.get('title', 'N/A')}', created: {new_created}")
                            
                            # Визначаємо яку картку залишити (новішу)
                            if new_created > existing_created:
                                # Нова картка новіша - залишаємо її
                                cards_collected[client_id] = card
                                logger.warning(f"   └─ 📝 CP{process_id}: Замінено на новішу картку ID {card.get('id')}")
                            else:
                                # Існуюча картка новіша - залишаємо її
                                logger.warning(f"   └─ 📝 CP{process_id}: Залишено існуючу картку ID {existing_card.get('id')}")
                        else:
                            # Нова унікальна картка
                            cards_collected[client_id] = card
                        
                        cards_with_client_id += 1
                    else:
                        cards_without_client_id += 1
                        # Логуємо детально картки без client_id
                        card_id = card.get('id', 'unknown')
                        card_title = card.get('title', 'без назви')
                        contact_info = "немає контакта" if not contact else f"контакт без client_id (contact_id: {contact.get('id', 'unknown')})"
                        
                        logger.warning(f"⚠️ CP{process_id} КАРТКА БЕЗ CLIENT_ID:")
                        logger.warning(f"   ├─ Card ID: {card_id}")
                        logger.warning(f"   ├─ Title: '{card_title}'")
                        logger.warning(f"   ├─ Contact: {contact_info}")
                        logger.warning(f"   └─ Created: {card.get('created_at', 'unknown')}")
                
                # Детальне логування кожної сторінки
                logger.info(f"📄 CP{process_id}: сторінка {page} - {len(cards)} карток загалом")
                logger.info(f"   ├─ З client_id: {cards_with_client_id}, без client_id: {cards_without_client_id}")
                logger.info(f"   └─ Дублікатів на сторінці: {page_duplicates}")
                
                # Логуємо прогрес кожні 5 сторінок
                if page % 5 == 0:
                    logger.info(f"📊 CP{process_id}: сторінка {page}/{end_page}, оброблено {len(cards_collected)} унікальних карток")
                
                # Затримка між запитами
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ CP{process_id}: Помилка обробки сторінки {page}: {e}")
                time.sleep(2)  # Затримка при помилці
                continue
    
    except Exception as e:
        logger.error(f"❌ CP{process_id}: Критична помилка: {e}")
    
    pages_processed = end_page - start_page + 1
    logger.info(f"✅ Процес {process_id} завершено: {pages_processed} сторінок, {len(cards_collected)} унікальних карток")
    
    return {
        'process_id': process_id,
        'start_page': start_page,
        'end_page': end_page,
        'cards': cards_collected,
        'total_processed': len(cards_collected),
        'duplicates_within_process': duplicates_found
    }


def process_customers_page_range(args):
    """
    Функція для обробки діапазону сторінок клієнтів в окремому процесі
    """
    start_page, end_page, api_key, proxy_config, process_id, existing_client_ids, funnel_id, dry_run = args
    
    # Налаштовуємо логування для процесу
    logger = logging.getLogger(f"Process-{process_id}")
    
    # Створюємо менеджер для цього процесу (без видалення дублікатів у процесах)
    manager = KeyCRMNewsletterManager(api_key, proxy_config, auto_delete_duplicates=False)
    
    logger.info(f"🚀 Процес {process_id}: обробляємо сторінки {start_page}-{end_page}")
    
    new_leads_created = 0
    total_customers_checked = 0
    
    try:
        for page in range(start_page, end_page + 1):
            try:
                # Отримуємо покупців з поточної сторінки (без include параметра)
                response = manager.make_request(f"/buyer?page={page}&limit=50")
                
                if not response or 'data' not in response:
                    logger.info(f"P{process_id}: Сторінка {page} порожня або помилка")
                    continue
                
                customers = response['data']
                if not customers:
                    logger.info(f"P{process_id}: Досягнуто кінця списку на сторінці {page}")
                    break
                
                for customer in customers:
                    buyer_id = customer.get('id')
                    if not buyer_id:
                        continue
                    
                    total_customers_checked += 1
                    
                    # Перевіряємо чи клієнт вже є в воронці (по client_id)
                    if buyer_id not in existing_client_ids:
                        if not dry_run:
                            # Створюємо нову картку
                            result = manager.create_pipeline_card(
                                buyer_id=buyer_id,
                                pipeline_id=funnel_id,
                                status_id=241,  # Правильний статус
                                buyer_info=customer
                            )
                            if result:
                                new_leads_created += 1
                                existing_client_ids.add(buyer_id)  # Додаємо до кешу
                                logger.info(f"➕ P{process_id}: Додано клієнта {buyer_id} ({customer.get('full_name', 'Невідомо')}) до воронки")
                            else:
                                logger.warning(f"⚠️ P{process_id}: Не вдалося додати клієнта {buyer_id}")
                        else:
                            new_leads_created += 1
                            if new_leads_created <= 5:  # Показуємо перші 5 для демонстрації
                                logger.info(f"➕ P{process_id}: [DRY RUN] Був би доданий клієнт {buyer_id} ({customer.get('full_name', 'Невідомо')})")
                
                # Логуємо прогрес кожні 10 сторінок
                if page % 10 == 0:
                    logger.info(f"📊 P{process_id}: сторінка {page}/{end_page}, перевірено {total_customers_checked} клієнтів, додано {new_leads_created}")
                
                # Затримка між запитами
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ P{process_id}: Помилка обробки сторінки {page}: {e}")
                time.sleep(5)  # Більша затримка при помилці
                continue
    
    except Exception as e:
        logger.error(f"❌ P{process_id}: Критична помилка: {e}")
    
    pages_processed = end_page - start_page + 1
    logger.info(f"✅ Процес {process_id} завершено: {pages_processed} сторінок, {total_customers_checked} клієнтів перевірено, {new_leads_created} нових лідів")
    
    return {
        'process_id': process_id,
        'start_page': start_page,
        'end_page': end_page,
        'customers_checked': total_customers_checked,
        'new_leads_created': new_leads_created
    }


def process_all_customers_multiprocess(manager, existing_cards, existing_client_ids, num_processes=5, dry_run=True, test_mode=False):
    """
    Багатопроцесорна перевірка всіх клієнтів та додавання їх до воронки розсилки
    """
    logger.info("=" * 80)
    logger.info("📨 БАГАТОПРОЦЕСОРНЕ ДОДАВАННЯ КЛІЄНТІВ ДО ВОРОНКИ РОЗСИЛКИ")
    logger.info("=" * 80)
    
    try:
        # Отримуємо загальну кількість сторінок
        total_pages, total_records = manager.get_total_customers_pages(limit=50)
        
        if total_pages == 0:
            logger.error("❌ Не вдалося отримати кількість сторінок")
            return {}
        
        # В тестовому режимі обробляємо тільки перші 5 сторінок
        if test_mode:
            total_pages = min(5, total_pages)
            logger.info(f"🧪 ТЕСТОВИЙ РЕЖИМ: обробляємо тільки {total_pages} сторінок")
        
        logger.info(f"📊 Загальна кількість сторінок: {total_pages}")
        logger.info(f"👥 Клієнтів вже в воронці: {len(existing_cards)}")
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
            (start_page, end_page, manager.api_key, manager.proxy_config, 
             process_id, existing_client_ids, manager.newsletter_funnel_id, dry_run)
            for start_page, end_page, process_id in process_ranges
        ]
        
        # Запускаємо багатопроцесорну обробку
        start_time = datetime.now()
        
        logger.info(f"🚀 Запускаємо {len(process_args)} процесів...")
        
        all_results = {}
        total_customers_checked = 0
        total_new_leads = 0
        completed_processes = 0
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            # Подаємо завдання
            future_to_process = {
                executor.submit(process_customers_page_range, args): args[4]  # process_id
                for args in process_args
            }
            
            # Збираємо результати
            for future in as_completed(future_to_process):
                process_id = future_to_process[future]
                
                try:
                    result = future.result()
                    total_customers_checked += result['customers_checked']
                    total_new_leads += result['new_leads_created']
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
        logger.info(f"Клієнтів перевірено: {total_customers_checked}")
        logger.info(f"Нових лідів {'додано' if not dry_run else 'знайдено'}: {total_new_leads}")
        logger.info(f"Час виконання: {execution_time:.1f} секунд")
        
        if total_customers_checked > 0:
            logger.info(f"Швидкість: {total_customers_checked/execution_time:.1f} клієнтів/сек")
        
        logger.info("=" * 80)
        
        return {
            'total_customers_checked': total_customers_checked,
            'new_leads_created': total_new_leads,
            'execution_time': execution_time
        }
        
    except Exception as e:
        logger.error(f"❌ Критична помилка багатопроцесорної обробки: {e}")
        return {}


def generate_report(existing_cards, results, filename=None):
    """Генерація звіту"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"newsletter_leads_report_{timestamp}"
    
    # Вилучаємо client_id з карток
    existing_client_ids = []
    if existing_cards:
        for card in existing_cards:
            contact = card.get('contact', {})
            if contact and 'client_id' in contact:
                existing_client_ids.append(contact['client_id'])
    
    report_data = {
        'generation_date': datetime.now().isoformat(),
        'summary': {
            'existing_leads_count': len(existing_cards) if existing_cards else 0,
            'unique_clients_count': len(existing_client_ids),
            'customers_checked': results.get('total_customers_checked', 0),
            'new_leads_created': results.get('new_leads_created', 0),
            'execution_time_seconds': results.get('execution_time', 0)
        },
        'existing_client_ids': existing_client_ids
    }
    
    # JSON звіт
    with open(f"{filename}.json", 'w', encoding='utf-8') as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)
    
    # CSV звіт
    with open(f"{filename}.csv", 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Метрика', 'Значення'])
        writer.writerow(['Дата генерації', report_data['generation_date']])
        writer.writerow(['Існуючих лідів у воронці', len(existing_cards) if existing_cards else 0])
        writer.writerow(['Унікальних клієнтів', len(existing_client_ids)])
        writer.writerow(['Клієнтів перевірено', results.get('total_customers_checked', 0)])
        writer.writerow(['Нових лідів створено', results.get('new_leads_created', 0)])
        writer.writerow(['Час виконання (сек)', results.get('execution_time', 0)])
    
    logger.info(f"📋 Звіти збережено: {filename}.json та {filename}.csv")

def load_config():
    """Завантаження конфігурації"""
    try:
        with open('config_newsletter.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logger.warning("⚠️ Конфігураційний файл config_newsletter.json не знайдено, використовуємо за замовчуванням")
        return {
            "api_key": "M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ",
            "newsletter_funnel_id": 15,
            "auto_delete_duplicates": True,  # Автоматичне переміщення дублікатів в статус 257
            "duplicate_status_id": 257,  # Статус для дублікатів
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
    print("📨 УПРАВЛІННЯ ЛІДАМИ В ВОРОНЦІ РОЗСИЛКИ KEYCRM")
    print("=" * 80)
    
    # Завантажуємо конфігурацію
    config = load_config()
    
    # Налаштовуємо проксі
    proxy_config = None
    if config.get('proxy', {}).get('enabled', False):
        proxy_config = config['proxy']
        print(f"🌐 Проксі: {proxy_config['host']}:{proxy_config['port']}")
    else:
        print("⚠️ Проксі вимкнено або не налаштовано в конфігурації")
    
    # Визначаємо кількість процесів
    if proxy_config:
        num_processes = 10  # Принудово 5 процесів з проксі
    else:
        num_processes = min(mp.cpu_count(), 2)  # Максимум 2 процеси без проксі
    print(f"🔄 Кількість процесів: {num_processes}")
    
    # Створюємо менеджер
    manager = KeyCRMNewsletterManager(
        api_key=config['api_key'],
        proxy_config=proxy_config,
        auto_delete_duplicates=config.get('auto_delete_duplicates', True),
        duplicate_status_id=config.get('duplicate_status_id', 257)
    )
    
    # Перевизначаємо ID воронки з конфігурації якщо є
    if 'newsletter_funnel_id' in config:
        manager.newsletter_funnel_id = config['newsletter_funnel_id']
        print(f"📨 Воронка розсилки: ID {manager.newsletter_funnel_id}")
    else:
        print(f"📨 Воронка розсилки: ID {manager.newsletter_funnel_id} (за замовчуванням)")
    
    try:
        start_time = datetime.now()
        
        # Спочатку отримуємо всіх існуючих карток з воронки розсилки
        print(f"🔍 Отримуємо існуючі картки з воронки {manager.newsletter_funnel_id}...")
        
        # Запитуємо користувача про кількість сторінок карток для обробки
        while True:
            print(f"\nОберіть режим отримання існуючих карток з воронки:")
            print("1. Тестовий (10 сторінок) - швидко, але неточно")
            print("2. Частковий (100 сторінок) - компроміс між швидкістю і точністю")
            print("3. Повний (всі 539 сторінок) - повна точність, але повільно")
            
            cards_choice = input("Ваш вибір (1/2/3): ").strip()
            
            if cards_choice == "1":
                test_mode_pages = 10
                print("⚡ Швидкий режим: 10 сторінок")
                break
            elif cards_choice == "2":
                test_mode_pages = 100
                print("⚖️ Збалансований режим: 100 сторінок")
                break
            elif cards_choice == "3":
                test_mode_pages = None  # Всі сторінки
                print("🔍 Повний режим: всі сторінки")
                break
            else:
                print("❌ Невірний вибір, спробуйте ще раз")
                continue
        
        # Визначаємо чи використовувати мультипроцесорне отримання карток
        use_multiprocess_cards = True  # Можна зробити налаштування
        
        # Отримуємо інформацію про воронку для попередження
        first_response = manager.get_pipeline_cards(manager.newsletter_funnel_id, 1)
        total_records = first_response.get('total', 0) if first_response else 0
        
        if test_mode_pages and test_mode_pages < 500:
            print("⚠️  УВАГА: Обмежений режим може призвести до створення дублікатів!")
            print(f"   У воронці є {total_records} карток, а ви сканируєте тільки {test_mode_pages} сторінок")
            print("   Рекомендується використовувати повний режим для точності")
        
        if use_multiprocess_cards:
            # Мультипроцесорне отримання карток (швидше для великих воронок)
            cards_processes = max(3, num_processes)  # Не більше 3 процесів для карток
            existing_cards = manager.get_all_pipeline_cards_multiprocess(
                manager.newsletter_funnel_id, 
                max_pages=test_mode_pages,
                num_processes=cards_processes
            )
        else:
            # Звичайне послідовне отримання карток
            existing_cards = manager.get_all_pipeline_cards(
                manager.newsletter_funnel_id, 
                max_pages=test_mode_pages
            )
        
        # Створюємо множину client_id для швидкого пошуку
        existing_client_ids = set()
        if existing_cards:
            # existing_cards тепер є словником {client_id: card}
            existing_client_ids = set(existing_cards.keys())
        
        # Конвертуємо словник карток в список для сумісності
        existing_cards_list = list(existing_cards.values()) if existing_cards else []
        
        print(f"📊 Знайдено {len(existing_cards_list)} карток, з них {len(existing_client_ids)} унікальних клієнтів")
        
        if not existing_cards_list:
            logger.warning("⚠️ Не знайдено існуючих карток у воронці розсилки")
        
        # Запитуємо користувача про режим виконання
        dry_run = True
        test_mode = False
        
        while True:
            print("\nОберіть режим роботи:")
            print("1. Тестовий прогон (перші 5 сторінок, без змін)")
            print("2. Повний прогон (без змін - показати скільки буде додано)")
            print("3. Повний прогон (з реальними змінами)")
            
            choice = input("Ваш вибір (1/2/3): ").strip()
            
            if choice == "1":
                dry_run = True
                test_mode = True
                break
            elif choice == "2":
                dry_run = True
                test_mode = False
                break
            elif choice == "3":
                dry_run = False
                test_mode = False
                print("⚠️ УВАГА: Будуть внесені реальні зміни!")
                confirm = input("Ви впевнені? (y/N): ").strip().lower()
                if confirm == 'y':
                    break
                else:
                    continue
            else:
                print("❌ Невірний вибір, спробуйте ще раз")
                continue
        
        # Виконуємо багатопроцесорну обробку
        results = process_all_customers_multiprocess(
            manager=manager,
            existing_cards=existing_cards_list,
            existing_client_ids=existing_client_ids,
            num_processes=num_processes,
            dry_run=dry_run,
            test_mode=test_mode
        )
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if results:
            # Генеруємо звіт
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            generate_report(existing_cards, results, f"newsletter_leads_report_{timestamp}")
            
            print(f"\n✅ Обробку завершено!")
            print(f"⏱️  Загальний час виконання: {execution_time:.1f} секунд")
            print(f"👥 Існуючих лідів у воронці: {len(existing_cards) if existing_cards else 0}")
            print(f"🔍 Клієнтів перевірено: {results.get('total_customers_checked', 0)}")
            print(f"➕ Нових лідів {'додано' if not dry_run else 'знайдено'}: {results.get('new_leads_created', 0)}")
        
    except KeyboardInterrupt:
        print("\n❌ Операцію перервано користувачем")
    except Exception as e:
        logger.error(f"Критична помилка: {e}")
        print(f"\n❌ Помилка: {e}")


if __name__ == "__main__":
    # Налаштування для multiprocessing
    mp.set_start_method('spawn', force=True)
    
    # Запуск основної функції
    main()
