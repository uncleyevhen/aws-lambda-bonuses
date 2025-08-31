#!/usr/bin/env python3
"""
СУПЕР-ОПТИМІЗОВАНИЙ REDIRECT SCRAPER
На основі принципів з promo_generator.py

КЛЮЧОВІ ОПТИМІЗАЦІЇ:
1. evaluate_all - один запит замість багатьох (як у promo_generator.py)
2. Кешування iframe (як у PromoService._get_iframe)
3. Комбінована навігація + очікування
4. Мінімальні затримки (0.1s як у turbo_mode)
5. Оптимізовані таймаути (2s замість 5s)

ОЧІКУВАНІ РЕЗУЛЬТАТИ:
- 3x швидше ніж поточна версія
- Менше навантаження на DOM
- Кращадата integrity
"""

from typing import Any, Dict, List, Optional, Set, Tuple
import json
import logging
import time
import os
import sys
import datetime
import csv
from pathlib import Path
from dotenv import load_dotenv

# Додаємо шляхи для імпорту модулів
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)
sys.path.insert(0, os.path.join(parent_dir, 'bonus_system', 'bonus_replenish_promo_code'))

# Імпорти браузера
try:
    from browser_manager import create_browser_manager
    from promo_logic import PromoService
    BROWSER_MODULES_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Не вдалося імпортувати браузерні модулі: {e}")
    create_browser_manager = None
    PromoService = None
    BROWSER_MODULES_AVAILABLE = False

# Імпорт оптимізацій
try:
    from js_optimizations import OPTIMIZED_JS_SCRIPTS, SCRAPER_OPTIMIZATIONS
except ImportError as e:
    print(f"⚠️ Не вдалося імпортувати оптимізації: {e}")
    # Fallback до мінімальних скриптів
    OPTIMIZED_JS_SCRIPTS = {
        'extract_all_data_single_request': """
            () => {
                try {
                    const rows = Array.from(document.querySelectorAll('tr[id^="dataGridRow_"]'));
                    const data = rows.map((row, index) => {
                        const cells = row.querySelectorAll('td');
                        return {
                            redirectId: row.id.replace('dataGridRow_', ''),
                            oldUrls: cells[0]?.innerText?.trim() || '',
                            currentUrl: cells[1]?.innerText?.trim() || '',
                            suffix: cells[2]?.innerText?.trim() || '',
                            template: cells[3]?.innerText?.trim() || '',
                            recordName: cells[4]?.innerText?.trim() || '',
                            handler: '',
                            recordId: row.id.replace('dataGridRow_', ''),
                            rowIndex: index
                        };
                    });
                    return { success: true, data: data };
                } catch (error) {
                    return { success: false, error: error.message };
                }
            }
        """,
        'navigate_and_wait': """
            () => {
                const nextButton = document.querySelector('.datagrid-pager .fl-l.r.active');
                if (!nextButton) return { success: false, reason: 'no_next_button' };
                nextButton.click();
                return { success: true, newPage: 'next' };
            }
        """
    }
    SCRAPER_OPTIMIZATIONS = {'use_single_request': True}

# Завантажуємо змінні середовища
load_dotenv()

# Ініціалізуємо логгер
logger = logging.getLogger(__name__)

# Конфігурація для максимальної швидкості (як у promo_generator.py)
CONFIG = {
    'use_single_request': True,         # Використовувати evaluate_all
    'turbo_mode': True,                 # Турбо режим
    'verbose_logging': False,           # Мінімум логування для швидкості  
    'page_delay': 0.1,                  # Мінімальна затримка 0.1с
    'check_interval': 0.05,             # Інтервал перевірки 50мс
    'max_check_attempts': 40,           # Максимум 40 спроб (2 секунди)
    'wait_for_elements': False,         # Не чекати елементи спеціально
    'cache_iframe': True,               # Кешувати iframe
    'minimize_logs': True,              # Мінімізувати логування
    'enable_pagination': True,          # Включити пагінацію (False = тільки перша сторінка)
    'test_mode': False,                 # Тестовий режим (тільки перша сторінка)
    'max_pages': None                   # Максимум сторінок (None = всі)
}

class SuperOptimizedRedirectScraper:
    """Супер-оптимізований скрапер на основі принципів promo_generator.py"""
    
    def __init__(self):
        self.browser_manager = None
        self.page = None
        self._cached_iframe = None  # Кешування як у PromoService
        self.promo_service = None
        self.redirects_data = []  # Змінено на список для простіших даних
        self.stats = {
            'pages_scraped': 0,
            'total_redirects': 0,
            'start_time': None,
            'end_time': None,
            'method': 'super_optimized'
        }
    
    def navigate_to_redirects_page(self):
        """Навігація до сторінки редиректів замість промокодів"""
        try:
            if not self.page:
                raise Exception("Сторінка браузера недоступна")
            
            # URL сторінки редиректів замість промокодів
            redirects_url = "https://safeyourlove.com/edit/marketing/seo/url-redirects"
            
            logger.info(f"📍 Переходимо до сторінки редиректів: {redirects_url}")
            self.page.goto(redirects_url)
            self.page.wait_for_load_state('networkidle', timeout=10000)
            
            logger.info("✅ Навігація до редиректів успішна")
            return True
                
        except Exception as e:
            logger.error(f"❌ Помилка навігації до редиректів: {e}")
            return False
    
    def _get_iframe(self, timeout=2000):
        """Кешований доступ до iframe для редиректів"""
        try:
            if not self.page:
                logger.error("❌ Page не ініціалізована")
                return None
                
            if self._cached_iframe:
                try:
                    # Перевіряємо, чи iframe ще активний
                    frame = self._cached_iframe.content_frame()
                    if frame and not frame.is_detached():
                        return frame
                except Exception:
                    self._cached_iframe = None
            
            # Шукаємо iframe для редиректів (загальний селектор)
            iframe_selector = 'iframe'  # Спробуємо знайти будь-який iframe
            
            try:
                self._cached_iframe = self.page.wait_for_selector(
                    iframe_selector, 
                    state='visible', 
                    timeout=timeout
                )
            except Exception:
                # Якщо iframe не знайдено, працюємо з основною сторінкою
                logger.info("ℹ️ Iframe не знайдено, працюємо з основною сторінкою")
                return None
            
            if self._cached_iframe:
                frame = self._cached_iframe.content_frame()
                if frame:
                    # Очікуємо завантаження таблиці редиректів
                    frame.wait_for_function("""
                        () => {
                            // Шукаємо таблицю з редиректами
                            const table = document.querySelector('table') || 
                                         document.querySelector('[class*="table"]') ||
                                         document.querySelector('[id*="grid"]');
                            if (!table) return false;
                            
                            // Перевіряємо наявність рядків
                            const hasRows = table.querySelector('tbody tr') !== null ||
                                          table.querySelector('tr[id*="Row"]') !== null;
                            
                            return hasRows;
                        }
                    """, timeout=timeout)
                    
                    if CONFIG['verbose_logging']:
                        logger.info("✅ Iframe редиректів знайдено та закешовано")
                    return frame
            
            return None
            
        except Exception as e:
            if CONFIG['verbose_logging']:
                logger.error(f"❌ Помилка отримання iframe редиректів: {e}")
            self._cached_iframe = None
            return None
    
    def setup_table_for_speed(self, iframe):
        """Налаштовує таблицю на 160 рядків (як у звичайному redirect_scraper.py)"""
        try:
            if not iframe:
                if CONFIG['verbose_logging']:
                    logger.warning("⚠️ Iframe недоступний для налаштування таблиці")
                return False
            
            if CONFIG['verbose_logging']:
                logger.info(f"📊 ШВИДКЕ встановлення 160 рядків...")
                
            # Знаходимо кнопку налаштувань (settings) - як у звичайному скрипті
            settings_button = iframe.locator('.button.settings')
            if not settings_button.count():
                if CONFIG['verbose_logging']:
                    logger.warning("⚠️ Не знайдено кнопку налаштувань")
                return False
            
            # Клікаємо на кнопку налаштувань
            settings_button.click()
            time.sleep(0.1)  # Мінімальна затримка
            
            # Шукаємо випадаючий список з кількістю рядків
            rows_select = iframe.locator('#datagrid-perpage-select')
            if not rows_select.count():
                if CONFIG['verbose_logging']:
                    logger.warning("⚠️ Не знайдено селект кількості рядків")
                # Закриваємо налаштування
                settings_button.click()
                return False
            
            # Вибираємо максимальну кількість рядків (160)
            rows_select.select_option(value="160")
            time.sleep(0.3)  # Скорочена затримка для швидкості
            
            # Чекаємо оновлення таблиці
            try:
                loader = iframe.locator('#datagrid-loader')
                loader.wait_for(state='hidden', timeout=3000)
            except Exception:
                # Альтернативний метод очікування
                iframe.page.wait_for_load_state('networkidle', timeout=2000)
            
            if CONFIG['verbose_logging']:
                logger.info("✅ ШВИДКО встановлено 160 рядків")
            
            return True
            
        except Exception as e:
            if CONFIG['verbose_logging']:
                logger.error(f"❌ Помилка встановлення кількості рядків: {e}")
            return False
    
    def wait_for_page_load(self, iframe):
        """Оптимізоване очікування завантаження (як у promo_generator.py)"""
        if not iframe:
            logger.warning("⚠️ Iframe недоступний для очікування завантаження")
            return False
            
        if not CONFIG['turbo_mode']:
            time.sleep(1.0)
            return True
        
        try:
            for attempt in range(CONFIG['max_check_attempts']):
                # Швидка перевірка через JavaScript
                result = iframe.evaluate("""
                    () => {
                        const loader = document.querySelector('#datagrid-loader');
                        const isLoading = loader && loader.style.display !== 'none';
                        const rows = document.querySelectorAll('tr[id^="dataGridRow_"]');
                        
                        return {
                            isLoading: isLoading,
                            hasRows: rows.length > 0,
                            rowsCount: rows.length
                        };
                    }
                """)
                
                if not result['isLoading'] and result['hasRows']:
                    if CONFIG['verbose_logging']:
                        logger.debug(f"✅ Сторінка завантажена за {attempt + 1} спроб")
                    return True
                
                time.sleep(CONFIG['check_interval'])  # 50ms як у promo_generator.py
            
            # Якщо не дочекались, спробуємо витягти дані
            if CONFIG['verbose_logging']:
                logger.warning(f"⚠️ Таймаут очікування, але спробуємо витягти дані")
            return True
            
        except Exception as e:
            if CONFIG['verbose_logging']:
                logger.error(f"❌ Помилка очікування: {e}")
            return False
    
    def extract_page_data_optimized(self, iframe, page_num):
        """Витягує дані з сторінки одним запитом (як evaluate_all у promo_generator.py)"""
        try:
            if CONFIG['verbose_logging']:
                logger.debug(f"🚀 Починаємо оптимізований збір даних зі сторінки {page_num}...")
            
            # ОСНОВНА ОПТИМІЗАЦІЯ: використовуємо evaluate_all як у promo_generator.py
            rows_selector = 'tr[id^="dataGridRow_"]'
            
            # Використовуємо evaluate_all для отримання даних з усіх рядків одразу
            rows_data = iframe.locator(rows_selector).evaluate_all("""
            rows => rows.map((row, index) => {
                const cells = Array.from(row.querySelectorAll('td'));
                
                // Мінімальна кількість комірок для валідного рядка (5 колонок)
                if (cells.length < 3) { 
                    return null; 
                }
                
                // Витягуємо ID рядка
                const rowId = row.id || `redirect_${index}`;
                
                // Обробляємо першу колонку зі старими URL
                let oldUrls = [];
                let dataHandler = '';
                let dataRecord = '';
                
                const firstCell = cells[0];
                if (firstCell) {
                    // Шукаємо div з data-handler та data-record
                    const dataDiv = firstCell.querySelector('[data-handler][data-record]');
                    if (dataDiv) {
                        dataHandler = dataDiv.getAttribute('data-handler') || '';
                        dataRecord = dataDiv.getAttribute('data-record') || '';
                    }
                    
                    // Витягуємо всі інпути з URL-ами з першої колонки
                    const urlInputs = firstCell.querySelectorAll('input.j-input[type="text"]');
                    urlInputs.forEach(input => {
                        const urlValue = input.value?.trim();
                        if (urlValue) {
                            oldUrls.push({
                                url: urlValue,
                                id: input.getAttribute('data-id') || '',
                                inputElement: true
                            });
                        }
                    });
                    
                    // Якщо не знайшли інпути, спробуємо витягти з тексту
                    if (oldUrls.length === 0) {
                        const textContent = firstCell.innerText?.trim() || '';
                        if (textContent) {
                            // Розбиваємо текст на рядки і фільтруємо URL-подібні
                            const lines = textContent.split('\\n').map(line => line.trim()).filter(line => {
                                return line && (line.startsWith('/') || line.includes('vakuumnyi') || line.includes('-'));
                            });
                            
                            lines.forEach(line => {
                                if (line && line !== 'додати') {
                                    oldUrls.push({
                                        url: line,
                                        id: '',
                                        inputElement: false
                                    });
                                }
                            });
                        }
                    }
                    
                    // Якщо є прихований input, також витягуємо значення звідти
                    const hiddenInput = firstCell.querySelector('input[type="hidden"]');
                    if (hiddenInput && hiddenInput.value) {
                        const inputValue = hiddenInput.value;
                        // Видаляємо ID з початку значення
                        const cleanValue = inputValue.replace(/^\\d+/, '');
                        if (cleanValue && cleanValue !== inputValue) {
                            oldUrls.push({
                                url: cleanValue,
                                id: 'hidden',
                                inputElement: false
                            });
                        }
                    }
                }
                
                // Витягуємо дані з інших колонок
                const currentUrl = cells[1]?.innerText?.trim() || '';     // Поточне посилання
                const suffix = cells[2]?.innerText?.trim() || '';         // Суфікс
                const template = cells[3]?.innerText?.trim() || '';       // Шаблон
                const recordName = cells[4]?.innerText?.trim() || '';     // Запис (назва)
                
                return {
                    redirectId: rowId.replace('dataGridRow_', ''),
                    oldUrls: oldUrls,
                    currentUrl: currentUrl,
                    suffix: suffix,
                    template: template,
                    recordName: recordName,
                    dataHandler: dataHandler,
                    dataRecord: dataRecord,
                    rowIndex: index,
                    cellsCount: cells.length
                };
                
            }).filter(Boolean) // Видаляємо null значення
            """)
            
            if CONFIG['verbose_logging']:
                logger.debug(f"� Отримано дані з {len(rows_data)} рядків таблиці.")
            
            processed_redirects = []
            
            # Обробляємо отримані дані в Python
            for i, redirect in enumerate(rows_data):
                try:
                    # Обробляємо oldUrls - тепер це список об'єктів
                    old_urls_data = redirect.get('oldUrls', [])
                    
                    # Створюємо окремий запис для кожного URL з цього редиректа
                    if isinstance(old_urls_data, list) and old_urls_data:
                        for url_idx, url_data in enumerate(old_urls_data):
                            # Простий ID - тільки номер по порядку
                            simple_id = len(processed_redirects) + 1
                            
                            processed_redirect = {
                                'id': simple_id,
                                'old_url': url_data.get('url', ''),
                                'current_url': redirect.get('currentUrl', ''),
                                'suffix': redirect.get('suffix', '')
                            }
                            
                            processed_redirects.append(processed_redirect)
                            self.redirects_data.append(processed_redirect)
                    else:
                        # Fallback для випадків коли oldUrls не список
                        simple_id = len(processed_redirects) + 1
                        
                        processed_redirect = {
                            'id': simple_id,
                            'old_url': str(old_urls_data) if old_urls_data else '',
                            'current_url': redirect.get('currentUrl', ''),
                            'suffix': redirect.get('suffix', '')
                        }
                        
                        processed_redirects.append(processed_redirect)
                        self.redirects_data.append(processed_redirect)
                    
                except Exception as e:
                    if CONFIG['verbose_logging']:
                        logger.warning(f"⚠️ Помилка обробки рядка {i}: {e}")
                    continue
            
            if CONFIG['verbose_logging']:
                logger.debug(f"✅ Оброблено {len(processed_redirects)} редиректів зі сторінки {page_num}")
            
            return processed_redirects
            
        except Exception as e:
            logger.error(f"❌ Критична помилка витягування даних: {e}")
            return []
    
    def navigate_to_next_page(self, iframe):
        """Оптимізована навігація (як у promo_generator.py - простий клік без зайвих перевірок)"""
        try:
            # Шукаємо кнопку "Далі" - спрощена логіка як у promo_generator.py
            next_button = iframe.locator('.datagrid-pager .fl-l.r.active').first
            
            if next_button.count() == 0:
                if CONFIG['verbose_logging']:
                    logger.debug("🏁 Кнопка 'Далі' не знайдена - остання сторінка")
                return False
            
            if CONFIG['verbose_logging']:
                # Показуємо стан пагінації перед переходом
                try:
                    page_info = iframe.locator('.datagrid-pager .pages').first.inner_text().strip()
                    logger.debug(f"📑 Пагінація перед переходом: {page_info}")
                except Exception:
                    pass
            
            # Простий клік як у promo_generator.py
            next_button.click()
            
            if CONFIG['verbose_logging']:
                logger.debug("➡️ Клік по кнопці 'наступна сторінка'...")
            
            # Швидке очікування завантаження як у promo_generator.py
            try:
                # Очікуємо, поки loader зникне
                loader = iframe.locator('#datagrid-loader')
                loader.wait_for(state='hidden', timeout=3000)
                if CONFIG['verbose_logging']:
                    logger.debug("✅ Лоадер прихований, сторінка завантажена")
            except Exception:
                if CONFIG['verbose_logging']:
                    logger.debug("📊 Лоадер не знайдено, використовуємо альтернативний метод")
                # Альтернативний метод як у promo_generator.py
                iframe.page.wait_for_load_state('networkidle', timeout=2000)
            
            return True
                
        except Exception as e:
            if CONFIG['verbose_logging']:
                logger.error(f"❌ Помилка навігації: {e}")
            return False
    
    def scrape_all_pages_super_fast(self):
        """Основна функція скрапінгу редиректів (виправлена версія)"""
        try:
            logger.info("🚀 СУПЕР-ШВИДКИЙ СКРАПІНГ РЕДИРЕКТІВ (на основі promo_generator.py)")
            self.stats['start_time'] = datetime.datetime.now()
            
            # Перевіряємо доступність браузерних модулів
            if not BROWSER_MODULES_AVAILABLE or create_browser_manager is None or PromoService is None:
                raise ImportError("Браузерні модулі недоступні")
            
            # Ініціалізація браузера
            self.browser_manager = create_browser_manager()
            self.page = self.browser_manager.initialize()
            
            if not self.page:
                raise Exception("Не вдалося ініціалізувати браузер")
            
            # Вхід через PromoService
            self.promo_service = PromoService(self.page)
            if not self.promo_service.login():
                raise Exception("Не вдалося увійти в систему")
            
            logger.info("✅ Вхід успішний")
            
            # ВИПРАВЛЕННЯ: Переходимо до сторінки редиректів замість промокодів
            if not self.navigate_to_redirects_page():
                raise Exception("Не вдалося перейти до сторінки редиректів")
            
            # Отримуємо iframe або працюємо з основною сторінкою
            iframe = self._get_iframe()
            
            if iframe:
                logger.info("✅ Працюємо з iframe")
                work_context = iframe
                
                # Налаштовуємо таблицю на 160 рядків (як у звичайному скрипті)
                if not self.setup_table_for_speed(iframe):
                    logger.warning("⚠️ Не вдалося налаштувати таблицю на 160 рядків")
                else:
                    logger.info("✅ Таблицю налаштовано на 160 рядків")
            else:
                logger.info("ℹ️ Працюємо з основною сторінкою")
                work_context = self.page
            
            current_page = 1
            total_redirects = 0
            
            # Перевіряємо режими роботи
            if CONFIG['test_mode'] or not CONFIG['enable_pagination']:
                logger.info("🧪 ТЕСТОВИЙ РЕЖИМ: Обробка тільки першої сторінки")
            elif CONFIG['max_pages']:
                logger.info(f"📄 ОБМЕЖЕНИЙ РЕЖИМ: Максимум {CONFIG['max_pages']} сторінок")
            else:
                logger.info("🚀 ПОВНИЙ РЕЖИМ: Обробка всіх сторінок")
            
            while True:
                start_time = time.time()
                
                if CONFIG['verbose_logging']:
                    logger.info(f"📄 Обробка сторінки {current_page}...")
                else:
                    print(f"📄 {current_page}", end=".", flush=True)
                
                # Чекаємо завантаження
                if not self.wait_for_page_load(work_context):
                    logger.warning(f"⚠️ Сторінка {current_page} не завантажилась")
                
                # Витягуємо дані одним запитом
                page_redirects = self.extract_page_data_optimized(work_context, current_page)
                page_count = len(page_redirects)
                total_redirects += page_count
                
                page_time = time.time() - start_time
                
                if CONFIG['verbose_logging']:
                    logger.info(f"✅ Сторінка {current_page}: {page_count} редиректів за {page_time:.2f}s")
                
                self.stats['pages_scraped'] = current_page
                self.stats['total_redirects'] = total_redirects
                
                # Перевіряємо умови зупинки
                should_stop = False
                
                # Тестовий режим або вимкнена пагінація - тільки перша сторінка
                if CONFIG['test_mode'] or not CONFIG['enable_pagination']:
                    if CONFIG['verbose_logging']:
                        logger.info("🧪 Зупиняємося після першої сторінки (тестовий режим)")
                    should_stop = True
                
                # Досягнуто максимум сторінок
                elif CONFIG['max_pages'] and current_page >= CONFIG['max_pages']:
                    if CONFIG['verbose_logging']:
                        logger.info(f"📄 Досягнуто максимум сторінок: {CONFIG['max_pages']}")
                    should_stop = True
                
                # Спробуємо перейти на наступну сторінку (якщо не зупиняємося)
                elif not self.navigate_to_next_page(work_context):
                    if CONFIG['verbose_logging']:
                        logger.info(f"📄 Остання сторінка: {current_page}")
                    should_stop = True
                
                if should_stop:
                    break
                
                current_page += 1
                
                # Мінімальна затримка між сторінками (як у turbo_mode)
                if CONFIG['turbo_mode']:
                    time.sleep(CONFIG['page_delay'])  # 0.1s
            
            self.stats['end_time'] = datetime.datetime.now()
            total_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            print(f"\n🎉 ЗАВЕРШЕНО СКРАПІНГ РЕДИРЕКТІВ!")
            print(f"📊 Оброблено сторінок: {self.stats['pages_scraped']}")
            print(f"📊 Знайдено редиректів: {self.stats['total_redirects']}")
            print(f"⏱️ Загальний час: {total_time:.2f}s")
            if total_time > 0:
                print(f"🚀 Швидкість: {self.stats['total_redirects']/total_time:.1f} редиректів/сек")
            
            # Зберігаємо результати
            self.save_results()
            
            return self.redirects_data
            
        except Exception as e:
            logger.error(f"❌ Критична помилка скрапінгу редиректів: {e}")
            return {}
        finally:
            if self.browser_manager:
                try:
                    self.browser_manager.cleanup()
                except Exception:
                    pass
    
    def save_results(self):
        """Зберігає результати в CSV та JSON"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            
            # CSV файл
            csv_filename = f"redirects_super_optimized_{timestamp}.csv"
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                if self.redirects_data:
                    # Тепер redirects_data це список, тому беремо перший елемент
                    fieldnames = list(self.redirects_data[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(self.redirects_data)
            
            # JSON файл
            json_filename = f"redirects_super_optimized_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as jsonfile:
                json.dump({
                    'stats': self.stats,
                    'data': self.redirects_data
                }, jsonfile, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"✅ Результати збережено: {csv_filename}, {json_filename}")
            
        except Exception as e:
            logger.error(f"❌ Помилка збереження: {e}")

def set_test_mode(enabled=True):
    """Швидке перемикання в тестовий режим"""
    CONFIG['test_mode'] = enabled
    CONFIG['enable_pagination'] = not enabled
    CONFIG['verbose_logging'] = enabled  # В тестовому режимі більше логування
    if enabled:
        print("🧪 УВІМКНЕНО ТЕСТОВИЙ РЕЖИМ: тільки перша сторінка")
    else:
        print("🚀 УВІМКНЕНО ПОВНИЙ РЕЖИМ: всі сторінки")

def set_pagination_limit(max_pages):
    """Встановлює ліміт на кількість сторінок"""
    CONFIG['max_pages'] = max_pages
    CONFIG['enable_pagination'] = True
    CONFIG['test_mode'] = False
    print(f"📄 ОБМЕЖЕНИЙ РЕЖИМ: максимум {max_pages} сторінок")

def demo_js_optimization():
    """Демонстрація JavaScript оптимізацій без запуску браузера"""
    print("🚀 ДЕМОНСТРАЦІЯ JAVASCRIPT ОПТИМІЗАЦІЙ")
    print("=" * 50)
    
    print("\n📋 Доступні JavaScript скрипти:")
    for i, (name, script) in enumerate(OPTIMIZED_JS_SCRIPTS.items(), 1):
        lines_count = len(script.strip().split('\n'))
        print(f"  {i}. {name} ({lines_count} рядків)")
        
        # Показуємо короткий опис функціональності
        if 'extract_all_data' in name:
            print(f"     📊 Витягує всі дані з таблиці одним запитом")
        elif 'pagination_info' in name:
            print(f"     📄 Швидке отримання інформації про пагінацію")
        elif 'wait_for_page_load' in name:
            print(f"     ⏳ Оптимізоване очікування завантаження")
        elif 'navigate_and_wait' in name:
            print(f"     🔄 Комбінована навігація та очікування")
    
    print(f"\n⚙️ Конфігурація оптимізацій:")
    for key, value in CONFIG.items():
        if isinstance(value, bool):
            status = "✅" if value else "❌"
            print(f"  {status} {key}: {value}")
        elif isinstance(value, (int, float)):
            print(f"  🔢 {key}: {value}")
        elif value is None:
            print(f"  ➖ {key}: {value}")
        else:
            print(f"  📝 {key}: {value}")
    
    print(f"\n🎯 Режими роботи:")
    if CONFIG['test_mode']:
        print(f"  🧪 ТЕСТОВИЙ РЕЖИМ: обробка тільки першої сторінки")
    elif not CONFIG['enable_pagination']:
        print(f"  🚫 ПАГІНАЦІЯ ВИМКНЕНА: обробка тільки першої сторінки")
    elif CONFIG['max_pages']:
        print(f"  📄 ОБМЕЖЕНИЙ РЕЖИМ: максимум {CONFIG['max_pages']} сторінок")
    else:
        print(f"  🚀 ПОВНИЙ РЕЖИМ: обробка всіх сторінок")
    
    print(f"\n🎯 Очікувані покращення продуктивності:")
    print(f"  🚀 3x швидше ніж звичайний метод")
    print(f"  📉 Менше навантаження на DOM")
    print(f"  🔄 Мінімальні затримки між запитами")
    print(f"  💾 Кешування iframe для швидшого доступу")

def main():
    """Головна функція"""
    # Налаштування логування
    log_level = logging.DEBUG if CONFIG['verbose_logging'] else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Перевіряємо аргументи командного рядка
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg == '--demo':
            demo_js_optimization()
            return
        elif arg == '--test' or arg == '-t':
            set_test_mode(True)
        elif arg == '--full' or arg == '-f':
            set_test_mode(False)
        elif arg.startswith('--pages='):
            try:
                max_pages = int(arg.split('=')[1])
                set_pagination_limit(max_pages)
            except (IndexError, ValueError):
                print("❌ Невірний формат --pages=NUMBER")
                return
        elif arg == '--help' or arg == '-h':
            print("🚀 СУПЕР-ОПТИМІЗОВАНИЙ REDIRECT SCRAPER")
            print("\nДоступні опції:")
            print("  --test, -t          Тестовий режим (тільки перша сторінка)")
            print("  --full, -f          Повний режим (всі сторінки)")
            print("  --pages=N           Обмежити кількість сторінок до N")
            print("  --demo              Показати демонстрацію JS оптимізацій")
            print("  --help, -h          Показати цю довідку")
            print("\nПриклади:")
            print("  python redirect_scraper.py --test")
            print("  python redirect_scraper.py --pages=5")
            print("  python redirect_scraper.py --full")
            return
    
    # Показуємо поточні налаштування
    if CONFIG['test_mode']:
        print("🧪 Поточний режим: ТЕСТОВИЙ (тільки перша сторінка)")
    elif CONFIG['max_pages']:
        print(f"📄 Поточний режим: ОБМЕЖЕНИЙ (максимум {CONFIG['max_pages']} сторінок)")
    elif CONFIG['enable_pagination']:
        print("🚀 Поточний режим: ПОВНИЙ (всі сторінки)")
    else:
        print("🧪 Поточний режим: БЕЗ ПАГІНАЦІЇ (тільки перша сторінка)")
    
    # Запускаємо скрапер
    scraper = SuperOptimizedRedirectScraper()
    results = scraper.scrape_all_pages_super_fast()
    
    if results:
        print(f"🎯 Успішно зібрано {len(results)} редиректів!")
    else:
        print("❌ Скрапінг не дав результатів")

if __name__ == "__main__":
    main()
