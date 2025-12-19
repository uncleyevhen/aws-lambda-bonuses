"""
Сервіс поповнення промокодів через Playwright.

Портовано з AWS Lambda: bonus_system/bonus_replenish_promo_code/promo_logic.py
Адаптовано для PostgreSQL замість S3.

Основні можливості:
- Генерація промокодів за форматом BON{сума}{рандом}
- Створення промокодів в адмін-панелі з автозаповненням форми
- Керування сесіями з кешуванням
- Поповнення запасів промокодів в PostgreSQL
"""

import time
import logging
import os
import random
import string
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from playwright.sync_api import Page
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.config import settings
from app.models.promo_code import PromoCode
from app.models.used_codes_count import UsedCodesCount
from app.services.browser_manager import create_browser_manager, cleanup_global_browser

logger = logging.getLogger(__name__)

# Контроль детальності логування
VERBOSE_LOGGING = os.environ.get('VERBOSE_LOGGING', 'false').lower() == 'true'

# Визначаємо чи запущено як subprocess (для коректного логування)
IS_SUBPROCESS = os.environ.get('REPLENISH_SUBPROCESS', 'false').lower() == 'true'


def log_verbose(message):
    """Виводить детальні логи тільки якщо увімкнено VERBOSE_LOGGING"""
    if VERBOSE_LOGGING:
        if IS_SUBPROCESS:
            print(f"DEBUG: {message}", flush=True)
        else:
            logger.debug(message)


def log_info(message):
    """Виводить важливі логи завжди"""
    if IS_SUBPROCESS:
        print(message, flush=True)
    else:
        logger.info(message)


def log_error(message):
    """Виводить помилки завжди"""
    if IS_SUBPROCESS:
        print(f"ERROR: {message}", flush=True)
    else:
        logger.error(message)


class PromoReplenishService:
    """
    Сервіс для поповнення промокодів через Playwright.
    
    Портовано з AWS Lambda, адаптовано для PostgreSQL.
    
    Основні можливості:
    - Генерація поодиноких промокодів за форматом BON{сума}{рандом}
    - Створення промокодів в адмін-панелі з автозаповненням форми
    - Керування сесіями з кешуванням
    - Поповнення запасів промокодів в БД
    """
    
    def __init__(self, db: Session, page: Page = None):
        self.db = db
        self.page = page
        self.admin_url = settings.admin_url or 'https://safeyourlove.com/edit/discounts/codes'
        self.admin_username = settings.admin_username
        self.admin_password = settings.admin_password
        self._cached_iframe = None
        self._session_cookies = None
        self._session_timestamp = None
        self._session_timeout = 3600  # 1 година
        self._session_file = '/tmp/session_cookies.json'
        
        # Цільова кількість кодів (з конфігу або за замовчуванням)
        self.target_codes_per_amount = getattr(settings, 'target_codes_per_amount', 10)
    
    def _generate_single_code_string(self, amount: int) -> str:
        """Генерує один рядок промокоду для заданої суми."""
        amount_str = str(amount)
        prefix = f'BON{amount_str}'
        random_part_length = max(3, 7 - len(amount_str))
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=random_part_length))
        return prefix + random_part
    
    def _get_iframe(self, timeout=300000):
        """Отримує iframe з кешуванням та очікуванням завантаження таблиці"""
        try:
            if self._cached_iframe:
                try:
                    frame = self._cached_iframe.content_frame()
                    if frame and not frame.is_detached():
                        return frame
                except Exception:
                    self._cached_iframe = None

            iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
            self._cached_iframe = self.page.wait_for_selector(iframe_selector, state='visible', timeout=timeout)
            frame = self._cached_iframe.content_frame()

            if frame:
                log_info("⏳ Очікуємо завантаження таблиці промокодів...")
                
                frame.wait_for_function("""
                    () => {
                        const table = document.querySelector('table');
                        if (!table) return false;
                        const hasRows = table.querySelector('tbody tr') !== null;
                        const hasHeaders = table.querySelector('thead tr') !== null;
                        return hasRows || hasHeaders;
                    }
                """, timeout=60000)
                
                log_info("✅ Таблиця промокодів завантажена")

            return frame

        except Exception as e:
            log_error(f"❌ Не вдалося отримати або дочекатися iframe: {e}")
            self._cached_iframe = None
            return None
    
    def apply_amount_filter(self, amount: int) -> bool:
        """Застосовує фільтр по сумі промокодів в адмін-панелі."""
        start_time = time.time()
        
        try:
            frame = self._get_iframe()
            if not frame:
                log_error("❌ Не вдалося отримати iframe для застосування фільтра")
                return False
                
            amount_header = frame.locator('#header_id_4778')
            if not amount_header.count():
                log_error("⚠️ Не знайдено заголовок 'Розмір знижки'")
                return False
            
            amount_header.hover()
            time.sleep(1.0)
            
            filter_block = frame.locator('#sortingBlock_4778')
            if not filter_block.count():
                log_error("⚠️ Не знайдено блок фільтрації")
                return False
            
            from_field = filter_block.locator('input[name="text1"]')
            to_field = filter_block.locator('input[name="text2"]')
            
            if not from_field.count() or not to_field.count():
                log_error("⚠️ Поля фільтрації не знайдено")
                return False
            
            from_field.click()
            from_field.fill('')
            from_field.fill(str(amount))
            time.sleep(0.3)
            
            to_field.click()
            to_field.fill('')
            to_field.fill(str(amount))
            time.sleep(0.3)
            
            to_field.press('Enter')
            time.sleep(3.0)
            
            total_time = time.time() - start_time
            log_info(f"✅ Фільтр по сумі {amount} застосовано за {total_time:.3f}с")
            return True
            
        except Exception as e:
            total_time = time.time() - start_time
            log_error(f"❌ Помилка застосування фільтра після {total_time:.3f}с: {e}")
            return False
    
    def get_active_codes_from_admin(self, amount: int) -> List[str]:
        """
        Отримує активні промокоди для заданої суми з адмін-панелі.
        """
        try:
            frame = self._get_iframe()
            if not frame:
                log_error("❌ Не вдалося отримати iframe для отримання кодів")
                return []
                
            log_info(f"🔍 Отримуємо коди для суми {amount} з адмін-панелі...")
            
            self.apply_amount_filter(amount)
            
            try:
                rows = frame.locator('tbody tr').all()
                total_rows = len(rows)
                log_verbose(f"📊 Всього рядків в таблиці: {total_rows}")
            except Exception as e:
                log_error(f"❌ Помилка при отриманні рядків таблиці: {e}")
                return []
            
            active_codes = []
            
            for i, row in enumerate(rows):
                try:
                    cells = row.locator('td').all()
                    cell_count = len(cells)
                    
                    if cell_count < 4:
                        continue
                    
                    status_cell = cells[2]
                    code_cell = cells[3]
                    
                    status = status_cell.inner_text().strip()
                    code = code_cell.inner_text().strip()
                    
                    if (code.lower() in ['код', 'code', 'промокод', 'promo_code', 'дійсні до'] or
                        'january' in code.lower() or
                        'calendar' in code.lower() or
                        len(code) > 50 or
                        not code.startswith('BON')):
                        continue
                    
                    if not code.strip() or not status.strip():
                        continue
                    
                    is_active = status.lower() in ['так', 'yes', 'активний', 'active']
                    
                    if is_active:
                        active_codes.append(code)
                        
                except Exception as e:
                    log_verbose(f"⚠️ Помилка обробки рядка {i}: {e}")
                    continue
            
            log_info(f"📊 Знайдено {len(active_codes)} активних кодів для суми {amount}")
            return active_codes
            
        except Exception as e:
            log_error(f"❌ Помилка при отриманні кодів для суми {amount}: {e}")
            return []
    
    def create_promo_code(self, promo_code: str, amount: int) -> bool:
        """Створює конкретний промокод в адмін-панелі."""
        start_time = time.time()
        
        try:
            frame = self._get_iframe()
            if not frame:
                raise Exception("Не знайдено iframe після логіну")

            try:
                frame.locator('a.button.add.plus').click()
            except Exception:
                selectors = [
                    'a.button.add',
                    '.button.add.plus',
                    '[class*="add"][class*="button"]',
                    'a[href*="add"]',
                    'button[type="button"]'
                ]
                button_found = False
                for selector in selectors:
                    try:
                        frame.locator(selector).click()
                        button_found = True
                        break
                    except:
                        continue
                
                if not button_found:
                    raise Exception("Не вдалося знайти кнопку 'Додати'")
            
            frame.wait_for_selector('input[name="names[code]"]', timeout=300000)

            date_to = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
            
            fill_form_js = f"""
            (function() {{
                const codeInput = document.querySelector('input[name="names[code]"]');
                const typeSelect = document.querySelector('select[name="names[type]"]');
                const amountInput = document.querySelector('input[name="names[amount]"]');
                const dateLimitInput = document.querySelector('input[name="names[date_limit]"]');
                const activeCheckbox = document.querySelector('input[name="names[active]"][type="checkbox"]');
                
                if (codeInput) {{
                    codeInput.value = '{promo_code}';
                    codeInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                }}
                
                if (typeSelect) {{
                    typeSelect.value = '1';
                    typeSelect.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}
                
                if (amountInput) {{
                    amountInput.value = '{amount}';
                    amountInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                }}
                
                if (dateLimitInput) {{
                    dateLimitInput.value = '{date_to}';
                    dateLimitInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                }}
                
                if (activeCheckbox && !activeCheckbox.checked) {{
                    activeCheckbox.checked = true;
                    activeCheckbox.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}
                
                return 'success';
            }})();
            """
            
            frame.evaluate(fill_form_js)

            select_all_js = """
            (function() {
                const brandsSelect = document.querySelector('select[name="names[brands][]"]');
                if (brandsSelect) {
                    Array.from(brandsSelect.options).forEach(option => {
                        if (option.value && option.value !== '0') option.selected = true;
                    });
                    brandsSelect.dispatchEvent(new Event('change', { bubbles: true }));
                }
                
                const categoriesSelect = document.querySelector('select[name="names[categories][]"]');
                if (categoriesSelect) {
                    Array.from(categoriesSelect.options).forEach(option => {
                        if (option.value && option.value !== '0') option.selected = true;
                    });
                    categoriesSelect.dispatchEvent(new Event('change', { bubbles: true }));
                }
            })();
            """
            
            frame.evaluate(select_all_js)

            frame.locator('input.save-exit-button[type="submit"]').click()
            frame.locator('a.button.add.plus').wait_for(state='visible', timeout=60000)
            
            total_time = time.time() - start_time
            log_info(f"✅ Промокод '{promo_code}' створено за {total_time:.3f}с")
            
            return True
        except Exception as e:
            total_time = time.time() - start_time
            log_error(f"❌ Помилка створення промокоду '{promo_code}' після {total_time:.3f}с: {e}")
            if self.page:
                try:
                    self.page.screenshot(path=f'/tmp/promo_creation_error_{promo_code}.png')
                except:
                    pass
            return False
    
    def _load_session_from_file(self) -> bool:
        """Завантажує сесію з файлу"""
        try:
            if os.path.exists(self._session_file):
                with open(self._session_file, 'r') as f:
                    session_data = json.load(f)
                    self._session_cookies = session_data.get('cookies', [])
                    self._session_timestamp = session_data.get('timestamp', 0)
                    log_info(f"📂 Завантажено сесію з файлу: {len(self._session_cookies)} cookies")
                    return True
        except Exception as e:
            log_error(f"⚠️ Не вдалося завантажити сесію з файлу: {e}")
        return False
    
    def _save_session_to_file(self):
        """Зберігає сесію у файл"""
        try:
            session_data = {
                'cookies': self._session_cookies,
                'timestamp': self._session_timestamp,
                'saved_at': datetime.now().isoformat()
            }
            os.makedirs(os.path.dirname(self._session_file), exist_ok=True)
            
            with open(self._session_file, 'w') as f:
                json.dump(session_data, f, indent=2)
            
            log_info(f"💾 Сесію збережено у файл: {self._session_file}")
        except Exception as e:
            log_error(f"⚠️ Не вдалося зберегти сесію у файл: {e}")
    
    def _save_session(self):
        """Зберігає поточну сесію (cookies)"""
        try:
            self._session_cookies = self.page.context.cookies()
            self._session_timestamp = time.time()
            self._save_session_to_file()
            log_info(f"💾 Збережено сесію з {len(self._session_cookies)} cookies")
        except Exception as e:
            log_error(f"⚠️ Не вдалося зберегти сесію: {e}")
    
    def _is_session_valid(self) -> bool:
        """Перевіряє, чи дійсна збережена сесія"""
        if not self._session_cookies or not self._session_timestamp:
            return False
        
        current_time = time.time()
        age = current_time - self._session_timestamp
        if age > self._session_timeout:
            log_info(f"🕐 Збережена сесія застаріла (вік: {age:.0f}с)")
            return False
        
        log_info(f"✅ Сесія дійсна (вік: {age:.0f}с)")
        return True
    
    def _restore_session(self) -> bool:
        """Відновлює збережену сесію"""
        if not self._session_cookies:
            self._load_session_from_file()
            
        if not self._is_session_valid() or not self._session_cookies:
            return False
            
        try:
            self.page.goto('https://safeyourlove.com/', timeout=30000, wait_until='domcontentloaded')
            
            cookies_to_set = []
            current_time = time.time()
            
            for cookie in self._session_cookies:
                if 'name' not in cookie or 'value' not in cookie:
                    continue
                
                if 'expires' in cookie and cookie['expires'] and cookie['expires'] != -1:
                    if cookie['expires'] < current_time:
                        continue
                
                cookie_dict = {
                    'name': cookie['name'],
                    'value': cookie['value']
                }
                
                if 'domain' in cookie and cookie['domain']:
                    domain = cookie['domain'].lstrip('.')
                    cookie_dict['domain'] = domain
                    if 'path' in cookie and cookie['path']:
                        cookie_dict['path'] = cookie['path']
                else:
                    cookie_dict['url'] = 'https://safeyourlove.com/'
                
                cookies_to_set.append(cookie_dict)
            
            if cookies_to_set:
                self.page.context.add_cookies(cookies_to_set)
                log_info(f"✅ Додано {len(cookies_to_set)} cookies до контексту")
            
            return True
        except Exception as e:
            log_error(f"⚠️ Не вдалося відновити сесію: {e}")
            return False
    
    def login(self) -> bool:
        """Виконує логін в адмін-панель з використанням збереженої сесії"""
        start_time = time.time()
        log_info("🔐 Початок процесу логіну...")
        
        if self._restore_session():
            try:
                log_info("🔄 Перевіряємо збережену сесію...")
                self.page.goto(self.admin_url, timeout=30000, wait_until='domcontentloaded')
                
                try:
                    iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
                    iframe = self.page.wait_for_selector(iframe_selector, state='visible', timeout=300000)
                    if iframe:
                        log_info(f"✅ Сесія дійсна! Знайдено iframe за {time.time() - start_time:.2f}с")
                        return True
                except:
                    pass
                
                try:
                    login_field = self.page.wait_for_selector(
                        'input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]',
                        state='visible',
                        timeout=30000
                    )
                    if login_field:
                        log_info("🔐 Знайдено поля логіну - сесія недійсна")
                except:
                    pass
                    
            except Exception as e:
                log_error(f"⚠️ Помилка при перевірці сесії: {e}")
        
        # Повний логін
        login_start = time.time()
        log_info("🔐 Виконуємо логін...")
        
        self.page.goto(self.admin_url, timeout=30000, wait_until='domcontentloaded')
        
        self.page.wait_for_selector(
            'input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]',
            state='visible',
            timeout=300000
        )
        
        username_field = self.page.query_selector('input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]')
        password_field = self.page.query_selector('input[type="password"]')
        submit_button = self.page.query_selector('button[type="submit"]')
        
        if not username_field or not password_field or not submit_button:
            log_error("❌ Не знайдено поля форми логіну")
            raise Exception("Поля форми логіну не знайдені")
        
        if not self.admin_username or not self.admin_password:
            log_error("❌ Відсутні дані для входу")
            raise Exception("Відсутні дані для входу (ADMIN_USERNAME, ADMIN_PASSWORD)")
            
        username_field.fill(self.admin_username)
        password_field.fill(self.admin_password)
        submit_button.click()
        
        iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
        self.page.wait_for_selector(iframe_selector, state='visible', timeout=60000)
        
        login_time = time.time() - login_start
        log_info(f"✅ Логін завершено за {login_time:.2f}с")
        
        self._save_session()
        
        total_time = time.time() - start_time
        log_info(f"🏁 Весь процес логіну завершено за {total_time:.2f}с")
        return True
    
    # ==================== PostgreSQL методи (замість S3) ====================
    
    def save_promo_code_to_db(self, code: str, amount: int, restore_if_exists: bool = False) -> bool:
        """
        Зберігає промокод в PostgreSQL.
        
        Args:
            code: Код промокоду
            amount: Сума знижки
            restore_if_exists: Якщо True і код існує з is_used=True, відновити статус на is_used=False
        """
        try:
            existing = self.db.query(PromoCode).filter(PromoCode.code == code).first()
            if existing:
                # Якщо код існує і потрібно відновити статус
                if restore_if_exists and existing.is_used:
                    existing.is_used = False
                    self.db.commit()
                    log_info(f"♻️ Промокод {code} відновлено (is_used: True → False)")
                    return True
                else:
                    log_verbose(f"⚠️ Промокод {code} вже існує в БД (is_used={existing.is_used})")
                    return False
            
            promo = PromoCode(
                code=code,
                amount=amount,
                is_used=False
            )
            self.db.add(promo)
            self.db.commit()
            log_info(f"💾 Промокод {code} збережено в БД")
            return True
        except Exception as e:
            self.db.rollback()
            log_error(f"❌ Помилка збереження промокоду в БД: {e}")
            return False
    
    def get_available_codes_count(self, amount: int) -> int:
        """Повертає кількість доступних промокодів для суми"""
        return self.db.query(func.count(PromoCode.id)).filter(
            PromoCode.amount == amount,
            PromoCode.is_used == False
        ).scalar() or 0
    
    def clear_used_codes_count(self, amount: int) -> bool:
        """Очищає лічильник використаних кодів для суми"""
        try:
            self.db.query(UsedCodesCount).filter(
                UsedCodesCount.amount == amount
            ).delete()
            self.db.commit()
            log_info(f"🗑️ Очищено лічильник для суми {amount}")
            return True
        except Exception as e:
            self.db.rollback()
            log_error(f"❌ Помилка очищення лічильника: {e}")
            return False
    
    def replenish_promo_codes(self, used_codes_count: Dict[str, int]) -> bool:
        """
        Поповнює запаси промокодів.
        
        Аналог AWS Lambda replenish_promo_codes, адаптований для PostgreSQL.
        
        Args:
            used_codes_count: dict {"100": 5, "200": 3} - суми які потребують перевірки
        """
        log_info(f"🧠 [Smart] Розумне поповнення для сум: {list(used_codes_count.keys())}")
        log_info(f"🎯 [Smart] Цільова кількість кодів на суму: {self.target_codes_per_amount}")
        
        # Ініціалізуємо браузер якщо ще не ініціалізований
        if not self.page:
            browser_manager = create_browser_manager(headed_mode=False)
            self.page = browser_manager.get_page()
        
        # Логін
        if not self.login():
            log_error("❌ [Smart] Не вдалося увійти в адмін-панель")
            return False
        
        total_created = 0
        
        for amount_str in used_codes_count.keys():
            amount = int(amount_str)
            
            log_info(f"💰 [Smart] Обробляємо суму {amount}...")
            
            # Перевіряємо скільки кодів є в БД
            current_count = self.get_available_codes_count(amount)
            log_info(f"📦 [Smart] Поточна кількість в БД для суми {amount}: {current_count}")
            
            if current_count >= self.target_codes_per_amount:
                log_info(f"✅ [Smart] Для суми {amount} достатньо кодів ({current_count} >= {self.target_codes_per_amount})")
                self.clear_used_codes_count(amount)
                continue
            
            # Отримуємо активні коди з адмін-панелі
            active_codes = self.get_active_codes_from_admin(amount)
            log_info(f"🔍 [Smart] Активних кодів в адмін-панелі: {len(active_codes)}")
            
            # Визначаємо скільки потрібно створити
            codes_to_create = self.target_codes_per_amount - len(active_codes)
            
            if codes_to_create <= 0:
                log_info(f"✅ [Smart] Для суми {amount} достатньо активних кодів в адмін-панелі")
                # Синхронізуємо БД з адмін-панеллю (відновлюємо статус якщо код активний в адмінці)
                restored_count = 0
                for code in active_codes:
                    if self.save_promo_code_to_db(code, amount, restore_if_exists=True):
                        restored_count += 1
                if restored_count > 0:
                    log_info(f"♻️ [Smart] Відновлено {restored_count} кодів для суми {amount}")
                self.clear_used_codes_count(amount)
                continue
            
            log_info(f"⚙️ [Smart] Для суми {amount} створюємо {codes_to_create} нових кодів")
            
            for i in range(codes_to_create):
                try:
                    new_promo_code = self._generate_single_code_string(amount)
                    log_info(f"⚙️ [Smart] Створюємо код {i+1}/{codes_to_create}: {new_promo_code}")
                    
                    if self.create_promo_code(new_promo_code, amount):
                        if self.save_promo_code_to_db(new_promo_code, amount):
                            total_created += 1
                            log_info(f"✅ [Smart] Промокод {new_promo_code} створено та збережено")
                        
                        time.sleep(1)
                    else:
                        log_error(f"❌ [Smart] Не вдалося створити код: {new_promo_code}")
                        
                except Exception as e:
                    log_error(f"❌ [Smart] Помилка при створенні коду {i+1}/{codes_to_create}: {e}")
            
            # Очищаємо лічильник після обробки
            self.clear_used_codes_count(amount)
            
            log_info(f"📊 [Smart] Для суми {amount}: створено кодів у цій ітерації")
        
        log_info(f"🎉 [Smart] Розумне поповнення завершено! Створено {total_created} нових кодів")
        return True
    
    def cleanup(self):
        """Очищення ресурсів"""
        cleanup_global_browser()
