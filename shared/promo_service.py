"""
PromoService - Спрощений сервіс для роботи з промокодами.

Standalone версія для використання в скриптах поза VPS системою.
Містить тільки методи для роботи з браузером, без S3/БД залежностей.

Основні можливості:
- Логін в адмін-панель
- Отримання iframe
- Створення промокодів
- Керування сесіями через файли
"""

import time
import logging
import os
import random
import string
import json
from datetime import datetime, timedelta
from playwright.sync_api import Page

logger = logging.getLogger(__name__)

# Контроль детальності логування
VERBOSE_LOGGING = os.environ.get('VERBOSE_LOGGING', 'false').lower() == 'true'


def log_verbose(message):
    """Виводить детальні логи тільки якщо увімкнено VERBOSE_LOGGING"""
    if VERBOSE_LOGGING:
        print(message)


def log_info(message):
    """Виводить важливі логи завжди"""
    print(message)


def log_error(message):
    """Виводить помилки завжди"""
    print(message)


class PromoService:
    """
    Спрощений сервіс для роботи з промокодами через браузер.
    
    Основні можливості:
    - Генерація поодиноких промокодів за форматом BON{сума}{рандом}
    - Створення промокодів в адмін-панелі з автозаповненням форми
    - Керування сесіями з кешуванням у файлах
    - Швидка авторизація через збережені cookies
    """
    
    def __init__(self, page: Page = None):
        self.page = page
        self.admin_url = os.getenv('ADMIN_URL', 'https://safeyourlove.com/edit/discounts/codes')
        self.admin_username = os.getenv('ADMIN_USERNAME')
        self.admin_password = os.getenv('ADMIN_PASSWORD')
        self._cached_iframe = None
        self._session_cookies = None
        self._session_timestamp = None
        self._session_timeout = 3600  # 1 година
        self._session_file = '/tmp/session_cookies.json'
        
        # Спробуємо завантажити збережену сесію
        self._load_session_from_file()
    
    def _generate_single_code_string(self, amount: int) -> str:
        """Генерує один рядок промокоду для заданої суми."""
        amount_str = str(amount)
        prefix = f'BON{amount_str}'
        random_part_length = max(3, 7 - len(amount_str))
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=random_part_length))
        return prefix + random_part
    
    def _is_session_valid(self):
        """Перевіряє, чи дійсна збережена сесія"""
        if not self._session_cookies or not self._session_timestamp:
            log_verbose("🔍 Немає збережених cookies або timestamp")
            return False
        
        current_time = time.time()
        age = current_time - self._session_timestamp
        if age > self._session_timeout:
            log_verbose(f"🕐 Збережена сесія застаріла (вік: {age:.0f}с, ліміт: {self._session_timeout}с)")
            return False
            
        log_verbose(f"✅ Сесія дійсна (вік: {age:.0f}с)")
        return True
    
    def _load_session_from_file(self):
        """Завантажує сесію з файлу"""
        try:
            if os.path.exists(self._session_file):
                with open(self._session_file, 'r') as f:
                    session_data = json.load(f)
                    self._session_cookies = session_data.get('cookies', [])
                    self._session_timestamp = session_data.get('timestamp', 0)
                    log_verbose(f"📂 Завантажено сесію з файлу: {len(self._session_cookies)} cookies")
                    return True
        except Exception as e:
            log_verbose(f"⚠️ Не вдалося завантажити сесію з файлу: {e}")
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
            
            log_verbose(f"💾 Сесію збережено у файл: {self._session_file}")
    
        except Exception as e:
            log_verbose(f"⚠️ Не вдалося зберегти сесію у файл: {e}")
    
    def _save_session(self):
        """Зберігає поточну сесію (cookies)"""
        try:
            self._session_cookies = self.page.context.cookies()
            self._session_timestamp = time.time()
            self._save_session_to_file()
            log_info(f"💾 Збережено сесію з {len(self._session_cookies)} cookies")
                
        except Exception as e:
            log_error(f"⚠️ Не вдалося зберегти сесію: {e}")
    
    def _restore_session(self):
        """Відновлює збережену сесію"""
        if not self._session_cookies:
            self._load_session_from_file()
            
        if not self._is_session_valid() or not self._session_cookies:
            return False
            
        try:
            # Спочатку переходимо на сайт для створення контексту домену
            log_verbose("🌐 Переходимо на сайт для створення контексту...")
            self.page.goto('https://safeyourlove.com/', timeout=15000, wait_until='domcontentloaded')
            
            # Фільтруємо і очищуємо cookies
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
            
            log_verbose(f"🔄 Підготовлено {len(cookies_to_set)} cookies")
            
            if cookies_to_set:
                self.page.context.add_cookies(cookies_to_set)
                log_verbose(f"✅ Додано {len(cookies_to_set)} cookies до контексту")
            
            return True
        except Exception as e:
            log_verbose(f"⚠️ Не вдалося відновити сесію: {e}")
            return False
        
    def _get_iframe(self, timeout=8000):
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
                log_verbose("⏳ Очікуємо завантаження таблиці промокодів...")
                
                frame.wait_for_function("""
                    () => {
                        const table = document.querySelector('table');
                        if (!table) return false;
                        const hasRows = table.querySelector('tbody tr') !== null;
                        const hasHeaders = table.querySelector('thead tr') !== null;
                        return hasRows || hasHeaders;
                    }
                """, timeout=10000)
                
                log_verbose("✅ Таблиця промокодів завантажена")

            return frame

        except Exception as e:
            log_error(f"❌ Не вдалося отримати або дочекатися iframe: {e}")
            self._cached_iframe = None
            return None
    
    def login(self):
        """Виконує швидкий логін з використанням збереженої сесії"""
        start_time = time.time()
        log_info("🔐 Початок процесу логіну...")
        
        # Спочатку намагаємося відновити збережену сесію
        if self._restore_session():
            try:
                log_verbose("🔄 Перевіряємо збережену сесію...")
                self.page.goto(self.admin_url, timeout=20000, wait_until='domcontentloaded')
                
                try:
                    iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
                    iframe = self.page.wait_for_selector(iframe_selector, state='visible', timeout=8000)
                    if iframe:
                        log_info(f"✅ Сесія дійсна! Знайдено iframe за {time.time() - start_time:.2f}с")
                        return True
                except:
                    pass
                
                try:
                    login_field = self.page.wait_for_selector(
                        'input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]',
                        state='visible',
                        timeout=3000
                    )
                    if login_field:
                        log_verbose("🔐 Знайдено поля логіну - сесія недійсна")
                except:
                    pass
                    
            except Exception as e:
                log_verbose(f"⚠️ Помилка при перевірці сесії: {e}")
        
        # Виконуємо повний логін
        login_start = time.time()
        log_info("🔐 Виконуємо логін...")
        
        self.page.goto(self.admin_url, timeout=15000, wait_until='domcontentloaded')
        
        login_fields = self.page.wait_for_selector(
            'input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]',
            state='visible',
            timeout=8000
        )
        
        username_field = self.page.query_selector('input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]')
        password_field = self.page.query_selector('input[type="password"]')
        submit_button = self.page.query_selector('button[type="submit"]')
        
        if not username_field or not password_field or not submit_button:
            log_error("❌ Не знайдено поля форми логіну")
            raise Exception("Поля форми логіну не знайдені")
        
        if not self.admin_username or not self.admin_password:
            log_error("❌ Відсутні дані для входу")
            raise Exception("Відсутні дані для входу")
            
        username_field.fill(self.admin_username)
        password_field.fill(self.admin_password)
        
        submit_button.click()
        
        iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
        self.page.wait_for_selector(iframe_selector, state='visible', timeout=10000)
        
        login_time = time.time() - login_start
        log_info(f"✅ Логін завершено за {login_time:.2f}с")
        
        self._save_session()
        
        total_time = time.time() - start_time
        log_info(f"🏁 Весь процес логіну завершено за {total_time:.2f}с")
        return True

    def create_promo_code(self, promo_code, amount):
        """Створює конкретний промокод в адмін-панелі."""
        start_time = time.time()
        
        try:
            frame = self._get_iframe()
            if not frame:
                raise Exception("Не знайдено iframe після логіну")

            try:
                frame.locator('a.button.add.plus').click()
            except Exception as e:
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
            
            frame.wait_for_selector('input[name="names[code]"]', timeout=30000)

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
                let brandsCount = 0;
                let categoriesCount = 0;
                
                const brandsSelect = document.querySelector('select[name="names[brands][]"]');
                if (brandsSelect) {
                    const options = Array.from(brandsSelect.options);
                    options.forEach(option => {
                        if (option.value && option.value !== '0') {
                            option.selected = true;
                            brandsCount++;
                        }
                    });
                    brandsSelect.dispatchEvent(new Event('change', { bubbles: true }));
                }
                
                const categoriesSelect = document.querySelector('select[name="names[categories][]"]');
                if (categoriesSelect) {
                    const options = Array.from(categoriesSelect.options);
                    options.forEach(option => {
                        if (option.value && option.value !== '0') {
                            option.selected = true;
                            categoriesCount++;
                        }
                    });
                    categoriesSelect.dispatchEvent(new Event('change', { bubbles: true }));
                }
                
                return { brands: brandsCount, categories: categoriesCount };
            })();
            """
            
            frame.evaluate(select_all_js)

            frame.locator('input.save-exit-button[type="submit"]').click()

            frame.locator('a.button.add.plus').wait_for(state='visible', timeout=10000)
            
            total_time = time.time() - start_time
            log_info(f"✅ Промокод '{promo_code}' створено за {total_time:.3f}с")
            
            return True
        except Exception as e:
            total_time = time.time() - start_time
            log_error(f"❌ Помилка створення промокоду '{promo_code}' після {total_time:.3f}с: {e}")
            import traceback
            traceback.print_exc()
            try:
                self.page.screenshot(path=f'/tmp/promo_creation_error_{promo_code}.png')
            except:
                pass
            return False

    def apply_amount_filter(self, amount):
        """
        Застосовує фільтр по сумі промокодів в адмін-панелі через UI.
        """
        start_time = time.time()
        
        try:
            frame = self._get_iframe()
            if not frame:
                log_error("❌ Не вдалося отримати iframe для застосування фільтра")
                return False
                
            amount_header = frame.locator('#header_id_4778')
            if not amount_header.count():
                log_verbose("⚠️ Не знайдено заголовок 'Розмір знижки'")
                return False
            
            amount_header.hover()
            time.sleep(1.0)
            
            filter_block = frame.locator('#sortingBlock_4778')
            if not filter_block.count():
                log_verbose("⚠️ Не знайдено блок фільтрації")
                return False
            
            from_field = filter_block.locator('input[name="text1"]')
            to_field = filter_block.locator('input[name="text2"]')
            
            if not from_field.count() or not to_field.count():
                log_verbose("⚠️ Поля фільтрації не знайдено")
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
