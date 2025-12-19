"""
Базова логіка для промокодів - core функціональність для Lambda.

Цей модуль містить тільки необхідні методи для роботи Lambda функції:
- Генерація промокодів (single code)
- Створення промокодів в адмін-панелі  
- Авторизація та керування сесіями
- Інтеграція з S3 для збереження кодів та сесій
- Базові утиліти для BON промокодів

Складна логіка винесена в окремі модулі:
- promo_smart.py - аналіз, фільтрація, пакетні операції
- generate_promo_codes_complex.py - розумна генерація з аналізом запасів
"""

import time
import logging
import os
import random
import string
import time
import json
from datetime import datetime, timedelta
from playwright.sync_api import Page
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Контроль детальності логування
VERBOSE_LOGGING = os.environ.get('VERBOSE_LOGGING', 'false').lower() == 'true'

# Функція для умовного логування
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

try:
    from .bon_utils import is_bon_promo_code, extract_amount_from_bon_code
except ImportError:
    # Fallback для запуску без пакета
    from bon_utils import is_bon_promo_code, extract_amount_from_bon_code

# Перевірка доступності S3
try:
    boto3.client('s3')  # Просто перевірка, чи можна створити клієнт
    S3_AVAILABLE = True
except (NoCredentialsError, ClientError):
    S3_AVAILABLE = False


class PromoService:
    """
    Базовий сервіс для роботи з промокодами (core функціональність).
    
    Основні можливості:
    - Генерація поодиноких промокодів за форматом BON{сума}{рандом}
    - Створення промокодів в адмін-панелі з автозаповненням форми
    - Керування сесіями з кешуванням в S3/файлах
    - Поповнення запасів промокодів в S3
    - Швидка авторизація через збережені cookies
    
    Для складних операцій використовуйте PromoSmartManager з promo_smart.py
    """
    
    def __init__(self, page: Page = None):
        self.page = page
        self.admin_url = os.getenv('ADMIN_URL', 'https://safeyourlove.com/edit/discounts/codes')  # Одразу на сторінку знижок
        self.admin_username = os.getenv('ADMIN_USERNAME')
        self.admin_password = os.getenv('ADMIN_PASSWORD')
        self._cached_iframe = None  # Кешування iframe
        self._session_cookies = None  # Кешування сесії в пам'яті
        self._session_timestamp = None
        self._session_timeout = 3600  # 1 година у секундах
        self._session_file = '/tmp/session_cookies.json'  # Файл для збереження сесії (fallback)
        
        # S3 конфігурація для збереження сесії
        self.s3_bucket = os.getenv('SESSION_S3_BUCKET', 'lambda-promo-sessions')
        self.s3_key = f'sessions/{os.getenv("AWS_LAMBDA_FUNCTION_NAME", "local")}_session.json'
        self.promo_codes_key = os.getenv('PROMO_CODES_S3_KEY', 'promo-codes/available_codes.json')  # Ключ для файлу з промокодами
        self.used_codes_key = os.getenv('USED_CODES_S3_KEY', 'promo-codes/used_codes_count.json')  # Ключ для файлу з лічильниками
        self.use_s3 = S3_AVAILABLE and bool(os.getenv('AWS_LAMBDA_FUNCTION_NAME'))
        
        self.s3_client = None
        if self.use_s3:
            try:
                self.s3_client = boto3.client('s3')
                print(f"🔧 AWS Lambda: Використовуємо S3 для збереження сесії (bucket: {self.s3_bucket})")
            except Exception as e:
                print(f"⚠️ Не вдалося ініціалізувати S3 клієнт: {e}")
                self.use_s3 = False
        
        # При ініціалізації в AWS Lambda відразу намагаємося завантажити збережену сесію
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
            print("🔧 AWS Lambda: Спроба завантажити збережену сесію при ініціалізації...")
            if self.use_s3:
                self._load_session_from_s3()
            else:
                self._load_session_from_file()
    
    def _generate_single_code_string(self, amount: int) -> str:
        """Генерує один рядок промокоду для заданої суми."""
        amount_str = str(amount)
        prefix = f'BON{amount_str}'
        random_part_length = max(3, 7 - len(amount_str))
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=random_part_length))
        return prefix + random_part
    
    def apply_amount_filter(self, amount):
        """
        💰 Застосовує фільтр по сумі промокодів в адмін-панелі через UI.
        """
        start_time = time.time()
        
        try:
            frame = self._get_iframe()
            if not frame:
                print("❌ Не вдалося отримати iframe для застосування фільтра")
                return False
                
            # 1. Знаходимо заголовок колонки з сумою (працює точно)
            amount_header = frame.locator('#header_id_4778')
            if not amount_header.count():
                print("⚠️ Не знайдено заголовок 'Розмір знижки'")
                return False
            
            # 2. Наводимо мишку для активації фільтра
            amount_header.hover()
            time.sleep(1.0)  # Даємо час для появи блока
            
            # 3. Знаходимо блок фільтрації (працює точно)
            filter_block = frame.locator('#sortingBlock_4778')
            if not filter_block.count():
                print("⚠️ Не знайдено блок фільтрації")
                return False
            
            # 4. Знаходимо поля введення (працюють точно)
            from_field = filter_block.locator('input[name="text1"]')
            to_field = filter_block.locator('input[name="text2"]')
            
            if not from_field.count() or not to_field.count():
                print("⚠️ Поля фільтрації не знайдено")
                return False
            
            # 5. Застосовуємо фільтр
            from_field.click()
            from_field.fill('')  # Очищуємо
            from_field.fill(str(amount))
            time.sleep(0.3)
            
            to_field.click()
            to_field.fill('')  # Очищуємо  
            to_field.fill(str(amount))
            time.sleep(0.3)
            
            # 6. Застосовуємо через Enter
            to_field.press('Enter')
            
            # 7. Очікуємо завершення фільтрації
            time.sleep(3.0)
            
            total_time = time.time() - start_time
            print(f"✅ Фільтр по сумі {amount} застосовано за {total_time:.3f}с")
            return True
            
        except Exception as e:
            total_time = time.time() - start_time
            print(f"❌ Помилка застосування фільтра після {total_time:.3f}с: {e}")
            return False

    def get_active_codes(self, amount):
        """
        Знаходить всі активні промокоди для заданої суми,
        перевіряє їх активність, видаляє неактивні і повертає тільки активні коди.
        
        Повертає: active_codes (список активних кодів)
        """
        try:
            frame = self._get_iframe()
            if not frame:
                print("❌ Не вдалося отримати iframe для отримання кодів")
                return []
                
            print(f"🔍 Отримуємо та перевіряємо коди для суми {amount} з адмін-панелі...")
            
            # Застосовуємо UI фільтр по сумі
            self.apply_amount_filter(amount)
            
            # Отримуємо всі рядки таблиці
            try:
                rows = frame.locator('tbody tr').all()
                total_rows = len(rows)
                print(f"📊 Всього рядків в таблиці: {total_rows}")
            except Exception as e:
                print(f"❌ Помилка при отриманні рядків таблиці: {e}")
                return []
            
            active_codes = []
            inactive_codes_to_delete = []
            processed_rows = 0
            skipped_rows = 0
            
            for i, row in enumerate(rows):
                try:
                    # Отримуємо всі комірки в рядку
                    cells = row.locator('td').all()
                    cell_count = len(cells)
                    
                    if cell_count < 4:  # Потрібно мінімум 4 колонки
                        skipped_rows += 1
                        continue
                    
                    # Структура: col0 | col1 | col2(статус) | col3(код) | col4(сума) | col5(дата) | col6
                    status_cell = cells[2]
                    code_cell = cells[3]
                    
                    status = status_cell.inner_text().strip() 
                    code = code_cell.inner_text().strip()
                    
                    # Пропускаємо заголовкові та службові рядки  
                    if (code.lower() in ['код', 'code', 'промокод', 'promo_code', 'дійсні до'] or 
                        'january' in code.lower() or 
                        'calendar' in code.lower() or 
                        len(code) > 50 or  # Занадто довгі рядки - це не промокоди
                        not code.startswith('BON')):  # Промокоди повинні починатись з BON
                        log_verbose(f"  📋 Рядок {i+4}: пропускаємо заголовок/не-промокод - '{code[:50]}...'")
                        skipped_rows += 1
                        continue
                    
                    # Пропускаємо рядки з порожніми значеннями
                    if not code.strip() or not status.strip():
                        log_verbose(f"  ⚠️ Рядок {i+4}: пропускаємо - порожній код/статус")
                        skipped_rows += 1
                        continue
                    
                    # Перевіряємо активність коду
                    is_active = status.lower() in ['так', 'yes', 'активний', 'active']
                    
                    if is_active:
                        active_codes.append(code)
                        log_verbose(f"  ✅ Активний код '{code}' (рядок {i+4})")
                    else:
                        inactive_codes_to_delete.append(code)
                        log_verbose(f"  ❌ Неактивний код '{code}' (статус: '{status}') буде видалено (рядок {i+4})")
                    
                    processed_rows += 1
                        
                except Exception as e:
                    log_error(f"⚠️ Помилка при обробці рядка {i}: {e}")
                    skipped_rows += 1
                    continue
            
            log_info(f"📊 Результат аналізу для суми {amount}:")
            print(f"   📋 Всього рядків в таблиці: {total_rows}")
            print(f"   ✅ Оброблено рядків: {processed_rows}")
            print(f"   ⚠️ Пропущено рядків: {skipped_rows}")
            print(f"   🟢 Активних кодів: {len(active_codes)}")
            print(f"   🔴 Неактивних кодів для видалення: {len(inactive_codes_to_delete)}")
            
            # Якщо є неактивні коди - видаляємо їх одразу
            if inactive_codes_to_delete:
                print(f"🗑️ Видаляємо {len(inactive_codes_to_delete)} неактивних кодів: {inactive_codes_to_delete}")
                
                try:
                    # 1. Вибираємо неактивні коди через чекбокси (як в промо генераторі)
                    selected_count = 0
                    for code in inactive_codes_to_delete:
                        try:
                            # Шукаємо рядок з промокодом використовуючи XPath як в промо генераторі
                            row_locator = frame.locator(f'//tr[td[4][normalize-space(.)="{code}"]]')
                            
                            if row_locator.count() == 0:
                                print(f"⚠️ Не знайдено рядок для коду: {code}")
                                continue

                            # Шукаємо чекбокс в рядку
                            checkbox = row_locator.locator('input[type="checkbox"]').first
                            if checkbox.count() > 0:
                                if not checkbox.is_checked():
                                    checkbox.click()
                                    selected_count += 1
                                    print(f"  ✅ Відмічено неактивний код: {code}")
                                    
                        except Exception as e:
                            print(f"⚠️ Помилка при відмітці коду {code}: {e}")
                    
                    print(f"☑️ Відмічено {selected_count} неактивних кодів для видалення")
                    
                    if selected_count > 0:
                        # 2. Видаляємо вибрані коди (headless режим)
                        # Налаштовуємо обробник діалогу підтвердження
                        dialog_handled = False
                        def handle_dialog(dialog):
                            nonlocal dialog_handled
                            print(f"💬 Отримано діалог підтвердження: {dialog.message}")
                            dialog.accept()
                            dialog_handled = True
                            print("✅ Діалог підтверджено")
                        
                        # Отримуємо page з браузер менеджера
                        page = frame.page
                        page.on('dialog', handle_dialog)
                        
                        # Перевіряємо доступність функції видалення
                        function_exists = frame.evaluate('typeof removeSelectedGrids === "function"')
                        
                        if function_exists:
                            print("🔧 Викликаємо removeSelectedGrids()...")
                            frame.evaluate('removeSelectedGrids()')
                        else:
                            print("⚠️ Функція removeSelectedGrids не знайдена, шукаємо альтернативні способи...")
                            
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
                                buttons = frame.locator(selector)
                                if buttons.count() > 0:
                                    print(f"✅ Знайдено кнопку видалення: {selector}")
                                    buttons.first.click()
                                    button_found = True
                                    break
                            
                            if not button_found:
                                print("❌ Не вдалося знайти способ видалення")
                                return active_codes, []
                        
                        # Очікуємо появи діалогу підтвердження (швидше)
                        log_verbose("⏳ Очікуємо появи діалогу підтвердження...")
                        import time
                        
                        # Шукаємо модальне вікно одразу, без зайвого чекання
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
                        # Спробуємо знайти кнопку протягом 3 секунд (замість фіксованого sleep)
                        for attempt in range(6):  # 6 спроб по 0.5 секунди
                            for selector in confirm_selectors:
                                confirm_buttons = frame.locator(selector)
                                if confirm_buttons.count() > 0:
                                    log_info(f"✅ Знайдено кнопку підтвердження: {selector}")
                                    confirm_buttons.first.click()
                                    log_info("🎯 Кнопку підтвердження натиснуто!")
                                    button_found = True
                                    break
                            
                            if button_found:
                                break
                            time.sleep(0.5)
                        
                        # Перевіряємо, чи був оброблений стандартний діалог
                        if dialog_handled:
                            log_verbose("✅ Стандартний діалог оброблено, операція має завершитись")
                        elif not button_found:
                                print("⌨️ Кнопка підтвердження не знайдена, пробуємо Enter та JavaScript...")
                                
                                # Спроба 1: Enter на iframe
                                try:
                                    frame.press('body', 'Enter')
                                    time.sleep(0.5)
                                except Exception:
                                    pass
                                
                                # Спроба 2: JavaScript підтвердження
                                try:
                                    frame.evaluate("""
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
                                    print("🔧 Виконано JavaScript підтвердження")
                                except Exception as js_error:
                                    print(f"⚠️ JavaScript підтвердження не вдалося: {js_error}")
                        
                        # Очікуємо завершення операції
                        print("⏳ Очікуємо завершення операції видалення...")
                        time.sleep(3.0)
                        
                        print(f"✅ {len(inactive_codes_to_delete)} неактивних кодів видалено з адмін-панелі")
                    else:
                        print("⚠️ Немає неактивних кодів для видалення")
                        
                except Exception as e:
                    print(f"❌ Помилка при видаленні неактивних кодів: {e}")
            
            print(f"🎉 Завершено аналіз для суми {amount}: залишилось {len(active_codes)} активних кодів")
            return active_codes
            
        except Exception as e:
            print(f"❌ Помилка при отриманні та перевірці кодів для суми {amount}: {e}")
            return []

    def update_s3_codes(self, updated_codes):
        """
        Оновлює промокоди в S3 для вказаних сум.
        
        Args:
            updated_codes: dict {"100": count, "200": count} - дані для оновлення
        """
        if not self.s3_client:
            print("❌ S3 клієнт не ініціалізовано")
            return False
        
        try:
            # Завантажуємо існуючі промокоди з S3
            try:
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.promo_codes_key)
                existing_codes = json.loads(response['Body'].read().decode('utf-8'))
                print(f"📦 Завантажено існуючі промокоди: {[(k, len(v)) for k, v in existing_codes.items()]}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print("📦 Файл промокодів не існує, створюємо новий")
                    existing_codes = {}
                else:
                    print(f"❌ Помилка при завантаженні існуючих промокодів: {e}")
                    return False
            
            # Оновлюємо тільки вказані суми
            for amount_str, codes_list in updated_codes.items():
                old_count = len(existing_codes.get(amount_str, []))
                existing_codes[amount_str] = codes_list
                print(f"🔄 Оновлено суму {amount_str}: було {old_count} кодів, стало {len(codes_list)}")
            
            # Зберігаємо оновлений файл
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.promo_codes_key,
                Body=json.dumps(existing_codes, indent=2),
                ContentType='application/json'
            )
            
            print(f"💾 Промокоди оновлено в S3 для сум: {list(updated_codes.keys())}")
            print(f"📊 Загальний стан S3: {[(k, len(v)) for k, v in existing_codes.items()]}")
            return True
            
        except Exception as e:
            print(f"❌ Помилка при оновленні промокодів в S3: {e}")
            return False

    def replenish_promo_codes(self, used_codes_count):
        """
        Поповнює запаси промокодів:
        1. Завантажує існуючі коди з S3
        2. Перевіряє які з них активні в адмін-панелі
        3. Видаляє неактивні коди
        4. Створює нові коди до цільової кількості активних
        5. Оновлює S3 з новими даними
        
        Args:
            used_codes_count: dict {"100": 5, "200": 3} - лічільники використаних
        """
        print(f"🧠 [Smart] Розумне поповнення для сум: {list(used_codes_count.keys())}")
        
        # Отримуємо цільову кількість з змінних оточення
        target_codes_per_amount = int(os.getenv('TARGET_CODES_PER_AMOUNT', '10'))
        print(f"🎯 [Smart] Цільова кількість кодів на суму: {target_codes_per_amount}")
        
        # 1. Логін виконуємо один раз ПЕРЕД аналізом існуючих кодів
        if not self.login():
            print("❌ [Smart] Не вдалося увійти в адмін-панель")
            return False

        updated_codes = {}
        total_created = 0
        
        # 2. Обробляємо кожну суму
        for amount_str in used_codes_count.keys():
            amount = int(amount_str)
            amount_key = str(amount)
            
            print(f"💰 [Smart] Обробляємо суму {amount}: цільова кількість {target_codes_per_amount}")
            
            # Отримуємо активні коди для цієї суми (неактивні видаляються автоматично)
            active_codes = self.get_active_codes(amount)
            print(f"📦 [Smart] Поточна кількість активних кодів для суми {amount}: {len(active_codes)}")
            
            # Неактивні коди вже видалено в get_active_codes()
            print(f"🔍 [Smart] Маємо {len(active_codes)} активних кодів для суми {amount}")
            
            # Якщо активних кодів достатньо або більше - НЕ ОНОВЛЮЄМО S3 для цієї суми
            if len(active_codes) >= target_codes_per_amount:
                log_info(f"✅ [Smart] Для суми {amount} достатньо активних кодів ({len(active_codes)} >= {target_codes_per_amount}), залишаємо як є в S3")
                updated_codes[amount_key] = active_codes
                
                # Очищаємо лічільник використаних кодів навіть якщо не створювали нові коди
                # Це запобігає повторним перевіркам цієї суми у майбутніх запусках
                if self.clear_used_codes_count(amount):
                    log_info(f"🗑️ [Smart] Лічільник використаних кодів для суми {amount} очищено (достатньо кодів)")
                else:
                    log_error(f"⚠️ [Smart] Не вдалося очистити лічільник для суми {amount}")
                continue
            
            # Якщо кодів не достатньо - створюємо нові до цільової кількості
            codes_to_create = target_codes_per_amount - len(active_codes)
            print(f"⚙️ [Smart] Для суми {amount} створюємо {codes_to_create} нових кодів (до цільової {target_codes_per_amount})")
            
            new_codes = active_codes.copy()
            
            for i in range(codes_to_create):
                try:
                    # Генерація нового коду
                    new_promo_code = self._generate_single_code_string(amount)
                    print(f"⚙️ [Smart] Створюємо код {i+1}/{codes_to_create}: {new_promo_code}")
                    
                    # Створення коду в адмін-панелі
                    if self.create_promo_code(new_promo_code, amount):
                        new_codes.append(new_promo_code)
                        total_created += 1
                        log_info(f"✅ [Smart] Промокод {new_promo_code} створено")
                        
                        # Інкрементальне збереження - оновлюємо S3 одразу після створення коду
                        try:
                            temp_updated_codes = updated_codes.copy()
                            temp_updated_codes[amount_key] = new_codes
                            self.update_s3_codes(temp_updated_codes)
                            log_verbose(f"💾 [Smart] Проміжне збереження: {len(new_codes)} кодів для суми {amount}")
                        except Exception as save_error:
                            log_error(f"⚠️ [Smart] Помилка проміжного збереження: {save_error}")
                        
                        time.sleep(1)  # Затримка між створеннями
                    else:
                        print(f"❌ [Smart] Не вдалося створити код: {new_promo_code}")
                        
                except Exception as e:
                    print(f"❌ [Smart] Помилка при створенні коду {i+1}/{codes_to_create}: {e}")
            
            updated_codes[amount_key] = new_codes
            log_info(f"📊 [Smart] Для суми {amount}: було {len(active_codes)} активних, створено {codes_to_create}, тепер маємо {len(new_codes)}")
            
            # Очищаємо лічільник використаних кодів для цієї суми після успішної обробки
            if codes_to_create > 0:  # Тільки якщо дійсно створили нові коди
                if self.clear_used_codes_count(amount):
                    log_info(f"🗑️ [Smart] Лічільник використаних кодів для суми {amount} очищено")
                else:
                    log_error(f"⚠️ [Smart] Не вдалося очистити лічільник для суми {amount}")
        
        # 4. Оновлюємо коди в S3 тільки для оброблених сум (зберігаючи інші суми)
        if self.update_s3_codes(updated_codes):
            log_info(f"🎉 [Smart] Розумне поповнення завершено! Створено {total_created} нових кодів")
            
            # Виводимо підсумок по кожній сумі та очищаємо лічільники
            for amount_str, codes_list in updated_codes.items():
                amount = int(amount_str)
                log_info(f"💰 [Smart] Сума {amount}: {len(codes_list)} активних кодів оновлено в S3")
                
                # Очищаємо лічільник використаних кодів для цієї суми
                if self.clear_used_codes_count(amount):
                    log_info(f"🗑️ [Smart] Лічільник використаних кодів для суми {amount} очищено")
                else:
                    log_error(f"⚠️ [Smart] Не вдалося очистити лічільник для суми {amount}")
            
            return True
        else:
            log_error("❌ [Smart] Не вдалося оновити коди в S3")
            return False

    def create_promo_code(self, promo_code, amount):
        """Створює конкретний промокод в адмін-панелі."""
        start_time = time.time()
        
        try:
            # Отримання iframe
            frame = self._get_iframe()
            if not frame:
                raise Exception("Не знайдено iframe після логіну")

            # Клік "Додати промокод" через Playwright
            try:
                frame.locator('a.button.add.plus').click()
            except Exception as e:
                # Спробуємо альтернативні селектори
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
            
            # Очікування появи форми
            frame.wait_for_selector('input[name="names[code]"]', timeout=30000)

            # Заповнення всіх полів через JavaScript
            date_to = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
            
            fill_form_js = f"""
            (function() {{
                // Заповнюємо основні поля
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

            # Швидкий вибір всіх брендів та категорій через JavaScript
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

            # Збереження
            frame.locator('input.save-exit-button[type="submit"]').click()

            # Очікування повернення на список
            frame.locator('a.button.add.plus').wait_for(state='visible', timeout=10000)
            
            total_time = time.time() - start_time
            print(f"✅ Промокод '{promo_code}' створено за {total_time:.3f}с")
            
            return True
        except Exception as e:
            total_time = time.time() - start_time
            print(f"❌ Помилка створення промокоду '{promo_code}' після {total_time:.3f}с: {e}")
            import traceback
            traceback.print_exc()
            self.page.screenshot(path=f'/tmp/promo_creation_error_{promo_code}.png')
            return False

    def _is_session_valid(self):
        """Перевіряє, чи дійсна збережена сесія"""
        if not self._session_cookies or not self._session_timestamp:
            print("🔍 Немає збережених cookies або timestamp")
            return False
        
        # Для Lambda використовуємо скорочений час життя сесії через обмеження середовища
        # AWS Lambda може очищати /tmp між викликами, тому скорочуємо час життя
        session_timeout = 1800 if os.getenv('AWS_LAMBDA_FUNCTION_NAME') else self._session_timeout  # 30 хвилин для Lambda
        
        # Перевіряємо таймаут сесії
        current_time = time.time()
        age = current_time - self._session_timestamp
        if age > session_timeout:
            print(f"🕐 Збережена сесія застаріла (вік: {age:.0f}с, ліміт: {session_timeout}с)")
            return False
            
        print(f"✅ Сесія дійсна (вік: {age:.0f}с)")
        return True
    
    def _load_session_from_s3(self):
        """Завантажує сесію з S3"""
        if not self.use_s3:
            return False
            
        try:
            print(f"☁️ Завантаження сесії з S3: {self.s3_bucket}/{self.s3_key}")
            
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
            session_data = json.loads(response['Body'].read().decode('utf-8'))
            
            self._session_cookies = session_data.get('cookies', [])
            self._session_timestamp = session_data.get('timestamp', 0)
            
            print(f"☁️ Завантажено сесію з S3: {len(self._session_cookies)} cookies")
            print(f"☁️ Timestamp сесії: {self._session_timestamp} ({datetime.fromtimestamp(self._session_timestamp) if self._session_timestamp else 'None'})")
            
            if self._session_cookies:
                sample_cookie = self._session_cookies[0]
                print(f"☁️ Приклад cookie: {list(sample_cookie.keys())}")
            
            return True
            
        except Exception as e:
            # Обробляємо специфічні помилки S3
            error_str = str(e)
            if 'NoSuchKey' in error_str:
                print(f"☁️ Файл сесії не існує в S3: {self.s3_key}")
            elif 'NoSuchBucket' in error_str:
                print(f"⚠️ Bucket не існує: {self.s3_bucket}")
            else:
                print(f"⚠️ Не вдалося завантажити сесію з S3: {e}")
        return False
    
    def _save_session_to_s3(self):
        """Зберігає сесію в S3"""
        if not self.use_s3:
            return False
            
        try:
            session_data = {
                'cookies': self._session_cookies,
                'timestamp': self._session_timestamp,
                'lambda_function': os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'local'),
                'saved_at': datetime.now().isoformat()
            }
            
            # Створюємо bucket якщо не існує
            try:
                self.s3_client.head_bucket(Bucket=self.s3_bucket)
            except Exception as e:
                error_str = str(e)
                if '404' in error_str or 'NoSuchBucket' in error_str:
                    print(f"☁️ Створюємо bucket: {self.s3_bucket}")
                    self.s3_client.create_bucket(
                        Bucket=self.s3_bucket,
                        CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'}
                    )
            
            # Зберігаємо дані
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.s3_key,
                Body=json.dumps(session_data, indent=2),
                ContentType='application/json'
            )
            
            print(f"☁️ Сесію збережено в S3: {self.s3_bucket}/{self.s3_key}")
            return True
            
        except Exception as e:
            print(f"⚠️ Не вдалося зберегти сесію в S3: {e}")
            return False

    def _load_session_from_file(self):
        """Завантажує сесію з файлу"""
        try:
            if os.path.exists(self._session_file):
                # Перевіряємо розмір файлу
                file_size = os.path.getsize(self._session_file)
                print(f"📂 Знайдено файл сесії: {self._session_file} (розмір: {file_size} байт)")
                
                with open(self._session_file, 'r') as f:
                    session_data = json.load(f)
                    self._session_cookies = session_data.get('cookies', [])
                    self._session_timestamp = session_data.get('timestamp', 0)
                    
                    # Додаткова інформація для дебагу
                    print(f"📂 Завантажено сесію з файлу: {len(self._session_cookies)} cookies")
                    print(f"📂 Timestamp сесії: {self._session_timestamp} ({datetime.fromtimestamp(self._session_timestamp) if self._session_timestamp else 'None'})")
                    
                    # Перевіряємо структуру cookies
                    if self._session_cookies:
                        sample_cookie = self._session_cookies[0]
                        print(f"📂 Приклад cookie: {list(sample_cookie.keys())}")
                    
                    return True
            else:
                print(f"📂 Файл сесії не існує: {self._session_file}")
        except Exception as e:
            print(f"⚠️ Не вдалося завантажити сесію з файлу: {e}")
        return False
    
    def _save_session_to_file(self):
        """Зберігає сесію у файл"""
        try:
            session_data = {
                'cookies': self._session_cookies,
                'timestamp': self._session_timestamp,
                'lambda_function': os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'local'),
                'saved_at': datetime.now().isoformat()
            }
            # Створюємо директорію якщо не існує
            os.makedirs(os.path.dirname(self._session_file), exist_ok=True)
            
            with open(self._session_file, 'w') as f:
                json.dump(session_data, f, indent=2)
            
            # Перевіряємо що файл створено
            file_size = os.path.getsize(self._session_file)
            print(f"💾 Сесію збережено у файл: {self._session_file} (розмір: {file_size} байт)")
            
            # В AWS Lambda додатково логуємо стан /tmp
            if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
                try:
                    tmp_files = os.listdir('/tmp')
                    print(f"💾 Файли в /tmp: {len(tmp_files)} файлів")
                except:
                    pass
    
        except Exception as e:
            print(f"⚠️ Не вдалося зберегти сесію у файл: {e}")
            # В AWS Lambda це критично, тому додаємо більше інформації
            if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
                try:
                    print(f"⚠️ Дебаг /tmp: доступність={os.access('/tmp', os.W_OK)}")
                except:
                    pass
    
    def _save_session(self):
        """Зберігає поточну сесію (cookies)"""
        try:
            self._session_cookies = self.page.context.cookies()
            self._session_timestamp = time.time()
            
            # Спочатку зберігаємо в S3 (якщо доступно), потім в файл як backup
            if self.use_s3:
                s3_success = self._save_session_to_s3()
                if s3_success:
                    print(f"☁️ Сесію збережено в S3 з {len(self._session_cookies)} cookies")
                else:
                    print("⚠️ Не вдалося зберегти в S3, використовуємо файл")
                    self._save_session_to_file()
            else:
                self._save_session_to_file()
                print(f"💾 Збережено сесію у файл з {len(self._session_cookies)} cookies")
                
        except Exception as e:
            print(f"⚠️ Не вдалося зберегти сесію: {e}")
    
    def _restore_session(self):
        """Відновлює збережену сесію"""
        # Спочатку намагаємося завантажити з S3 або файлу
        if not self._session_cookies:
            if self.use_s3:
                print("🔄 Спроба завантажити сесію з S3...")
                if not self._load_session_from_s3():
                    print("🔄 S3 не спрацював, пробуємо файл...")
                    self._load_session_from_file()
            else:
                self._load_session_from_file()
            
        if not self._is_session_valid() or not self._session_cookies:
            return False
            
        try:
            # КРОК 1: Спочатку переходимо на сайт для створення контексту домену
            print("🌐 Переходимо на сайт для створення контексту...")
            self.page.goto('https://safeyourlove.com/', timeout=15000, wait_until='domcontentloaded')
            
            # КРОК 2: Фільтруємо і очищуємо cookies
            cookies_to_set = []
            current_time = time.time()
            
            for cookie in self._session_cookies:
                # Перевіряємо обов'язкові поля
                if 'name' not in cookie or 'value' not in cookie:
                    print(f"⚠️ Пропускаємо cookie без name/value: {cookie}")
                    continue
                
                # Перевіряємо термін дії
                if 'expires' in cookie and cookie['expires'] and cookie['expires'] != -1:
                    if cookie['expires'] < current_time:
                        print(f"⚠️ Пропускаємо протухлий cookie: {cookie['name']}")
                        continue
                
                cookie_dict = {
                    'name': cookie['name'],
                    'value': cookie['value']
                }
                
                # Додаємо domain правильно
                if 'domain' in cookie and cookie['domain']:
                    # Очищуємо domain від точки на початку
                    domain = cookie['domain'].lstrip('.')
                    cookie_dict['domain'] = domain
                    if 'path' in cookie and cookie['path']:
                        cookie_dict['path'] = cookie['path']
                else:
                    # Якщо немає domain, використовуємо URL
                    cookie_dict['url'] = 'https://safeyourlove.com/'
                
                cookies_to_set.append(cookie_dict)
                
            print(f"🔄 Підготовлено {len(cookies_to_set)} cookies (з {len(self._session_cookies)})")
            
            # КРОК 3: Додаємо cookies
            if cookies_to_set:
                self.page.context.add_cookies(cookies_to_set)
                print(f"✅ Додано {len(cookies_to_set)} cookies до контексту")
            
            # КРОК 4: Перевіряємо, чи cookies застосувалися
            current_cookies = self.page.context.cookies()
            print(f"🔍 Поточна кількість cookies в контексті: {len(current_cookies)}")
            
            return True
        except Exception as e:
            print(f"⚠️ Не вдалося відновити сесію: {e}")
            return False
        
    def _get_iframe(self, timeout=8000):
        """Отримує iframe з кешуванням та очікуванням завантаження таблиці"""
        try:
            if self._cached_iframe:
                try:
                    # Перевіряємо, чи iframe ще активний і не від'єднаний від DOM
                    frame = self._cached_iframe.content_frame()
                    if frame and not frame.is_detached():
                        return frame
                except Exception:
                    # Якщо є помилка, скидаємо кеш
                    self._cached_iframe = None

            # Шукаємо iframe заново
            iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
            self._cached_iframe = self.page.wait_for_selector(iframe_selector, state='visible', timeout=timeout)
            frame = self._cached_iframe.content_frame()

            if frame:
                # ВАЖЛИВО: Очікуємо завантаження таблиці з промокодами
                print("⏳ Очікуємо завантаження таблиці промокодів...")
                
                # Використовуємо стандартний JavaScript, який не залежить від :has-text
                frame.wait_for_function("""
                    () => {
                        // Перевіряємо наявність таблиці
                        const table = document.querySelector('table');
                        if (!table) return false;

                        // Перевіряємо, чи є в таблиці хоча б один рядок або заголовок
                        const hasRows = table.querySelector('tbody tr') !== null;
                        const hasHeaders = table.querySelector('thead tr') !== null;
                        
                        // Повертаємо true, якщо є хоч щось, що свідчить про завантаження
                        return hasRows || hasHeaders;
                    }
                """, timeout=10000)
                
                print("✅ Таблиця промокодів завантажена")

            return frame

        except Exception as e:
            print(f"❌ Не вдалося отримати або дочекатися iframe: {e}")
            self._cached_iframe = None  # Скидаємо кеш при помилці
            return None
    
    def login(self):
        """Виконує швидкий логін з використанням збереженої сесії"""
        start_time = time.time()
        print("🔐 Початок процесу логіну...")
        
        # Спочатку намагаємося відновити збережену сесію
        if self._restore_session():
            try:
                # КРОК 1: Переходимо на адмін сторінку і перевіряємо чи ми авторизовані
                print("🔄 Перевіряємо збережену сесію...")
                self.page.goto(self.admin_url, timeout=20000, wait_until='domcontentloaded')
                
                # КРОК 2: Більш ретельна перевірка авторизації
                # Чекаємо або iframe (успішна авторизація) або поля логіну (потрібна авторизація)
                try:
                    # Спочатку швидка перевірка на iframe
                    iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
                    iframe = self.page.wait_for_selector(iframe_selector, state='visible', timeout=8000)
                    if iframe:
                        print(f"✅ Сесія дійсна! Знайдено iframe за {time.time() - start_time:.2f}с")
                        return True
                except:
                    # Якщо iframe не знайдено - можливо сесія недійсна
                    pass
                
                # КРОК 3: Перевіряємо чи з'явилися поля логіну (означає недійсну сесію)
                try:
                    login_field = self.page.wait_for_selector(
                        'input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]',
                        state='visible',
                        timeout=3000
                    )
                    if login_field:
                        print("🔐 Знайдено поля логіну - сесія недійсна")
                        return False
                except:
                    pass
                
                # КРОК 4: Якщо немає ні iframe ні полів логіну - чекаємо ще трохи
                try:
                    self.page.wait_for_timeout(2000)  # Додаткове очікування
                    iframe = self.page.query_selector('iframe[src*="adminLegacy/data.php"]')
                    if iframe:
                        print(f"✅ Сесія дійсна! Iframe з'явився після очікування за {time.time() - start_time:.2f}с")
                        return True
                    else:
                        print("🔐 Iframe не з'явився - сесія недійсна")
                        return False
                except:
                    print("🔐 Помилка при очікуванні iframe - сесія недійсна")
                    return False
                    
            except Exception as e:
                print(f"⚠️ Помилка при перевірці сесії: {e}")
                return False
        
        # AWS Lambda: спробуємо використати cookies навіть якщо сесія здається застарілою
        # Але тільки якщо cookies не дуже старі (до 2 годин)
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME') and self._session_cookies and self._session_timestamp:
            current_time = time.time()
            age = current_time - self._session_timestamp
            # Спробуємо навіть застарілі cookies, якщо вони не старше 2 годин
            if age <= 7200:  # 2 години
                print(f"🔄 AWS Lambda: Спробуємо застарілі cookies (вік: {age:.0f}с)...")
                try:
                    # Підставляємо cookies навіть якщо вони застарілі
                    cookies_to_set = []
                    for cookie in self._session_cookies:
                        if 'name' not in cookie or 'value' not in cookie:
                            continue
                        cookie_dict = {
                            'name': cookie['name'],
                            'value': cookie['value']
                        }
                        if 'domain' in cookie and cookie['domain']:
                            cookie_dict['domain'] = cookie['domain']
                            if 'path' in cookie and cookie['path']:
                                cookie_dict['path'] = cookie['path']
                        else:
                            cookie_dict['url'] = 'https://safeyourlove.com/'
                        cookies_to_set.append(cookie_dict)
                    
                    self.page.context.add_cookies(cookies_to_set)
                    print(f"🔄 Додано {len(cookies_to_set)} застарілих cookies")
                    
                    # Перевіряємо чи працюють застарілі cookies
                    self.page.goto(self.admin_url, timeout=20000, wait_until='domcontentloaded')
                    try:
                        iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
                        self.page.wait_for_selector(iframe_selector, state='visible', timeout=8000)
                        print(f"🎉 Застарілі cookies працюють! Логін завершено за {time.time() - start_time:.2f}с")
                        # Оновлюємо timestamp та зберігаємо
                        self._session_timestamp = time.time()
                        self._save_session_to_file()
                        return True
                    except:
                        print("❌ Застарілі cookies не працюють")
                except Exception as e:
                    print(f"⚠️ Помилка з застарілими cookies: {e}")
                
                # Очищуємо cookies та контекст для нормального логіну
                try:
                    self.page.context.clear_cookies()
                    print("🧹 Контекст очищено після застарілих cookies")
                except:
                    pass
            else:
                print(f"🕐 Cookies занадто старі (вік: {age:.0f}с), пропускаємо спробу")
        
        # Якщо сесія недійсна, виконуємо повний логін
        login_start = time.time()
        print("🔐 Виконуємо логін...")
        
        # Переходимо на сторінку логіну з збільшеним таймаутом для Lambda
        timeout = 25000 if os.getenv('AWS_LAMBDA_FUNCTION_NAME') else 15000
        self.page.goto(self.admin_url, timeout=timeout, wait_until='domcontentloaded')
        
        # Заповнюємо форму логіну (чекаємо появи полів з правильними селекторами)
        # Збільшуємо таймаути для AWS Lambda
        login_timeout = 25000 if os.getenv('AWS_LAMBDA_FUNCTION_NAME') else 8000
        
        login_fields = self.page.wait_for_selector(
            'input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]',
            state='visible',
            timeout=login_timeout
        )
        
        # Знаходимо всі поля форми за правильними селекторами
        username_field = self.page.query_selector('input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]')
        password_field = self.page.query_selector('input[type="password"]')
        submit_button = self.page.query_selector('button[type="submit"]')
        
        if not username_field or not password_field or not submit_button:
            print("❌ Не знайдено поля форми логіну")
            raise Exception("Поля форми логіну не знайдені")
        
        # Швидке заповнення форми
        if not self.admin_username or not self.admin_password:
            print("❌ Відсутні дані для входу")
            raise Exception("Відсутні дані для входу")
            
        username_field.fill(self.admin_username)
        password_field.fill(self.admin_password)
        
        # Відправляємо форму
        submit_button.click()
        
        # Чекаємо успішного логіну (iframe з'являється після логіну)
        iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
        iframe_timeout = 20000 if os.getenv('AWS_LAMBDA_FUNCTION_NAME') else 10000
        self.page.wait_for_selector(iframe_selector, state='visible', timeout=iframe_timeout)
        
        login_time = time.time() - login_start
        print(f"✅ Логін завершено за {login_time:.2f}с")
        
        # Зберігаємо сесію для майбутніх використань
        self._save_session()
        
        total_time = time.time() - start_time
        print(f"🏁 Весь процес логіну завершено за {total_time:.2f}с")
        return True

    def get_used_codes_count(self):
        """
        Отримує всі лічільники використаних промокодів БЕЗ очищення.
        Використовується для аналізу потреб у поповненні.
        """
        used_codes_key = self.used_codes_key
        
        if not self.s3_client:
            print("❌ S3 клієнт не доступний для роботи з лічільниками")
            return {}
            
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=used_codes_key)
            used_data = json.loads(response['Body'].read().decode('utf-8'))
            
            print(f"📊 Знайдено лічільники: {used_data}")
            return used_data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("ℹ️ Файл з лічільниками не існує.")
                return {}
            else:
                print(f"❌ Помилка при отриманні лічільників: {e}")
                return {}
        except Exception as e:
            print(f"❌ Неочікувана помилка при роботі з лічільниками: {e}")
            return {}

    def clear_used_codes_count(self, amount: int):
        """
        Очищає лічільник використаних промокодів тільки для конкретної суми.
        Залишає інші суми недоторканими для безпеки.
        """
        used_codes_key = self.used_codes_key
        amount_key = str(amount)
        
        if not self.s3_client:
            print("❌ S3 клієнт не доступний для очищення лічільників")
            return False
            
        try:
            # Завантажуємо поточні дані
            try:
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=used_codes_key)
                used_data = json.loads(response['Body'].read().decode('utf-8'))
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print("ℹ️ Файл з лічільниками не існує - нічого очищати.")
                    return True
                else:
                    raise
            
            # Зберігаємо старе значення для логування
            old_count = used_data.get(amount_key, 0)
            
            # Видаляємо тільки конкретну суму
            if amount_key in used_data:
                del used_data[amount_key]
                print(f"🗑️ Очищено лічільник для суми {amount} (було: {old_count})")
            else:
                print(f"ℹ️ Лічільник для суми {amount} вже відсутній")
            
            # Зберігаємо оновлені дані
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=used_codes_key,
                Body=json.dumps(used_data, indent=2),
                ContentType='application/json'
            )
            
            # Логуємо що залишилось
            remaining_sums = list(used_data.keys())
            if remaining_sums:
                print(f"💾 Залишились лічільники для сум: {remaining_sums}")
            else:
                print("💾 Всі лічільники очищено")
            
            return True
            
        except Exception as e:
            print(f"❌ Помилка при очищенні лічільника для суми {amount}: {e}")
            return False