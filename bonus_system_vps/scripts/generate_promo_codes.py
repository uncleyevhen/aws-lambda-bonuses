#!/usr/bin/env python3
"""
Генератор промокодів для VPS версії бонусної системи.

Використовує Playwright для створення кодів в адмін-панелі
і зберігає їх в PostgreSQL.

Запуск:
    # Генерувати 10 кодів для сум 100-500 грн
    python scripts/generate_promo_codes.py --start-amount 100 --end-amount 500 --count 10
    
    # Тільки для конкретної суми
    python scripts/generate_promo_codes.py --amount 100 --count 20
    
    # Dry-run (без збереження в БД)
    python scripts/generate_promo_codes.py --amount 100 --count 5 --dry-run
"""

import os
import sys
import time
import json
import random
import string
import argparse
import logging
from datetime import datetime, timedelta

# Playwright
from playwright.sync_api import sync_playwright, Page

# SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Додаємо шлях до app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.promo_code import PromoCode
from app.database import Base

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VPSPromoGenerator:
    """
    Генератор промокодів для VPS.
    Створює коди в адмін-панелі та зберігає в PostgreSQL.
    """
    
    def __init__(self, database_url: str, dry_run: bool = False):
        self.admin_url = os.getenv('ADMIN_URL', 'https://safeyourlove.com/edit/discounts/codes')
        self.admin_username = os.getenv('ADMIN_USERNAME')
        self.admin_password = os.getenv('ADMIN_PASSWORD')
        self.dry_run = dry_run
        
        # Database
        self.engine = create_engine(database_url)
        Session = sessionmaker(bind=self.engine)
        self.db_session = Session()
        
        # Playwright
        self.playwright = None
        self.browser = None
        self.page = None
        self._cached_iframe = None
        
        # Stats
        self.stats = {
            "created_admin": 0,
            "saved_db": 0,
            "errors": 0
        }
    
    def _generate_code_string(self, amount: int) -> str:
        """Генерує рядок промокоду для заданої суми."""
        amount_str = str(amount)
        prefix = f'BON{amount_str}'
        random_part_length = max(3, 7 - len(amount_str))
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=random_part_length))
        return prefix + random_part
    
    def start_browser(self):
        """Запускає браузер."""
        logger.info("🌐 Запуск браузера...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        self.page = self.browser.new_page()
        logger.info("✅ Браузер запущено")
    
    def stop_browser(self):
        """Зупиняє браузер."""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("🛑 Браузер зупинено")
    
    def login(self) -> bool:
        """Виконує логін в адмін-панель."""
        logger.info("🔐 Логін в адмін-панель...")
        
        if not self.admin_username or not self.admin_password:
            logger.error("❌ Відсутні ADMIN_USERNAME або ADMIN_PASSWORD")
            return False
        
        try:
            # Переходимо на сторінку
            self.page.goto(self.admin_url, timeout=30000, wait_until='domcontentloaded')
            
            # Шукаємо поля логіну
            username_field = self.page.wait_for_selector(
                'input[placeholder*="пошта"], input[placeholder*="логін"], input[type="text"]',
                state='visible',
                timeout=10000
            )
            password_field = self.page.query_selector('input[type="password"]')
            submit_button = self.page.query_selector('button[type="submit"]')
            
            if not username_field or not password_field or not submit_button:
                logger.error("❌ Не знайдено поля форми логіну")
                return False
            
            # Заповнюємо форму
            username_field.fill(self.admin_username)
            password_field.fill(self.admin_password)
            submit_button.click()
            
            # Чекаємо на iframe (підтвердження успішного логіну)
            iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
            self.page.wait_for_selector(iframe_selector, state='visible', timeout=20000)
            
            logger.info("✅ Логін успішний")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка логіну: {e}")
            return False
    
    def _get_iframe(self, timeout=8000):
        """Отримує iframe з адмін-панелі."""
        try:
            if self._cached_iframe:
                try:
                    frame = self._cached_iframe.content_frame()
                    if frame and not frame.is_detached():
                        return frame
                except:
                    self._cached_iframe = None
            
            iframe_selector = 'iframe[src*="adminLegacy/data.php"]'
            self._cached_iframe = self.page.wait_for_selector(iframe_selector, state='visible', timeout=timeout)
            frame = self._cached_iframe.content_frame()
            
            if frame:
                # Чекаємо завантаження таблиці
                frame.wait_for_function("""
                    () => {
                        const table = document.querySelector('table');
                        if (!table) return false;
                        return table.querySelector('tbody tr') !== null || table.querySelector('thead tr') !== null;
                    }
                """, timeout=10000)
            
            return frame
            
        except Exception as e:
            logger.error(f"❌ Помилка отримання iframe: {e}")
            self._cached_iframe = None
            return None
    
    def create_promo_code_in_admin(self, promo_code: str, amount: int) -> bool:
        """Створює промокод в адмін-панелі."""
        try:
            frame = self._get_iframe()
            if not frame:
                raise Exception("Не знайдено iframe")
            
            # Клік "Додати промокод"
            try:
                frame.locator('a.button.add.plus').click()
            except:
                # Альтернативні селектори
                selectors = ['a.button.add', '.button.add.plus', '[class*="add"][class*="button"]']
                for selector in selectors:
                    try:
                        frame.locator(selector).click()
                        break
                    except:
                        continue
            
            # Чекаємо появи форми
            frame.wait_for_selector('input[name="names[code]"]', timeout=30000)
            
            # Заповнюємо форму через JavaScript
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
            
            # Вибираємо всі бренди та категорії
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
            
            # Зберігаємо
            frame.locator('input.save-exit-button[type="submit"]').click()
            
            # Чекаємо повернення на список
            frame.locator('a.button.add.plus').wait_for(state='visible', timeout=10000)
            
            logger.info(f"✅ Промокод {promo_code} створено в адмін-панелі")
            self.stats["created_admin"] += 1
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка створення промокоду {promo_code}: {e}")
            self.stats["errors"] += 1
            return False
    
    def save_to_database(self, code: str, amount: int) -> bool:
        """Зберігає промокод в PostgreSQL."""
        if self.dry_run:
            logger.info(f"🔍 DRY RUN: Промокод {code} був би збережений в БД")
            return True
        
        try:
            # Перевіряємо чи код вже існує
            existing = self.db_session.query(PromoCode).filter(PromoCode.code == code).first()
            if existing:
                logger.warning(f"⚠️ Промокод {code} вже існує в БД")
                return False
            
            promo = PromoCode(
                code=code,
                amount=amount,
                is_used=False
            )
            self.db_session.add(promo)
            self.db_session.commit()
            
            logger.info(f"💾 Промокод {code} збережено в БД")
            self.stats["saved_db"] += 1
            return True
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"❌ Помилка збереження в БД: {e}")
            return False
    
    def generate_codes(self, amount: int, count: int) -> list:
        """
        Генерує промокоди для конкретної суми.
        
        Args:
            amount: Сума промокоду
            count: Кількість кодів для генерації
            
        Returns:
            list: Список згенерованих кодів
        """
        logger.info(f"💰 Генерація {count} промокодів для суми {amount} грн...")
        
        generated_codes = []
        
        for i in range(count):
            code = self._generate_code_string(amount)
            logger.info(f"⚙️ [{i+1}/{count}] Створюємо код: {code}")
            
            # Створюємо в адмін-панелі
            if self.create_promo_code_in_admin(code, amount):
                # Зберігаємо в БД
                if self.save_to_database(code, amount):
                    generated_codes.append(code)
                
                # Пауза між створеннями
                time.sleep(1)
            else:
                logger.warning(f"⚠️ Пропускаємо код {code}")
        
        logger.info(f"✅ Для суми {amount}: створено {len(generated_codes)} кодів")
        return generated_codes
    
    def generate_for_range(self, start_amount: int, end_amount: int, count: int):
        """
        Генерує промокоди для діапазону сум.
        
        Args:
            start_amount: Початкова сума
            end_amount: Кінцева сума
            count: Кількість кодів для кожної суми
        """
        logger.info(f"🎯 Генерація промокодів для сум {start_amount}-{end_amount} грн, по {count} кодів")
        
        all_codes = {}
        
        for amount in range(start_amount, end_amount + 1):
            codes = self.generate_codes(amount, count)
            all_codes[amount] = codes
        
        return all_codes
    
    def run(self, start_amount: int = None, end_amount: int = None, 
            single_amount: int = None, count: int = 10):
        """
        Головна функція генерації.
        
        Args:
            start_amount: Початкова сума діапазону
            end_amount: Кінцева сума діапазону
            single_amount: Конкретна сума (замість діапазону)
            count: Кількість кодів
        """
        try:
            # Створюємо таблиці якщо не існують
            if not self.dry_run:
                Base.metadata.create_all(self.engine)
            
            # Запускаємо браузер
            self.start_browser()
            
            # Логін
            if not self.login():
                logger.error("❌ Не вдалося увійти в адмін-панель")
                return
            
            # Генеруємо коди
            if single_amount:
                self.generate_codes(single_amount, count)
            else:
                self.generate_for_range(start_amount, end_amount, count)
            
            # Підсумок
            self.print_stats()
            
        except Exception as e:
            logger.error(f"❌ Критична помилка: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.stop_browser()
            self.db_session.close()
    
    def print_stats(self):
        """Виводить статистику."""
        logger.info("\n" + "=" * 50)
        logger.info("📊 СТАТИСТИКА ГЕНЕРАЦІЇ:")
        logger.info("=" * 50)
        logger.info(f"  Створено в адмін-панелі: {self.stats['created_admin']}")
        logger.info(f"  Збережено в БД: {self.stats['saved_db']}")
        logger.info(f"  Помилок: {self.stats['errors']}")
        logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Генератор промокодів для VPS")
    
    # Сума
    amount_group = parser.add_mutually_exclusive_group(required=True)
    amount_group.add_argument(
        "--amount",
        type=int,
        help="Конкретна сума для генерації"
    )
    amount_group.add_argument(
        "--start-amount",
        type=int,
        help="Початкова сума діапазону"
    )
    
    parser.add_argument(
        "--end-amount",
        type=int,
        help="Кінцева сума діапазону (потрібно разом з --start-amount)"
    )
    
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Кількість кодів для кожної суми (за замовчуванням: 10)"
    )
    
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "postgresql://bonus:bonuspass123@localhost:5432/bonus_db"),
        help="URL бази даних PostgreSQL"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Тестовий запуск без збереження в БД"
    )
    
    args = parser.parse_args()
    
    # Перевірка аргументів
    if args.start_amount and not args.end_amount:
        parser.error("--end-amount потрібен разом з --start-amount")
    
    logger.info("🚀 Запуск генератора промокодів")
    logger.info(f"   Режим: {'DRY RUN' if args.dry_run else 'PRODUCTION'}")
    
    generator = VPSPromoGenerator(
        database_url=args.database_url,
        dry_run=args.dry_run
    )
    
    if args.amount:
        generator.run(single_amount=args.amount, count=args.count)
    else:
        generator.run(
            start_amount=args.start_amount,
            end_amount=args.end_amount,
            count=args.count
        )


if __name__ == "__main__":
    main()
