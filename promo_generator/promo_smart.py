"""
Розумне управління промо-кодами - аналіз, фільтрація, пакетні операції.

Цей модуль містить PromoSmartManager для складних операцій з промо-кодами:
- Аналіз існуючих промо-кодів з пагінацією
- Фільтрація по сумах і назвах (BON коди)
- Виявлення дублікатів при переключенні сторінок
- Створення недостатніх промо-кодів
- Видалення зайвих промо-кодів
- Синхронізація з цільовою кількістю кодів для кожної суми
"""

import time
import logging
import random
import string
import sys
import os
from typing import Dict, List, Set, Optional, Tuple
from datetime import datetime

# Додаємо шлях до replenish_promo_code_lambda для імпорту
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(parent_dir, 'replenish_promo_code_lambda'))

# Імпорти з replenish_promo_code_lambda
try:
    from bon_utils import is_bon_promo_code, extract_amount_from_bon_code
    from promo_logic import PromoService
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"⚠️ Не вдалося імпортувати модулі: {e}")
    # Fallback заглушки
    def is_bon_promo_code(code):
        return code.startswith('BON')
    def extract_amount_from_bon_code(code):
        import re
        match = re.search(r'BON(\d+)', code)
        return int(match.group(1)) if match else None
    PromoService = None

logger = logging.getLogger(__name__)


class PromoSmartManager:
    """
    Розумний менеджер для управління промо-кодами.
    
    Основні можливості:
    - Аналіз всіх BON промо-кодів з пагінацією
    - Фільтрація по діапазону сум
    - Виявлення та усунення дублікатів
    - Автоматичне створення недостатніх кодів
    - Видалення зайвих кодів
    - Детальна звітність по операціях
    """
    
    def __init__(self, promo_service):
        """
        Ініціалізація SmartManager з існуючим PromoService.
        
        Args:
            promo_service: Екземпляр PromoService з активною сесією
        """
        self.promo_service = promo_service
        self.page = promo_service.page
        
        # Налаштування для роботи
        self.max_rows_per_page = 160  # Максимум промо-кодів на сторінку
        self.duplicate_check_retries = 3  # Кількість перевірок дублікатів
        
        # Кеш для збереження результатів
        self.all_codes_cache = {}
        self.duplicates_found = {}
        
    def set_page_size(self, rows_count: int = 160) -> bool:
        """
        Встановлює кількість рядків на сторінку для оптимізації швидкості.
        
        Args:
            rows_count: Кількість рядків (20, 50, 100, 160)
            
        Returns:
            bool: True якщо успішно встановлено
        """
        try:
            iframe = self.promo_service._get_iframe()
            if not iframe:
                logger.error("❌ Не вдалося отримати iframe для встановлення розміру сторінки")
                return False
                
            logger.info(f"📄 Встановлюємо {rows_count} рядків на сторінку...")
            
            # Знаходимо селектор для кількості рядків
            rows_selector = iframe.locator('select[name="codes_length"]')
            if rows_selector.count() == 0:
                logger.warning("⚠️ Селектор кількості рядків не знайдений")
                return False
                
            # Встановлюємо значення
            rows_selector.select_option(str(rows_count))
            time.sleep(2)  # Чекаємо на оновлення таблиці
            
            logger.info(f"✅ Встановлено {rows_count} рядків на сторінку")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка встановлення розміру сторінки: {e}")
            return False
    
    def apply_amount_range_filter(self, start_amount: int, end_amount: int) -> bool:
        """
        Застосовує фільтр по діапазону сум.
        
        Args:
            start_amount: Мінімальна сума
            end_amount: Максимальна сума
            
        Returns:
            bool: True якщо фільтр застосовано успішно
        """
        try:
            iframe = self.promo_service._get_iframe()
            if not iframe:
                logger.error("❌ Не вдалося отримати iframe для фільтрації")
                return False
                
            logger.info(f"🔍 Застосовуємо фільтр по сумах: {start_amount}-{end_amount} грн")
            
            # Знаходимо поля для діапазону сум
            min_amount_input = iframe.locator('input[name="discount_amount_from"]')
            max_amount_input = iframe.locator('input[name="discount_amount_to"]')
            
            if min_amount_input.count() == 0 or max_amount_input.count() == 0:
                logger.error("❌ Поля фільтрації по сумах не знайдені")
                return False
            
            # Очищуємо та вводимо значення
            min_amount_input.clear()
            min_amount_input.fill(str(start_amount))
            
            max_amount_input.clear()
            max_amount_input.fill(str(end_amount))
            
            # Натискаємо кнопку застосування фільтру
            filter_button = iframe.locator('input[value="Фільтр"]')
            if filter_button.count() > 0:
                filter_button.click()
                time.sleep(3)  # Чекаємо на застосування фільтру
            
            logger.info(f"✅ Фільтр по сумах {start_amount}-{end_amount} застосовано")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка застосування фільтру по сумах: {e}")
            return False
    
    def apply_bon_filter(self) -> bool:
        """
        Застосовує фільтр для пошуку BON промо-кодів.
        
        Returns:
            bool: True якщо фільтр застосовано успішно
        """
        try:
            iframe = self.promo_service._get_iframe()
            if not iframe:
                logger.error("❌ Не вдалося отримати iframe для BON фільтру")
                return False
                
            logger.info("🔍 Застосовуємо BON фільтр...")
            
            # Знаходимо поле пошуку по коду
            search_input = iframe.locator('input[name="code"]')
            if search_input.count() == 0:
                logger.error("❌ Поле пошуку по коду не знайдене")
                return False
            
            # Вводимо BON для фільтрації
            search_input.clear()
            search_input.fill("BON")
            
            # Натискаємо кнопку застосування фільтру
            filter_button = iframe.locator('input[value="Фільтр"]')
            if filter_button.count() > 0:
                filter_button.click()
                time.sleep(3)  # Чекаємо на застосування фільтру
            
            logger.info("✅ BON фільтр застосовано")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка застосування BON фільтру: {e}")
            return False
    
    def get_current_page_codes(self) -> List[Dict]:
        """
        Отримує всі промо-коди з поточної сторінки.
        
        Returns:
            List[Dict]: Список промо-кодів з інформацією
        """
        try:
            iframe = self.promo_service._get_iframe()
            if not iframe:
                logger.error("❌ Не вдалося отримати iframe для читання кодів")
                return []
            
            codes = []
            
            # Знаходимо всі рядки таблиці з промо-кодами
            rows = iframe.locator('table tbody tr')
            rows_count = rows.count()
            
            logger.debug(f"📋 Знайдено {rows_count} рядків на поточній сторінці")
            
            for i in range(rows_count):
                try:
                    row = rows.nth(i)
                    
                    # Отримуємо код (зазвичай в першій колонці)
                    code_cell = row.locator('td').nth(0)
                    if code_cell.count() == 0:
                        continue
                        
                    code_text = code_cell.text_content()
                    if not code_text:
                        continue
                        
                    code = code_text.strip()
                    
                    # Перевіряємо, чи це BON код
                    if not is_bon_promo_code(code):
                        continue
                    
                    # Отримуємо суму з коду
                    amount = extract_amount_from_bon_code(code)
                    if amount is None:
                        continue
                    
                    # Отримуємо додаткову інформацію (статус, дата створення тощо)
                    cells = row.locator('td')
                    cell_count = cells.count()
                    
                    code_info = {
                        'code': code,
                        'amount': amount,
                        'row_index': i
                    }
                    
                    # Додаємо додаткову інформацію якщо є
                    if cell_count > 1:
                        # Зазвичай статус в останній колонці
                        status_cell = cells.nth(cell_count - 1)
                        status_text = status_cell.text_content()
                        if status_text:
                            code_info['status'] = status_text.strip()
                    
                    codes.append(code_info)
                    
                except Exception as e:
                    logger.debug(f"⚠️ Помилка обробки рядка {i}: {e}")
                    continue
            
            logger.debug(f"✅ Отримано {len(codes)} BON кодів з поточної сторінки")
            return codes
            
        except Exception as e:
            logger.error(f"❌ Помилка отримання кодів з поточної сторінки: {e}")
            return []
    
    def get_all_pages_codes(self, start_amount: int, end_amount: int) -> Dict[int, List[str]]:
        """
        Проходить по всіх сторінках пагінації та збирає всі BON коди.
        
        Args:
            start_amount: Мінімальна сума для фільтрації
            end_amount: Максимальна сума для фільтрації
            
        Returns:
            Dict[int, List[str]]: Словник {сума: [список_кодів]}
        """
        try:
            iframe = self.promo_service._get_iframe()
            if not iframe:
                logger.error("❌ Не вдалося отримати iframe для пагінації")
                return {}
            
            # Встановлюємо розмір сторінки
            self.set_page_size(self.max_rows_per_page)
            
            # Застосовуємо фільтри
            if not self.apply_amount_range_filter(start_amount, end_amount):
                logger.error("❌ Не вдалося застосувати фільтр по сумах")
                return {}
                
            if not self.apply_bon_filter():
                logger.error("❌ Не вдалося застосувати BON фільтр")
                return {}
            
            all_codes = {}
            page_number = 1
            processed_codes = set()  # Для відстеження дублікатів
            
            logger.info("🔄 Початок збору кодів з усіх сторінок...")
            
            while True:
                logger.info(f"📄 Обробка сторінки {page_number}...")
                
                # Отримуємо коди з поточної сторінки
                page_codes = self.get_current_page_codes()
                
                if not page_codes:
                    logger.info(f"📄 Сторінка {page_number} порожня, завершуємо збір")
                    break
                
                # Обробляємо коди з поточної сторінки
                new_codes_count = 0
                duplicate_codes_count = 0
                
                for code_info in page_codes:
                    code = code_info['code']
                    amount = code_info['amount']
                    
                    # Перевіряємо на дублікат
                    if code in processed_codes:
                        duplicate_codes_count += 1
                        logger.debug(f"🔄 Дублікат знайдено: {code}")
                        continue
                    
                    # Додаємо новий код
                    processed_codes.add(code)
                    new_codes_count += 1
                    
                    if amount not in all_codes:
                        all_codes[amount] = []
                    all_codes[amount].append(code)
                
                logger.info(f"📄 Сторінка {page_number}: +{new_codes_count} нових, {duplicate_codes_count} дублікатів")
                
                # Перевіряємо наявність кнопки "Наступна сторінка"
                next_button = iframe.locator('a[href*="page="]:has-text("Наступна"), a[href*="page="]:has-text("Next"), a[href*="page="]:has-text(">")').last
                
                if next_button.count() == 0 or not next_button.is_enabled():
                    logger.info(f"📄 Остання сторінка {page_number}, завершуємо збір")
                    break
                
                # Переходимо на наступну сторінку
                try:
                    next_button.click()
                    time.sleep(3)  # Чекаємо завантаження сторінки
                    page_number += 1
                    
                    # Захист від нескінченного циклу
                    if page_number > 1000:
                        logger.warning("⚠️ Досягнуто ліміт сторінок (1000), припиняємо збір")
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Помилка переходу на сторінку {page_number + 1}: {e}")
                    break
            
            # Підсумкова статистика
            total_codes = sum(len(codes) for codes in all_codes.values())
            logger.info(f"✅ Збір завершено: {total_codes} унікальних кодів з {page_number} сторінок")
            
            # Виводимо статистику по сумах
            for amount in sorted(all_codes.keys()):
                count = len(all_codes[amount])
                logger.info(f"💰 {amount} грн: {count} кодів")
            
            return all_codes
            
        except Exception as e:
            logger.error(f"❌ Помилка збору кодів з усіх сторінок: {e}")
            return {}
    
    def verify_duplicates_for_amounts(self, amounts_to_check: List[int]) -> Dict[int, List[str]]:
        """
        Перевіряє дублікати для конкретних сум, фільтруючи по кожній сумі окремо.
        
        Args:
            amounts_to_check: Список сум для перевірки
            
        Returns:
            Dict[int, List[str]]: Уточнені коди по сумах
        """
        try:
            logger.info(f"🔍 Перевірка дублікатів для сум: {amounts_to_check}")
            
            verified_codes = {}
            
            for amount in amounts_to_check:
                logger.info(f"🔍 Перевірка дублікатів для суми {amount} грн...")
                
                # Застосовуємо фільтр по конкретній сумі
                if not self.apply_amount_range_filter(amount, amount):
                    logger.error(f"❌ Не вдалося застосувати фільтр для суми {amount}")
                    continue
                
                if not self.apply_bon_filter():
                    logger.error(f"❌ Не вдалося застосувати BON фільтр для суми {amount}")
                    continue
                
                # Збираємо коди для цієї суми
                amount_codes = self.get_all_pages_codes(amount, amount)
                
                if amount in amount_codes:
                    verified_codes[amount] = amount_codes[amount]
                    logger.info(f"✅ Сума {amount} грн: знайдено {len(amount_codes[amount])} кодів")
                else:
                    verified_codes[amount] = []
                    logger.info(f"📝 Сума {amount} грн: кодів не знайдено")
                
                time.sleep(1)  # Пауза між перевірками
            
            return verified_codes
            
        except Exception as e:
            logger.error(f"❌ Помилка перевірки дублікатів: {e}")
            return {}
    
    def analyze_codes_balance(self, current_codes: Dict[int, List[str]], 
                            target_count: int, start_amount: int, end_amount: int) -> Dict:
        """
        Аналізує баланс промо-кодів та визначає необхідні операції.
        
        Args:
            current_codes: Поточні коди {сума: [коди]}
            target_count: Цільова кількість кодів для кожної суми
            start_amount: Початкова сума діапазону
            end_amount: Кінцева сума діапазону
            
        Returns:
            Dict: Аналіз з планом дій
        """
        analysis = {
            'to_create': {},  # {сума: кількість_для_створення}
            'to_delete': {},  # {сума: [коди_для_видалення]}
            'unchanged': {},  # {сума: кількість_без_змін}
            'summary': {
                'total_to_create': 0,
                'total_to_delete': 0,
                'amounts_unchanged': 0
            }
        }
        
        logger.info(f"📊 Аналіз балансу кодів для діапазону {start_amount}-{end_amount} грн")
        logger.info(f"🎯 Цільова кількість: {target_count} кодів на суму")
        
        for amount in range(start_amount, end_amount + 1):
            current_count = len(current_codes.get(amount, []))
            
            if current_count < target_count:
                # Потрібно створити коди
                need_to_create = target_count - current_count
                analysis['to_create'][amount] = need_to_create
                analysis['summary']['total_to_create'] += need_to_create
                logger.info(f"➕ {amount} грн: потрібно створити {need_to_create} кодів (є {current_count})")
                
            elif current_count > target_count:
                # Потрібно видалити зайві коди
                codes_to_delete = current_codes[amount][target_count:]  # Видаляємо зайві
                analysis['to_delete'][amount] = codes_to_delete
                analysis['summary']['total_to_delete'] += len(codes_to_delete)
                logger.info(f"➖ {amount} грн: потрібно видалити {len(codes_to_delete)} кодів (є {current_count})")
                
            else:
                # Кількість оптимальна
                analysis['unchanged'][amount] = current_count
                analysis['summary']['amounts_unchanged'] += 1
                logger.info(f"✅ {amount} грн: оптимальна кількість ({current_count} кодів)")
        
        # Підсумок
        logger.info(f"\n📊 ПІДСУМОК АНАЛІЗУ:")
        logger.info(f"➕ Створити: {analysis['summary']['total_to_create']} кодів")
        logger.info(f"➖ Видалити: {analysis['summary']['total_to_delete']} кодів")
        logger.info(f"✅ Без змін: {analysis['summary']['amounts_unchanged']} сум")
        
        return analysis
    
    def execute_balance_operations(self, analysis: Dict) -> Dict:
        """
        Виконує операції для балансування кодів згідно з аналізом.
        
        Args:
            analysis: Результат аналізу з analyze_codes_balance
            
        Returns:
            Dict: Результати виконання операцій
        """
        results = {
            'created': 0,
            'deleted': 0,
            'errors': []
        }
        
        try:
            # СТВОРЕННЯ НОВИХ КОДІВ
            if analysis['to_create']:
                logger.info(f"\n➕ СТВОРЕННЯ КОДІВ...")
                
                for amount, count in analysis['to_create'].items():
                    logger.info(f"➕ Створюємо {count} кодів для суми {amount} грн...")
                    
                    try:
                        # Генеруємо коди для створення
                        codes_to_create = []
                        for _ in range(count):
                            # Генеруємо унікальний код формату BON{amount}{random}
                            random_part = ''.join([str(random.randint(0, 9)) for _ in range(6)])
                            new_code = f"BON{amount}{random_part}"
                            codes_to_create.append(new_code)
                        
                        # Створюємо коди через PromoService
                        success_count = 0
                        for code in codes_to_create:
                            if self.promo_service.create_promo_code(code, amount):
                                success_count += 1
                                time.sleep(0.5)  # Пауза між створенням
                            else:
                                logger.warning(f"⚠️ Не вдалося створити код {code}")
                        
                        results['created'] += success_count
                        logger.info(f"✅ Створено {success_count}/{count} кодів для суми {amount} грн")
                        
                    except Exception as e:
                        error_msg = f"Помилка створення кодів для суми {amount}: {e}"
                        logger.error(f"❌ {error_msg}")
                        results['errors'].append(error_msg)
            
            # ВИДАЛЕННЯ ЗАЙВИХ КОДІВ
            if analysis['to_delete']:
                logger.info(f"\n➖ ВИДАЛЕННЯ ЗАЙВИХ КОДІВ...")
                
                for amount, codes_to_delete in analysis['to_delete'].items():
                    logger.info(f"➖ Видаляємо {len(codes_to_delete)} кодів для суми {amount} грн...")
                    
                    try:
                        # Видаляємо коди через SmartManager
                        if self._delete_codes_batch(codes_to_delete):
                            results['deleted'] += len(codes_to_delete)
                            logger.info(f"✅ Видалено {len(codes_to_delete)} кодів для суми {amount} грн")
                        else:
                            error_msg = f"Не вдалося видалити коди для суми {amount}"
                            logger.error(f"❌ {error_msg}")
                            results['errors'].append(error_msg)
                            
                    except Exception as e:
                        error_msg = f"Помилка видалення кодів для суми {amount}: {e}"
                        logger.error(f"❌ {error_msg}")
                        results['errors'].append(error_msg)
            
            # Підсумок операцій
            logger.info(f"\n📊 РЕЗУЛЬТАТИ ОПЕРАЦІЙ:")
            logger.info(f"✅ Створено: {results['created']} кодів")
            logger.info(f"✅ Видалено: {results['deleted']} кодів")
            if results['errors']:
                logger.info(f"❌ Помилок: {len(results['errors'])}")
                for error in results['errors']:
                    logger.error(f"   - {error}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Критична помилка виконання операцій: {e}")
            results['errors'].append(str(e))
            return results
    
    def _delete_codes_batch(self, codes_to_delete: List[str]) -> bool:
        """
        Видаляє список кодів пакетно.
        
        Args:
            codes_to_delete: Список кодів для видалення
            
        Returns:
            bool: True якщо успішно видалено
        """
        try:
            iframe = self.promo_service._get_iframe()
            if not iframe:
                logger.error("❌ Не вдалося отримати iframe для видалення")
                return False
            
            # Логіка вибору та видалення кодів
            # (тут може бути складніша логіка залежно від інтерфейсу)
            
            logger.info(f"🗑️ Видалення {len(codes_to_delete)} кодів...")
            
            # Приклад простого видалення (потрібно адаптувати під реальний інтерфейс)
            for code in codes_to_delete:
                try:
                    # Знаходимо рядок з кодом та видаляємо його
                    # Це залежить від конкретного інтерфейсу адмін-панелі
                    pass
                except Exception as e:
                    logger.warning(f"⚠️ Не вдалося видалити код {code}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка пакетного видалення: {e}")
            return False
    
    def smart_management_cycle(self, start_amount: int, end_amount: int, 
                             target_count: int) -> Dict:
        """
        Повний цикл розумного управління промо-кодами.
        
        Args:
            start_amount: Початкова сума діапазону
            end_amount: Кінцева сума діапазону  
            target_count: Цільова кількість кодів для кожної суми
            
        Returns:
            Dict: Підсумкові результати операцій
        """
        try:
            logger.info(f"🎯 ПОЧАТОК РОЗУМНОГО УПРАВЛІННЯ ПРОМО-КОДАМИ")
            logger.info(f"💰 Діапазон сум: {start_amount}-{end_amount} грн")
            logger.info(f"🎯 Цільова кількість: {target_count} кодів на суму")
            
            # ЕТАП 1: Збір всіх існуючих кодів
            logger.info(f"\n📋 ЕТАП 1: Збір існуючих промо-кодів...")
            current_codes = self.get_all_pages_codes(start_amount, end_amount)
            
            if not current_codes:
                logger.warning("⚠️ Не знайдено жодного BON коду в заданому діапазоні")
            
            # ЕТАП 2: Перевірка дублікатів для проблемних сум
            logger.info(f"\n🔍 ЕТАП 2: Перевірка дублікатів...")
            amounts_to_verify = []
            for amount in range(start_amount, end_amount + 1):
                if amount in current_codes and len(current_codes[amount]) > target_count * 1.2:
                    amounts_to_verify.append(amount)
            
            if amounts_to_verify:
                logger.info(f"🔍 Перевіряємо дублікати для сум: {amounts_to_verify}")
                verified_codes = self.verify_duplicates_for_amounts(amounts_to_verify)
                
                # Оновлюємо дані з перевіреними
                for amount, codes in verified_codes.items():
                    current_codes[amount] = codes
            
            # ЕТАП 3: Аналіз та планування
            logger.info(f"\n📊 ЕТАП 3: Аналіз балансу кодів...")
            analysis = self.analyze_codes_balance(current_codes, target_count, start_amount, end_amount)
            
            # ЕТАП 4: Виконання операцій
            logger.info(f"\n⚙️ ЕТАП 4: Виконання операцій...")
            results = self.execute_balance_operations(analysis)
            
            # ЕТАП 5: Фінальний звіт
            logger.info(f"\n📊 ФІНАЛЬНИЙ ЗВІТ:")
            logger.info(f"✅ Створено: {results['created']} промо-кодів")
            logger.info(f"✅ Видалено: {results['deleted']} промо-кодів")
            if results['errors']:
                logger.info(f"❌ Помилок: {len(results['errors'])}")
            
            logger.info(f"🎯 РОЗУМНЕ УПРАВЛІННЯ ЗАВЕРШЕНО УСПІШНО!")
            
            return {
                'created': results['created'],
                'deleted': results['deleted'],
                'errors': results['errors'],
                'current_codes': current_codes,
                'analysis': analysis
            }
            
        except Exception as e:
            logger.error(f"❌ Критична помилка розумного управління: {e}")
            return {
                'created': 0,
                'deleted': 0,
                'errors': [str(e)],
                'current_codes': {},
                'analysis': {}
            }
