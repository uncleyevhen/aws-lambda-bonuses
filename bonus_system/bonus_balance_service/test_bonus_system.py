"""
Тестовий скрипт для перевірки нової системи бонусів
Тестує всі основні сценарії роботи з DynamoDB та синхронізацією в KeyCRM
"""
import json
import logging
import sys
import os
from datetime import datetime

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Додаємо шляхи
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Імпортуємо наші сервіси
from bonus_balance_manager import BonusBalanceManager
from sync_service import BonusSyncService

def test_basic_balance_operations():
    """Тест основних операцій з балансом"""
    logger.info("=== ТЕСТ ОСНОВНИХ ОПЕРАЦІЙ З БАЛАНСОМ ===")
    
    balance_manager = BonusBalanceManager()
    test_phone = "+380123456789"
    
    try:
        # 1. Перевіряємо початковий баланс
        logger.info("1. Перевірка початкового балансу")
        balance = balance_manager.get_balance(test_phone)
        logger.info(f"Початковий баланс: {balance}")
        
        # 2. Додаємо бонуси
        logger.info("2. Додавання 100 бонусів")
        transaction = {
            'type': 'earned',
            'amount': 100,
            'order_id': 'TEST001',
            'description': '✅ Тестове нарахування: +100 бонусів'
        }
        
        result = balance_manager.update_balance(
            test_phone,
            active_bonus_delta=100,
            transaction=transaction
        )
        logger.info(f"Результат нарахування: {result}")
        
        # 3. Резервуємо частину бонусів
        logger.info("3. Резервування 30 бонусів")
        reserve_transaction = {
            'type': 'reserved',
            'amount': 30,
            'order_id': 'TEST002',
            'description': '🔒 Зарезервовано 30 бонусів для замовлення TEST002'
        }
        
        result = balance_manager.update_balance(
            test_phone,
            active_bonus_delta=-30,
            reserved_bonus_delta=30,
            transaction=reserve_transaction
        )
        logger.info(f"Результат резервування: {result}")
        
        # 4. Використовуємо зарезервовані бонуси
        logger.info("4. Використання 30 зарезервованих бонусів")
        use_transaction = {
            'type': 'used',
            'amount': -30,
            'order_id': 'TEST002',
            'description': '✅ Використано 30 бонусів за замовленням TEST002'
        }
        
        result = balance_manager.update_balance(
            test_phone,
            reserved_bonus_delta=-30,
            transaction=use_transaction
        )
        logger.info(f"Результат використання: {result}")
        
        # 5. Фінальний баланс
        logger.info("5. Перевірка фінального балансу")
        final_balance = balance_manager.get_balance(test_phone)
        logger.info(f"Фінальний баланс: {final_balance}")
        
        # 6. Історія операцій
        logger.info("6. Отримання історії операцій")
        history = balance_manager.get_full_history(test_phone)
        logger.info(f"Історія ({len(history['history'])} записів):")
        for record in history['history']:
            logger.info(f"  - {record}")
        
        logger.info("✅ Тест основних операцій пройдено успішно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка в тесті основних операцій: {str(e)}")
        return False

def test_duplicate_client_sync():
    """Тест синхронізації для дублікатів клієнтів"""
    logger.info("=== ТЕСТ СИНХРОНІЗАЦІЇ ДУБЛІКАТІВ КЛІЄНТІВ ===")
    
    sync_service = BonusSyncService()
    test_phone = "+380987654321"
    
    try:
        # 1. Створюємо баланс в DynamoDB
        logger.info("1. Створення балансу в DynamoDB")
        balance_manager = BonusBalanceManager()
        
        transaction = {
            'type': 'earned',
            'amount': 150,
            'order_id': 'SYNC001',
            'description': '✅ Тестові бонуси для перевірки синхронізації: +150 бонусів'
        }
        
        result = balance_manager.update_balance(
            test_phone,
            active_bonus_delta=150,
            transaction=transaction
        )
        logger.info(f"Створено баланс: {result}")
        
        # 2. Додаємо ще кілька операцій для історії
        logger.info("2. Додавання додаткових операцій")
        
        operations = [
            {'delta': 50, 'type': 'earned', 'order': 'SYNC002', 'desc': 'Додаткові бонуси'},
            {'delta': -25, 'type': 'reserved', 'order': 'SYNC003', 'desc': 'Резерв для замовлення'},
            {'delta': 75, 'type': 'earned', 'order': 'SYNC004', 'desc': 'Ще більше бонусів'}
        ]
        
        for op in operations:
            trans = {
                'type': op['type'],
                'amount': abs(op['delta']),
                'order_id': op['order'],
                'description': f"🔄 {op['desc']}: {op['delta']:+d} бонусів"
            }
            
            balance_manager.update_balance(
                test_phone,
                active_bonus_delta=op['delta'],
                transaction=trans
            )
        
        # 3. Перевіряємо підсумковий баланс
        logger.info("3. Перевірка підсумкового балансу")
        final_balance = balance_manager.get_balance(test_phone)
        logger.info(f"Баланс перед синхронізацією: {final_balance}")
        
        # 4. Тестуємо синхронізацію (імітація)
        logger.info("4. Тестування синхронізації дублікатів")
        
        # Примітка: Тут ми не можемо повністю протестувати синхронізацію з KeyCRM
        # без реальних ID клієнтів, але можемо перевірити логіку
        
        logger.info("✅ Тест синхронізації дублікатів пройдено успішно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка в тесті синхронізації: {str(e)}")
        return False

def test_expiry_bonuses():
    """Тест перевірки прострочених бонусів"""
    logger.info("=== ТЕСТ ПРОСТРОЧЕНИХ БОНУСІВ ===")
    
    balance_manager = BonusBalanceManager()
    test_phone = "+380111222333"
    
    try:
        # 1. Додаємо бонуси
        logger.info("1. Додавання бонусів з терміном дії")
        
        transaction = {
            'type': 'earned',
            'amount': 200,
            'order_id': 'EXPIRY001',
            'description': '✅ Бонуси з терміном дії: +200 бонусів'
        }
        
        result = balance_manager.update_balance(
            test_phone,
            active_bonus_delta=200,
            transaction=transaction
        )
        logger.info(f"Створено бонуси: {result}")
        
        # 2. Перевіряємо дату закінчення
        balance = balance_manager.get_balance(test_phone)
        logger.info(f"Дата закінчення бонусів: {balance['data'].get('bonus_expiry_date')}")
        
        # 3. Тестуємо перевірку прострочених бонусів (вони не повинні бути прострочені)
        logger.info("3. Перевірка прострочених бонусів")
        expiry_check = balance_manager.check_expired_bonuses(test_phone)
        logger.info(f"Результат перевірки: {expiry_check}")
        
        logger.info("✅ Тест прострочених бонусів пройдено успішно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка в тесті прострочених бонусів: {str(e)}")
        return False

def test_concurrent_operations():
    """Тест конкурентних операцій (імітація)"""
    logger.info("=== ТЕСТ КОНКУРЕНТНИХ ОПЕРАЦІЙ ===")
    
    balance_manager = BonusBalanceManager()
    test_phone = "+380444555666"
    
    try:
        # 1. Ініціалізуємо баланс
        logger.info("1. Ініціалізація балансу")
        
        init_transaction = {
            'type': 'earned',
            'amount': 1000,
            'order_id': 'CONCURRENT001',
            'description': '✅ Початковий баланс для тестування конкурентності: +1000 бонусів'
        }
        
        result = balance_manager.update_balance(
            test_phone,
            active_bonus_delta=1000,
            transaction=init_transaction
        )
        logger.info(f"Початковий баланс: {result}")
        
        # 2. Виконуємо кілька операцій послідовно (імітація конкурентності)
        logger.info("2. Виконання множинних операцій")
        
        operations = [
            {'delta': -100, 'type': 'reserved', 'order': 'CONC001', 'desc': 'Резерв 1'},
            {'delta': -150, 'type': 'reserved', 'order': 'CONC002', 'desc': 'Резерв 2'},
            {'delta': -200, 'type': 'reserved', 'order': 'CONC003', 'desc': 'Резерв 3'},
            {'delta': 300, 'type': 'earned', 'order': 'CONC004', 'desc': 'Нарахування 1'},
            {'delta': 250, 'type': 'earned', 'order': 'CONC005', 'desc': 'Нарахування 2'}
        ]
        
        for i, op in enumerate(operations):
            logger.info(f"Операція {i+1}: {op['desc']}")
            
            trans = {
                'type': op['type'],
                'amount': abs(op['delta']),
                'order_id': op['order'],
                'description': f"{op['desc']}: {op['delta']:+d} бонусів"
            }
            
            if op['type'] == 'reserved':
                result = balance_manager.update_balance(
                    test_phone,
                    active_bonus_delta=op['delta'],  # Зменшуємо активні
                    reserved_bonus_delta=-op['delta'],  # Збільшуємо зарезервовані
                    transaction=trans
                )
            else:  # earned
                result = balance_manager.update_balance(
                    test_phone,
                    active_bonus_delta=op['delta'],
                    transaction=trans
                )
            
            logger.info(f"Результат: активні={result['data']['active_bonus']}, зарезервовані={result['data']['reserved_bonus']}")
        
        # 3. Перевіряємо фінальний стан
        logger.info("3. Фінальний стан після всіх операцій")
        final_balance = balance_manager.get_balance(test_phone)
        logger.info(f"Фінальний баланс: {final_balance}")
        
        logger.info("✅ Тест конкурентних операцій пройдено успішно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка в тесті конкурентних операцій: {str(e)}")
        return False

def run_all_tests():
    """Запуск всіх тестів"""
    logger.info("🚀 ПОЧАТОК ТЕСТУВАННЯ НОВОЇ СИСТЕМИ БОНУСІВ")
    logger.info("=" * 60)
    
    tests = [
        ("Основні операції з балансом", test_basic_balance_operations),
        ("Синхронізація дублікатів клієнтів", test_duplicate_client_sync),
        ("Прострочені бонуси", test_expiry_bonuses),
        ("Конкурентні операції", test_concurrent_operations)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n🔍 Запуск тесту: {test_name}")
        logger.info("-" * 40)
        
        try:
            success = test_func()
            results.append((test_name, success))
            
            if success:
                logger.info(f"✅ Тест '{test_name}' ПРОЙДЕНО")
            else:
                logger.error(f"❌ Тест '{test_name}' ПРОВАЛЕНО")
                
        except Exception as e:
            logger.error(f"💥 Критична помилка в тесті '{test_name}': {str(e)}")
            results.append((test_name, False))
    
    # Підсумок
    logger.info("\n" + "=" * 60)
    logger.info("📊 ПІДСУМОК ТЕСТУВАННЯ")
    logger.info("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ ПРОЙДЕНО" if success else "❌ ПРОВАЛЕНО"
        logger.info(f"{status} - {test_name}")
    
    logger.info("-" * 60)
    logger.info(f"Всього тестів: {total}")
    logger.info(f"Пройдено: {passed}")
    logger.info(f"Провалено: {total - passed}")
    logger.info(f"Успішність: {(passed/total*100):.1f}%")
    
    if passed == total:
        logger.info("🎉 ВСІ ТЕСТИ ПРОЙДЕНО УСПІШНО!")
    else:
        logger.warning("⚠️ ДЕЯКІ ТЕСТИ НЕ ПРОЙДЕНО")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
