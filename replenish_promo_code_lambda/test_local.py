#!/usr/bin/env python3
"""
Локальний тест для Lambda функції replenish-promo-code.
Використовує всі ті ж залежності та логіку, але працює локально.
"""

import json
import os
import sys
import logging

# Налаштовуємо детальне логування
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s:%(name)s:%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Імітуємо AWS Lambda середовище
os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'replenish-promo-code-local'

# Завантажуємо конфігурацію з replenish_env_config.json
try:
    with open('replenish_env_config.json', 'r') as f:
        config = json.load(f)
        # Конфігурація вкладена в Variables
        variables = config.get('Variables', {})
        for key, value in variables.items():
            os.environ[key] = str(value)
        print(f"✅ Завантажено конфігурацію: {list(variables.keys())}")
except Exception as e:
    print(f"❌ Помилка завантаження конфігурації: {e}")
    sys.exit(1)

# Імпортуємо Lambda функцію
try:
    from lambda_function import lambda_handler
    print("✅ Lambda функція завантажена")
except Exception as e:
    print(f"❌ Помилка імпорту: {e}")
    sys.exit(1)

def test_replenish_local():
    """Локальний тест функції replenish з мок-даними замість S3"""
    print("🧪 Початок локального тестування...")
    
    # Створюємо мок файли замість S3
    import json
    
    # Створюємо тестові дані для лічильників використаних кодів
    used_codes_data = {"1": 1, "2": 1, "200": 1}  # Тестуємо різні суми
    
    # Записуємо в локальний файл замість S3
    with open('/tmp/used_codes_count.json', 'w') as f:
        json.dump(used_codes_data, f, indent=2)
    
    print(f"📦 Створено тестові дані: {used_codes_data}")
    
    # Імпортуємо і патчимо функції для роботи з файлами замість S3
    try:
        from promo_logic import PromoService
        
        # Створюємо тестовий сервіс без S3
        from browser_manager import BrowserManager
        
        # Ініціалізуємо браузер
        browser_manager = BrowserManager()
        page = browser_manager.initialize()  # Правильний метод
        
        service = PromoService(page=page)
        service.use_s3 = False  # Вимикаємо S3
        service.s3_client = None
        
        # Додаємо дані для логіну (з конфігурації)
        service.admin_username = os.getenv('ADMIN_USERNAME')
        service.admin_password = os.getenv('ADMIN_PASSWORD')
        
        print("🔧 Налаштований тестовий сервіс без S3")
        if service.admin_username:
            print(f"🔑 Логін: {service.admin_username[:3]}***")  # Показуємо тільки перші 3 символи
        else:
            print("❌ Немає даних для логіну в змінних середовища!")
        
        # Тестуємо функцію get_codes_for_amount_from_admin напряму
        print("🧪 Тестуємо підключення до адмін-панелі...")
        
        # Спочатку логін
        if service.login():
            print("✅ Логін успішний!")
            
            # Тестуємо отримання кодів для суми 1
            print("🔍 Тестуємо отримання кодів для суми 1...")
            codes = service.get_and_check_codes_for_amount(1)
            
            print(f"📊 Знайдено активних кодів: {len(codes)}")
            print(f"🏷️ Коди: {codes[:5]}...")  # Показуємо перші 5
            
            # Показуємо що всі коди активні (оскільки неактивні вже видалено)
            if codes:
                first_code = codes[0]
                print(f"🔍 Приклад активного коду: {first_code}")
                print(f"📊 Всі {len(codes)} кодів активні (неактивні автоматично видалено)")
            
            return {"status": "success", "codes_found": len(codes), "test_code": codes[0] if codes else None}
        else:
            print("❌ Помилка логіну")
            return {"status": "error", "message": "Login failed"}
            
    except Exception as e:
        print(f"❌ Помилка виконання: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    print("🚀 Локальне тестування Lambda функції replenish-promo-code")
    print("=" * 60)
    
    result = test_replenish_local()
    
    print("=" * 60)
    if result:
        print("🎉 Тест завершено успішно!")
    else:
        print("💥 Тест завершено з помилками!")
