#!/usr/bin/env python3
"""
Скрипт для пошуку клієнтів з великими бонусами
Використовує мультипроцесорний підхід для швидкого пошуку
"""

import json
from keycrm_bonus_calculator import main, load_config

def setup_search_config(min_bonus_amount=3000):
    """Налаштовує конфігурацію для пошуку клієнтів з великими бонусами"""
    
    # Завантажуємо поточну конфігурацію
    config = load_config()
    
    # Налаштовуємо режим пошуку
    config['search_mode'] = {
        'enabled': True,
        'only_search': True,  # Тільки пошук, без нарахування
        'min_bonus_amount': min_bonus_amount
    }
    
    # Зберігаємо оновлену конфігурацію
    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Конфігурацію оновлено для пошуку клієнтів з бонусами ≥ {min_bonus_amount}")
    return config

def restore_normal_config():
    """Відновлює звичайну конфігурацію після пошуку"""
    
    config = load_config()
    
    # Вимикаємо режим пошуку
    config['search_mode'] = {
        'enabled': False,
        'only_search': False,
        'min_bonus_amount': 3000
    }
    
    # Зберігаємо оновлену конфігурацію
    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    
    print("✅ Конфігурацію відновлено до звичайного режиму")

if __name__ == "__main__":
    print("🔍 ПОШУК КЛІЄНТІВ З ВЕЛИКИМИ БОНУСАМИ")
    print("=" * 50)
    
    try:
        # Запитуємо мінімальну кількість бонусів
        min_bonus = input("Введіть мінімальну кількість бонусів для пошуку (за замовчуванням 3000): ").strip()
        
        if min_bonus:
            try:
                min_bonus_amount = int(min_bonus)
            except ValueError:
                print("❌ Невірне число, використовую 3000")
                min_bonus_amount = 3000
        else:
            min_bonus_amount = 3000
        
        print(f"🔍 Шукаємо клієнтів з бонусами ≥ {min_bonus_amount}")
        print()
        
        # Налаштовуємо конфігурацію для пошуку
        setup_search_config(min_bonus_amount)
        
        # Запускаємо пошук
        main()
        
    except KeyboardInterrupt:
        print("\n❌ Пошук перервано користувачем")
    
    except Exception as e:
        print(f"\n❌ Помилка: {e}")
    
    finally:
        # Відновлюємо звичайну конфігурацію
        restore_normal_config()
        print("\n🔄 Конфігурацію відновлено")
