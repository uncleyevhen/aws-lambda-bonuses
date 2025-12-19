"""
Утиліти для роботи з телефонними номерами.
Нормалізація до єдиного формату 380XXXXXXXXX - ключова для унікальності клієнтів.
"""
import re
from typing import Optional
import phonenumbers


def normalize_phone(phone: str) -> Optional[str]:
    """
    Нормалізує телефонний номер до формату 380XXXXXXXXX.
    
    Приклади:
        +380501234567 -> 380501234567
        380501234567 -> 380501234567
        0501234567 -> 380501234567
        050-123-45-67 -> 380501234567
        +38 (050) 123-45-67 -> 380501234567
        
    Returns:
        Нормалізований номер або None якщо неможливо розпарсити
    """
    if not phone:
        return None
    
    # Видаляємо всі символи крім цифр
    clean_phone = re.sub(r'[^\d]', '', str(phone))
    
    if not clean_phone:
        return None
    
    # Нормалізуємо до українського формату
    if clean_phone.startswith('0') and len(clean_phone) == 10:
        # 0501234567 -> 380501234567
        clean_phone = '38' + clean_phone
    elif clean_phone.startswith('380') and len(clean_phone) == 12:
        # Вже в правильному форматі
        pass
    elif clean_phone.startswith('80') and len(clean_phone) == 11:
        # 80501234567 -> 380501234567
        clean_phone = '3' + clean_phone
    elif len(clean_phone) == 9:
        # 501234567 -> 380501234567
        clean_phone = '380' + clean_phone
    
    # Валідація довжини
    if len(clean_phone) != 12 or not clean_phone.startswith('380'):
        return None
    
    return clean_phone


def is_valid_phone(phone: str) -> bool:
    """
    Перевіряє чи телефон валідний український номер.
    """
    normalized = normalize_phone(phone)
    if not normalized:
        return False
    
    # Перевірка через phonenumbers для додаткової валідації
    try:
        parsed = phonenumbers.parse(f"+{normalized}", None)
        return phonenumbers.is_valid_number(parsed)
    except Exception:
        # Якщо phonenumbers не може розпарсити - довіряємо базовій валідації
        return len(normalized) == 12 and normalized.startswith('380')


def format_phone_display(phone: str) -> str:
    """
    Форматує телефон для відображення: +38 (050) 123-45-67
    """
    normalized = normalize_phone(phone)
    if not normalized or len(normalized) != 12:
        return phone
    
    return f"+{normalized[:2]} ({normalized[2:5]}) {normalized[5:8]}-{normalized[8:10]}-{normalized[10:12]}"

