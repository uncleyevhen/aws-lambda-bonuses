"""
Утилітарні функції для роботи з BON промокодами
"""
import re


def is_bon_promo_code(code):
    """
    Перевіряє чи є код BON промокодом
    BON коди мають формат: BONxxxx+ де xxxx - це число за яким можуть бути літери
    """
    if not code:
        return False
    
    # Перевіряємо чи починається з BON
    if not code.upper().startswith('BON'):
        return False
    
    # Паттерн для BON кодів: BON + число + будь-які символи
    pattern = r'^BON\d+.*$'
    return bool(re.match(pattern, code.upper()))


def extract_amount_from_bon_code(code):
    """
    Витягує суму з BON промокода
    Повертає суму як число або None якщо не вдалося витягти
    """
    if not is_bon_promo_code(code):
        return None
    
    # Витягуємо число після BON
    match = re.search(r'BON(\d+)', code.upper())
    if match:
        return int(match.group(1))
    
    return None


def format_bon_code(amount):
    """
    Форматує суму в BON код
    """
    return f"BON{amount}"


def validate_bon_amount(amount):
    """
    Валідує суму для BON кода
    """
    if not isinstance(amount, (int, float)):
        return False
    
    return amount > 0
