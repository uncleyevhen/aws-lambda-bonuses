"""
Валідація конфігурації середовища
"""
import os
import logging

logger = logging.getLogger(__name__)

def validate_environment():
    """
    Валідує всі необхідні змінні середовища
    """
    required_vars = {
        'ADMIN_URL': 'URL адмін-панелі',
        'ADMIN_USERNAME': 'Логін адміністратора',
        'ADMIN_PASSWORD': 'Пароль адміністратора'
    }
    
    optional_vars = {
        'TARGET_CODES_PER_AMOUNT': ('10', int),
        'LOG_LEVEL': ('INFO', str),
        'PLAYWRIGHT_BROWSERS_PATH': ('/opt/playwright-browsers', str),
        'SESSION_S3_BUCKET': ('lambda-promo-sessions', str),
        'PROMO_CODES_S3_KEY': ('promo-codes/available_codes.json', str),
        'USED_CODES_S3_KEY': ('promo-codes/used_codes_count.json', str)
    }
    
    errors = []
    warnings = []
    
    # Перевіряємо обов'язкові змінні
    for var_name, description in required_vars.items():
        value = os.getenv(var_name)
        if not value:
            errors.append(f"❌ Відсутня обов'язкова змінна {var_name} ({description})")
        else:
            logger.info(f"✅ {var_name}: встановлено")
    
    # Перевіряємо опціональні змінні та встановлюємо за замовчуванням
    config = {}
    for var_name, (default_value, var_type) in optional_vars.items():
        value = os.getenv(var_name, default_value)
        
        # Перевіряємо тип та валідність
        try:
            if var_type == int:
                typed_value = int(value)
                if typed_value <= 0:
                    warnings.append(f"⚠️ {var_name}={value} має бути додатнім числом, використовуємо {default_value}")
                    typed_value = int(default_value)
            else:
                typed_value = var_type(value)
            
            config[var_name] = typed_value
            if value == default_value:
                logger.info(f"ℹ️ {var_name}: використовуємо за замовчуванням ({default_value})")
            else:
                logger.info(f"✅ {var_name}: {value}")
                
        except (ValueError, TypeError) as e:
            warnings.append(f"⚠️ Некоректне значення {var_name}={value}, використовуємо {default_value}")
            config[var_name] = var_type(default_value)
    
    # Виводимо результат валідації
    if errors:
        for error in errors:
            logger.error(error)
        raise ValueError(f"Невалідна конфігурація: {len(errors)} помилок")
    
    if warnings:
        for warning in warnings:
            logger.warning(warning)
    
    logger.info(f"🎯 Валідація конфігурації пройшла успішно ({len(warnings)} попереджень)")
    return config

def get_validated_config():
    """
    Повертає валідовану конфігурацію
    """
    return validate_environment()
