"""
Конфігурація та константи для системи бонусів
"""
import os

# Основні константи
BONUS_PERCENTAGE = 0.10
MAX_BONUS_USAGE_PERCENT = 0.50  # Максимум 50% від суми замовлення

# KeyCRM API конфігурація
KEYCRM_API_TOKEN = os.environ.get('KEYCRM_API_TOKEN', 'M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ')
KEYCRM_BASE_URL = os.environ.get('KEYCRM_BASE_URL', 'https://openapi.keycrm.app/v1')

# UUID кастомних полів KeyCRM
BONUS_FIELD_UUID = "CT_1023"  # Поле "Бонусні бали" (активні)
RESERVED_BONUS_FIELD_UUID = "CT_1034"  # Поле "Зарезервовані бонуси"
HISTORY_FIELD_UUID = "CT_1033"  # Поле "Історія бонусів"
BONUS_EXPIRY_FIELD_UUID = "CT_1024"  # Поле "Дата закінчення бонусів"
LEAD_BONUS_FIELD_UUID = "LD_1035"  # Поле "Бонуси до використання" в ліді

# Емодзі для логування операцій
OPERATION_EMOJIS = {
    'completed': '✅',      # Виконано
    'cancelled': '❌',      # Скасовано
    'reserved': '🔒',       # Резерв
    'manual_use': '👤',     # Ручне використання
    'manual_reserve': '🔐'  # Ручний резерв через лід
}

# Ліміти
MAX_HISTORY_RECORDS = 50
MAX_HISTORY_LENGTH = 4000
BONUS_EXPIRY_DAYS = 90  # 3 місяці
