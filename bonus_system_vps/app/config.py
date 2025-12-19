"""
Конфігурація додатку через змінні середовища
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Налаштування додатку"""
    
    # Базові налаштування
    app_name: str = "Bonus System API"
    debug: bool = False
    api_prefix: str = "/api/v1"
    
    # База даних
    db_password: str = "password"  # Пароль для PostgreSQL (використовується в docker-compose)
    database_url: str = "postgresql://bonus:password@db:5432/bonus_db"  # Формується з db_password
    
    # KeyCRM API
    keycrm_api_token: str = ""
    keycrm_base_url: str = "https://openapi.keycrm.app/v1"
    
    # UUID кастомних полів KeyCRM (для синхронізації)
    bonus_field_uuid: str = "CT_1023"
    reserved_bonus_field_uuid: str = "CT_1034"
    history_field_uuid: str = "CT_1033"
    bonus_expiry_field_uuid: str = "CT_1024"
    
    # Налаштування бонусів
    bonus_percentage: float = 0.10  # 10%
    max_bonus_usage_percent: float = 0.50  # Максимум 50% від суми
    bonus_expiry_days: int = 90  # 3 місяці
    
    # Адмін-панель для Playwright
    admin_url: str = ""
    admin_username: str = ""
    admin_password: str = ""
    
    # Playwright налаштування
    playwright_headless: bool = True
    
    # Поповнення промокодів (як на AWS)
    batch_threshold: int = 20  # Поріг для запуску поповнення
    min_codes_threshold: int = 3  # Мінімум кодів для тригера
    target_codes_per_amount: int = 10  # Цільова кількість кодів на суму
    
    # Sentry (опціонально)
    sentry_dsn: str = ""
    
    # CORS
    cors_origins: list = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "allow"  # Дозволяємо додаткові поля з .env


@lru_cache()
def get_settings() -> Settings:
    """Повертає закешовані налаштування"""
    settings = Settings()
    # Формуємо database_url з db_password якщо не вказано явно
    if settings.database_url == "postgresql://bonus:password@db:5432/bonus_db":
        settings.database_url = f"postgresql://bonus:{settings.db_password}@db:5432/bonus_db"
    return settings


settings = get_settings()

