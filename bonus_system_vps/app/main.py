"""
Головний файл FastAPI додатку бонусної системи.
"""
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import engine, Base
from app.routers import webhooks, balance, promo, migration

# Створюємо папку для логів
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Налаштування логування з ротацією на 7 днів
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        TimedRotatingFileHandler(
            filename=os.path.join(LOG_DIR, "bonus-api.log"),
            when="midnight",
            interval=1,
            backupCount=7,  # Зберігати логи за останні 7 днів
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle events - виконується при старті та зупинці додатку.
    """
    # Startup
    logger.info("🚀 Bonus System API запускається...")
    
    # Створюємо таблиці якщо їх немає
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Таблиці БД перевірено/створено")
    
    # Ініціалізація Sentry якщо налаштовано
    if settings.sentry_dsn:
        try:
            import sentry_sdk
            sentry_sdk.init(
                dsn=settings.sentry_dsn,
                traces_sample_rate=0.1,
            )
            logger.info("✅ Sentry ініціалізовано")
        except Exception as e:
            logger.warning(f"⚠️ Не вдалося ініціалізувати Sentry: {e}")
    
    logger.info("✅ Bonus System API готовий до роботи!")
    
    yield
    
    # Shutdown
    logger.info("👋 Bonus System API зупиняється...")


# Створення FastAPI додатку
app = FastAPI(
    title=settings.app_name,
    description="""
    ## Бонусна система SafeYourLove
    
    API для управління бонусами клієнтів.
    
    ### Основні можливості:
    - Обробка вебхуків від KeyCRM (нарахування, резервування, скасування)
    - Перевірка балансу бонусів
    - Видача промокодів
    
    ### Інтеграції:
    - KeyCRM - синхронізація даних клієнтів
    - Сайт SafeYourLove - відображення балансу, видача промокодів
    """,
    version="2.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Підключення роутерів
app.include_router(
    webhooks.router, 
    prefix="/webhooks", 
    tags=["KeyCRM Webhooks"]
)
app.include_router(
    balance.router, 
    prefix="/balance", 
    tags=["Balance"]
)
app.include_router(
    promo.router, 
    prefix="/promo", 
    tags=["Promo Codes"]
)
app.include_router(
    migration.router, 
    prefix="/migration", 
    tags=["Migration"]
)


@app.get("/")
async def root():
    """Головна сторінка API"""
    return {
        "name": settings.app_name,
        "version": "2.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    """Перевірка здоров'я сервісу"""
    return {
        "status": "healthy",
        "service": "bonus-system-api"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug
    )

