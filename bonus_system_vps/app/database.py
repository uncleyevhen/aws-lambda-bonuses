"""
Налаштування підключення до PostgreSQL
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import settings

# Створюємо engine
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,  # Перевірка з'єднання перед використанням
    pool_size=10,
    max_overflow=20
)

# Сесія
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Базовий клас для моделей
Base = declarative_base()


def get_db():
    """
    Dependency для отримання сесії БД.
    Використовується в роутерах: db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

