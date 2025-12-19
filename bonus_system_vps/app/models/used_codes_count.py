"""
Модель лічильника використаних промокодів.

Аналог S3 файлу used_codes_count.json з AWS Lambda.
Зберігає кількість використаних промокодів для кожної суми,
використовується для визначення коли запускати поповнення.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, DateTime
from app.database import Base


class UsedCodesCount(Base):
    """
    Лічильник використаних промокодів по сумах.
    
    Аналог S3 файлу:
    {
        "100": 5,
        "200": 3,
        ...
    }
    
    Коли total >= BATCH_THRESHOLD (20) - тригериться поповнення.
    """
    __tablename__ = "used_codes_count"

    id = Column(Integer, primary_key=True, index=True)
    amount = Column(Integer, unique=True, nullable=False, index=True)
    count = Column(Integer, default=0, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<UsedCodesCount(amount={self.amount}, count={self.count})>"
