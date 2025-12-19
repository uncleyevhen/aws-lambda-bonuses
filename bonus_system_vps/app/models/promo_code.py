"""
Модель промокоду для бонусної системи.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from app.database import Base


class PromoCode(Base):
    """Модель промокоду в БД"""
    __tablename__ = "promo_codes"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    amount = Column(Integer, nullable=False, index=True)
    is_used = Column(Boolean, default=False, index=True)
    used_at = Column(DateTime(timezone=True), nullable=True)
    used_by_phone = Column(String(20), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    def __repr__(self):
        return f"<PromoCode(code={self.code}, amount={self.amount}, is_used={self.is_used})>"
