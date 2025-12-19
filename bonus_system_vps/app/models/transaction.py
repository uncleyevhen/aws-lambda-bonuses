"""
Модель транзакції бонусів.
Зберігає повну історію всіх операцій з бонусами.
"""
from sqlalchemy import Column, Integer, String, Numeric, DateTime, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
import enum


class TransactionType(str, enum.Enum):
    """Типи бонусних транзакцій"""
    COMPLETED = "completed"       # Замовлення виконано - нараховано бонуси, списано резерв
    CANCELLED = "cancelled"       # Замовлення скасовано - повернуто резерв
    RESERVED = "reserved"         # Автоматичне резервування при створенні замовлення
    MANUAL_RESERVE = "manual_reserve"  # Ручне резервування через лід
    INITIAL = "initial"           # Початкове нарахування (міграція)
    ADJUSTMENT = "adjustment"     # Ручне коригування адміністратором
    EXPIRED = "expired"           # Бонуси згоріли


class BonusTransaction(Base):
    """
    Транзакція бонусів.
    
    Кожна операція з бонусами записується сюди для повної історії
    та можливості аудиту.
    """
    __tablename__ = "bonus_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Зв'язок з клієнтом
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False, index=True)
    client = relationship("Client", back_populates="transactions")
    
    # Тип операції
    type = Column(String(20), nullable=False, index=True)
    
    # Деталі замовлення
    order_id = Column(String(50), nullable=True, index=True)
    lead_id = Column(String(50), nullable=True)
    
    # Суми
    amount = Column(Integer, nullable=False)  # Сума операції (може бути від'ємною)
    order_total = Column(Numeric(10, 2), nullable=True)  # Сума замовлення
    
    # Баланс до і після операції (для аудиту)
    balance_before = Column(Integer, nullable=False)
    balance_after = Column(Integer, nullable=False)
    reserved_before = Column(Integer, default=0)
    reserved_after = Column(Integer, default=0)
    
    # Який саме дублікат в KeyCRM ініціював операцію
    keycrm_buyer_id = Column(Integer, nullable=True)
    
    # Додаткові дані
    description = Column(String(500), nullable=True)
    
    # Мітка часу
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    
    def __repr__(self):
        return f"<Transaction(type={self.type}, amount={self.amount}, order={self.order_id})>"
    
    @property
    def emoji(self) -> str:
        """Емодзі для типу операції"""
        emojis = {
            TransactionType.COMPLETED.value: "✅",
            TransactionType.CANCELLED.value: "❌",
            TransactionType.RESERVED.value: "🔒",
            TransactionType.MANUAL_RESERVE.value: "🔐",
            TransactionType.INITIAL.value: "🎁",
            TransactionType.ADJUSTMENT.value: "✏️",
            TransactionType.EXPIRED.value: "⏰",
        }
        return emojis.get(self.type, "📝")
    
    def to_history_string(self) -> str:
        """
        Форматує транзакцію для відображення в історії KeyCRM.
        Зберігає сумісність з поточним форматом.
        """
        from datetime import datetime
        
        date_str = self.created_at.strftime('%d.%m.%y %H:%M') if self.created_at else datetime.now().strftime('%d.%m.%y %H:%M')
        order_str = f"#{self.order_id}" if self.order_id else ""
        order_total_str = f"{int(self.order_total)}₴" if self.order_total else ""
        
        # Формуємо опис операції
        if self.type == TransactionType.COMPLETED.value:
            operation = f"нараховано {self.amount}" if self.amount > 0 else ""
        elif self.type == TransactionType.CANCELLED.value:
            operation = f"повернуто {abs(self.amount)}"
        elif self.type in [TransactionType.RESERVED.value, TransactionType.MANUAL_RESERVE.value]:
            prefix = "ручний " if self.type == TransactionType.MANUAL_RESERVE.value else ""
            operation = f"{prefix}резерв {abs(self.amount)}"
        elif self.type == TransactionType.INITIAL.value:
            operation = f"початкові бонуси {self.amount}"
        else:
            operation = self.description or "операція"
        
        balance_change = f"{self.balance_before}→{self.balance_after}"
        
        # Формат: emoji дата | #замовлення | сума | операція | було→стало
        parts = [self.emoji, date_str]
        if order_str:
            parts.append(f"| {order_str}")
        if order_total_str:
            parts.append(f"| {order_total_str}")
        parts.append(f"| {operation}")
        parts.append(f"| {balance_change}")
        
        return " ".join(parts)

