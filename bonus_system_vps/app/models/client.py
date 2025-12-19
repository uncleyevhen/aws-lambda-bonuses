"""
Модель клієнта з бонусами.
Телефон є унікальним ідентифікатором - це вирішує проблему дублікатів KeyCRM.
"""
from sqlalchemy import Column, Integer, String, Date, DateTime, ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class Client(Base):
    """
    Клієнт бонусної системи.
    
    Ключова ідея: один телефон = один клієнт, незалежно від кількості 
    дублікатів в KeyCRM. Всі keycrm_ids зберігаються в масиві.
    """
    __tablename__ = "clients"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Унікальний ідентифікатор - нормалізований телефон (380XXXXXXXXX)
    phone = Column(String(20), unique=True, nullable=False, index=True)
    
    # Баланси бонусів
    bonus_balance = Column(Integer, default=0, nullable=False)
    reserved_balance = Column(Integer, default=0, nullable=False)
    
    # Дата закінчення бонусів
    bonus_expiry = Column(Date, nullable=True)
    
    # Масиви для зберігання всіх відомих даних клієнта
    # Це дозволяє зберігати інформацію з усіх дублікатів KeyCRM
    emails = Column(ARRAY(String), default=list)
    names = Column(ARRAY(String), default=list)
    
    # Всі buyer_id з KeyCRM, які відповідають цьому клієнту
    # При синхронізації оновлюємо бонуси у ВСІХ цих записах
    keycrm_ids = Column(ARRAY(Integer), default=list)
    
    # Мітки часу
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Зв'язок з транзакціями
    transactions = relationship("BonusTransaction", back_populates="client")
    
    def __repr__(self):
        return f"<Client(phone={self.phone}, bonus={self.bonus_balance}, reserved={self.reserved_balance})>"
    
    def add_keycrm_id(self, keycrm_id: int) -> bool:
        """Додає новий KeyCRM ID якщо його ще немає"""
        if self.keycrm_ids is None:
            self.keycrm_ids = []
        if keycrm_id not in self.keycrm_ids:
            self.keycrm_ids = self.keycrm_ids + [keycrm_id]
            return True
        return False
    
    def add_email(self, email: str) -> bool:
        """Додає email якщо його ще немає"""
        if not email:
            return False
        if self.emails is None:
            self.emails = []
        email_lower = email.lower().strip()
        if email_lower not in [e.lower() for e in self.emails]:
            self.emails = self.emails + [email_lower]
            return True
        return False
    
    def add_name(self, name: str) -> bool:
        """Додає ім'я якщо його ще немає"""
        if not name:
            return False
        if self.names is None:
            self.names = []
        name_clean = name.strip()
        if name_clean not in self.names:
            self.names = self.names + [name_clean]
            return True
        return False
    
    @property
    def total_balance(self) -> int:
        """Загальний баланс (активні + зарезервовані)"""
        return self.bonus_balance + self.reserved_balance
    
    @property
    def primary_name(self) -> str:
        """Основне ім'я клієнта (перше в списку)"""
        if self.names:
            return self.names[0]
        return "Невідомий клієнт"
    
    @property
    def primary_email(self) -> str:
        """Основний email клієнта"""
        if self.emails:
            return self.emails[0]
        return ""

