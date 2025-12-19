"""
Pydantic схеми для клієнтів
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime


class ClientBase(BaseModel):
    """Базова схема клієнта"""
    phone: str = Field(..., description="Номер телефону в форматі 380XXXXXXXXX")


class ClientCreate(ClientBase):
    """Схема для створення клієнта"""
    name: Optional[str] = None
    email: Optional[str] = None
    keycrm_id: Optional[int] = None
    initial_bonus: int = 0


class ClientBalance(BaseModel):
    """Схема балансу бонусів"""
    phone: str
    bonus_balance: int = Field(..., description="Активні бонуси")
    reserved_balance: int = Field(..., description="Зарезервовані бонуси")
    total_balance: int = Field(..., description="Загальний баланс")
    bonus_expiry: Optional[date] = None
    
    class Config:
        from_attributes = True


class ClientResponse(BaseModel):
    """Повна схема клієнта для відповіді"""
    id: int
    phone: str
    bonus_balance: int
    reserved_balance: int
    bonus_expiry: Optional[date] = None
    emails: List[str] = []
    names: List[str] = []
    keycrm_ids: List[int] = []
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class BalanceCheckRequest(BaseModel):
    """Запит на перевірку балансу"""
    phone: str = Field(..., description="Номер телефону")


class BalanceCheckResponse(BaseModel):
    """Відповідь на перевірку балансу"""
    success: bool = True
    bonus_balance: int
    phone: str
    original_phone: Optional[str] = None

