"""
Pydantic схеми для транзакцій
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class TransactionResponse(BaseModel):
    """Схема транзакції для відповіді"""
    id: int
    client_id: int
    type: str
    order_id: Optional[str] = None
    lead_id: Optional[str] = None
    amount: int
    order_total: Optional[float] = None
    balance_before: int
    balance_after: int
    reserved_before: int = 0
    reserved_after: int = 0
    keycrm_buyer_id: Optional[int] = None
    description: Optional[str] = None
    created_at: datetime
    
    class Config:
        from_attributes = True


class TransactionHistoryResponse(BaseModel):
    """Відповідь зі списком транзакцій"""
    phone: str
    total_transactions: int
    transactions: list[TransactionResponse]

