"""
Pydantic схеми для вебхуків KeyCRM
"""
from pydantic import BaseModel, Field
from typing import Optional, Any, Dict, List


class OrderContext(BaseModel):
    """Контекст замовлення з вебхуку KeyCRM"""
    id: int = Field(..., description="ID замовлення")
    client_id: Optional[int] = None
    grand_total: Optional[float] = None
    products_total: Optional[float] = None
    discount_amount: Optional[float] = 0
    promocode: Optional[str] = None
    
    # Контактні дані клієнта
    buyer_comment: Optional[str] = None
    
    class Config:
        extra = "allow"  # Дозволяємо додаткові поля


class LeadContext(BaseModel):
    """Контекст ліда з вебхуку KeyCRM"""
    id: int = Field(..., description="ID ліда")
    contact_id: Optional[int] = None
    pipeline_id: Optional[int] = None
    target_id: Optional[int] = None
    target_type: Optional[str] = None
    status_id: Optional[int] = None
    custom_fields: Optional[List[Dict[str, Any]]] = []
    
    class Config:
        extra = "allow"


class KeyCRMWebhook(BaseModel):
    """
    Загальна схема вебхуку від KeyCRM.
    Підтримує різні типи подій: order.create, order.complete, lead.change_status тощо.
    """
    event: Optional[str] = Field(None, description="Тип події")
    context: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Контекст події")
    
    # Прямі поля для запитів скасування
    order_id: Optional[int] = None
    client_id: Optional[int] = None
    used_bonus_amount: Optional[float] = None
    
    class Config:
        extra = "allow"
    
    def get_order_context(self) -> Optional[OrderContext]:
        """Парсить контекст як OrderContext"""
        if self.context:
            try:
                return OrderContext(**self.context)
            except Exception:
                return None
        return None
    
    def get_lead_context(self) -> Optional[LeadContext]:
        """Парсить контекст як LeadContext"""
        if self.context:
            try:
                return LeadContext(**self.context)
            except Exception:
                return None
        return None


class WebhookResponse(BaseModel):
    """Стандартна відповідь на вебхук"""
    success: bool
    message: str
    operation: Optional[str] = None
    order_id: Optional[int] = None
    buyer_id: Optional[int] = None
    
    # Деталі бонусів
    previous_bonus: Optional[int] = None
    new_bonus: Optional[int] = None
    previous_reserved: Optional[int] = None
    new_reserved: Optional[int] = None
    used_bonus: Optional[float] = None
    accrued_bonus: Optional[float] = None
    reserved_amount: Optional[float] = None
    returned_bonus: Optional[float] = None
    
    class Config:
        extra = "allow"

