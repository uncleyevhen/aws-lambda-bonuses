"""
Роутер для перевірки балансу бонусів.
Використовується GTM скриптами на сайті.
"""
import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.bonus_service import BonusService
from app.schemas.client import BalanceCheckResponse
from app.utils.phone import normalize_phone

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/check", response_model=BalanceCheckResponse)
async def check_balance(
    phone: str = Query(..., description="Номер телефону клієнта"),
    db: Session = Depends(get_db)
):
    """
    Перевірка балансу бонусів за номером телефону.
    
    Використовується GTM скриптами для відображення балансу на сайті.
    Замінює Lambda bonus_balance_checker.
    
    Приклади запитів:
    - GET /balance/check?phone=0501234567
    - GET /balance/check?phone=+380501234567
    - GET /balance/check?phone=380501234567
    """
    logger.info(f"📊 Запит балансу для телефону: {phone}")
    
    normalized = normalize_phone(phone)
    if not normalized:
        return BalanceCheckResponse(
            success=False,
            bonus_balance=0,
            phone="",
            original_phone=phone
        )
    
    service = BonusService(db)
    result = service.get_balance(normalized)
    
    return BalanceCheckResponse(
        success=True,
        bonus_balance=result["bonus_balance"],
        phone=result["phone"],
        original_phone=phone
    )


@router.get("/full")
async def get_full_balance(
    phone: str = Query(..., description="Номер телефону клієнта"),
    db: Session = Depends(get_db)
):
    """
    Повна інформація про баланс клієнта.
    
    Повертає активні бонуси, резерв, дату закінчення.
    """
    service = BonusService(db)
    result = service.get_balance(phone)
    
    return result


@router.get("/history")
async def get_history(
    phone: str = Query(..., description="Номер телефону клієнта"),
    limit: int = Query(50, description="Кількість записів"),
    db: Session = Depends(get_db)
):
    """
    Історія бонусних операцій клієнта.
    """
    service = BonusService(db)
    result = service.get_transaction_history(phone, limit)
    
    if not result["success"]:
        return result
    
    # Конвертуємо транзакції в dict для JSON
    transactions = [
        {
            "id": t.id,
            "type": t.type,
            "order_id": t.order_id,
            "amount": t.amount,
            "balance_before": t.balance_before,
            "balance_after": t.balance_after,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "description": t.description
        }
        for t in result["transactions"]
    ]
    
    return {
        "success": True,
        "phone": result["phone"],
        "total_transactions": result["total_transactions"],
        "transactions": transactions
    }

