"""
Роутер для обробки вебхуків від KeyCRM.
Основний вхідний пункт для всіх операцій з бонусами.
"""
import logging
from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.bonus_service import BonusService
from app.schemas.webhook import WebhookResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/order-complete", response_model=WebhookResponse)
async def order_complete(request: Request, db: Session = Depends(get_db)):
    """
    Вебхук виконання замовлення.
    
    Коли замовлення переходить у статус "Виконано":
    - Списуються зарезервовані бонуси
    - Нараховуються нові бонуси (10% від суми)
    """
    body = await request.json()
    logger.info(f"📦 Отримано order-complete: {body}")
    
    service = BonusService(db)
    result = await service.handle_order_completion(body)
    
    return WebhookResponse(**result)


@router.post("/order-reserve", response_model=WebhookResponse)
async def order_reserve(request: Request, db: Session = Depends(get_db)):
    """
    Вебхук створення замовлення з промокодом.
    
    Коли створюється замовлення з бонусним промокодом:
    - Бонуси переводяться з активних у зарезервовані
    """
    body = await request.json()
    logger.info(f"🔒 Отримано order-reserve: {body}")
    
    service = BonusService(db)
    result = await service.handle_order_reservation(body)
    
    return WebhookResponse(**result)


@router.post("/order-cancel", response_model=WebhookResponse)
async def order_cancel(request: Request, db: Session = Depends(get_db)):
    """
    Вебхук скасування замовлення.
    
    Коли замовлення скасовується:
    - Зарезервовані бонуси повертаються до активних
    """
    body = await request.json()
    logger.info(f"❌ Отримано order-cancel: {body}")
    
    service = BonusService(db)
    result = await service.handle_order_cancellation(body)
    
    return WebhookResponse(**result)


@router.post("/lead-reserve", response_model=WebhookResponse)
async def lead_reserve(request: Request, db: Session = Depends(get_db)):
    """
    Вебхук мануального резервування через ліди.
    
    Коли лід змінює статус (воронка "Використання бонусів"):
    - Резервуються бонуси для конкретного замовлення
    """
    body = await request.json()
    logger.info(f"🔐 Отримано lead-reserve: {body}")
    
    service = BonusService(db)
    result = await service.handle_lead_bonus_reservation(body)
    
    return WebhookResponse(**result)


@router.post("/test-log")
async def test_log(request: Request):
    """
    Тестовий ендпоінт для логування вебхуків.
    Використовується для дебагу нових інтеграцій.
    """
    body = await request.json()
    logger.info(f"🧪 TEST LOG - Webhook body: {body}")
    
    return {
        "success": True,
        "message": "Webhook logged successfully",
        "body_received": body
    }

