"""
Роутер для міграції даних з KeyCRM.
Дозволяє імпортувати клієнтів через API.
"""
import logging
from datetime import datetime, date
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models.client import Client
from app.models.transaction import BonusTransaction, TransactionType

logger = logging.getLogger(__name__)
router = APIRouter()


# ==================== Pydantic моделі ====================

class ClientImport(BaseModel):
    """Дані клієнта для імпорту"""
    phone: str
    bonus_balance: int = 0
    reserved_balance: int = 0
    bonus_expiry: Optional[str] = None  # YYYY-MM-DD
    keycrm_ids: List[int] = []
    emails: List[str] = []
    names: List[str] = []


class BulkImportRequest(BaseModel):
    """Запит на масовий імпорт клієнтів"""
    clients: List[ClientImport]
    overwrite: bool = False  # Якщо True - перезаписує існуючі дані


class ImportStats(BaseModel):
    """Статистика імпорту"""
    total: int
    inserted: int
    updated: int
    skipped: int
    errors: int


# ==================== Ендпоінти ====================

@router.post("/clients/import", response_model=ImportStats)
async def import_clients(
    request: BulkImportRequest,
    db: Session = Depends(get_db)
):
    """
    Масовий імпорт клієнтів з KeyCRM.
    
    Приймає список клієнтів та зберігає їх в БД.
    Якщо клієнт вже існує (за телефоном):
    - overwrite=False: пропускає
    - overwrite=True: оновлює якщо нові бонуси більші
    """
    logger.info(f"📥 Початок імпорту {len(request.clients)} клієнтів")
    
    stats = {
        "total": len(request.clients),
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0
    }
    
    for client_data in request.clients:
        try:
            # Парсимо дату закінчення бонусів
            bonus_expiry = None
            if client_data.bonus_expiry:
                try:
                    bonus_expiry = datetime.strptime(client_data.bonus_expiry[:10], "%Y-%m-%d").date()
                except ValueError:
                    pass
            
            # Перевіряємо чи клієнт існує
            existing = db.query(Client).filter(Client.phone == client_data.phone).first()
            
            if existing:
                if not request.overwrite:
                    stats["skipped"] += 1
                    continue
                
                # Оновлюємо якщо нові бонуси більші
                updated = False
                if client_data.bonus_balance > existing.bonus_balance:
                    existing.bonus_balance = client_data.bonus_balance
                    updated = True
                if client_data.reserved_balance > existing.reserved_balance:
                    existing.reserved_balance = client_data.reserved_balance
                    updated = True
                
                # Оновлюємо keycrm_ids
                for kid in client_data.keycrm_ids:
                    if existing.add_keycrm_id(kid):
                        updated = True
                
                # Оновлюємо emails
                for email in client_data.emails:
                    if existing.add_email(email):
                        updated = True
                
                # Оновлюємо names
                for name in client_data.names:
                    if existing.add_name(name):
                        updated = True
                
                # Оновлюємо дату закінчення
                if bonus_expiry and (not existing.bonus_expiry or bonus_expiry > existing.bonus_expiry):
                    existing.bonus_expiry = bonus_expiry
                    updated = True
                
                if updated:
                    stats["updated"] += 1
                else:
                    stats["skipped"] += 1
            else:
                # Створюємо нового клієнта
                new_client = Client(
                    phone=client_data.phone,
                    bonus_balance=client_data.bonus_balance,
                    reserved_balance=client_data.reserved_balance,
                    bonus_expiry=bonus_expiry,
                    keycrm_ids=client_data.keycrm_ids,
                    emails=client_data.emails,
                    names=client_data.names
                )
                db.add(new_client)
                
                # Створюємо початкову транзакцію
                if client_data.bonus_balance > 0 or client_data.reserved_balance > 0:
                    db.flush()  # Отримуємо ID нового клієнта
                    
                    transaction = BonusTransaction(
                        client_id=new_client.id,
                        type=TransactionType.INITIAL.value,
                        amount=client_data.bonus_balance,
                        balance_before=0,
                        balance_after=client_data.bonus_balance,
                        reserved_before=0,
                        reserved_after=client_data.reserved_balance,
                        description=f"Міграція з KeyCRM"
                    )
                    db.add(transaction)
                
                stats["inserted"] += 1
                
        except Exception as e:
            logger.error(f"❌ Помилка імпорту клієнта {client_data.phone}: {e}")
            stats["errors"] += 1
    
    try:
        db.commit()
        logger.info(f"✅ Імпорт завершено: inserted={stats['inserted']}, updated={stats['updated']}, skipped={stats['skipped']}, errors={stats['errors']}")
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Помилка збереження: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    return ImportStats(**stats)


@router.get("/clients/stats")
async def get_clients_stats(db: Session = Depends(get_db)):
    """
    Статистика клієнтів в БД.
    """
    total_clients = db.query(func.count(Client.id)).scalar()
    clients_with_bonus = db.query(func.count(Client.id)).filter(Client.bonus_balance > 0).scalar()
    clients_with_reserved = db.query(func.count(Client.id)).filter(Client.reserved_balance > 0).scalar()
    total_bonus = db.query(func.sum(Client.bonus_balance)).scalar() or 0
    total_reserved = db.query(func.sum(Client.reserved_balance)).scalar() or 0
    
    return {
        "total_clients": total_clients,
        "clients_with_bonus": clients_with_bonus,
        "clients_with_reserved": clients_with_reserved,
        "total_bonus_amount": total_bonus,
        "total_reserved_amount": total_reserved
    }


@router.delete("/clients/clear")
async def clear_all_clients(
    confirm: bool = False,
    db: Session = Depends(get_db)
):
    """
    Видаляє всіх клієнтів (для перезапуску міграції).
    
    ОБЕРЕЖНО: Видаляє всі дані!
    """
    if not confirm:
        raise HTTPException(status_code=400, detail="Для підтвердження передайте confirm=true")
    
    # Видаляємо транзакції
    deleted_transactions = db.query(BonusTransaction).delete()
    
    # Видаляємо клієнтів
    deleted_clients = db.query(Client).delete()
    
    db.commit()
    
    logger.warning(f"⚠️ Видалено {deleted_clients} клієнтів та {deleted_transactions} транзакцій")
    
    return {
        "success": True,
        "deleted_clients": deleted_clients,
        "deleted_transactions": deleted_transactions
    }
