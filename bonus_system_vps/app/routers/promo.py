"""
Роутер для роботи з промокодами.
Видача та управління бонусними промокодами.

Портовано з AWS Lambda: bonus_system/bonus_get_promo_code/lambda_function.py
Логіка тригерів поповнення ідентична AWS.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db, SessionLocal
from app.models.promo_code import PromoCode
from app.models.used_codes_count import UsedCodesCount

logger = logging.getLogger(__name__)
router = APIRouter()

# ==================== Конфігурація (як на AWS) ====================
BATCH_THRESHOLD = 20      # Поріг для запуску поповнення (сумарно використаних)
MIN_CODES_THRESHOLD = 3   # Мінімум кодів для запуску поповнення
TARGET_CODES_PER_AMOUNT = 10  # Цільова кількість кодів на суму


# ==================== Pydantic моделі ====================

class PromoRequest(BaseModel):
    """Запит на отримання промокоду"""
    phone: Optional[str] = None
    amount: int


class PromoResponse(BaseModel):
    """Відповідь з промокодом"""
    success: bool
    promo_code: Optional[str] = None
    amount: Optional[int] = None
    error: Optional[str] = None


class PromoAddRequest(BaseModel):
    """Запит на додавання промокодів"""
    amount: int
    codes: List[str]


# ==================== Допоміжні функції (аналог AWS) ====================

def increment_used_codes_count(db: Session, amount: int) -> bool:
    """
    Збільшує лічильник використаних кодів для суми.
    
    Аналог add_used_code_count з AWS Lambda.
    
    Returns:
        bool: True якщо досягнуто BATCH_THRESHOLD
    """
    try:
        record = db.query(UsedCodesCount).filter(UsedCodesCount.amount == amount).first()
        if not record:
            record = UsedCodesCount(amount=amount, count=1)
            db.add(record)
        else:
            record.count += 1
        
        db.commit()
        
        # Перевіряємо загальну кількість використаних
        total = db.query(func.sum(UsedCodesCount.count)).scalar() or 0
        logger.info(f"📝 Використано промокодів: {total} (поріг: {BATCH_THRESHOLD})")
        
        return total >= BATCH_THRESHOLD
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Помилка при оновленні лічильника: {e}")
        return False


def get_all_used_codes_count(db: Session) -> Dict[str, int]:
    """
    Отримує всі лічильники використаних кодів.
    
    Аналог get_used_codes_count з AWS Lambda.
    """
    records = db.query(UsedCodesCount).all()
    return {str(r.amount): r.count for r in records}


def _run_replenish_subprocess(amounts_json: str):
    """
    Запускає поповнення як окремий Python процес.
    Playwright sync API вимагає запуску з main thread, тому використовуємо subprocess.
    """
    import subprocess
    import sys
    
    script = f'''
import json
import sys
import os

# Встановлюємо змінну для коректного логування в subprocess
os.environ['REPLENISH_SUBPROCESS'] = 'true'

sys.path.insert(0, '/app')

from app.database import SessionLocal
from app.services.promo_replenish_service import PromoReplenishService

used_codes_count = json.loads('{amounts_json}')
print(f"🚀 [Subprocess] Запуск поповнення для сум: {{list(used_codes_count.keys())}}", flush=True)

db = SessionLocal()
service = None
try:
    service = PromoReplenishService(db)
    success = service.replenish_promo_codes(used_codes_count)
    
    if success:
        print("✅ [Subprocess] Поповнення завершено успішно", flush=True)
        sys.exit(0)
    else:
        print("❌ [Subprocess] Поповнення завершилось з помилкою", flush=True)
        sys.exit(1)
        
except Exception as e:
    print(f"❌ [Subprocess] Критична помилка: {{e}}", flush=True)
    sys.exit(1)
finally:
    if service:
        service.cleanup()
    db.close()
'''
    
    try:
        result = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True,
            text=True,
            timeout=600  # 10 хвилин максимум
        )
        
        # Логуємо stdout та stderr
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                logger.info(line)
        if result.stderr:
            for line in result.stderr.strip().split('\n'):
                logger.error(line)
                
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logger.error("❌ [Subprocess] Таймаут поповнення (10 хвилин)")
        return False
    except Exception as e:
        logger.error(f"❌ [Subprocess] Помилка запуску: {e}")
        return False


async def replenish_promo_codes_task(used_codes_count: Dict[str, int]):
    """
    Background task для поповнення промокодів.
    Запускає Playwright як окремий subprocess (бо sync API вимагає main thread).
    
    Аналог Lambda replenish-promo-code invoke.
    """
    import asyncio
    import json
    
    logger.info(f"🚀 Запуск фонового поповнення промокодів для сум: {list(used_codes_count.keys())}")
    
    try:
        # Серіалізуємо дані для subprocess
        amounts_json = json.dumps(used_codes_count)
        
        # Запускаємо subprocess в окремому потоці
        success = await asyncio.to_thread(_run_replenish_subprocess, amounts_json)
        
        if success:
            logger.info("✅ Фонове поповнення завершено успішно")
        else:
            logger.error("❌ Фонове поповнення завершилось з помилкою")
            
    except Exception as e:
        logger.error(f"❌ Критична помилка фонового поповнення: {e}")


# ==================== Ендпоінти ====================

@router.post("/get", response_model=PromoResponse)
async def get_promo_code(
    request: PromoRequest, 
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Отримання промокоду на певну суму.
    
    Логіка ідентична AWS Lambda bonus_get_promo_code:
    1. Бере перший доступний промокод з БД
    2. Збільшує лічильник використаних кодів
    3. Якщо remaining <= 3 OR total_used >= 20 → запускає поповнення
    """
    amount = request.amount
    phone = request.phone
    
    logger.info(f"🎟️ Запит промокоду на суму {amount} (телефон: {phone})")
    
    # 1. Знаходимо перший доступний промокод для цієї суми
    promo = db.query(PromoCode).filter(
        PromoCode.amount == amount,
        PromoCode.is_used == False
    ).first()
    
    if not promo:
        logger.warning(f"⚠️ Немає промокодів для суми {amount}")
        
        # Запускаємо поповнення негайно
        used_codes = get_all_used_codes_count(db)
        if str(amount) not in used_codes:
            used_codes[str(amount)] = 1
        background_tasks.add_task(replenish_promo_codes_task, used_codes)
        logger.info(f"🚀 Запущено негайне поповнення для суми {amount}")
        
        return PromoResponse(
            success=False,
            error=f"Промокоди для суми {amount} грн закінчилися"
        )
    
    # 2. Позначаємо як використаний
    promo.is_used = True
    promo.used_at = datetime.utcnow()
    promo.used_by_phone = phone
    
    # 3. Рахуємо скільки залишилось (аналог should_trigger_low_count)
    remaining = db.query(func.count(PromoCode.id)).filter(
        PromoCode.amount == amount,
        PromoCode.is_used == False
    ).scalar() or 0
    
    # 4. Збільшуємо лічильник (аналог add_used_code_count)
    should_trigger_batch = increment_used_codes_count(db, amount)
    
    # 5. Перевіряємо чи потрібно запускати поповнення
    should_trigger_low_count = remaining <= MIN_CODES_THRESHOLD
    
    try:
        db.commit()
        logger.info(f"✅ Видано промокод {promo.code} на суму {amount} (залишилось: {remaining})")
        
        # 6. Запускаємо поповнення якщо потрібно (аналог lambda.invoke)
        if should_trigger_batch or should_trigger_low_count:
            reason = []
            if should_trigger_batch:
                reason.append(f"досягнуто поріг батчу ({BATCH_THRESHOLD})")
            if should_trigger_low_count:
                reason.append(f"залишилось мало кодів ({remaining} <= {MIN_CODES_THRESHOLD})")
            
            logger.info(f"🚀 Запускаємо поповнення. Причини: {', '.join(reason)}")
            
            used_codes = get_all_used_codes_count(db)
            background_tasks.add_task(replenish_promo_codes_task, used_codes)
        
        return PromoResponse(
            success=True,
            promo_code=promo.code,
            amount=amount
        )
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Помилка при збереженні: {e}")
        return PromoResponse(
            success=False,
            error="Помилка при видачі промокоду"
        )


@router.post("/replenish")
async def trigger_replenish(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ручний запуск поповнення промокодів.
    
    Аналог прямого виклику Lambda replenish-promo-code.
    """
    used_codes = get_all_used_codes_count(db)
    
    if not used_codes:
        # Якщо немає лічильників - перевіряємо всі суми з низьким залишком
        low_amounts = db.query(PromoCode.amount).filter(
            PromoCode.is_used == False
        ).group_by(PromoCode.amount).having(
            func.count(PromoCode.id) < TARGET_CODES_PER_AMOUNT
        ).all()
        
        used_codes = {str(row.amount): 1 for row in low_amounts}
        
        if not used_codes:
            return {
                "success": True,
                "message": "Поповнення не потрібне - всі суми мають достатньо кодів"
            }
    
    background_tasks.add_task(replenish_promo_codes_task, used_codes)
    
    return {
        "success": True,
        "message": f"Поповнення запущено для сум: {list(used_codes.keys())}",
        "amounts": list(used_codes.keys())
    }


@router.post("/add")
async def add_promo_codes(request: PromoAddRequest, db: Session = Depends(get_db)):
    """
    Додавання промокодів до БД.
    
    Приймає суму та список кодів.
    """
    amount = request.amount
    codes = request.codes
    
    logger.info(f"📝 Додавання {len(codes)} промокодів для суми {amount}")
    
    added = 0
    duplicates = 0
    
    for code in codes:
        existing = db.query(PromoCode).filter(PromoCode.code == code).first()
        if existing:
            duplicates += 1
            continue
        
        promo = PromoCode(
            code=code,
            amount=amount,
            is_used=False
        )
        db.add(promo)
        added += 1
    
    try:
        db.commit()
        logger.info(f"✅ Додано {added} промокодів, пропущено дублікатів: {duplicates}")
        
        return {
            "success": True,
            "message": f"Додано {added} промокодів для суми {amount}",
            "added": added,
            "duplicates": duplicates
        }
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Помилка при додаванні: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_promo_status(db: Session = Depends(get_db)):
    """
    Статус промокодів - скільки доступно для кожної суми.
    
    Також показує лічильники використаних кодів.
    """
    # Отримуємо кількість доступних промокодів по сумах
    result = db.query(
        PromoCode.amount,
        func.count(PromoCode.id).label('count')
    ).filter(
        PromoCode.is_used == False
    ).group_by(
        PromoCode.amount
    ).all()
    
    status = {str(row.amount): row.count for row in result}
    
    # Загальна статистика
    total_available = sum(status.values()) if status else 0
    total_used = db.query(PromoCode).filter(PromoCode.is_used == True).count()
    
    # Лічильники використаних (для тригерів)
    used_counts = get_all_used_codes_count(db)
    total_used_counter = sum(used_counts.values()) if used_counts else 0
    
    # Суми які потребують поповнення
    low_amounts = [
        amount for amount, count in status.items() 
        if count <= MIN_CODES_THRESHOLD
    ]
    
    return {
        "success": True,
        "promo_codes": status,
        "total_available": total_available,
        "total_used": total_used,
        "used_codes_counter": used_counts,
        "total_used_counter": total_used_counter,
        "batch_threshold": BATCH_THRESHOLD,
        "min_codes_threshold": MIN_CODES_THRESHOLD,
        "low_amounts": low_amounts,
        "needs_replenish": total_used_counter >= BATCH_THRESHOLD or len(low_amounts) > 0
    }


@router.delete("/clear")
async def clear_promo_codes(amount: Optional[int] = None, db: Session = Depends(get_db)):
    """
    Видалення промокодів (для тестування).
    
    Якщо вказано amount - видаляє тільки для цієї суми.
    Якщо не вказано - видаляє всі.
    """
    query = db.query(PromoCode)
    
    if amount:
        query = query.filter(PromoCode.amount == amount)
    
    deleted = query.delete()
    db.commit()
    
    return {
        "success": True,
        "deleted": deleted
    }


@router.delete("/clear-counters")
async def clear_used_codes_counters(amount: Optional[int] = None, db: Session = Depends(get_db)):
    """
    Очищення лічильників використаних кодів.
    
    Якщо вказано amount - очищає тільки для цієї суми.
    Якщо не вказано - очищає всі.
    """
    query = db.query(UsedCodesCount)
    
    if amount:
        query = query.filter(UsedCodesCount.amount == amount)
    
    deleted = query.delete()
    db.commit()
    
    return {
        "success": True,
        "cleared_counters": deleted
    }
