"""
Головний сервіс бонусної системи.
Вся бізнес-логіка операцій з бонусами.
"""
import logging
import re
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import or_

from app.models.client import Client
from app.models.transaction import BonusTransaction, TransactionType
from app.services.keycrm_client import KeyCRMClient
from app.utils.phone import normalize_phone
from app.config import settings

logger = logging.getLogger(__name__)


class BonusService:
    """
    Сервіс для операцій з бонусами.
    
    Вся логіка працює з локальною PostgreSQL БД як єдиним джерелом правди.
    KeyCRM використовується тільки для синхронізації (відображення).
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.keycrm = KeyCRMClient()
    
    # ==================== Робота з клієнтами ====================
    
    def get_client_by_phone(self, phone: str) -> Optional[Client]:
        """Знаходить клієнта за нормалізованим телефоном"""
        normalized = normalize_phone(phone)
        if not normalized:
            return None
        return self.db.query(Client).filter(Client.phone == normalized).first()
    
    def get_client_by_keycrm_id(self, keycrm_id: int) -> Optional[Client]:
        """Знаходить клієнта за KeyCRM buyer_id"""
        return self.db.query(Client).filter(
            Client.keycrm_ids.contains([keycrm_id])
        ).first()
    
    def get_or_create_client(
        self, 
        phone: str, 
        keycrm_id: Optional[int] = None,
        name: Optional[str] = None,
        email: Optional[str] = None
    ) -> Tuple[Client, bool]:
        """
        Знаходить або створює клієнта.
        
        Пошук йде по телефону. Якщо клієнт знайдений - оновлюємо його дані
        (додаємо новий keycrm_id, email, name якщо їх ще немає).
        
        Returns:
            Tuple[Client, bool]: (клієнт, чи був створений новий)
        """
        normalized_phone = normalize_phone(phone)
        if not normalized_phone:
            raise ValueError(f"Невалідний телефон: {phone}")
        
        client = self.get_client_by_phone(normalized_phone)
        created = False
        
        if not client:
            # Створюємо нового клієнта
            client = Client(
                phone=normalized_phone,
                bonus_balance=0,
                reserved_balance=0,
                keycrm_ids=[keycrm_id] if keycrm_id else [],
                emails=[email.lower()] if email else [],
                names=[name] if name else []
            )
            self.db.add(client)
            self.db.commit()
            self.db.refresh(client)
            created = True
            logger.info(f"✅ Створено нового клієнта: {normalized_phone}")
        else:
            # Оновлюємо існуючого клієнта
            updated = False
            
            if keycrm_id and client.add_keycrm_id(keycrm_id):
                logger.info(f"➕ Додано KeyCRM ID {keycrm_id} до клієнта {normalized_phone}")
                updated = True
            
            if email and client.add_email(email):
                updated = True
            
            if name and client.add_name(name):
                updated = True
            
            if updated:
                self.db.commit()
                self.db.refresh(client)
        
        return client, created
    
    def get_balance(self, phone: str) -> Dict[str, Any]:
        """Отримує баланс клієнта за телефоном"""
        client = self.get_client_by_phone(phone)
        
        if not client:
            return {
                "success": True,
                "bonus_balance": 0,
                "reserved_balance": 0,
                "total_balance": 0,
                "phone": normalize_phone(phone),
                "found": False
            }
        
        return {
            "success": True,
            "bonus_balance": client.bonus_balance,
            "reserved_balance": client.reserved_balance,
            "total_balance": client.total_balance,
            "bonus_expiry": client.bonus_expiry.isoformat() if client.bonus_expiry else None,
            "phone": client.phone,
            "found": True
        }
    
    # ==================== Операції з бонусами ====================
    
    async def handle_order_completion(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обробка виконаного замовлення:
        1. Списуємо зарезервовані бонуси
        2. Нараховуємо нові бонуси (10% від суми)
        """
        context = webhook_data.get("context", {})
        
        order_id = context.get("id")
        client_id = context.get("client_id")
        order_total = float(context.get("grand_total") or 0)
        
        if not order_id or not client_id:
            return {"success": False, "error": "Відсутній order_id або client_id"}
        
        # Отримуємо телефон клієнта з KeyCRM
        phone = await self.keycrm.get_client_phone(client_id)
        if not phone:
            return {"success": False, "error": f"Не вдалося отримати телефон для client_id={client_id}"}
        
        # Знаходимо або створюємо клієнта в нашій БД
        client, _ = self.get_or_create_client(phone, keycrm_id=client_id)
        
        # Шукаємо скільки було зарезервовано для цього замовлення
        reserved_for_order = self._find_reserved_for_order(client.id, str(order_id))
        
        # Зберігаємо попередні значення
        balance_before = client.bonus_balance
        reserved_before = client.reserved_balance
        
        # Розраховуємо нові бонуси (10% від суми замовлення)
        new_bonus = int(order_total * settings.bonus_percentage)
        
        # Оновлюємо баланси
        client.reserved_balance = max(0, client.reserved_balance - reserved_for_order)
        client.bonus_balance += new_bonus
        client.bonus_expiry = datetime.utcnow().date() + timedelta(days=settings.bonus_expiry_days)
        
        # Записуємо транзакцію
        transaction = BonusTransaction(
            client_id=client.id,
            type=TransactionType.COMPLETED.value,
            order_id=str(order_id),
            amount=new_bonus,
            order_total=order_total,
            balance_before=balance_before,
            balance_after=client.bonus_balance,
            reserved_before=reserved_before,
            reserved_after=client.reserved_balance,
            keycrm_buyer_id=client_id,
            description=f"Виконано замовлення #{order_id}, списано резерв {reserved_for_order}, нараховано {new_bonus}"
        )
        self.db.add(transaction)
        self.db.commit()
        
        # Синхронізуємо з KeyCRM (всі дублікати)
        history = self._get_history_string(client.id, limit=50)
        await self.keycrm.sync_all_duplicates(
            client.keycrm_ids, 
            client.bonus_balance, 
            client.reserved_balance,
            history
        )
        
        logger.info(f"✅ Замовлення #{order_id} виконано: нараховано {new_bonus} бонусів, "
                   f"списано {reserved_for_order} з резерву")
        
        return {
            "success": True,
            "message": "Бонуси успішно оброблені для виконаного замовлення",
            "operation": "order_completed",
            "order_id": order_id,
            "previous_bonus": balance_before,
            "new_bonus": client.bonus_balance,
            "previous_reserved": reserved_before,
            "new_reserved": client.reserved_balance,
            "accrued_bonus": new_bonus,
            "used_bonus": reserved_for_order
        }
    
    async def handle_order_reservation(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Резервування бонусів при створенні замовлення з промокодом.
        Переводить бонуси з активних у зарезервовані.
        """
        event_type = webhook_data.get("event")
        if event_type not in ["order.create", "order.change_order_status"]:
            return {"success": True, "message": f"Подія {event_type} ігнорується"}
        
        context = webhook_data.get("context", {})
        
        order_id = context.get("id")
        client_id = context.get("client_id")
        discount_amount = float(context.get("discount_amount") or 0)
        promo_code = context.get("promocode", "")
        
        if not order_id or not client_id:
            return {"success": False, "error": "Відсутній order_id або client_id"}
        
        # Перевіряємо чи є промокод і знижка
        if not promo_code or not discount_amount:
            return {"success": True, "message": "Замовлення без бонусів"}
        
        # Отримуємо телефон клієнта
        phone = await self.keycrm.get_client_phone(client_id)
        if not phone:
            return {"success": False, "error": f"Не вдалося отримати телефон"}
        
        client, _ = self.get_or_create_client(phone, keycrm_id=client_id)
        
        # Розраховуємо суму для резервування
        bonus_to_reserve = min(int(discount_amount), client.bonus_balance)
        
        if bonus_to_reserve <= 0:
            return {"success": True, "message": "Немає доступних бонусів для резервування"}
        
        # Зберігаємо попередні значення
        balance_before = client.bonus_balance
        reserved_before = client.reserved_balance
        
        # Оновлюємо баланси
        client.bonus_balance -= bonus_to_reserve
        client.reserved_balance += bonus_to_reserve
        
        # Записуємо транзакцію
        transaction = BonusTransaction(
            client_id=client.id,
            type=TransactionType.RESERVED.value,
            order_id=str(order_id),
            amount=bonus_to_reserve,
            order_total=context.get("products_total"),
            balance_before=balance_before,
            balance_after=client.bonus_balance,
            reserved_before=reserved_before,
            reserved_after=client.reserved_balance,
            keycrm_buyer_id=client_id,
            description=f"Резерв для замовлення #{order_id}"
        )
        self.db.add(transaction)
        self.db.commit()
        
        # Синхронізуємо з KeyCRM
        history = self._get_history_string(client.id, limit=50)
        await self.keycrm.sync_all_duplicates(
            client.keycrm_ids,
            client.bonus_balance,
            client.reserved_balance,
            history
        )
        
        logger.info(f"🔒 Замовлення #{order_id}: зарезервовано {bonus_to_reserve} бонусів")
        
        return {
            "success": True,
            "message": "Бонуси успішно зарезервовані",
            "operation": "order_reserved",
            "order_id": order_id,
            "reserved_amount": bonus_to_reserve,
            "previous_bonus": balance_before,
            "new_bonus": client.bonus_balance,
            "new_reserved": client.reserved_balance
        }
    
    async def handle_order_cancellation(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Скасування замовлення - повертаємо зарезервовані бонуси.
        """
        # Підтримуємо як прямий запит, так і webhook
        order_id = webhook_data.get("order_id") or webhook_data.get("context", {}).get("id")
        client_id = webhook_data.get("client_id") or webhook_data.get("context", {}).get("client_id")
        
        if not order_id or not client_id:
            return {"success": False, "error": "Відсутній order_id або client_id"}
        
        # Отримуємо телефон клієнта
        phone = await self.keycrm.get_client_phone(client_id)
        if not phone:
            return {"success": False, "error": f"Не вдалося отримати телефон"}
        
        client = self.get_client_by_phone(phone)
        if not client:
            return {"success": False, "error": "Клієнта не знайдено"}
        
        # Шукаємо скільки було зарезервовано для цього замовлення
        reserved_for_order = self._find_reserved_for_order(client.id, str(order_id))
        
        if reserved_for_order <= 0:
            return {"success": True, "message": "Немає зарезервованих бонусів для повернення"}
        
        # Зберігаємо попередні значення
        balance_before = client.bonus_balance
        reserved_before = client.reserved_balance
        
        # Повертаємо бонуси
        return_amount = min(reserved_for_order, client.reserved_balance)
        client.bonus_balance += return_amount
        client.reserved_balance -= return_amount
        
        # Записуємо транзакцію
        transaction = BonusTransaction(
            client_id=client.id,
            type=TransactionType.CANCELLED.value,
            order_id=str(order_id),
            amount=-return_amount,  # Від'ємне значення для повернення
            balance_before=balance_before,
            balance_after=client.bonus_balance,
            reserved_before=reserved_before,
            reserved_after=client.reserved_balance,
            keycrm_buyer_id=client_id,
            description=f"Скасовано замовлення #{order_id}, повернуто {return_amount}"
        )
        self.db.add(transaction)
        self.db.commit()
        
        # Синхронізуємо з KeyCRM
        history = self._get_history_string(client.id, limit=50)
        await self.keycrm.sync_all_duplicates(
            client.keycrm_ids,
            client.bonus_balance,
            client.reserved_balance,
            history
        )
        
        logger.info(f"❌ Замовлення #{order_id} скасовано: повернуто {return_amount} бонусів")
        
        return {
            "success": True,
            "message": "Бонуси успішно повернуті",
            "operation": "order_cancelled",
            "order_id": order_id,
            "returned_bonus": return_amount,
            "previous_bonus": balance_before,
            "new_bonus": client.bonus_balance,
            "new_reserved": client.reserved_balance
        }
    
    async def handle_lead_bonus_reservation(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Мануальне резервування бонусів через ліди.
        """
        event_type = webhook_data.get("event")
        if event_type != "lead.change_lead_status":
            return {"success": True, "message": f"Подія {event_type} ігнорується"}
        
        context = webhook_data.get("context", {})
        
        lead_id = context.get("id")
        contact_id = context.get("contact_id")
        target_id = context.get("target_id")
        target_type = context.get("target_type")
        
        if not lead_id or not contact_id:
            return {"success": False, "error": "Відсутній lead_id або contact_id"}
        
        # Отримуємо номер замовлення та суму резерву з кастомних полів ліда
        order_id = None
        reserve_amount = 0
        
        custom_fields = context.get("custom_fields", [])
        for field in custom_fields:
            if field.get("uuid") == "LD_1022":  # Номер замовлення
                order_id = field.get("value")
            elif field.get("uuid") == "LD_1035":  # Сума резерву
                try:
                    reserve_amount = float(field.get("value") or 0)
                except (ValueError, TypeError):
                    pass
        
        # Якщо номер замовлення не в кастомних полях - беремо з target_id
        if not order_id and target_type == "order" and target_id:
            order_id = str(target_id)
        
        if not order_id:
            return {"success": False, "error": "Не вдалося визначити номер замовлення"}
        
        if reserve_amount <= 0:
            return {"success": False, "error": "Не вказана сума резерву"}
        
        # Отримуємо телефон клієнта
        phone = await self.keycrm.get_client_phone(contact_id)
        if not phone:
            return {"success": False, "error": f"Не вдалося отримати телефон"}
        
        client, _ = self.get_or_create_client(phone, keycrm_id=contact_id)
        
        # Перевіряємо чи вже є резервування для цього замовлення
        existing_reserve = self._find_reserved_for_order(client.id, str(order_id))
        if existing_reserve > 0:
            return {
                "success": True,
                "message": f"Для замовлення {order_id} вже є резерв {existing_reserve}",
                "operation": "already_reserved"
            }
        
        # Розраховуємо суму резервування
        bonus_to_reserve = min(int(reserve_amount), client.bonus_balance)
        
        if bonus_to_reserve <= 0:
            return {"success": True, "message": "Немає доступних бонусів"}
        
        # Зберігаємо попередні значення
        balance_before = client.bonus_balance
        reserved_before = client.reserved_balance
        
        # Оновлюємо баланси
        client.bonus_balance -= bonus_to_reserve
        client.reserved_balance += bonus_to_reserve
        
        # Записуємо транзакцію
        transaction = BonusTransaction(
            client_id=client.id,
            type=TransactionType.MANUAL_RESERVE.value,
            order_id=str(order_id),
            lead_id=str(lead_id),
            amount=bonus_to_reserve,
            balance_before=balance_before,
            balance_after=client.bonus_balance,
            reserved_before=reserved_before,
            reserved_after=client.reserved_balance,
            keycrm_buyer_id=contact_id,
            description=f"Ручний резерв через лід #{lead_id} для замовлення #{order_id}"
        )
        self.db.add(transaction)
        self.db.commit()
        
        # Синхронізуємо з KeyCRM
        history = self._get_history_string(client.id, limit=50)
        await self.keycrm.sync_all_duplicates(
            client.keycrm_ids,
            client.bonus_balance,
            client.reserved_balance,
            history
        )
        
        logger.info(f"🔐 Мануальний резерв: лід #{lead_id}, замовлення #{order_id}, "
                   f"зарезервовано {bonus_to_reserve}")
        
        return {
            "success": True,
            "message": "Бонуси успішно зарезервовані через лід",
            "operation": "manual_reserve",
            "lead_id": lead_id,
            "order_id": order_id,
            "reserved_amount": bonus_to_reserve,
            "previous_bonus": balance_before,
            "new_bonus": client.bonus_balance,
            "new_reserved": client.reserved_balance
        }
    
    # ==================== Допоміжні методи ====================
    
    def _find_reserved_for_order(self, client_id: int, order_id: str) -> int:
        """
        Знаходить загальну суму резерву для конкретного замовлення.
        Сумує всі операції резервування для цього замовлення.
        """
        transactions = self.db.query(BonusTransaction).filter(
            BonusTransaction.client_id == client_id,
            BonusTransaction.order_id == order_id,
            BonusTransaction.type.in_([
                TransactionType.RESERVED.value,
                TransactionType.MANUAL_RESERVE.value
            ])
        ).all()
        
        total_reserved = sum(t.amount for t in transactions)
        return total_reserved
    
    def _get_history_string(self, client_id: int, limit: int = 50) -> str:
        """
        Формує рядок історії бонусів для відображення в KeyCRM.
        """
        transactions = self.db.query(BonusTransaction).filter(
            BonusTransaction.client_id == client_id
        ).order_by(BonusTransaction.created_at.desc()).limit(limit).all()
        
        history_lines = [t.to_history_string() for t in transactions]
        return "\n\n".join(history_lines)
    
    def get_transaction_history(
        self, 
        phone: str, 
        limit: int = 50
    ) -> Dict[str, Any]:
        """Отримує історію транзакцій клієнта"""
        client = self.get_client_by_phone(phone)
        
        if not client:
            return {"success": False, "error": "Клієнта не знайдено"}
        
        transactions = self.db.query(BonusTransaction).filter(
            BonusTransaction.client_id == client.id
        ).order_by(BonusTransaction.created_at.desc()).limit(limit).all()
        
        return {
            "success": True,
            "phone": client.phone,
            "total_transactions": len(transactions),
            "transactions": transactions
        }

