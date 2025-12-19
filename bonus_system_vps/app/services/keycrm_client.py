"""
Клієнт для роботи з KeyCRM API.
Використовується для синхронізації бонусів у всі дублікати клієнта.
"""
import httpx
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from app.config import settings

logger = logging.getLogger(__name__)


class KeyCRMClient:
    """
    Клієнт для KeyCRM API.
    
    Основна функція - синхронізація бонусів з нашої БД до KeyCRM.
    Коли оновлюємо бонуси клієнта, оновлюємо ВСІ його записи в KeyCRM.
    """
    
    def __init__(self):
        self.api_token = settings.keycrm_api_token
        self.base_url = settings.keycrm_base_url
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.timeout = 30.0
    
    async def make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Виконує HTTP запит до KeyCRM API"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"KeyCRM API: {method} {url}")
                
                if method == "GET":
                    response = await client.get(url, headers=self.headers)
                elif method == "PUT":
                    response = await client.put(url, headers=self.headers, json=data)
                elif method == "POST":
                    response = await client.post(url, headers=self.headers, json=data)
                else:
                    raise ValueError(f"Непідтримуваний метод: {method}")
                
                response.raise_for_status()
                return {"success": True, "data": response.json()}
                
        except httpx.HTTPStatusError as e:
            logger.error(f"KeyCRM API помилка HTTP: {e.response.status_code} - {e.response.text}")
            return {"success": False, "error": f"HTTP {e.response.status_code}: {e.response.text}"}
        except Exception as e:
            logger.error(f"KeyCRM API помилка: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def get_buyer(self, buyer_id: int) -> Dict[str, Any]:
        """Отримує дані покупця за ID"""
        return await self.make_request("GET", f"/buyer/{buyer_id}?include=custom_fields")
    
    async def get_buyer_by_phone(self, phone: str) -> Dict[str, Any]:
        """Шукає покупця за телефоном"""
        import urllib.parse
        encoded_phone = urllib.parse.quote(phone)
        return await self.make_request("GET", f"/buyer?filter[buyer_phone]={encoded_phone}&include=custom_fields")
    
    async def update_buyer_bonus(
        self, 
        buyer_id: int, 
        bonus_balance: int, 
        reserved_balance: int,
        bonus_expiry: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Оновлює баланс бонусів покупця в KeyCRM.
        
        Args:
            buyer_id: ID покупця в KeyCRM
            bonus_balance: Активні бонуси
            reserved_balance: Зарезервовані бонуси
            bonus_expiry: Дата закінчення бонусів
        """
        # Якщо дата не передана - розраховуємо
        if bonus_expiry is None:
            bonus_expiry = datetime.utcnow() + timedelta(days=settings.bonus_expiry_days)
        
        expiry_str = bonus_expiry.strftime('%Y-%m-%d')
        
        update_data = {
            "custom_fields": [
                {
                    "uuid": settings.bonus_field_uuid,
                    "value": str(bonus_balance)
                },
                {
                    "uuid": settings.reserved_bonus_field_uuid,
                    "value": str(reserved_balance)
                },
                {
                    "uuid": settings.bonus_expiry_field_uuid,
                    "value": expiry_str
                }
            ]
        }
        
        result = await self.make_request("PUT", f"/buyer/{buyer_id}", update_data)
        
        if result["success"]:
            logger.info(f"✅ KeyCRM: оновлено бонуси для buyer {buyer_id}: "
                       f"активні={bonus_balance}, резерв={reserved_balance}")
        
        return result
    
    async def update_bonus_history(self, buyer_id: int, history: str) -> Dict[str, Any]:
        """Оновлює історію бонусів покупця"""
        update_data = {
            "custom_fields": [
                {
                    "uuid": settings.history_field_uuid,
                    "value": history
                }
            ]
        }
        
        return await self.make_request("PUT", f"/buyer/{buyer_id}", update_data)
    
    async def sync_all_duplicates(
        self, 
        keycrm_ids: List[int], 
        bonus_balance: int, 
        reserved_balance: int,
        history: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Синхронізує бонуси у всі записи клієнта в KeyCRM.
        
        Це ключова функція - коли клієнт має кілька записів в CRM 
        (дублікати), ми оновлюємо бонуси у ВСІХ них.
        
        Args:
            keycrm_ids: Список всіх buyer_id клієнта в KeyCRM
            bonus_balance: Активні бонуси
            reserved_balance: Зарезервовані бонуси
            history: Історія бонусів для відображення
        """
        results = {
            "success": True,
            "updated": [],
            "failed": []
        }
        
        for buyer_id in keycrm_ids:
            try:
                # Оновлюємо баланс
                result = await self.update_buyer_bonus(buyer_id, bonus_balance, reserved_balance)
                
                if result["success"]:
                    # Якщо є історія - оновлюємо її теж
                    if history:
                        await self.update_bonus_history(buyer_id, history)
                    results["updated"].append(buyer_id)
                else:
                    results["failed"].append({"id": buyer_id, "error": result.get("error")})
                    
            except Exception as e:
                logger.error(f"Помилка синхронізації buyer {buyer_id}: {e}")
                results["failed"].append({"id": buyer_id, "error": str(e)})
        
        if results["failed"]:
            results["success"] = len(results["failed"]) < len(keycrm_ids)
            logger.warning(f"⚠️ KeyCRM sync: {len(results['updated'])} успішно, "
                          f"{len(results['failed'])} помилок")
        else:
            logger.info(f"✅ KeyCRM sync: всі {len(results['updated'])} записів оновлено")
        
        return results
    
    async def get_client_phone(self, buyer_id: int) -> Optional[str]:
        """Отримує телефон клієнта з KeyCRM"""
        result = await self.get_buyer(buyer_id)
        
        if result["success"] and result.get("data"):
            buyer_data = result["data"]
            phones = buyer_data.get("phone", [])
            if phones:
                return phones[0] if isinstance(phones, list) else phones
        
        return None

