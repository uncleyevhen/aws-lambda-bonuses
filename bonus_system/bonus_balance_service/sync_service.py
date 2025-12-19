"""
Сервіс синхронізації даних між DynamoDB та KeyCRM
Дублює дані з основного сховища (DynamoDB) в KeyCRM для кожного клієнта
"""
import json
import logging
from typing import Dict, Any, List
from bonus_balance_manager import BonusBalanceManager

# Імпортуємо KeyCRM клієнт з поточного проекту
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'bonus_operations'))
from keycrm_client import KeyCRMClient

logger = logging.getLogger()

class BonusSyncService:
    """
    Сервіс синхронізації бонусних даних між DynamoDB (основне сховище) та KeyCRM (дублікати)
    
    Принципи роботи:
    1. DynamoDB - єдине джерело правди для балансів бонусів
    2. KeyCRM - отримує копію даних для кожного клієнта (навіть дублікатів)
    3. При кожній операції з бонусами спочатку оновлюється DynamoDB, потім KeyCRM
    4. При створенні/знаходженні клієнта в KeyCRM - копіюється вся історія з DynamoDB
    """
    
    def __init__(self):
        self.balance_manager = BonusBalanceManager()
        self.keycrm = KeyCRMClient()
    
    def sync_to_keycrm_client(self, phone_number: str, keycrm_client_id: str) -> Dict[str, Any]:
        """
        Синхронізація даних з DynamoDB в конкретного клієнта KeyCRM
        
        Args:
            phone_number: номер телефону клієнта (ключ в DynamoDB)
            keycrm_client_id: ID клієнта в KeyCRM
            
        Returns:
            {'success': True/False, 'synced_data': {...}, 'error': '...'}
        """
        try:
            logger.info(f"Початок синхронізації для телефону {phone_number} → KeyCRM клієнт {keycrm_client_id}")
            
            # Отримуємо актуальні дані з DynamoDB
            balance_result = self.balance_manager.get_balance(phone_number)
            if not balance_result['success']:
                return {
                    'success': False,
                    'error': f"Не вдалося отримати баланс з DynamoDB: {balance_result.get('error')}"
                }
            
            balance_data = balance_result['data']
            
            # Формуємо дані для синхронізації в KeyCRM
            sync_data = {
                'active_bonus': balance_data['active_bonus'],
                'reserved_bonus': balance_data['reserved_bonus'],
                'bonus_expiry_date': balance_data['bonus_expiry_date'],
                'bonus_history': self._format_history_for_keycrm(balance_data['transaction_history'])
            }
            
            # Оновлюємо дані клієнта в KeyCRM
            update_result = self.keycrm.update_buyer_bonus(
                keycrm_client_id,
                sync_data['active_bonus'],
                sync_data['reserved_bonus']
            )
            
            if not update_result['success']:
                return {
                    'success': False,
                    'error': f"Помилка оновлення бонусів в KeyCRM: {update_result.get('error')}"
                }
            
            # Оновлюємо історію в KeyCRM
            history_result = self._update_history_in_keycrm(keycrm_client_id, sync_data['bonus_history'])
            if not history_result['success']:
                logger.warning(f"Не вдалося оновити історію в KeyCRM: {history_result.get('error')}")
            
            # Оновлюємо дату закінчення бонусів в KeyCRM
            if sync_data['bonus_expiry_date']:
                expiry_result = self._update_expiry_date_in_keycrm(keycrm_client_id, sync_data['bonus_expiry_date'])
                if not expiry_result['success']:
                    logger.warning(f"Не вдалося оновити дату закінчення в KeyCRM: {expiry_result.get('error')}")
            
            logger.info(f"Успішна синхронізація для {phone_number}: {sync_data}")
            return {
                'success': True,
                'synced_data': sync_data
            }
            
        except Exception as e:
            logger.error(f"Помилка синхронізації для {phone_number} → {keycrm_client_id}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def sync_to_all_client_duplicates(self, phone_number: str) -> Dict[str, Any]:
        """
        Синхронізація даних з DynamoDB до всіх дублікатів клієнта в KeyCRM
        
        Знаходить всіх клієнтів з таким номером телефону в KeyCRM і синхронізує дані до кожного
        
        Args:
            phone_number: номер телефону клієнта
            
        Returns:
            {'success': True/False, 'synced_clients': [...], 'errors': [...]}
        """
        try:
            logger.info(f"Пошук всіх дублікатів клієнта з телефоном {phone_number}")
            
            # Знаходимо всіх клієнтів з таким номером в KeyCRM
            search_result = self.keycrm.find_buyer(phone_number)
            
            if not search_result['success']:
                return {
                    'success': False,
                    'error': f"Помилка пошуку клієнтів в KeyCRM: {search_result.get('error')}"
                }
            
            clients = search_result.get('data', [])
            if not isinstance(clients, list):
                clients = [clients] if clients else []
            
            logger.info(f"Знайдено {len(clients)} клієнтів для синхронізації")
            
            synced_clients = []
            errors = []
            
            for client in clients:
                client_id = client.get('id')
                if not client_id:
                    continue
                
                sync_result = self.sync_to_keycrm_client(phone_number, str(client_id))
                
                if sync_result['success']:
                    synced_clients.append({
                        'client_id': client_id,
                        'name': client.get('full_name', ''),
                        'email': client.get('email', [''])[0] if client.get('email') else '',
                        'synced_data': sync_result['synced_data']
                    })
                else:
                    errors.append({
                        'client_id': client_id,
                        'error': sync_result.get('error')
                    })
            
            success = len(synced_clients) > 0
            result = {
                'success': success,
                'synced_clients': synced_clients,
                'total_found': len(clients),
                'total_synced': len(synced_clients)
            }
            
            if errors:
                result['errors'] = errors
            
            logger.info(f"Синхронізація завершена: {len(synced_clients)} успішних, {len(errors)} помилок")
            return result
            
        except Exception as e:
            logger.error(f"Помилка синхронізації дублікатів для {phone_number}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def sync_new_client(self, phone_number: str, keycrm_client_id: str) -> Dict[str, Any]:
        """
        Синхронізація для нового клієнта в KeyCRM
        
        При створенні нового клієнта (або знаходженні існуючого) в KeyCRM,
        копіює всю історію та баланс з DynamoDB до цього клієнта
        
        Args:
            phone_number: номер телефону
            keycrm_client_id: ID нового клієнта в KeyCRM
            
        Returns:
            {'success': True/False, 'synced_data': {...}, 'is_new_balance': True/False}
        """
        try:
            # Перевіряємо чи існує запис в DynamoDB
            balance_result = self.balance_manager.get_balance(phone_number)
            if not balance_result['success']:
                return balance_result
            
            balance_data = balance_result['data']
            is_new_balance = balance_data['active_bonus'] == 0 and len(balance_data['transaction_history']) == 0
            
            if is_new_balance:
                logger.info(f"Новий клієнт {phone_number} без історії бонусів")
                return {
                    'success': True,
                    'synced_data': balance_data,
                    'is_new_balance': True
                }
            else:
                logger.info(f"Синхронізація існуючої історії для нового клієнта KeyCRM {keycrm_client_id}")
                sync_result = self.sync_to_keycrm_client(phone_number, keycrm_client_id)
                sync_result['is_new_balance'] = False
                return sync_result
                
        except Exception as e:
            logger.error(f"Помилка синхронізації нового клієнта {phone_number}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _format_history_for_keycrm(self, transaction_history: List[Dict]) -> str:
        """
        Форматування історії транзакцій для збереження в KeyCRM
        
        Args:
            transaction_history: список транзакцій з DynamoDB
            
        Returns:
            Відформатована строка історії для KeyCRM
        """
        if not transaction_history:
            return ""
        
        formatted_entries = []
        
        for transaction in transaction_history:
            timestamp = transaction.get('timestamp', '')
            description = transaction.get('description', '')
            
            # Форматуємо дату
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                date_str = dt.strftime('%d.%m.%Y %H:%M')
            except:
                date_str = timestamp[:16].replace('T', ' ') if timestamp else ''
            
            entry = f"[{date_str}] {description}"
            formatted_entries.append(entry)
        
        # Обмежуємо довжину історії для KeyCRM
        history_text = '\n'.join(formatted_entries)
        
        # KeyCRM має ліміт на довжину поля, обрізаємо якщо потрібно
        max_length = 4000
        if len(history_text) > max_length:
            history_text = history_text[:max_length] + '\n... (історія обрізана)'
        
        return history_text
    
    def _update_history_in_keycrm(self, client_id: str, history: str) -> Dict[str, Any]:
        """Оновлення історії бонусів в KeyCRM клієнта"""
        try:
            if not history:
                return {'success': True}
            
            return self.keycrm.update_buyer_bonus_history(client_id, history)
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _update_expiry_date_in_keycrm(self, client_id: str, expiry_date: str) -> Dict[str, Any]:
        """Оновлення дати закінчення бонусів в KeyCRM клієнта"""
        try:
            return self.keycrm.update_buyer_bonus_expiry(client_id, expiry_date)
        except Exception as e:
            return {'success': False, 'error': str(e)}


# Глобальний екземпляр сервісу синхронізації
bonus_sync_service = BonusSyncService()
