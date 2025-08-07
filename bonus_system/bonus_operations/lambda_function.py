"""
Основна Lambda функція для обробки операцій з бонусами
Рефакторинг для кращої читабельності та модульності
"""
import json
import logging
from typing import Dict, Any

from bonus_operations import BonusOperations
from utils import create_response, parse_event

# Налаштування логування
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Основна функція Lambda для обробки вебхуків від KeyCRM
    """
    try:
        logger.info(f"Отримано запит: {json.dumps(event)}")
        
        # Парсинг запиту
        method, path, body = parse_event(event)
        
        # Ініціалізація бізнес-логіки
        bonus_ops = BonusOperations()
        
        # Маршрутизація запитів
        if path.endswith('/order-complete') and method == 'POST':
            return bonus_ops.handle_order_completion(body)
        
        elif path.endswith('/order-cancel') and method == 'POST':
            return bonus_ops.handle_order_cancellation(body)
        
        elif path.endswith('/order-reserve') and method == 'POST':
            return bonus_ops.handle_order_reservation(body)
        
        elif path.endswith('/lead-reserve') and method == 'POST':
            return bonus_ops.handle_lead_bonus_reservation(body)
        
        elif path.endswith('/test-log') and method == 'POST':
            # Тестовий роут для логування будь-яких вебхуків
            logger.info(f"TEST LOG - Webhook body: {json.dumps(body, ensure_ascii=False, indent=2)}")
            return create_response(200, {
                'message': 'Webhook logged successfully',
                'success': True,
                'body_received': body
            })
        
        else:
            return create_response(405, {
                'error': 'Метод не підтримується.',
                'success': False
            })
            
    except Exception as e:
        logger.error(f"Помилка обробки запиту: {str(e)}")
        return create_response(500, {
            'error': f'Внутрішня помилка сервера: {str(e)}',
            'success': False
        })
