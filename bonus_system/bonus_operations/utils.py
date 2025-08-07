"""
Допоміжні функції
"""
import json
import logging
from typing import Dict, Any


def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Створення HTTP відповіді
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        },
        'body': json.dumps(body, ensure_ascii=False)
    }


def normalize_phone(phone: str) -> str:
    """
    Нормалізація номера телефону до міжнародного формату
    """
    if not phone:
        return ""
    
    # Видаляємо всі не-цифрові символи
    clean_phone = ''.join(filter(str.isdigit, phone))
    
    # Перетворюємо на міжнародний формат
    if clean_phone.startswith('380') and len(clean_phone) == 12:
        return clean_phone
    elif clean_phone.startswith('80') and len(clean_phone) == 11:
        return '3' + clean_phone
    elif clean_phone.startswith('0') and len(clean_phone) == 10:
        return '38' + clean_phone
    elif len(clean_phone) == 9:
        return '380' + clean_phone
    
    return clean_phone


def parse_event(event: Dict[str, Any]) -> tuple[str, str, Dict[str, Any]]:
    """
    Парсинг Lambda event для отримання методу, шляху та тіла запиту
    
    Returns:
        tuple: (method, path, body)
    """
    # Визначаємо тип запиту для різних форматів event
    if 'requestContext' in event and 'http' in event['requestContext']:
        # Формат event версії 2.0 (HTTP API)
        method = event['requestContext']['http']['method']
        path = event['rawPath']
    else:
        # Формат event версії 1.0 (REST API)
        method = event.get('httpMethod', 'POST')
        path = event.get('path', '/')
    
    # Парсинг запиту
    if 'body' in event and event['body']:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
    else:
        body = event
    
    return method, path, body
