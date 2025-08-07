# 🎯 Bonus Operations API Gateway

Спеціалізований API Gateway для Lambda функції `bonus-operations` з усіма її ендпоінтами.

## 📋 Ендпоінти

### 1. POST /order-complete
**Призначення**: Обробка завершення замовлення та нарахування бонусів  
**Джерело**: Webhook від KeyCRM при зміні статусу замовлення на "виконано"

```json
{
  "event": "order.change_order_status",
  "context": {
    "id": "12345",
    "client_id": "67890",
    "grand_total": 1000,
    "status_name": "completed",
    "discount_amount": 50
  }
}
```

**Логіка**:
- Списує використані бонуси з резерву
- Нараховує нові бонуси (10% від суми замовлення)
- Логує транзакцію в історію бонусів

### 2. POST /order-cancel  
**Призначення**: Скасування замовлення та повернення бонусів з резерву  
**Джерело**: Прямий API виклик або webhook при скасуванні замовлення

```json
{
  "order_id": "12345",
  "phone": "380123456789",
  "used_bonus_amount": 50
}
```

**Логіка**:
- Шукає в історії бонусів суму резерву для замовлення
- Повертає зарезервовані бонуси до активних
- НЕ нараховує нові бонуси

### 3. POST /order-reserve
**Призначення**: Резервування бонусів при створенні замовлення  
**Джерело**: Webhook від KeyCRM при створенні замовлення з промокодом

```json
{
  "event": "order.create",
  "context": {
    "id": "12345",
    "client_id": "67890",
    "grand_total": 1000,
    "status_name": "new",
    "discount_amount": 100,
    "promo_code": "BONUS100"
  }
}
```

**Логіка**:
- Перевіряє наявність промокоду та знижки
- Резервує бонуси з активного балансу клієнта
- Зменшує активні бонуси, збільшує зарезервовані

### 4. POST /lead-reserve
**Призначення**: Мануальне резервування бонусів через ліди  
**Джерело**: Webhook від KeyCRM при роботі з лідами

```json
{
  "event": "lead.change_status",
  "context": {
    "id": "54321",
    "status": "active",
    "custom_fields": [
      {
        "uuid": "LD_1035",
        "value": "100"
      }
    ]
  }
}
```

**Логіка**:
- Поки що в режимі логування для аналізу структури
- Планується автоматичне резервування бонусів через ліди

### 5. POST /test-log
**Призначення**: Тестовий ендпоінт для логування webhook'ів  
**Джерело**: Тестові запити, налагодження

```json
{
  "test": "Тестовий запит",
  "timestamp": "2025-07-30T12:00:00Z"
}
```

**Логіка**:
- Просто логує отримані дані
- Корисно для налагодження webhook'ів

## 🚀 Швидкий старт

### 1. Надання прав виконання
```bash
chmod +x setup_api_gateway.sh
```

### 2. Створення API Gateway
```bash
./setup_api_gateway.sh setup
```

### 3. Перевірка створених ендпоінтів
```bash
./setup_api_gateway.sh info
```

### 4. Тестування API
```bash
./setup_api_gateway.sh test
```

## 🔧 Налаштування KeyCRM Webhooks

Після створення API Gateway налаштуйте webhook'и в KeyCRM:

### Webhook для замовлень:
- **URL**: `https://[api-id].execute-api.eu-north-1.amazonaws.com/order-complete`
- **События**: `order.change_order_status`
- **Статуси**: completed, cancelled, delivered

### Webhook для резервування:
- **URL**: `https://[api-id].execute-api.eu-north-1.amazonaws.com/order-reserve`  
- **События**: `order.create`, `order.change_order_status`
- **Статуси**: new, pending

### Webhook для лідів:
- **URL**: `https://[api-id].execute-api.eu-north-1.amazonaws.com/lead-reserve`
- **События**: `lead.change_status`, `lead.update`

## 📊 Моніторинг та логи

### CloudWatch логи
```bash
# Перегляд логів Lambda функції
aws logs filter-log-events \
  --log-group-name /aws/lambda/bonus-operations \
  --start-time $(date -d '1 hour ago' +%s)000 \
  --region eu-north-1
```

### API Gateway метрики
```bash
# Статистика викликів API
aws apigatewayv2 get-apis --region eu-north-1 \
  --query 'Items[?Name==`bonus-operations-api`]'
```

## 🔍 Налагодження

### Перевірка статусу Lambda
```bash
aws lambda get-function --function-name bonus-operations --region eu-north-1
```

### Перевірка роутів API
```bash
# Отримайте API ID
API_ID=$(aws apigatewayv2 get-apis --region eu-north-1 \
  --query 'Items[?Name==`bonus-operations-api`].ApiId' --output text)

# Переглядніть роути
aws apigatewayv2 get-routes --api-id $API_ID --region eu-north-1
```

### Тестування окремих ендпоінтів
```bash
# Базовий URL API
BASE_URL="https://[api-id].execute-api.eu-north-1.amazonaws.com"

# Тест логування
curl -X POST "$BASE_URL/test-log" \
  -H "Content-Type: application/json" \
  -d '{"test": "connection check"}'

# Тест резервування (тестові дані)
curl -X POST "$BASE_URL/order-reserve" \
  -H "Content-Type: application/json" \
  -d '{
    "event": "order.create",
    "context": {
      "id": "test-123",
      "client_id": "test-client",
      "grand_total": 500,
      "status_name": "new",
      "discount_amount": 50,
      "promo_code": "TEST50"
    }
  }'
```

## ⚠️ Важливі зауваження

1. **CORS**: API налаштований для прийому запитів з будь-яких доменів
2. **Аутентифікація**: На данний момент відсутня (webhook'и від KeyCRM)
3. **Ліміти**: За замовчуванням AWS лімітує до 10,000 запитів/сек
4. **Таймаути**: Lambda функція має таймаут 30 секунд

## 🆘 Підтримка

При проблемах перевірте:
1. Статус Lambda функції в AWS консолі
2. CloudWatch логи для детальної інформації
3. Правильність payload'ів webhook'ів
4. Налаштування ролей та дозволів AWS

---

🎉 **API Gateway готовий для обробки всіх бонусних операцій!**
