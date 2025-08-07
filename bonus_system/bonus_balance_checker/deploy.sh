#!/bin/bash

# Скрипт для деплою Bonus Balance Checker Lambda функції

echo "🚀 Починаємо деплой Bonus Balance Checker Lambda..."

# Створюємо zip архів
zip -r bonus-balance-checker.zip lambda_function.py

echo "📦 Архів створено: bonus-balance-checker.zip"

# Спочатку видаляємо стару функцію якщо існує
echo "🗑️ Видаляємо стару функцію якщо існує..."
aws lambda delete-function --function-name bonus-balance-checker-prod --region eu-north-1 2>/dev/null || echo "Стара функція не знайдена"

# Створюємо Lambda функцію
echo "⚙️ Створюємо нову Lambda функцію..."
aws lambda create-function \
    --function-name bonus-balance-checker-prod \
    --runtime python3.13 \
    --role arn:aws:iam::881490108668:role/lambda-promo-role \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://bonus-balance-checker.zip \
    --description "Перевірка балансу бонусів клієнта через KeyCRM API" \
    --timeout 30 \
    --region eu-north-1 \
    --environment Variables='{}' echo "🌐 Створюємо Function URL..."
aws lambda create-function-url-config \
    --function-name bonus-balance-checker-prod \
    --auth-type NONE \
    --cors '{"AllowCredentials":false,"AllowHeaders":["*"],"AllowMethods":["GET","POST","OPTIONS"],"AllowOrigins":["*"]}' \
    --region eu-north-1

echo "📡 Отримуємо Function URL..."
FUNCTION_URL=$(aws lambda get-function-url-config \
    --function-name bonus-balance-checker-prod \
    --region eu-north-1 \
    --query 'FunctionUrl' \
    --output text)

echo "✅ Деплой завершено!"
echo ""
echo "📋 Function URL: $FUNCTION_URL"
echo "📋 Наступні кроки:"
echo "1. Замініть KEYCRM_PROXY_URL в GTM скрипті на: $FUNCTION_URL"
echo "2. Протестуйте роботу функції: curl \"$FUNCTION_URL?phone=380991234567\""
