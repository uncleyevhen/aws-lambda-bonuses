#!/bin/bash

# Скрипт для деплою KeyCRM проксі Lambda функції

echo "🚀 Починаємо деплой KeyCRM Proxy Lambda..."

# Створюємо zip архів
cd keycrm_proxy_lambda
zip -r keycrm-proxy.zip lambda_function.py

echo "📦 Архів створено: keycrm-proxy.zip"

# Створюємо Lambda функцію
aws lambda create-function \
    --function-name keycrm-proxy \
    --runtime python3.9 \
    --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://keycrm-proxy.zip \
    --description "Проксі для KeyCRM API (обхід CORS)" \
    --timeout 30

echo "⚙️ Lambda функція створена"

# Створюємо API Gateway для Lambda
aws apigateway create-rest-api \
    --name keycrm-proxy-api \
    --description "API Gateway для KeyCRM проксі"

# Отримуємо API ID (потрібно буде вручну налаштувати ресурси та методи)
echo "📡 API Gateway створено. Потрібно налаштувати ресурси та методи вручну в AWS консолі"

echo "✅ Деплой завершено!"
echo ""
echo "📋 Наступні кроки:"
echo "1. Налаштуйте API Gateway ресурси та методи"
echo "2. Замініть YOUR_KEYCRM_PROXY_LAMBDA_URL в GTM скрипті на справжній URL"
echo "3. Протестуйте роботу проксі"

cd ..
