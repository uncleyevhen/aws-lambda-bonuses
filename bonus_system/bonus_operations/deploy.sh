#!/bin/bash

# Деплоймент скрипт для bonus_operations Lambda
set -e

LAMBDA_NAME="bonus-operations"
REGION="eu-north-1"
LAMBDA_ROLE="arn:aws:iam::881490108668:role/lambda-execution-role"

echo "🚀 Починаємо деплоймент Lambda функції $LAMBDA_NAME..."

# Функція для очікування оновлення функції
wait_for_function_update() {
    local function_name=$1
    echo "⏳ Очікуємо завершення оновлення функції $function_name..."
    
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        local status=$(aws lambda get-function --function-name "$function_name" --region "$REGION" --query 'Configuration.LastUpdateStatus' --output text 2>/dev/null)
        
        if [ "$status" == "Successful" ]; then
            echo "✅ Функція '$function_name' успішно оновлена і активна."
            return 0
        elif [ "$status" == "Failed" ]; then
            echo "❌ Оновлення функції '$function_name' не вдалося. Перевірте логи в AWS Console."
            return 1
        fi
        
        echo "📊 Статус оновлення: $status. Очікуємо... ($attempt/$max_attempts)"
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "⚠️ Таймаут очікування оновлення функції"
    return 1
}

# Створюємо zip архів
echo "📦 Створення zip архіву..."
zip -r bonus-operations.zip . -x "*.git*" "*.DS_Store*" "deploy.sh" "README.md" "lambda_function_old.py"

# Перевіряємо AWS креденшали та регіон
echo "🔐 Перевіряємо AWS креденшали..."
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
if [ $? -eq 0 ]; then
    echo "✅ AWS Account ID: $AWS_ACCOUNT"
    echo "✅ Region: $REGION"
else
    echo "❌ Помилка AWS креденшалів!"
    exit 1
fi

# Перевіряємо чи існує функція
echo "🔍 Перевіряємо існування Lambda функції..."

# Спробуємо отримати інформацію про функцію
if aws lambda get-function --function-name $LAMBDA_NAME --region $REGION >/dev/null 2>&1; then
    FUNCTION_EXISTS="EXISTS"
else
    FUNCTION_EXISTS="NOT_EXISTS"
fi

echo "📊 Результат перевірки: $FUNCTION_EXISTS"

# Додаткова діагностика - показуємо всі наявні функції
echo "📋 Список всіх Lambda функцій в регіоні $REGION:"
aws lambda list-functions --region $REGION --query 'Functions[].FunctionName' --output table 2>/dev/null || echo "Не вдалося отримати список функцій"

if [ "$FUNCTION_EXISTS" = "EXISTS" ]; then
    echo "🔄 Функція існує. Оновлюємо код існуючої Lambda функції..."
    
    # Спочатку отримаємо поточну інформацію про функцію
    CURRENT_STATUS=$(aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query 'Configuration.State' --output text 2>/dev/null)
    echo "📊 Поточний статус функції: $CURRENT_STATUS"
    
    # Оновлюємо код
    aws lambda update-function-code \
        --function-name $LAMBDA_NAME \
        --zip-file fileb://bonus-operations.zip \
        --region $REGION
    
    echo "✅ Код функції оновлено"
    
    # Очікуємо завершення оновлення коду
    wait_for_function_update $LAMBDA_NAME
    
    echo "⚙️ Оновлюємо конфігурацію Lambda функції..."
    aws lambda update-function-configuration \
        --function-name $LAMBDA_NAME \
        --runtime python3.13 \
        --handler lambda_function.lambda_handler \
        --timeout 30 \
        --memory-size 256 \
        --environment Variables="{KEYCRM_API_TOKEN=M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ,KEYCRM_BASE_URL=https://openapi.keycrm.app/v1}" \
        --region $REGION
    
    echo "✅ Конфігурація функції оновлена"
    
    # Очікуємо завершення оновлення конфігурації
    wait_for_function_update $LAMBDA_NAME
else
    echo "🆕 Створюємо нову Lambda функцію..."
    aws lambda create-function \
        --function-name $LAMBDA_NAME \
        --runtime python3.13 \
        --role $LAMBDA_ROLE \
        --handler lambda_function.lambda_handler \
        --zip-file fileb://bonus-operations.zip \
        --timeout 30 \
        --memory-size 256 \
        --environment Variables="{KEYCRM_API_TOKEN=M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ,KEYCRM_BASE_URL=https://openapi.keycrm.app/v1}" \
        --region $REGION
    
    # Очікуємо готовності нової функції
    wait_for_function_update $LAMBDA_NAME
fi

# Очищуємо тимчасові файли
rm bonus-operations.zip

echo "✅ Lambda функція $LAMBDA_NAME успішно розгорнута!"

# Перевіряємо фінальний статус функції
echo "🔍 Перевіряємо фінальний статус функції..."
FUNCTION_STATUS=$(aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query 'Configuration.State' --output text 2>/dev/null)
echo "📊 Статус функції: $FUNCTION_STATUS"

if [ "$FUNCTION_STATUS" != "Active" ]; then
    echo "⚠️ УВАГА: Функція не в активному стані. Можливі проблеми з деплоєм."
fi

# Отримуємо ARN функції
LAMBDA_ARN=$(aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query 'Configuration.FunctionArn' --output text)
echo "📋 Lambda ARN: $LAMBDA_ARN"

echo "🌐 Перевіряємо API Gateway..."

# Створюємо API Gateway
API_NAME="bonus-operations-api"
echo "🔍 Перевіряємо існування API Gateway..."

# Отримуємо список API
API_ID=$(aws apigatewayv2 get-apis --region $REGION --query "Items[?Name=='$API_NAME'].ApiId" --output text)

if [ "$API_ID" = "" ] || [ "$API_ID" = "None" ]; then
    echo "🆕 Створюємо новий HTTP API Gateway..."
    API_ID=$(aws apigatewayv2 create-api \
        --name $API_NAME \
        --protocol-type HTTP \
        --cors-configuration AllowOrigins="*",AllowMethods="GET,POST,OPTIONS",AllowHeaders="content-type,x-amz-date,authorization,x-api-key" \
        --region $REGION \
        --query 'ApiId' --output text)
    echo "🆔 Створений API ID: $API_ID"
else
    echo "🔄 Використовуємо існуючий API ID: $API_ID"
fi

# Створюємо інтеграцію з Lambda
echo "🔗 Створюємо інтеграцію з Lambda..."
INTEGRATION_ID=$(aws apigatewayv2 create-integration \
    --api-id $API_ID \
    --integration-type AWS_PROXY \
    --integration-uri $LAMBDA_ARN \
    --payload-format-version "2.0" \
    --region $REGION \
    --query 'IntegrationId' --output text)

echo "🔗 Integration ID: $INTEGRATION_ID"

# Створюємо роути
echo "🛤️ Створюємо роути..."

ROUTES=(
    "POST /order-complete"
    "POST /order-cancel" 
    "POST /order-reserve"
    "POST /lead-reserve"
    "POST /test-log"
    "OPTIONS /{proxy+}"
)

for route in "${ROUTES[@]}"; do
    echo "📍 Створюємо роут: $route"
    aws apigatewayv2 create-route \
        --api-id $API_ID \
        --route-key "$route" \
        --target "integrations/$INTEGRATION_ID" \
        --region $REGION > /dev/null
done

# Створюємо stage
echo "🎭 Створюємо stage..."
aws apigatewayv2 create-stage \
    --api-id $API_ID \
    --stage-name "prod" \
    --auto-deploy \
    --region $REGION > /dev/null 2>&1 || echo "Stage вже існує"

# Додаємо дозвіл для API Gateway викликати Lambda
echo "🔐 Додаємо дозволи..."
aws lambda add-permission \
    --function-name $LAMBDA_NAME \
    --statement-id api-gateway-invoke \
    --action lambda:InvokeFunction \
    --principal apigateway.amazonaws.com \
    --source-arn "arn:aws:execute-api:$REGION:881490108668:$API_ID/*/*" \
    --region $REGION > /dev/null 2>&1 || echo "Дозвіл вже існує"

# Отримуємо URL API
API_URL="https://$API_ID.execute-api.$REGION.amazonaws.com/prod"

echo ""
echo "🎉 ДЕПЛОЙМЕНТ ЗАВЕРШЕНО!"
echo "=================================="
echo "🔗 API Gateway URL: $API_URL"
echo ""
echo "📋 Доступні endpoint'и:"
echo "  POST $API_URL/order-complete"
echo "  POST $API_URL/order-cancel"
echo "  POST $API_URL/order-reserve" 
echo "  POST $API_URL/lead-reserve"
echo "  POST $API_URL/test-log"
echo ""
echo "🧪 Тестовий запит:"
echo "curl -X POST $API_URL/test-log -H 'Content-Type: application/json' -d '{\"test\": \"data\"}'"
echo ""
