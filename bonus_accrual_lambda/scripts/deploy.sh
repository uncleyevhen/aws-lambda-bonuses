#!/bin/bash

# Скрипт для розгортання Lambda функції нарахування бонусів
set -e

FUNCTION_NAME="bonus-accrual"
REGION="eu-north-1"
ROLE_NAME="lambda-bonus-accrual-role"

echo "🚀 Початок розгортання Lambda функції для нарахування бонусів..."

# Перевірка наявності AWS CLI
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI не встановлено. Встановіть AWS CLI та налаштуйте credentials."
    exit 1
fi

# Створення директорії для збірки
echo "📦 Підготовка пакету..."
rm -rf build/
mkdir -p build/

# Копіювання файлів
cp lambda_function.py build/
cp requirements.txt build/

# Встановлення залежностей
cd build/
echo "📥 Встановлення залежностей..."
pip install -r requirements.txt -t .

# Створення ZIP архіву
echo "🗜️ Створення ZIP архіву..."
zip -r ../bonus-accrual.zip . -x "*.pyc" "__pycache__/*"
cd ..

# Пропускаємо створення DynamoDB таблиці - використовуємо тільки KeyCRM
echo "🗄️ Система працює тільки з KeyCRM API (без DynamoDB)"

# Отримання ARN ролі (створюємо якщо не існує)
echo "🔐 Перевірка IAM ролі..."
ROLE_ARN=$(aws iam get-role --role-name $ROLE_NAME --query 'Role.Arn' --output text 2>/dev/null || echo "")

if [ -z "$ROLE_ARN" ]; then
    echo "Створення IAM ролі..."
    
    # Створення політики довіри
    cat > trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

    # Створення ролі
    aws iam create-role \
        --role-name $ROLE_NAME \
        --assume-role-policy-document file://trust-policy.json

    # Прикріплення політик
    aws iam attach-role-policy \
        --role-name $ROLE_NAME \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

    # Створення мінімальної політики (тільки логування)
    cat > minimal-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
EOF

    aws iam put-role-policy \
        --role-name $ROLE_NAME \
        --policy-name MinimalLambdaAccess \
        --policy-document file://minimal-policy.json

    # Отримання ARN ролі
    ROLE_ARN=$(aws iam get-role --role-name $ROLE_NAME --query 'Role.Arn' --output text)
    
    # Очікування розповсюдження ролі
    echo "Очікування розповсюдження IAM ролі..."
    sleep 10
    
    rm trust-policy.json minimal-policy.json
    echo "✅ IAM роль створена"
else
    echo "✅ IAM роль вже існує"
fi

# Створення або оновлення Lambda функції
echo "🚀 Розгортання Lambda функції..."
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &> /dev/null; then
    echo "Оновлення існуючої функції..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://bonus-accrual.zip \
        --region $REGION
    
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --environment Variables="{KEYCRM_API_TOKEN=M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ,KEYCRM_BASE_URL=https://openapi.keycrm.app/v1}" \
        --timeout 30 \
        --memory-size 512 \
        --region $REGION
else
    echo "Створення нової функції..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime python3.9 \
        --role $ROLE_ARN \
        --handler lambda_function.lambda_handler \
        --zip-file fileb://bonus-accrual.zip \
        --environment Variables="{KEYCRM_API_TOKEN=M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ,KEYCRM_BASE_URL=https://openapi.keycrm.app/v1}" \
        --timeout 30 \
        --memory-size 512 \
        --region $REGION
fi

# Створення API Gateway (якщо потрібно)
echo "🌐 Налаштування API Gateway..."
read -p "Створити API Gateway для цієї функції? (y/n): " create_api

if [ "$create_api" = "y" ]; then
    # Створення API Gateway
    API_ID=$(aws apigateway create-rest-api \
        --name "bonus-accrual-api" \
        --description "API для нарахування бонусів" \
        --region $REGION \
        --query 'id' --output text)
    
    # Отримання root resource ID
    ROOT_ID=$(aws apigateway get-resources \
        --rest-api-id $API_ID \
        --region $REGION \
        --query 'items[0].id' --output text)
    
    # Створення resource
    RESOURCE_ID=$(aws apigateway create-resource \
        --rest-api-id $API_ID \
        --parent-id $ROOT_ID \
        --path-part "bonus-accrual" \
        --region $REGION \
        --query 'id' --output text)
    
    # Створення POST методу
    aws apigateway put-method \
        --rest-api-id $API_ID \
        --resource-id $RESOURCE_ID \
        --http-method POST \
        --authorization-type NONE \
        --region $REGION
    
    # Створення OPTIONS методу для CORS
    aws apigateway put-method \
        --rest-api-id $API_ID \
        --resource-id $RESOURCE_ID \
        --http-method OPTIONS \
        --authorization-type NONE \
        --region $REGION
    
    # Налаштування інтеграції з Lambda
    LAMBDA_ARN="arn:aws:lambda:$REGION:$(aws sts get-caller-identity --query Account --output text):function:$FUNCTION_NAME"
    
    aws apigateway put-integration \
        --rest-api-id $API_ID \
        --resource-id $RESOURCE_ID \
        --http-method POST \
        --type AWS_PROXY \
        --integration-http-method POST \
        --uri "arn:aws:apigateway:$REGION:lambda:path/2015-03-31/functions/$LAMBDA_ARN/invocations" \
        --region $REGION
    
    # Надання дозволу API Gateway викликати Lambda
    aws lambda add-permission \
        --function-name $FUNCTION_NAME \
        --statement-id apigateway-access \
        --action lambda:InvokeFunction \
        --principal apigateway.amazonaws.com \
        --source-arn "arn:aws:execute-api:$REGION:$(aws sts get-caller-identity --query Account --output text):$API_ID/*/*" \
        --region $REGION
    
    # Розгортання API
    aws apigateway create-deployment \
        --rest-api-id $API_ID \
        --stage-name prod \
        --region $REGION
    
    echo "✅ API Gateway створено!"
    echo "🔗 URL: https://$API_ID.execute-api.$REGION.amazonaws.com/prod/bonus-accrual"
    echo ""
    echo "Оновіть BONUS_ACCRUAL_API_URL в клієнтському скрипті:"
    echo "var BONUS_ACCRUAL_API_URL = \"https://$API_ID.execute-api.$REGION.amazonaws.com/prod/bonus-accrual\";"
fi

# Очищення
rm -rf build/
rm bonus-accrual.zip

echo ""
echo "✅ Розгортання завершено!"
echo "📊 Функція: $FUNCTION_NAME"
echo "🗄️ Таблиця: $TABLE_NAME"
echo "🔐 Роль: $ROLE_NAME"
echo ""
echo "Для тестування виконайте:"
echo "aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"orderId\":\"12345\",\"orderTotal\":1000,\"bonusAmount\":100,\"customer\":{\"phone\":\"+380123456789\",\"email\":\"test@example.com\"}}' response.json"
