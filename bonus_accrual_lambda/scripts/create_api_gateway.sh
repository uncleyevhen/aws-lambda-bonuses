#!/bin/bash

set -e

# Кольори
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Конфігурація
AWS_REGION="eu-north-1"
API_NAME="bonus-accrual-api"
FUNCTION_NAME="bonus-accrual" # Основна функція, яка викликається
ROUTE_KEY="POST /accrue-bonus"

# Отримання AWS Account ID
log_info "Отримуємо AWS Account ID..."
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)
log_success "AWS Account ID: $AWS_ACCOUNT_ID"

LAMBDA_ARN="arn:aws:lambda:$AWS_REGION:$AWS_ACCOUNT_ID:function:$FUNCTION_NAME"

# 1. Створення HTTP API
log_info "Створюємо HTTP API '$API_NAME'..."
API_ID=$(aws apigatewayv2 create-api \
    --name "$API_NAME" \
    --protocol-type HTTP \
    --target "$LAMBDA_ARN" \
    --query 'ApiId' --output text --region "$AWS_REGION")
log_success "API створено з ID: $API_ID"

API_ENDPOINT=$(aws apigatewayv2 get-api --api-id "$API_ID" --query 'ApiEndpoint' --output text --region "$AWS_REGION")

# 2. Надання прав API Gateway для виклику Lambda
log_info "Надаємо права API Gateway для виклику Lambda..."
aws lambda add-permission \
    --function-name "$FUNCTION_NAME" \
    --statement-id "apigateway-invoke" \
    --action "lambda:InvokeFunction" \
    --principal "apigateway.amazonaws.com" \
    --source-arn "arn:aws:execute-api:$AWS_REGION:$AWS_ACCOUNT_ID:$API_ID/*" \
    --region "$AWS_REGION" >/dev/null

log_success "Права надано."

log_success "API Gateway успішно створено та налаштовано!"
log_info "Endpoint URL: ${GREEN}${API_ENDPOINT}/accrue-bonus${NC}"
log_info "Використовуйте цей URL для POST запитів."
