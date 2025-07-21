#!/bin/bash

# Переходимо в директорію, де знаходиться сама функція
cd "$(dirname "$0")/.." || exit

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

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Функція для очікування, поки Lambda функція стане активною
wait_for_function_update() {
    local function_name=$1
    log_info "Очікуємо завершення оновлення для функції '$function_name'..."
    
    while true; do
        local status=$(aws lambda get-function --function-name "$function_name" --query 'Configuration.LastUpdateStatus' --output text 2>/dev/null)
        
        if [ "$status" == "Successful" ]; then
            log_success "Функція '$function_name' успішно оновлена і активна."
            break
        elif [ "$status" == "Failed" ]; then
            log_error "Оновлення функції '$function_name' не вдалося. Перевірте логи в AWS Console."
            exit 1
        fi
        
        log_info "Статус оновлення: $status. Очікуємо..."
        sleep 5
    done
}

# --- Конфігурація ---
AWS_REGION="eu-north-1"
FUNCTION_NAME="get-promo-code"
LAMBDA_ROLE_NAME="lambda-promo-role"
HANDLER="lambda_function.lambda_handler"
RUNTIME="python3.11"
ZIP_FILE="get-promo-code-deployment-package.zip"
BUILD_DIR="build"

# --- Крок 1: Підготовка ZIP-архіву для деплою ---
log_info "Створюємо директорію для збірки..."
rm -rf "$BUILD_DIR" "$ZIP_FILE"
mkdir -p "$BUILD_DIR"

log_info "Встановлюємо залежності з requirements.txt..."
pip install --target "$BUILD_DIR" -r requirements.txt > /dev/null

log_info "Копіюємо код Lambda функції..."
cp lambda_function.py promo_logic.py "$BUILD_DIR"/

log_info "Створюємо ZIP-архів..."
(cd "$BUILD_DIR" && zip -r ../"$ZIP_FILE" . > /dev/null)
log_success "ZIP-архів '$ZIP_FILE' успішно створено."

# --- Крок 2: Отримання ARN ролі ---
log_info "Отримуємо ARN для IAM ролі '$LAMBDA_ROLE_NAME'..."
ROLE_ARN=$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null)
if [ -z "$ROLE_ARN" ]; then
    log_error "Не вдалося знайти IAM роль '$LAMBDA_ROLE_NAME'. Переконайтесь, що вона створена з необхідними дозволами (S3, Lambda invocation, CloudWatch Logs)."
fi
log_success "ARN ролі: $ROLE_ARN"

# --- Крок 3: Створення або оновлення Lambda функції ---
log_info "Перевіряємо наявність Lambda функції '$FUNCTION_NAME'..."
if ! aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log_info "Функція не знайдена. Створюємо нову..."
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --runtime "$RUNTIME" \
        --handler "$HANDLER" \
        --role "$ROLE_ARN" \
        --zip-file "fileb://$ZIP_FILE" \
        --timeout 60 \
        --memory-size 256 \
        --environment "Variables={REPLENISH_FUNCTION_NAME=replenish-promo-code,SESSION_S3_BUCKET=lambda-promo-sessions,BATCH_THRESHOLD=20,MIN_CODES_THRESHOLD=3}" >/dev/null
    log_success "Lambda функцію '$FUNCTION_NAME' створено."
else
    log_info "Функція знайдена. Оновлюємо код..."
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --zip-file "fileb://$ZIP_FILE" >/dev/null
    log_success "Код Lambda функції оновлено."
    
    wait_for_function_update "$FUNCTION_NAME"
    
    log_info "Оновлюємо конфігурацію..."
    aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --runtime "$RUNTIME" \
        --handler "$HANDLER" \
        --role "$ROLE_ARN" \
        --timeout 60 \
        --memory-size 256 \
        --environment "Variables={REPLENISH_FUNCTION_NAME=replenish-promo-code,SESSION_S3_BUCKET=lambda-promo-sessions,BATCH_THRESHOLD=20,MIN_CODES_THRESHOLD=3}" >/dev/null
    log_success "Конфігурацію Lambda функції оновлено."
fi

# --- Крок 4: Очищення ---
log_info "Очищуємо тимчасові файли..."
rm -rf "$BUILD_DIR" "$ZIP_FILE"
log_success "Очищення завершено."

log_success "Деплой функції '$FUNCTION_NAME' успішно завершено!"
