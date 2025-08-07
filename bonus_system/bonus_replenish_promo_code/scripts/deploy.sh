#!/bin/bash

# Переходимо в кореневу директорію проєкту
cd "$(dirname "$0")/../.." || exit

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

# --- Конфігурація ---
AWS_REGION="eu-north-1"
FUNCTION_NAME="replenish-promo-code" # Нова назва для повільної функції
ECR_REPO_NAME="replenish-promo-code-repo" # Нова назва репозиторію
LAMBDA_ROLE_NAME="lambda-promo-role"

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

# --- Отримання AWS Account ID ---
log_info "Отримуємо AWS Account ID..."
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)
if [ -z "$AWS_ACCOUNT_ID" ]; then
    log_error "Не вдалося отримати AWS Account ID."
fi
log_success "AWS Account ID: $AWS_ACCOUNT_ID"

ECR_URI="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
IMAGE_URI="$ECR_URI/$ECR_REPO_NAME:latest"

# --- Крок 1: Створення ECR репозиторію (якщо не існує) ---
log_info "Перевіряємо наявність ECR репозиторію '$ECR_REPO_NAME'..."
if ! aws ecr describe-repositories --repository-names "$ECR_REPO_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log_info "Репозиторій не знайдено. Створюємо..."
    aws ecr create-repository \
        --repository-name "$ECR_REPO_NAME" \
        --region "$AWS_REGION" \
        --image-scanning-configuration scanOnPush=true >/dev/null
    log_success "ECR репозиторій '$ECR_REPO_NAME' створено."
else
    log_success "ECR репозиторій вже існує."
fi

# --- Крок 2: Автентифікація Docker в ECR ---
log_info "Автентифікація Docker в ECR..."
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_URI" >/dev/null
log_success "Docker автентифіковано."

# --- Крок 3: Build, Tag, Push Docker образу ---
log_info "Збираємо Docker образ для архітектури amd64..."
docker build --platform linux/amd64 -t "$ECR_REPO_NAME:latest" -f replenish_promo_code_lambda/Dockerfile ./replenish_promo_code_lambda
log_success "Образ зібрано."

log_info "Тегуємо образ..."
docker tag "$ECR_REPO_NAME:latest" "$IMAGE_URI"
log_success "Образ теговано: $IMAGE_URI"

log_info "Завантажуємо образ в ECR..."
docker push "$IMAGE_URI" >/dev/null
log_success "Образ завантажено в ECR."

# --- Крок 4: Отримання ARN ролі ---
log_info "Отримуємо ARN для IAM ролі '$LAMBDA_ROLE_NAME'..."
ROLE_ARN=$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query 'Role.Arn' --output text)
if [ -z "$ROLE_ARN" ]; then
    log_error "Не вдалося знайти IAM роль '$LAMBDA_ROLE_NAME'. Переконайтесь, що вона створена."
fi
log_success "ARN ролі: $ROLE_ARN"

# --- Крок 5: Створення або оновлення Lambda функції ---
log_info "Перевіряємо наявність Lambda функції '$FUNCTION_NAME'..."
if ! aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log_info "Функція не знайдена. Створюємо нову..."
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --package-type Image \
        --code ImageUri="$IMAGE_URI" \
        --role "$ROLE_ARN" \
        --timeout 120 \
        --memory-size 2048 \
        --environment "Variables={SESSION_S3_BUCKET=lambda-promo-sessions,ADMIN_URL=https://safeyourlove.com/edit/discounts/codes,ADMIN_USERNAME=owner,ADMIN_PASSWORD=sdjkdfsf4ir2rfkb,PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers,LOG_LEVEL=INFO}" >/dev/null
    log_success "Lambda функцію '$FUNCTION_NAME' створено."
else
    log_info "Функція знайдена. Оновлюємо код..."
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --image-uri "$IMAGE_URI" >/dev/null
    log_success "Код Lambda функції оновлено."
    
    wait_for_function_update "$FUNCTION_NAME"

    log_info "Оновлюємо конфігурацію..."
    aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --role "$ROLE_ARN" \
        --timeout 120 \
        --memory-size 2048 \
        --environment "Variables={SESSION_S3_BUCKET=lambda-promo-sessions,ADMIN_URL=https://safeyourlove.com/edit/discounts/codes,ADMIN_USERNAME=owner,ADMIN_PASSWORD=sdjkdfsf4ir2rfkb,PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers,LOG_LEVEL=INFO}" >/dev/null
    log_success "Конфігурацію Lambda функції оновлено."
fi

log_success "Деплой функції '$FUNCTION_NAME' успішно завершено!"
