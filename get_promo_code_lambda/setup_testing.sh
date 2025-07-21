#!/bin/bash

# Скрипт для автоматичного отримання API Gateway endpoint
# та оновлення тестових файлів

set -e

# Кольори
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Функція для отримання API endpoint
get_api_endpoint() {
    log_info "Шукаємо API Gateway endpoint..."
    
    # Спробуємо знайти API по імені
    API_ENDPOINT=$(aws apigatewayv2 get-apis --query 'Items[?Name==`promo-code-api`].ApiEndpoint' --output text 2>/dev/null)
    
    if [ -z "$API_ENDPOINT" ] || [ "$API_ENDPOINT" == "None" ]; then
        log_warning "API з назвою 'promo-code-api' не знайдено."
        log_info "Показуємо всі доступні HTTP APIs..."
        
        aws apigatewayv2 get-apis --query 'Items[].{Name:Name,Endpoint:ApiEndpoint,Id:ApiId}' --output table
        
        log_error "Не вдалося автоматично знайти endpoint."
        log_info "Будь ласка, виберіть правильний endpoint з таблиці вище"
        log_info "та оновіть файли test_*.py вручну"
        return 1
    fi
    
    log_success "Знайдено API endpoint: ${API_ENDPOINT}"
    return 0
}

# Функція для оновлення тестових файлів
update_test_files() {
    local endpoint="$1"
    local full_endpoint="${endpoint}/get-code"
    
    log_info "Оновлюємо тестові файли з endpoint: ${full_endpoint}"
    
    # Оновлюємо test_lambda_integration.py
    if [ -f "test_lambda_integration.py" ]; then
        sed -i.bak "s|https://YOUR_API_ID.execute-api.eu-north-1.amazonaws.com/get-code|${full_endpoint}|g" test_lambda_integration.py
        log_success "Оновлено test_lambda_integration.py"
    else
        log_warning "Файл test_lambda_integration.py не знайдено"
    fi
    
    # Оновлюємо test_api_quick.py
    if [ -f "test_api_quick.py" ]; then
        sed -i.bak "s|https://YOUR_API_ID.execute-api.eu-north-1.amazonaws.com/get-code|${full_endpoint}|g" test_api_quick.py
        log_success "Оновлено test_api_quick.py"
    else
        log_warning "Файл test_api_quick.py не знайдено"
    fi
    
    # Видаляємо backup файли
    rm -f test_lambda_integration.py.bak test_api_quick.py.bak 2>/dev/null || true
}

# Функція для перевірки AWS CLI
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI не встановлено!"
        log_info "Встановіть AWS CLI: https://aws.amazon.com/cli/"
        return 1
    fi
    
    # Перевіряємо AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials не налаштовані!"
        log_info "Налаштуйте credentials: aws configure"
        return 1
    fi
    
    log_success "AWS CLI налаштовано правильно"
    return 0
}

# Функція для тестування endpoint
test_endpoint() {
    local endpoint="$1/get-code"
    
    log_info "Тестуємо endpoint: ${endpoint}"
    
    # Простий тест з curl
    local response_code=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d '{"amount": 50}' \
        "$endpoint" \
        --connect-timeout 10 \
        --max-time 30)
    
    if [ "$response_code" == "200" ]; then
        log_success "Endpoint працює! (код відповіді: 200)"
        return 0
    elif [ "$response_code" == "000" ]; then
        log_error "Не вдалося підключитися до endpoint (таймаут або мережева помилка)"
        return 1
    else
        log_warning "Endpoint відповів з кодом: ${response_code}"
        log_info "Це може бути нормально, якщо промокодів немає в S3"
        return 0
    fi
}

# Головна функція
main() {
    echo "🔧 АВТОМАТИЧНА КОНФІГУРАЦІЯ ТЕСТУВАННЯ API"
    echo "=========================================="
    
    # Перевіряємо AWS CLI
    if ! check_aws_cli; then
        exit 1
    fi
    
    # Отримуємо endpoint
    if ! get_api_endpoint; then
        exit 1
    fi
    
    # Оновлюємо файли
    update_test_files "$API_ENDPOINT"
    
    # Тестуємо endpoint
    test_endpoint "$API_ENDPOINT"
    
    echo ""
    log_success "Конфігурація завершена!"
    log_info "Тепер можете запустити тести:"
    log_info "  python3 test_api_quick.py"
    log_info "  python3 test_lambda_integration.py"
}

# Запуск
main "$@"
