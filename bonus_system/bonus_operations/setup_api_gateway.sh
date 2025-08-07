#!/bin/bash

# ==============================================================================
# 🚀 СТВОРЕННЯ HTTP API GATEWAY ДЛЯ BONUS OPERATIONS LAMBDA
# Регіон: eu-north-1
# ==============================================================================

set -e

# Кольори для виводу
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функції для логування
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

# ==============================================================================
# КОНФІГУРАЦІЯ
# ==============================================================================

AWS_REGION="eu-north-1"
API_NAME="bonus-operations-api"
FUNCTION_NAME="bonus-operations"
STAGE_NAME="prod"

# Роути для bonus-operations Lambda (з аналізу коду)
ROUTES=(
    "POST /order-complete"    # Обробка завершення замовлення (нарахування бонусів)
    "POST /order-cancel"      # Скасування замовлення (повернення бонусів з резерву)
    "POST /order-reserve"     # Резервування бонусів при створенні замовлення
    "POST /lead-reserve"      # Мануальне резервування бонусів через ліди
    "POST /test-log"          # Тестовий endpoint для логування вебхуків
)

# OPTIONS роути для CORS
OPTIONS_ROUTES=(
    "OPTIONS /order-complete"
    "OPTIONS /order-cancel"
    "OPTIONS /order-reserve"
    "OPTIONS /lead-reserve"
    "OPTIONS /test-log"
    "OPTIONS /{proxy+}"       # Catch-all для інших OPTIONS запитів
)

# Отримання AWS Account ID
log_info "Отримуємо AWS Account ID..."
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)
log_success "AWS Account ID: $AWS_ACCOUNT_ID"

# ==============================================================================
# ФУНКЦІЯ ВАЛІДАЦІЇ
# ==============================================================================

validate_prerequisites() {
    log_info "Перевіряємо передумови..."
    
    # Перевірка AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI не встановлений"
        exit 1
    fi
    
    # Перевірка аутентифікації
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        log_error "AWS не налаштований. Запустіть 'aws configure'"
        exit 1
    fi
    
    # Перевірка існування Lambda функції
    if ! aws lambda get-function --function-name $FUNCTION_NAME --region $AWS_REGION >/dev/null 2>&1; then
        log_error "Lambda функція '$FUNCTION_NAME' не знайдена в регіоні $AWS_REGION"
        log_info "Спочатку розгорніть Lambda функцію:"
        log_info "cd bonus_system/bonus_operations && ./deploy.sh"
        exit 1
    fi
    
    log_success "Всі передумови виконані"
}

# ==============================================================================
# ФУНКЦІЯ СТВОРЕННЯ API GATEWAY
# ==============================================================================

create_api_gateway() {
    log_info "Створюємо HTTP API Gateway '$API_NAME'..."
    
    # Перевіряємо існування API
    existing_api_id=$(aws apigatewayv2 get-apis \
        --region $AWS_REGION \
        --query "Items[?Name=='$API_NAME'].ApiId" \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$existing_api_id" ] && [ "$existing_api_id" != "None" ]; then
        log_warning "API з назвою '$API_NAME' вже існує (ID: $existing_api_id)"
        read -p "Видалити існуючий API та створити новий? (y/N): " confirm
        
        if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
            log_info "Видаляємо існуючий API..."
            aws apigatewayv2 delete-api --api-id $existing_api_id --region $AWS_REGION
            log_success "Існуючий API видалено"
        else
            log_info "Використовуємо існуючий API з ID: $existing_api_id"
            API_ID=$existing_api_id
            return 0
        fi
    fi
    
    # Створюємо новий HTTP API з CORS
    API_ID=$(aws apigatewayv2 create-api \
        --name "$API_NAME" \
        --protocol-type HTTP \
        --description "API Gateway для обробки бонусних операцій" \
        --cors-configuration AllowOrigins="*",AllowMethods="GET,POST,OPTIONS",AllowHeaders="content-type,x-amz-date,authorization,x-api-key,x-requested-with" \
        --region $AWS_REGION \
        --query 'ApiId' --output text)
    
    log_success "HTTP API створено з ID: $API_ID"
    
    # Отримуємо endpoint URL зі стейджем
    API_BASE_URL=$(aws apigatewayv2 get-api \
        --api-id $API_ID \
        --region $AWS_REGION \
        --query 'ApiEndpoint' --output text)
    
    API_ENDPOINT="${API_BASE_URL}/${STAGE_NAME}"
    
    log_success "API Endpoint: $API_ENDPOINT"
}

# ==============================================================================
# ФУНКЦІЯ СТВОРЕННЯ ІНТЕГРАЦІЇ З LAMBDA
# ==============================================================================

create_lambda_integration() {
    log_info "Створюємо інтеграцію з Lambda функцією '$FUNCTION_NAME'..."
    
    # Отримуємо ARN Lambda функції
    LAMBDA_ARN=$(aws lambda get-function \
        --function-name $FUNCTION_NAME \
        --region $AWS_REGION \
        --query 'Configuration.FunctionArn' --output text)
    
    log_info "Lambda ARN: $LAMBDA_ARN"
    
    # Створюємо інтеграцію
    INTEGRATION_ID=$(aws apigatewayv2 create-integration \
        --api-id $API_ID \
        --integration-type AWS_PROXY \
        --integration-uri $LAMBDA_ARN \
        --payload-format-version "2.0" \
        --region $AWS_REGION \
        --query 'IntegrationId' --output text)
    
    log_success "Інтеграція створена з ID: $INTEGRATION_ID"
    
    # Надаємо дозвіл API Gateway викликати Lambda
    aws lambda add-permission \
        --function-name $FUNCTION_NAME \
        --statement-id "apigateway-invoke-$(date +%s)" \
        --action lambda:InvokeFunction \
        --principal apigateway.amazonaws.com \
        --source-arn "arn:aws:execute-api:$AWS_REGION:$AWS_ACCOUNT_ID:$API_ID/*/*" \
        --region $AWS_REGION >/dev/null 2>&1 || log_warning "Дозвіл вже існує"
    
    log_success "Дозволи налаштовані"
}

# ==============================================================================
# ФУНКЦІЯ СТВОРЕННЯ РОУТІВ
# ==============================================================================

create_routes() {
    log_info "Створюємо роути для бонусних операцій..."
    
    echo ""
    echo "📍 Основні роути:"
    for route in "${ROUTES[@]}"; do
        log_info "Створюємо роут: $route"
        
        aws apigatewayv2 create-route \
            --api-id $API_ID \
            --route-key "$route" \
            --target "integrations/$INTEGRATION_ID" \
            --region $AWS_REGION >/dev/null
        
        log_success "✓ $route"
    done
    
    echo ""
    echo "🔧 CORS роути:"
    for route in "${OPTIONS_ROUTES[@]}"; do
        log_info "Створюємо OPTIONS роут: $route"
        
        aws apigatewayv2 create-route \
            --api-id $API_ID \
            --route-key "$route" \
            --target "integrations/$INTEGRATION_ID" \
            --region $AWS_REGION >/dev/null 2>&1
        
        log_success "✓ $route"
    done
}

# ==============================================================================
# ФУНКЦІЯ СТВОРЕННЯ STAGE
# ==============================================================================

create_stage() {
    log_info "Створюємо production stage '$STAGE_NAME'..."
    
    aws apigatewayv2 create-stage \
        --api-id $API_ID \
        --stage-name $STAGE_NAME \
        --auto-deploy \
        --description "Production stage для бонусних операцій" \
        --region $AWS_REGION >/dev/null 2>&1 || log_warning "Stage '$STAGE_NAME' вже існує"
    
    log_success "Production stage створено"
}

# ==============================================================================
# ФУНКЦІЯ ТЕСТУВАННЯ API
# ==============================================================================

test_api() {
    log_info "Тестуємо API endpoints..."
    
    local base_url="$API_ENDPOINT"
    
    echo ""
    echo "🧪 Тестові запити:"
    
    # Тестуємо test-log endpoint
    echo "1. Тестуємо /test-log endpoint:"
    curl -s -X POST "$base_url/test-log" \
        -H "Content-Type: application/json" \
        -d '{"test": "API Gateway setup test", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | jq '.' 2>/dev/null || echo "Відповідь отримана (jq не встановлений)"
    
    echo ""
    echo "2. Тестуємо CORS preflight:"
    curl -s -X OPTIONS "$base_url/order-complete" \
        -H "Origin: https://example.com" \
        -H "Access-Control-Request-Method: POST" \
        -H "Access-Control-Request-Headers: Content-Type" -v
}

# ==============================================================================
# ФУНКЦІЯ ВИВОДУ РЕЗУЛЬТАТІВ
# ==============================================================================

display_results() {
    echo ""
    echo "🎉 ==============================================="
    echo "🎉 BONUS OPERATIONS API GATEWAY ГОТОВИЙ!"
    echo "🎉 ==============================================="
    echo ""
    log_success "API Gateway ID: $API_ID"
    log_success "Base URL: $API_ENDPOINT"
    echo ""
    echo "📋 Доступні ендпоінти бонусних операцій:"
    echo "────────────────────────────────────────────────"
    
    for route in "${ROUTES[@]}"; do
        method=$(echo "$route" | cut -d' ' -f1)
        path=$(echo "$route" | cut -d' ' -f2)
        echo "🔹 $method $API_ENDPOINT$path"
    done
    
    echo ""
    echo "────────────────────────────────────────────────"
    echo "📝 Приклади використання:"
    echo ""
    echo "# 1. Завершення замовлення (нарахування бонусів)"
    echo "curl -X POST '$API_ENDPOINT/order-complete' \\"
    echo "  -H 'Content-Type: application/json' \\"
    echo "  -d '{"
    echo "    \"event\": \"order.change_order_status\","
    echo "    \"context\": {"
    echo "      \"id\": \"12345\","
    echo "      \"client_id\": \"67890\","
    echo "      \"grand_total\": 1000,"
    echo "      \"status_name\": \"completed\","
    echo "      \"discount_amount\": 50"
    echo "    }"
    echo "  }'"
    echo ""
    echo "# 2. Скасування замовлення (повернення бонусів)"
    echo "curl -X POST '$API_ENDPOINT/order-cancel' \\"
    echo "  -H 'Content-Type: application/json' \\"
    echo "  -d '{"
    echo "    \"order_id\": \"12345\","
    echo "    \"phone\": \"380123456789\","
    echo "    \"used_bonus_amount\": 50"
    echo "  }'"
    echo ""
    echo "# 3. Резервування бонусів"
    echo "curl -X POST '$API_ENDPOINT/order-reserve' \\"
    echo "  -H 'Content-Type: application/json' \\"
    echo "  -d '{"
    echo "    \"event\": \"order.create\","
    echo "    \"context\": {"
    echo "      \"id\": \"12345\","
    echo "      \"client_id\": \"67890\","
    echo "      \"grand_total\": 1000,"
    echo "      \"status_name\": \"new\","
    echo "      \"discount_amount\": 100,"
    echo "      \"promo_code\": \"BONUS100\""
    echo "    }"
    echo "  }'"
    echo ""
    echo "# 4. Тестування (логування)"
    echo "curl -X POST '$API_ENDPOINT/test-log' \\"
    echo "  -H 'Content-Type: application/json' \\"
    echo "  -d '{\"test\": \"Мій тестовий запит\"}'"
    echo ""
    echo "🔧 Для інтеграції з KeyCRM вебхуками:"
    echo "   - Webhook URL для замовлень: $API_ENDPOINT/order-complete"
    echo "   - Webhook URL для резервування: $API_ENDPOINT/order-reserve"
    echo "   - Webhook URL для лідів: $API_ENDPOINT/lead-reserve"
    echo ""
}

# ==============================================================================
# ФУНКЦІЯ ОЧИЩЕННЯ
# ==============================================================================

cleanup() {
    log_warning "Ця операція видалить API Gateway '$API_NAME'"
    read -p "Ви впевнені? (y/N): " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        existing_api_id=$(aws apigatewayv2 get-apis \
            --region $AWS_REGION \
            --query "Items[?Name=='$API_NAME'].ApiId" \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$existing_api_id" ] && [ "$existing_api_id" != "None" ]; then
            log_info "Видаляємо API Gateway..."
            aws apigatewayv2 delete-api --api-id $existing_api_id --region $AWS_REGION
            log_success "API Gateway видалено"
        else
            log_warning "API Gateway не знайдено"
        fi
    else
        log_info "Операція скасована"
    fi
}

# ==============================================================================
# ФУНКЦІЯ ІНФОРМАЦІЇ
# ==============================================================================

show_info() {
    existing_api_id=$(aws apigatewayv2 get-apis \
        --region $AWS_REGION \
        --query "Items[?Name=='$API_NAME'].ApiId" \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$existing_api_id" ] && [ "$existing_api_id" != "None" ]; then
        API_ID=$existing_api_id
        API_BASE_URL=$(aws apigatewayv2 get-api \
            --api-id $API_ID \
            --region $AWS_REGION \
            --query 'ApiEndpoint' --output text)
        API_ENDPOINT="${API_BASE_URL}/${STAGE_NAME}"
        display_results
    else
        log_warning "API Gateway '$API_NAME' не знайдено"
        log_info "Запустіть: $0 setup"
    fi
}

# ==============================================================================
# ГОЛОВНА ФУНКЦІЯ
# ==============================================================================

main() {
    echo "🚀 Bonus Operations API Gateway Setup"
    echo "======================================"
    echo "Регіон: $AWS_REGION"
    echo "Lambda функція: $FUNCTION_NAME"
    echo "API назва: $API_NAME"
    echo ""
    
    case "${1:-setup}" in
        "setup")
            validate_prerequisites
            create_api_gateway
            create_lambda_integration
            create_routes
            create_stage
            display_results
            
            # Опціонально запускаємо тести
            read -p "Запустити тестування API? (y/N): " test_confirm
            if [ "$test_confirm" = "y" ] || [ "$test_confirm" = "Y" ]; then
                test_api
            fi
            ;;
        "test")
            validate_prerequisites
            show_info
            test_api
            ;;
        "info")
            show_info
            ;;
        "cleanup")
            cleanup
            ;;
        *)
            echo "Використання: $0 [setup|test|info|cleanup]"
            echo ""
            echo "Команди:"
            echo "  setup   - Створити API Gateway для bonus-operations (за замовчуванням)"
            echo "  test    - Протестувати існуючий API"
            echo "  info    - Показати інформацію про існуючий API"
            echo "  cleanup - Видалити API Gateway"
            echo ""
            echo "Ендпоінти які будуть створені:"
            for route in "${ROUTES[@]}"; do
                echo "  - $route"
            done
            exit 1
            ;;
    esac
}

# Запуск головної функції
main "$@"
