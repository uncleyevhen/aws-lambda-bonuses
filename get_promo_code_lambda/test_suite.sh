#!/bin/bash

# Головний скрипт для тестування get-promo-code ↔ replenish інтеграції

set -e

# Кольори
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

log_header() {
    echo -e "${PURPLE}$1${NC}"
}

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

# Функція показу меню
show_menu() {
    echo -e "${CYAN}🧪 ТЕСТУВАННЯ GET-PROMO-CODE ↔ REPLENISH ІНТЕГРАЦІЇ${NC}"
    echo "========================================================="
    echo ""
    echo "🔧 ПІДГОТОВКА:"
    echo "  1) Налаштувати тестування (знайти API endpoint)"
    echo "  2) Перевірити стан S3 (промокоди та лічильники)"
    echo "  3) Додати тестові промокоди"
    echo ""
    echo "🧪 ТЕСТУВАННЯ:"
    echo "  4) Швидкий тест API"
    echo "  5) Повний тест інтеграції"
    echo "  6) Тест тільки репленіш функції"
    echo ""
    echo "📊 МОНІТОРИНГ:"
    echo "  7) Моніторинг логів Lambda функцій"
    echo "  8) Статус функцій та метрики"
    echo ""
    echo "🔄 УТИЛІТИ:"
    echo "  9) Скинути лічильники використання"
    echo " 10) Примусово запустити поповнення"
    echo " 11) Показати інструкцію"
    echo ""
    echo " 12) ❌ Вихід"
    echo ""
    echo -n "Виберіть опцію (1-12): "
}

# Функція для перевірки залежностей
check_dependencies() {
    local missing_deps=()
    
    # Перевіряємо AWS CLI
    if ! command -v aws &> /dev/null; then
        missing_deps+=("aws-cli")
    fi
    
    # Перевіряємо Python
    if ! command -v python3 &> /dev/null; then
        missing_deps+=("python3")
    fi
    
    # Перевіряємо curl
    if ! command -v curl &> /dev/null; then
        missing_deps+=("curl")
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        log_error "Відсутні залежності: ${missing_deps[*]}"
        log_info "Встановіть їх та спробуйте знову"
        return 1
    fi
    
    # Перевіряємо AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials не налаштовані!"
        log_info "Налаштуйте credentials: aws configure"
        return 1
    fi
    
    return 0
}

# Функція для налаштування тестування
setup_testing() {
    log_header "🔧 НАЛАШТУВАННЯ ТЕСТУВАННЯ"
    echo ""
    
    if [ -f "./setup_testing.sh" ]; then
        ./setup_testing.sh
    else
        log_error "Скрипт setup_testing.sh не знайдено"
        return 1
    fi
}

# Функція для перевірки S3
check_s3_status() {
    log_header "📊 ПЕРЕВІРКА СТАНУ S3"
    echo ""
    
    if [ -f "./check_s3_state.py" ]; then
        python3 check_s3_state.py --verbose
    else
        log_error "Скрипт check_s3_state.py не знайдено"
        return 1
    fi
}

# Функція для додавання тестових промокодів
add_test_codes() {
    log_header "➕ ДОДАВАННЯ ТЕСТОВИХ ПРОМОКОДІВ"
    echo ""
    
    echo -n "Введіть суму (грн): "
    read amount
    echo -n "Введіть кількість кодів: "
    read count
    
    if [[ ! "$amount" =~ ^[0-9]+$ ]] || [[ ! "$count" =~ ^[0-9]+$ ]]; then
        log_error "Сума та кількість мають бути числами"
        return 1
    fi
    
    python3 check_s3_state.py --add-test "$amount" "$count"
}

# Функція для швидкого тесту API
quick_api_test() {
    log_header "🚀 ШВИДКИЙ ТЕСТ API"
    echo ""
    
    if [ -f "./test_api_quick.py" ]; then
        python3 test_api_quick.py
    else
        log_error "Скрипт test_api_quick.py не знайдено"
        return 1
    fi
}

# Функція для повного тесту інтеграції
full_integration_test() {
    log_header "🧪 ПОВНИЙ ТЕСТ ІНТЕГРАЦІЇ"
    echo ""
    
    log_warning "Це може зайняти кілька хвилин..."
    echo ""
    
    if [ -f "./test_lambda_integration.py" ]; then
        python3 test_lambda_integration.py
    else
        log_error "Скрипт test_lambda_integration.py не знайдено"
        return 1
    fi
}

# Функція для тестування тільки репленіш
test_replenish_only() {
    log_header "🔄 ТЕСТ РЕПЛЕНІШ ФУНКЦІЇ"
    echo ""
    
    log_info "Запускаємо replenish-promo-code функцію напряму..."
    
    # Створюємо payload для тестування
    local payload='{"trigger_source": "manual_test", "trigger_reasons": ["testing_from_script"]}'
    
    # Викликаємо функцію
    local response_file="/tmp/replenish_test_response.json"
    
    if aws lambda invoke \
        --function-name replenish-promo-code \
        --payload "$payload" \
        "$response_file" >/dev/null 2>&1; then
        
        log_success "Функція запущена!"
        log_info "Відповідь збережена в $response_file"
        
        if [ -f "$response_file" ]; then
            echo ""
            log_info "Відповідь функції:"
            cat "$response_file" | jq . 2>/dev/null || cat "$response_file"
        fi
        
        log_info "Перевіряйте логи для деталей:"
        log_info "aws logs tail /aws/lambda/replenish-promo-code --follow"
        
    else
        log_error "Не вдалося викликати replenish-promo-code функцію"
        return 1
    fi
}

# Функція для моніторингу логів
monitor_logs() {
    log_header "📋 МОНІТОРИНГ ЛОГІВ"
    echo ""
    
    if [ -f "./monitor_logs.sh" ]; then
        ./monitor_logs.sh
    else
        log_error "Скрипт monitor_logs.sh не знайдено"
        return 1
    fi
}

# Функція для показу статусу функцій
show_function_status() {
    log_header "📊 СТАТУС LAMBDA ФУНКЦІЙ"
    echo ""
    
    local functions=("get-promo-code" "replenish-promo-code")
    
    for func in "${functions[@]}"; do
        echo "🔍 Перевірка функції: $func"
        
        # Перевіряємо існування функції
        if aws lambda get-function --function-name "$func" >/dev/null 2>&1; then
            # Отримуємо базову інформацію
            local runtime=$(aws lambda get-function-configuration --function-name "$func" --query 'Runtime' --output text)
            local state=$(aws lambda get-function-configuration --function-name "$func" --query 'State' --output text)
            local last_modified=$(aws lambda get-function-configuration --function-name "$func" --query 'LastModified' --output text)
            
            echo "  ✅ Статус: $state"
            echo "  🐍 Runtime: $runtime"
            echo "  📅 Останнє оновлення: $last_modified"
            
            # CloudWatch метрики за останню добу
            local end_time=$(date -u +%Y-%m-%dT%H:%M:%S)
            local start_time=$(date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%S)
            
            local invocations=$(aws cloudwatch get-metric-statistics \
                --namespace AWS/Lambda \
                --metric-name Invocations \
                --dimensions Name=FunctionName,Value="$func" \
                --start-time "$start_time" \
                --end-time "$end_time" \
                --period 86400 \
                --statistics Sum \
                --query 'Datapoints[0].Sum' \
                --output text 2>/dev/null)
            
            if [ "$invocations" != "None" ] && [ -n "$invocations" ]; then
                echo "  📞 Викликів за 24 год: $invocations"
            else
                echo "  📞 Викликів за 24 год: 0"
            fi
            
        else
            echo "  ❌ Функція не знайдена або немає доступу"
        fi
        
        echo ""
    done
}

# Функція для скидання лічильників
reset_counters() {
    log_header "🔄 СКИДАННЯ ЛІЧИЛЬНИКІВ"
    echo ""
    
    log_warning "Це скине всі лічильники використання промокодів"
    echo -n "Продовжити? (y/N): "
    read confirm
    
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        python3 check_s3_state.py --reset
    else
        log_info "Скасовано"
    fi
}

# Функція для примусового запуску поповнення
force_replenish() {
    log_header "🚀 ПРИМУСОВИЙ ЗАПУСК ПОПОВНЕННЯ"
    echo ""
    
    log_warning "Це запустить поповнення промокодів незалежно від лічильників"
    echo -n "Продовжити? (y/N): "
    read confirm
    
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        test_replenish_only
    else
        log_info "Скасовано"
    fi
}

# Функція для показу інструкції
show_instructions() {
    log_header "📖 ІНСТРУКЦІЯ"
    echo ""
    
    if [ -f "./TESTING_GUIDE.md" ]; then
        if command -v bat &> /dev/null; then
            bat TESTING_GUIDE.md
        elif command -v less &> /dev/null; then
            less TESTING_GUIDE.md
        else
            cat TESTING_GUIDE.md
        fi
    else
        log_error "Файл TESTING_GUIDE.md не знайдено"
        log_info "Базова інструкція:"
        echo ""
        echo "1. Спочатку налаштуйте тестування (опція 1)"
        echo "2. Перевірте стан S3 (опція 2)"
        echo "3. Додайте тестові промокоди якщо потрібно (опція 3)"
        echo "4. Запустіть швидкий тест API (опція 4)"
        echo "5. Якщо API працює, запустіть повний тест (опція 5)"
        echo "6. Використовуйте моніторинг логів (опція 7) для відстеження"
    fi
}

# Головна функція
main() {
    # Перевіряємо залежності
    if ! check_dependencies; then
        exit 1
    fi
    
    while true; do
        echo ""
        show_menu
        read choice
        echo ""
        
        case $choice in
            1)
                setup_testing
                ;;
            2)
                check_s3_status
                ;;
            3)
                add_test_codes
                ;;
            4)
                quick_api_test
                ;;
            5)
                full_integration_test
                ;;
            6)
                test_replenish_only
                ;;
            7)
                monitor_logs
                ;;
            8)
                show_function_status
                ;;
            9)
                reset_counters
                ;;
            10)
                force_replenish
                ;;
            11)
                show_instructions
                ;;
            12)
                log_success "До побачення!"
                exit 0
                ;;
            *)
                log_warning "Невірний вибір. Спробуйте ще раз."
                ;;
        esac
        
        echo ""
        echo "Натисніть Enter для продовження..."
        read
    done
}

# Запуск
main "$@"
