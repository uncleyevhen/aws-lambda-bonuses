#!/bin/bash

# Скрипт для моніторингу логів Lambda функцій у реальному часі

set -e

# Кольори
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
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

# Функція для показу меню
show_menu() {
    echo -e "${PURPLE}📋 МОНІТОРИНГ ЛОГІВ LAMBDA ФУНКЦІЙ${NC}"
    echo "======================================="
    echo "1) 📱 get-promo-code логи"
    echo "2) 🔄 replenish-promo-code логи"
    echo "3) 👥 Обидві функції (розділені екрани)"
    echo "4) 🔍 Пошук в логах get-promo-code"
    echo "5) 🔍 Пошук в логах replenish-promo-code"
    echo "6) 📊 Статистика викликів функцій"
    echo "7) ❌ Вихід"
    echo ""
    echo -n "Виберіть опцію (1-7): "
}

# Функція для моніторингу логів однієї функції
monitor_function_logs() {
    local function_name="$1"
    local log_group="/aws/lambda/${function_name}"
    
    log_info "Моніторинг логів для функції: ${function_name}"
    log_info "Натисніть Ctrl+C для зупинки"
    echo ""
    
    # Перевіряємо, чи існує log group
    if ! aws logs describe-log-groups --log-group-name-prefix "$log_group" --query 'logGroups[0].logGroupName' --output text 2>/dev/null | grep -q "$log_group"; then
        log_warning "Log group $log_group не знайдено!"
        log_info "Можливо функція ще не викликалася або має іншу назву"
        return 1
    fi
    
    # Запускаємо tail логів
    aws logs tail "$log_group" --follow --format short
}

# Функція для пошуку в логах
search_logs() {
    local function_name="$1"
    local log_group="/aws/lambda/${function_name}"
    
    echo -n "Введіть пошуковий запит: "
    read search_query
    
    if [ -z "$search_query" ]; then
        log_warning "Пошуковий запит не може бути порожнім"
        return 1
    fi
    
    log_info "Пошук '$search_query' в логах функції $function_name за останню годину..."
    
    # Пошук за останню годину
    local start_time=$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%S')
    
    aws logs filter-log-events \
        --log-group-name "$log_group" \
        --start-time "$(date -d '1 hour ago' +%s)000" \
        --filter-pattern "$search_query" \
        --query 'events[].message' \
        --output text
}

# Функція для отримання статистики
get_function_stats() {
    local function_name="$1"
    
    log_info "Статистика для функції: $function_name"
    
    # CloudWatch метрики за останні 24 години
    local end_time=$(date -u +%Y-%m-%dT%H:%M:%S)
    local start_time=$(date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%S)
    
    echo "📊 Метрики за останні 24 години:"
    
    # Кількість викликів
    local invocations=$(aws cloudwatch get-metric-statistics \
        --namespace AWS/Lambda \
        --metric-name Invocations \
        --dimensions Name=FunctionName,Value="$function_name" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 86400 \
        --statistics Sum \
        --query 'Datapoints[0].Sum' \
        --output text 2>/dev/null)
    
    if [ "$invocations" != "None" ] && [ -n "$invocations" ]; then
        echo "  🔢 Всього викликів: $invocations"
    else
        echo "  🔢 Всього викликів: 0"
    fi
    
    # Помилки
    local errors=$(aws cloudwatch get-metric-statistics \
        --namespace AWS/Lambda \
        --metric-name Errors \
        --dimensions Name=FunctionName,Value="$function_name" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 86400 \
        --statistics Sum \
        --query 'Datapoints[0].Sum' \
        --output text 2>/dev/null)
    
    if [ "$errors" != "None" ] && [ -n "$errors" ]; then
        echo "  ❌ Помилки: $errors"
    else
        echo "  ❌ Помилки: 0"
    fi
    
    # Середня тривалість
    local duration=$(aws cloudwatch get-metric-statistics \
        --namespace AWS/Lambda \
        --metric-name Duration \
        --dimensions Name=FunctionName,Value="$function_name" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 86400 \
        --statistics Average \
        --query 'Datapoints[0].Average' \
        --output text 2>/dev/null)
    
    if [ "$duration" != "None" ] && [ -n "$duration" ]; then
        echo "  ⏱️ Середня тривалість: ${duration} мс"
    else
        echo "  ⏱️ Середня тривалість: немає даних"
    fi
}

# Функція для моніторингу обох функцій у розділених екранах
monitor_both_functions() {
    log_info "Запуск моніторингу обох функцій..."
    log_info "Використовується tmux для розділених екранів"
    
    # Перевіряємо, чи встановлений tmux
    if ! command -v tmux &> /dev/null; then
        log_warning "tmux не встановлено!"
        log_info "Встановіть tmux: brew install tmux (macOS) або apt install tmux (Ubuntu)"
        log_info "Або використовуйте окремі термінали для функцій 1 і 2"
        return 1
    fi
    
    # Створюємо нову tmux сесію
    local session_name="lambda-monitoring-$(date +%s)"
    
    tmux new-session -d -s "$session_name"
    
    # Розділяємо екран горизонтально
    tmux split-window -h
    
    # У першому вікні - get-promo-code
    tmux send-keys -t "$session_name:0.0" "aws logs tail /aws/lambda/get-promo-code --follow --format short" Enter
    
    # У другому вікні - replenish-promo-code  
    tmux send-keys -t "$session_name:0.1" "aws logs tail /aws/lambda/replenish-promo-code --follow --format short" Enter
    
    # Додаємо заголовки
    tmux send-keys -t "$session_name:0.0" "echo 'GET-PROMO-CODE LOGS:'" Enter
    tmux send-keys -t "$session_name:0.1" "echo 'REPLENISH-PROMO-CODE LOGS:'" Enter
    
    # Приєднуємося до сесії
    log_success "Запущено моніторинг у tmux сесії: $session_name"
    log_info "Натисніть Ctrl+B, потім D щоб відключитися від сесії"
    log_info "Щоб повернутися: tmux attach -t $session_name"
    
    tmux attach -t "$session_name"
}

# Головна функція
main() {
    while true; do
        show_menu
        read choice
        
        case $choice in
            1)
                echo ""
                monitor_function_logs "get-promo-code"
                ;;
            2)
                echo ""
                monitor_function_logs "replenish-promo-code"
                ;;
            3)
                echo ""
                monitor_both_functions
                ;;
            4)
                echo ""
                search_logs "get-promo-code"
                echo ""
                echo "Натисніть Enter для продовження..."
                read
                ;;
            5)
                echo ""
                search_logs "replenish-promo-code"
                echo ""
                echo "Натисніть Enter для продовження..."
                read
                ;;
            6)
                echo ""
                log_info "📊 СТАТИСТИКА ФУНКЦІЙ"
                echo "====================="
                get_function_stats "get-promo-code"
                echo ""
                get_function_stats "replenish-promo-code"
                echo ""
                echo "Натисніть Enter для продовження..."
                read
                ;;
            7)
                log_success "До побачення!"
                exit 0
                ;;
            *)
                log_warning "Невірний вибір. Спробуйте ще раз."
                ;;
        esac
        
        echo ""
    done
}

# Перевіряємо AWS CLI
if ! command -v aws &> /dev/null; then
    log_warning "AWS CLI не встановлено!"
    exit 1
fi

if ! aws sts get-caller-identity &> /dev/null; then
    log_warning "AWS credentials не налаштовані!"
    exit 1
fi

# Запуск
main "$@"
