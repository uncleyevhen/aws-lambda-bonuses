#!/bin/bash
# =============================================================================
# Деплой Bonus System VPS
# =============================================================================
# Використання: ./deploy.sh [опції]
#
# Опції:
#   --build     Перебілдити Docker образи
#   --restart   Тільки перезапустити контейнери (без білду)
#   --logs      Показати логи після деплою
#   --status    Тільки перевірити статус
#   --sync      Тільки синхронізувати файли (без перезапуску)
# =============================================================================

set -e

# Кольори для виводу
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Конфігурація
VPS_HOST="77.42.76.72"
VPS_USER="root"
SSH_KEY="$HOME/.ssh/bonus-system-key"
REMOTE_PATH="/opt/bonus-system"
LOCAL_PATH="$(cd "$(dirname "$0")/.." && pwd)"  # bonus_system_vps папка

# SSH команда
SSH_CMD="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=30"
SCP_CMD="scp -i $SSH_KEY -o StrictHostKeyChecking=no"

# =============================================================================
# Функції
# =============================================================================

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

check_ssh_key() {
    if [ ! -f "$SSH_KEY" ]; then
        log_error "SSH ключ не знайдено: $SSH_KEY"
        echo "Створіть ключ або вкажіть правильний шлях"
        exit 1
    fi
}

check_connection() {
    log_info "Перевірка з'єднання з VPS..."
    if $SSH_CMD $VPS_USER@$VPS_HOST "echo 'OK'" > /dev/null 2>&1; then
        log_success "З'єднання встановлено"
        return 0
    else
        log_error "Не вдалося підключитися до VPS"
        exit 1
    fi
}

sync_files() {
    log_info "Синхронізація файлів на VPS..."
    
    # Створюємо папку якщо не існує
    $SSH_CMD $VPS_USER@$VPS_HOST "mkdir -p $REMOTE_PATH"
    
    # Синхронізуємо через rsync
    rsync -avz --progress \
        --exclude '.git' \
        --exclude '__pycache__' \
        --exclude '*.pyc' \
        --exclude '.env' \
        --exclude 'certbot' \
        --exclude 'nginx/ssl' \
        --exclude '*.log' \
        --exclude 'venv' \
        --exclude '.venv' \
        -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        "$LOCAL_PATH/" \
        "$VPS_USER@$VPS_HOST:$REMOTE_PATH/"
    
    log_success "Файли синхронізовано"
}

build_and_restart() {
    log_info "Збірка та перезапуск Docker контейнерів..."
    
    $SSH_CMD $VPS_USER@$VPS_HOST << EOF
cd $REMOTE_PATH
docker compose up -d --build api
EOF
    
    log_success "Контейнери перезапущено"
}

restart_only() {
    log_info "Перезапуск Docker контейнерів..."
    
    $SSH_CMD $VPS_USER@$VPS_HOST << EOF
cd $REMOTE_PATH
docker compose restart api
EOF
    
    log_success "Контейнери перезапущено"
}

show_status() {
    log_info "Статус VPS..."
    echo ""
    
    # Docker статус
    echo -e "${YELLOW}=== Docker контейнери ===${NC}"
    $SSH_CMD $VPS_USER@$VPS_HOST "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"
    echo ""
    
    # API health check
    echo -e "${YELLOW}=== API Health ===${NC}"
    if curl -s --connect-timeout 5 https://api.safeyourlove.com/health | python3 -m json.tool 2>/dev/null; then
        log_success "API працює"
    else
        log_error "API недоступний"
    fi
    echo ""
    
    # Promo status
    echo -e "${YELLOW}=== Промокоди ===${NC}"
    curl -s https://api.safeyourlove.com/promo/status | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f\"Доступно: {d['total_available']}  |  Використано: {d['total_used']}\")" 2>/dev/null || echo "Не вдалося отримати статус"
}

show_logs() {
    log_info "Останні логи API..."
    $SSH_CMD $VPS_USER@$VPS_HOST "cd $REMOTE_PATH && docker compose logs --tail=50 api"
}

# =============================================================================
# Основна логіка
# =============================================================================

echo ""
echo -e "${BLUE}🚀 Bonus System VPS Deploy${NC}"
echo "=================================="
echo ""

check_ssh_key

# Парсинг аргументів
ACTION="full"
SHOW_LOGS=false

for arg in "$@"; do
    case $arg in
        --build)
            ACTION="build"
            ;;
        --restart)
            ACTION="restart"
            ;;
        --logs)
            SHOW_LOGS=true
            ;;
        --status)
            ACTION="status"
            ;;
        --sync)
            ACTION="sync"
            ;;
        --help|-h)
            echo "Використання: ./deploy.sh [опції]"
            echo ""
            echo "Опції:"
            echo "  --build     Синхронізувати + перебілдити Docker"
            echo "  --restart   Тільки перезапустити (без білду)"
            echo "  --sync      Тільки синхронізувати файли"
            echo "  --status    Показати статус VPS"
            echo "  --logs      Показати логи після деплою"
            echo "  --help      Показати цю довідку"
            echo ""
            echo "Без опцій: повний деплой (sync + build)"
            exit 0
            ;;
    esac
done

case $ACTION in
    status)
        check_connection
        show_status
        ;;
    sync)
        check_connection
        sync_files
        show_status
        ;;
    restart)
        check_connection
        restart_only
        sleep 3
        show_status
        ;;
    build)
        check_connection
        sync_files
        build_and_restart
        sleep 5
        show_status
        ;;
    full)
        check_connection
        sync_files
        build_and_restart
        sleep 5
        show_status
        ;;
esac

if [ "$SHOW_LOGS" = true ]; then
    echo ""
    show_logs
fi

echo ""
log_success "Деплой завершено!"
echo ""
