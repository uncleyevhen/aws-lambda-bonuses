#!/bin/bash

# Скрипт для перевірки статусу блокування Lambda функції

set -e

# Кольори для виводу
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Конфігурація
FUNCTION_NAME="bonus-replenish-promo-prod"
REGION="eu-north-1"
PAYLOAD_FILE="lock_status_payload.json"

echo -e "${BLUE}🔍 Перевірка статусу блокування Lambda функції: ${FUNCTION_NAME}${NC}"
echo "⏰ $(date)"

# Перевіряємо наявність payload файлу
if [ ! -f "$PAYLOAD_FILE" ]; then
    echo -e "${RED}❌ Файл payload не знайдено: $PAYLOAD_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}📡 Викликаємо функцію для перевірки статусу...${NC}"

# Викликаємо Lambda функцію
if aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION" \
    --payload "file://$PAYLOAD_FILE" \
    response_output.json; then
    
    echo -e "${GREEN}✅ Виклик Lambda функції успішний${NC}"
    echo ""
    
    # Показуємо результат
    if [ -f response_output.json ]; then
        echo -e "${BLUE}📋 Статус блокування:${NC}"
        cat response_output.json | python3 -m json.tool
        echo ""
        
        # Парсимо результат для кращого відображення
        locked=$(cat response_output.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
print('true' if data.get('locked', False) else 'false')
" 2>/dev/null || echo "unknown")
        
        if [ "$locked" = "true" ]; then
            echo -e "${RED}🔒 Функція заблокована - виконується іншим процесом${NC}"
            
            # Додаткова інформація про блокування
            execution_id=$(cat response_output.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(data.get('execution_id', 'unknown'))
" 2>/dev/null || echo "unknown")
            
            remaining_time=$(cat response_output.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f\"{data.get('remaining_time', 0):.0f}\")
" 2>/dev/null || echo "0")
            
            echo -e "${YELLOW}🔑 Execution ID: ${execution_id}${NC}"
            echo -e "${YELLOW}⏱️  Очікуваний час звільнення: ${remaining_time} секунд${NC}"
            
        elif [ "$locked" = "false" ]; then
            echo -e "${GREEN}🔓 Функція розблокована - доступна для виконання${NC}"
        else
            echo -e "${YELLOW}❓ Статус блокування невизначений${NC}"
        fi
        
    else
        echo -e "${RED}❌ Файл відповіді не знайдено${NC}"
    fi
    
else
    echo -e "${RED}❌ Помилка виклику Lambda функції${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}ℹ️  Для розблокування (ОБЕРЕЖНО): ./scripts/force_unlock.sh${NC}"
