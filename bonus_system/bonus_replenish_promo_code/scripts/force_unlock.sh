#!/bin/bash

# Скрипт для примусового розблокування Lambda функції
# ВИКОРИСТОВУВАТИ ОБЕРЕЖНО! Тільки якщо функція зависла

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
PAYLOAD_FILE="force_unlock_payload.json"

echo -e "${RED}⚠️  ПРИМУСОВЕ РОЗБЛОКУВАННЯ Lambda функції: ${FUNCTION_NAME}${NC}"
echo -e "${YELLOW}⚠️  ВИКОРИСТОВУВАТИ ТІЛЬКИ ЯКЩО ФУНКЦІЯ ЗАВИСЛА!${NC}"
echo "⏰ $(date)"
echo ""

# Підтвердження від користувача
read -p "Ви впевнені що хочете примусово розблокувати функцію? (yes/no): " -r
echo
if [[ ! $REPLY =~ ^[Yy]es$ ]]; then
    echo -e "${BLUE}ℹ️  Операцію скасовано${NC}"
    exit 0
fi

# Перевіряємо наявність payload файлу
if [ ! -f "$PAYLOAD_FILE" ]; then
    echo -e "${RED}❌ Файл payload не знайдено: $PAYLOAD_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}🔓 Примусове розблокування...${NC}"

# Викликаємо Lambda функцію
if aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION" \
    --payload "file://$PAYLOAD_FILE" \
    response_output.json; then
    
    echo -e "${GREEN}✅ Команда розблокування надіслана${NC}"
    echo ""
    
    # Показуємо результат
    if [ -f response_output.json ]; then
        echo -e "${BLUE}📋 Результат розблокування:${NC}"
        cat response_output.json | python3 -m json.tool
        echo ""
        
        # Перевіряємо статус
        status=$(cat response_output.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(data.get('status', 'unknown'))
" 2>/dev/null || echo "unknown")
        
        if [ "$status" = "success" ]; then
            echo -e "${GREEN}🎉 Функція успішно розблокована!${NC}"
        else
            echo -e "${RED}❌ Не вдалося розблокувати функцію${NC}"
        fi
        
    else
        echo -e "${RED}❌ Файл відповіді не знайдено${NC}"
    fi
    
else
    echo -e "${RED}❌ Помилка виклику Lambda функції${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}ℹ️  Перевірити поточний статус: ./scripts/check_lock_status.sh${NC}"
