#!/bin/bash

# Скрипт для запуску Lambda функції з логами
# Включає перевірку блокування перед запуском

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
LOG_GROUP="/aws/lambda/$FUNCTION_NAME"
PAYLOAD_FILE="invoke_payload.json"

echo -e "${BLUE}🚀 Запуск Lambda функції: $FUNCTION_NAME${NC}"
echo -e "${BLUE}⏰ $(date)${NC}"
echo ""

# Перевірка статусу блокування перед запуском
echo -e "${YELLOW}🔍 Перевірка статусу блокування...${NC}"
if ./scripts/check_lock_status.sh > /dev/null 2>&1; then
    # Перевіряємо чи функція заблокована
    locked=$(aws lambda invoke 
        --function-name "$FUNCTION_NAME" 
        --region "$REGION" 
        --payload 'file://lock_status_payload.json' 
        temp_status.json && 
        cat temp_status.json | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print('true' if data.get('locked', False) else 'false')
except:
    print('false')
" 2>/dev/null || echo "false")
    
    rm -f temp_status.json
    
    if [ "$locked" = "true" ]; then
        echo -e "${RED}🔒 Функція вже виконується іншим процесом!${NC}"
        echo -e "${YELLOW}ℹ️  Для детальної інформації: ./scripts/check_lock_status.sh${NC}"
        echo -e "${YELLOW}ℹ️  Для примусового розблокування: ./scripts/force_unlock.sh${NC}"
        exit 1
    else
        echo -e "${GREEN}🔓 Функція доступна для запуску${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Не вдалося перевірити статус блокування, продовжуємо...${NC}"
fi

echo ""

# Отримуємо поточний час для фільтрації логів
START_TIME=$(date +%s)000  # CloudWatch використовує мілісекунди

echo "📡 Викликаємо функцію..."

# Запускаємо функцію в фоновому режимі
aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --payload file://invoke_payload.json \
    --cli-binary-format raw-in-base64-out \
    response.json &

INVOKE_PID=$!

echo "📜 Моніторимо логи в реальному часі..."
echo "----------------------------------------"

# Моніторимо логи
aws logs tail "$LOG_GROUP" --since 1m --follow &
LOGS_PID=$!

# Чекаємо завершення виклику Lambda
wait $INVOKE_PID
INVOKE_EXIT_CODE=$?

# Чекаємо ще трохи для отримання всіх логів
sleep 5

# Зупиняємо моніторинг логів
kill $LOGS_PID 2>/dev/null

echo ""
echo "----------------------------------------"
echo ""

# Виводимо результат функції
echo "📋 Результат виконання:"
if [ -f response.json ]; then
    if command -v jq >/dev/null 2>&1; then
        cat response.json | jq '.' 2>/dev/null || cat response.json
    else
        cat response.json
    fi
else
    echo "⚠️ Файл з результатом не створено"
fi

echo ""
if [ $INVOKE_EXIT_CODE -eq 0 ]; then
    echo "✅ Lambda функція виконана успішно!"
else
    echo "❌ Помилка виконання Lambda функції (код: $INVOKE_EXIT_CODE)"
fi

# Очищуємо тимчасові файли
rm -f response.json
