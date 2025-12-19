#!/bin/bash

# Запуск Lambda функції з real-time логами

FUNCTION_NAME="bonus-replenish-promo-prod"
AWS_REGION="eu-north-1"

echo "🚀 Запуск Lambda функції: $FUNCTION_NAME"
echo "⏰ $(date)"
echo ""

# Викликаємо функцію і зберігаємо відповідь
echo "📡 Викликаємо функцію..."

aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --payload file://invoke_payload.json \
    --cli-binary-format raw-in-base64-out \
    --log-type Tail \
    response.json > invoke_output.json

echo ""

# Декодуємо і виводимо логи
echo "📜 Логи виконання:"
echo "----------------------------------------"

if [ -f invoke_output.json ]; then
    # Витягуємо LogResult і декодуємо base64
    LOG_RESULT=$(cat invoke_output.json | jq -r '.LogResult // empty' 2>/dev/null)
    
    if [ -n "$LOG_RESULT" ] && [ "$LOG_RESULT" != "null" ]; then
        echo "$LOG_RESULT" | base64 -d
    else
        echo "⚠️ Логи не знайдені в відповіді"
    fi
else
    echo "⚠️ Файл з результатом виклику не створено"
fi

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
echo "✅ Готово!"

# Очищуємо тимчасові файли
rm -f response.json invoke_output.json
