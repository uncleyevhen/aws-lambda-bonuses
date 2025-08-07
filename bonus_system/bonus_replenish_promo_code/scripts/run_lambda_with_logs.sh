#!/bin/bash

# Запуск Lambda функції з real-time логами через CloudWatch

FUNCTION_NAME="replenish-promo-code"
LOG_GROUP="/aws/lambda/$FUNCTION_NAME"

echo "🚀 Запуск Lambda функції: $FUNCTION_NAME"
echo "⏰ $(date)"
echo ""

# Отримуємо поточний час для фільтрації логів
START_TIME=$(date +%s)000  # CloudWatch використовує мілісекунди

echo "📡 Викликаємо функцію..."

# Запускаємо функцію в фоновому режимі
aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --payload '{}' \
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
