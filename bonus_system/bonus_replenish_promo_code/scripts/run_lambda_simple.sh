#!/bin/bash

# Простий запуск Lambda функції без real-time логів
FUNCTION_NAME="bonus-replenish-promo-prod"
REGION="eu-north-1"
PAYLOAD_FILE="invoke_payload.json"
RESPONSE_FILE="response_output.json"

echo "🚀 Запускаємо Lambda функцію: $FUNCTION_NAME"
echo "📋 Payload: $(cat $PAYLOAD_FILE)"

# Запуск функції
aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --payload "$(cat $PAYLOAD_FILE | tr -d '\n\r')" \
  "$RESPONSE_FILE"

if [ $? -eq 0 ]; then
    echo "✅ Функція запущена успішно"
    echo "📄 Відповідь:"
    cat "$RESPONSE_FILE"
    echo ""
    echo "📊 Розмір файлу відповіді: $(wc -c < "$RESPONSE_FILE") байт"
else
    echo "❌ Помилка запуску функції"
    exit 1
fi
