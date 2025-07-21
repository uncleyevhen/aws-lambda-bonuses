#!/bin/bash

# Скрипт для тестування швидкості створення промокодів
# Використання: ./test_speed.sh <сума>

if [ $# -eq 0 ]; then
    echo "❌ Помилка: Потрібно вказати суму"
    echo "Використання: $0 <сума>"
    echo "Приклад: $0 1500"
    exit 1
fi

AMOUNT=$1
FUNCTION_NAME="replenish-promo-code"
S3_BUCKET="lambda-promo-sessions"
S3_KEY="promo-codes/used_codes_count.json"

echo "🎯 Тестування швидкості для суми: $AMOUNT"
echo "⏰ $(date)"
echo ""

# 1. Створюємо тестовий файл з сумою
echo "📝 Створюємо тестовий файл з сумою $AMOUNT..."

# Створюємо JSON з високим значенням для цієї суми (щоб спрацював поріг)
cat > test_data.json << EOF
{
  "$AMOUNT": 25
}
EOF

# 2. Завантажуємо файл в S3
echo "☁️ Завантажуємо дані в S3: s3://$S3_BUCKET/$S3_KEY"
aws s3 cp test_data.json s3://$S3_BUCKET/$S3_KEY

if [ $? -eq 0 ]; then
    echo "✅ Дані успішно завантажено в S3"
else
    echo "❌ Помилка завантаження в S3"
    exit 1
fi

echo ""

# 3. Запускаємо Lambda функцію
echo "🚀 Запуск Lambda функції: $FUNCTION_NAME"
echo ""

# Викликаємо функцію і зберігаємо відповідь
echo "📡 Викликаємо функцію..."

aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    --log-type Tail \
    response.json > invoke_output.json

echo ""

# 4. Декодуємо і виводимо логи
echo "📜 Логи виконання:"
echo "========================================"

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
echo "========================================"
echo ""

# 5. Виводимо результат функції
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
echo "✅ Тест завершено!"

# Очищуємо тимчасові файли
rm -f response.json invoke_output.json test_data.json
