#!/bin/bash

echo "🐳 Локальне тестування Lambda функції в Docker"
echo "=============================================="

# Зупиняємо та видаляємо попередні контейнери
echo "🧹 Очищення попередніх контейнерів..."
docker stop replenish-local 2>/dev/null || true
docker rm replenish-local 2>/dev/null || true

# Збираємо образ
echo "🔨 Збираємо Docker образ..."
docker build -t replenish-promo-local .

if [ $? -ne 0 ]; then
    echo "❌ Помилка збірки образу"
    exit 1
fi

echo "✅ Образ зібрано успішно"

# Запускаємо контейнер з локальним тестом
echo "🚀 Запускаємо локальний тест..."
echo "📊 Всі логи будуть показані нижче:"
echo "=============================================="

docker run --name replenish-local \
    -it \
    --rm \
    --entrypoint="" \
    replenish-promo-local \
    python test_local.py

echo "=============================================="
echo "🏁 Локальний тест завершено"
