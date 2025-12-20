# Dockerfile для Bonus System API
FROM python:3.11-slim

# Встановлюємо системні залежності
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    # Залежності для Playwright
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    && rm -rf /var/lib/apt/lists/*

# Встановлюємо робочу директорію
WORKDIR /app

# Копіюємо файли залежностей
COPY requirements.txt .

# Встановлюємо Python залежності
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо код додатку
COPY . .

# Створюємо непривілейованого користувача
RUN adduser --disabled-password --gecos '' appuser

# Встановлюємо Playwright браузери як appuser
USER appuser
RUN playwright install chromium

# Повертаємось до root для chown
USER root

# Створюємо папку для логів з правильними правами
RUN mkdir -p /app/logs && chown -R appuser:appuser /app

# Запуск як appuser
USER appuser

# Порт
EXPOSE 8000

# Команда запуску
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

