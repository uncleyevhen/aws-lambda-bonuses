# syntax=docker/dockerfile:1
FROM public.ecr.aws/lambda/python:3.12

# Встановлюємо системні залежності для Chromium, які рідко змінюються
# Використовуємо microdnf, оскільки базовий образ - Amazon Linux
RUN microdnf install -y \
    mesa-libGL \
    wget \
    unzip \
    fontconfig \
    libX11 \
    libXcomposite \
    libXdamage \
    libXext \
    libXfixes \
    libXrandr \
    libXrender \
    libXtst \
    libxkbcommon \
    alsa-lib \
    at-spi2-atk \
    at-spi2-core \
    cups-libs \
    gtk3 \
    libdrm \
    libxshmfence \
    mesa-libgbm \
    nss \
    xorg-x11-server-Xvfb

# Встановлюємо робочу директорію
WORKDIR ${LAMBDA_TASK_ROOT}

# Встановлюємо змінну середовища для Playwright браузерів в постійній директорії
# /opt зберігається між викликами Lambda, на відміну від /tmp
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers

# Спочатку копіюємо тільки файл залежностей
COPY requirements.txt .

# Встановлюємо залежності. Цей шар буде кешуватися, якщо requirements.txt не змінився.
RUN pip install --no-cache-dir -r requirements.txt

# Встановлюємо Playwright браузери в постійну директорію
# Без --with-deps оскільки ми вже встановили системні залежності через microdnf
RUN python -m playwright install chromium

# Тепер копіюємо решту коду
COPY . .

# Визначаємо команду для запуску
# Використовуємо стандартний AWS Lambda handler
CMD [ "lambda_function.lambda_handler" ]
