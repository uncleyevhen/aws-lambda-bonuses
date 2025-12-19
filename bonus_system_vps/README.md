# Bonus System VPS

Бонусна система SafeYourLove на FastAPI + PostgreSQL.

## Архітектура

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Сайт/GTM      │────▶│   Nginx (SSL)   │────▶│    FastAPI      │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
┌─────────────────┐                             ┌────────▼────────┐
│     KeyCRM      │◀────── sync ───────────────│   PostgreSQL    │
│   (webhooks)    │────▶                        └─────────────────┘
└─────────────────┘
```

## Швидкий старт

### 1. Налаштування сервера

```bash
# SSH на сервер
ssh root@77.42.76.72

# Оновлення системи
apt update && apt upgrade -y

# Встановлення Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Встановлення Docker Compose
apt install docker-compose-plugin -y

# Перевірка
docker --version
docker compose version
```

### 2. Клонування та налаштування

```bash
# Клонування репозиторію
git clone <your-repo-url> /opt/bonus-system
cd /opt/bonus-system/bonus_system_vps

# Створення .env файлу
cp env.example .env
nano .env  # Заповніть змінні
```

### 3. SSL сертифікат

```bash
# Створюємо директорії
mkdir -p certbot/conf certbot/www nginx/ssl

# Тимчасовий nginx конфіг для certbot
# (замініть nginx.conf тимчасово на HTTP-only версію)

# Отримання сертифікату
docker run -it --rm \
  -v $(pwd)/certbot/conf:/etc/letsencrypt \
  -v $(pwd)/certbot/www:/var/www/certbot \
  -p 80:80 \
  certbot/certbot certonly \
  --standalone \
  -d api.safeyourlove.com \
  --email your@email.com \
  --agree-tos \
  --no-eff-email
```

### 4. Запуск

```bash
# Збірка та запуск
docker compose up -d --build

# Перевірка логів
docker compose logs -f api

# Перевірка статусу
curl https://api.safeyourlove.com/health
```

## API Endpoints

### Webhooks (для KeyCRM)

| Endpoint | Метод | Опис |
|----------|-------|------|
| `/webhooks/order-complete` | POST | Виконання замовлення |
| `/webhooks/order-reserve` | POST | Резервування бонусів |
| `/webhooks/order-cancel` | POST | Скасування замовлення |
| `/webhooks/lead-reserve` | POST | Мануальний резерв через лід |

### Balance (для сайту)

| Endpoint | Метод | Опис |
|----------|-------|------|
| `/balance/check?phone=...` | GET | Перевірка балансу |
| `/balance/full?phone=...` | GET | Повна інформація |
| `/balance/history?phone=...` | GET | Історія операцій |

### Promo (для сайту)

| Endpoint | Метод | Опис |
|----------|-------|------|
| `/promo/get` | POST | Отримати промокод |
| `/promo/status` | GET | Статус промокодів |

### Документація

- Swagger UI: `https://api.safeyourlove.com/docs`
- ReDoc: `https://api.safeyourlove.com/redoc`

## Корисні команди

```bash
# Перезапуск
docker compose restart api

# Перегляд логів
docker compose logs -f api

# Підключення до БД
docker compose exec db psql -U bonus bonus_db

# Backup БД
docker compose exec db pg_dump -U bonus bonus_db > backup.sql

# Оновлення коду
git pull
docker compose up -d --build api
```

## Моніторинг

1. **Uptime Robot** - моніторинг доступності
2. **Sentry** - логування помилок (налаштуйте SENTRY_DSN)
3. **Docker logs** - `docker compose logs -f`

## Міграція з AWS Lambda

Див. скрипт `scripts/migrate_from_keycrm.py` для міграції існуючих клієнтів.

