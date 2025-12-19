-- Ініціалізація бази даних
-- Цей скрипт виконується автоматично при першому запуску PostgreSQL контейнера

-- Створюємо розширення для роботи з масивами та UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Таблиця клієнтів (основна)
CREATE TABLE IF NOT EXISTS clients (
    id SERIAL PRIMARY KEY,
    phone VARCHAR(20) UNIQUE NOT NULL,
    bonus_balance INTEGER DEFAULT 0 NOT NULL,
    reserved_balance INTEGER DEFAULT 0 NOT NULL,
    bonus_expiry DATE,
    emails TEXT[] DEFAULT '{}',
    names TEXT[] DEFAULT '{}',
    keycrm_ids INTEGER[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Таблиця транзакцій (історія)
CREATE TABLE IF NOT EXISTS bonus_transactions (
    id SERIAL PRIMARY KEY,
    client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
    type VARCHAR(20) NOT NULL,
    order_id VARCHAR(50),
    lead_id VARCHAR(50),
    amount INTEGER NOT NULL,
    order_total NUMERIC(10,2),
    balance_before INTEGER NOT NULL,
    balance_after INTEGER NOT NULL,
    reserved_before INTEGER DEFAULT 0,
    reserved_after INTEGER DEFAULT 0,
    keycrm_buyer_id INTEGER,
    description VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Індекси для швидкого пошуку
CREATE INDEX IF NOT EXISTS idx_clients_phone ON clients(phone);
CREATE INDEX IF NOT EXISTS idx_clients_keycrm_ids ON clients USING GIN(keycrm_ids);
CREATE INDEX IF NOT EXISTS idx_transactions_client ON bonus_transactions(client_id);
CREATE INDEX IF NOT EXISTS idx_transactions_order ON bonus_transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created ON bonus_transactions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON bonus_transactions(type);

-- Функція для автоматичного оновлення updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Тригер для автоматичного оновлення updated_at
DROP TRIGGER IF EXISTS update_clients_updated_at ON clients;
CREATE TRIGGER update_clients_updated_at
    BEFORE UPDATE ON clients
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Коментарі до таблиць
COMMENT ON TABLE clients IS 'Клієнти бонусної системи. Унікальність за телефоном.';
COMMENT ON TABLE bonus_transactions IS 'Історія всіх операцій з бонусами.';

COMMENT ON COLUMN clients.phone IS 'Нормалізований телефон (380XXXXXXXXX) - унікальний ідентифікатор';
COMMENT ON COLUMN clients.keycrm_ids IS 'Масив всіх buyer_id з KeyCRM для цього клієнта (дублікати)';
COMMENT ON COLUMN bonus_transactions.type IS 'completed, cancelled, reserved, manual_reserve, initial, adjustment, expired';

-- Таблиця промокодів
CREATE TABLE IF NOT EXISTS promo_codes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    amount INTEGER NOT NULL,
    is_used BOOLEAN DEFAULT FALSE,
    used_at TIMESTAMP WITH TIME ZONE,
    used_by_phone VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Індекси для промокодів
CREATE INDEX IF NOT EXISTS idx_promo_codes_amount ON promo_codes(amount);
CREATE INDEX IF NOT EXISTS idx_promo_codes_is_used ON promo_codes(is_used);
CREATE INDEX IF NOT EXISTS idx_promo_codes_amount_available ON promo_codes(amount) WHERE is_used = FALSE;

COMMENT ON TABLE promo_codes IS 'Промокоди для бонусної системи';
COMMENT ON COLUMN promo_codes.code IS 'Унікальний код промокоду';
COMMENT ON COLUMN promo_codes.amount IS 'Сума промокоду в грн';
COMMENT ON COLUMN promo_codes.is_used IS 'Чи використано промокод';

-- Виводимо інформацію про створені таблиці
DO $$
BEGIN
    RAISE NOTICE 'Database initialized successfully!';
    RAISE NOTICE 'Tables: clients, bonus_transactions, promo_codes';
END $$;

