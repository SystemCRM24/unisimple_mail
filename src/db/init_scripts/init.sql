CREATE TABLE IF NOT EXISTS state_purchases (
    id SERIAL PRIMARY KEY,
    eis_url TEXT,
    winner_name TEXT,
    inn TEXT,
    time_zone TEXT,
    result_date DATE,
    customer_name TEXT,
    nmck NUMERIC(15, 2),
    contract_securing NUMERIC(15, 2),
    warranty_obligations_securing NUMERIC(15, 2),
    contract_end_date DATE,
    winner_price NUMERIC(15, 2),
    phone_1 TEXT,
    fio_1 TEXT,
    email_1 TEXT,
    phone_2 TEXT,
    fio_2 TEXT,
    email_2 TEXT,
    phone_3 TEXT,
    fio_3 TEXT,
    email_3 TEXT,
    smp_advantages TEXT,
    smp_status TEXT,
    extraction_dt TIMESTAMP WITH TIME ZONE NOT NULL,
    purchase_number TEXT UNIQUE NOT NULL
);

CREATE UNIQUE INDEX idx_state_purchase_purchase_number ON state_purchases (purchase_number);

COMMENT ON TABLE state_purchases IS 'Таблица для хранения информации о государственных закупках';
COMMENT ON COLUMN state_purchases.extraction_dt IS 'Дата и время выгрузки данных';
COMMENT ON COLUMN state_purchases.purchase_number IS 'Уникальный номер закупки';