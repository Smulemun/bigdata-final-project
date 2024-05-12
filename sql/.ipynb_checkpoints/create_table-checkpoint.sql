DROP TABLE IF EXISTS ecom;

CREATE TABLE ecom (
    event_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    product_id INTEGER,
    category_id BIGINT,
    category_code VARCHAR(100),
    brand VARCHAR(100),
    price FLOAT,
    user_id INTEGER,
    user_session VARCHAR(100)
);

DROP TABLE IF EXISTS ecom_large;

CREATE TABLE ecom_large (
    event_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    product_id INTEGER,
    category_id BIGINT,
    category_code VARCHAR(100),
    brand VARCHAR(100),
    price FLOAT,
    user_id INTEGER,
    user_session VARCHAR(100)
);

ALTER DATABASE team16_projectdb SET datestyle TO iso, ymd;