CREATE SCHEMA IF NOT EXISTS stage;

CREATE TABLE IF NOT EXISTS stage.stock_ohlcv (
    ingestion_time   timestamptz DEFAULT now(),
    symbol           text        NOT NULL,
    name             text,
    datetime         timestamptz NOT NULL,
    open_price       numeric,
    high_price       numeric,
    low_price        numeric,
    close_price      numeric,
    volume           bigint,
    volume_usd       numeric,
    return_24h       numeric
);
