CREATE SCHEMA IF NOT EXISTS stage;

CREATE TABLE IF NOT EXISTS stage.crypto_tickers (
    ingestion_time      timestamptz DEFAULT now(),
    id                  text        NOT NULL,
    symbol              text        NOT NULL,
    name                text,
    rank                integer,
    price_usd           numeric,
    volume_24h_usd      numeric,
    market_cap_usd      numeric,
    percent_change_24h  numeric
);
