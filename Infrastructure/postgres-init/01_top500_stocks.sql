CREATE SCHEMA IF NOT EXISTS config;

CREATE TABLE IF NOT EXISTS config.stock_universe (
    symbol     text PRIMARY KEY,
    name       text,
    marketcap  numeric
);

CREATE TABLE IF NOT EXISTS config.stock_universe_raw (
    exchange            text,
    symbol              text,
    shortname           text,
    longname            text,
    sector              text,
    industry            text,
    currentprice        numeric,
    marketcap           numeric,
    ebitda              numeric,
    revenuegrowth       numeric,
    city                text,
    state               text,
    country             text,
    fulltimeemployees   numeric,
    longbusinesssummary text,
    weight              numeric
);

\copy config.stock_universe_raw FROM '/docker-entrypoint-initdb.d/sp500_companies.csv' CSV HEADER;

INSERT INTO config.stock_universe (symbol, name, marketcap)
SELECT
    symbol,
    shortname AS name,
    marketcap
FROM config.stock_universe_raw
ON CONFLICT (symbol) DO UPDATE
SET
    name      = EXCLUDED.name,
    marketcap = EXCLUDED.marketcap;