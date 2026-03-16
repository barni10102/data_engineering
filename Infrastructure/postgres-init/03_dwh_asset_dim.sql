CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.asset_dim (
    asset_id     serial PRIMARY KEY,
    asset_type   text NOT NULL CHECK (asset_type IN ('crypto', 'stock')),
    symbol       text NOT NULL,
    name         text,
    UNIQUE(asset_type, symbol)
);