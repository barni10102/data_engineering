CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.intraday_price_fact (
    asset_id    int         NOT NULL REFERENCES dwh.asset_dim(asset_id),
    snapshot_ts timestamptz NOT NULL,
    close_price numeric,
    volume      numeric,
    volume_usd  numeric,
    return_24h  numeric,
    PRIMARY KEY (asset_id, snapshot_ts)
);
