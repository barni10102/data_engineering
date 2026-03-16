CREATE OR REPLACE VIEW dwh.daily_price_fact AS
WITH last_snap AS (
    SELECT DISTINCT ON (asset_id, (snapshot_ts AT TIME ZONE 'UTC')::date)
        asset_id,
        (snapshot_ts AT TIME ZONE 'UTC')::date AS price_date,
        snapshot_ts,
        close_price,
        volume,
        volume_usd,
        return_24h
    FROM dwh.intraday_price_fact
    ORDER BY asset_id,
             (snapshot_ts AT TIME ZONE 'UTC')::date,
             snapshot_ts DESC
)
SELECT
    asset_id,
    price_date,
    close_price,
    volume,
    volume_usd,
    return_24h AS return_1d
FROM last_snap;
