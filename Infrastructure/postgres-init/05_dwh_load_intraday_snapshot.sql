CREATE OR REPLACE FUNCTION dwh.load_intraday_snapshot()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dwh.asset_dim (asset_type, symbol, name)
    SELECT 'crypto' AS asset_type,
           c.symbol,
           c.name
    FROM stage.crypto_tickers c
    GROUP BY c.symbol, c.name
    ON CONFLICT (asset_type, symbol)
    DO UPDATE SET name = EXCLUDED.name;

    INSERT INTO dwh.asset_dim (asset_type, symbol, name)
    SELECT 'stock' AS asset_type,
           s.symbol,
           s.name
    FROM stage.stock_ohlcv s
    GROUP BY s.symbol, s.name
    ON CONFLICT (asset_type, symbol)
    DO UPDATE SET name = EXCLUDED.name;

    INSERT INTO dwh.intraday_price_fact (
        asset_id,
        snapshot_ts,
        close_price,
        volume,
        volume_usd,
        return_24h
    )
    SELECT
        ad.asset_id,
        c.ingestion_time AT TIME ZONE 'UTC'          AS snapshot_ts,
        c.price_usd                                   AS close_price,
        CASE
            WHEN c.price_usd IS NULL OR c.price_usd = 0
                THEN NULL
            ELSE c.volume_24h_usd / c.price_usd
        END                                           AS volume,
        c.volume_24h_usd                              AS volume_usd,
        c.percent_change_24h / 100.0                  AS return_24h
    FROM stage.crypto_tickers c
    JOIN dwh.asset_dim ad
      ON ad.asset_type = 'crypto'
     AND ad.symbol     = c.symbol
    WHERE c.ingestion_time > now() - interval '10 minutes'
    ON CONFLICT (asset_id, snapshot_ts)
    DO UPDATE SET
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume,
        volume_usd  = EXCLUDED.volume_usd,
        return_24h  = EXCLUDED.return_24h;


    INSERT INTO dwh.intraday_price_fact (
        asset_id,
        snapshot_ts,
        close_price,
        volume,
        volume_usd,
        return_24h
    )
    SELECT DISTINCT ON (ad.asset_id, s.datetime)
        ad.asset_id,
        s.datetime AT TIME ZONE 'UTC'      AS snapshot_ts,
        s.close_price                      AS close_price,
        s.volume::numeric                  AS volume,
       s.volume_usd                        AS volume_usd,
    s.return_24h                           AS return_24h
    FROM stage.stock_ohlcv s
    JOIN dwh.asset_dim ad
      ON ad.asset_type = 'stock'
     AND ad.symbol     = s.symbol
    WHERE s.ingestion_time > now() - interval '10 minutes'
    ORDER BY
        ad.asset_id,
        s.datetime,
        s.ingestion_time DESC
    ON CONFLICT (asset_id, snapshot_ts)
    DO UPDATE SET
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume,
        volume_usd  = EXCLUDED.volume_usd,
        return_24h  = EXCLUDED.return_24h;


END;
$$;
