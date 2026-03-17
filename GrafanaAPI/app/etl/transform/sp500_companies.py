from prefect import task, get_run_logger
from app.core import config
from app.db.duckdb_client import get_duckdb_connection


@task(retries=2, retry_delay_seconds=10)
def transform_sp500_data(s3_path: str) -> dict:
    logger = get_run_logger()

    logger.info(f"Starting DuckDB transformation for: {s3_path}")

    bucket = config.MINIO_DWH_BUCKET
    cleaned_parquet_path = f"s3://{bucket}/sp500/sp500_cleaned.parquet"
    agg_parquet_path = f"s3://{bucket}/sp500/sp500_sector_stats.parquet"

    conn = get_duckdb_connection()

    conn.execute(f"""
            CREATE OR REPLACE TEMP VIEW clean_sp500 AS
            SELECT 
                Exchange as exchange,
                upper(trim(Symbol)) as symbol,
                COALESCE(Shortname, 'Unknown') as shortname,
                COALESCE(Longname, 'Unknown') as longname,
                COALESCE(Sector, 'Unknown') as sector,
                COALESCE(Industry, 'Unknown') as industry,
                CAST(Currentprice AS NUMERIC) as currentprice,
                CAST(Marketcap AS NUMERIC) as marketcap,
                CAST(Ebitda AS NUMERIC) as ebitda,
                CAST(Revenuegrowth AS NUMERIC) as revenuegrowth,
                COALESCE(City, 'Unknown') as city,
                COALESCE(State, 'Unknown') as state,
                COALESCE(Country, 'Unknown') as country,
                CAST(Fulltimeemployees AS NUMERIC) as fulltimeemployees,
                Longbusinesssummary as longbusinesssummary,
                CAST(Weight AS NUMERIC) as weight
            FROM read_csv_auto('{s3_path}')
            WHERE Symbol IS NOT NULL;
        """)

    conn.execute(f"COPY (SELECT * FROM clean_sp500) TO '{cleaned_parquet_path}' (FORMAT PARQUET);")
    logger.info(f"Cleaned data saved to Parquet: {cleaned_parquet_path}")

    conn.execute(f"""
            COPY (
                SELECT 
                    sector,
                    COUNT(symbol) as company_count,
                    AVG(marketcap) as avg_marketcap,
                    SUM(weight) as total_weight
                FROM clean_sp500
                GROUP BY sector
            ) TO '{agg_parquet_path}' (FORMAT PARQUET);
        """)

    logger.info(f"Aggregated sector stats saved to Parquet: {agg_parquet_path}")

    conn.close()

    return {
        "cleaned_data": cleaned_parquet_path,
        "sector_stats": agg_parquet_path
    }