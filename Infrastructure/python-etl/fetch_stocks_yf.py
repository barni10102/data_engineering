import json
import sys
import os
import time
import psycopg2
import yfinance as yf

DEFAULT_TOP_N = 10

def get_top_n_from_args(default=DEFAULT_TOP_N):
    try:
        return int(sys.argv[1])
    except (IndexError, ValueError):
        return default


def fetch_ticker_info_with_retry(symbol, max_retries=10, base_delay=0.5):
    for attempt in range(1, max_retries + 1):
        try:
            info = yf.Ticker(symbol).info
            if info:
                return info
            else:
                raise ValueError("Empty info dict")
        except Exception as e:
            if attempt < max_retries:
                time.sleep(base_delay * attempt)
            else:
                return {} 
    

def get_stocks_from_db(top_n):

    conn = psycopg2.connect(
        host="postgres",
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, name
                FROM config.stock_universe
                WHERE marketcap IS NOT NULL
                ORDER BY marketcap DESC
                LIMIT %s
                """,
                (top_n,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return [{"symbol": r[0], "name": r[1]} for r in rows]


def fetch_ohlcv(stocks):

    if not stocks:
        return []
    
    tickers = [s["symbol"] for s in stocks]

    df = yf.download(
        tickers=tickers,
        period="1d",
        interval="5m",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
    )

    result = []

    available_symbols = list(df.columns.levels[0])

    name_by_symbol = {s["symbol"]: s["name"] for s in stocks}

    for symbol in tickers:
        if symbol not in available_symbols:
            continue

        sub = df[symbol].dropna()
        if sub.empty:
            continue

        last_row = sub.iloc[-1]
        idx = sub.index[-1]

        if hasattr(idx, "to_pydatetime"):
            ts = idx.to_pydatetime().replace(microsecond=0)
        else:
            ts = idx

        name = name_by_symbol.get(symbol)

        info = fetch_ticker_info_with_retry(symbol)

        regular_price = info.get("regularMarketPrice")
        regular_volume = info.get("regularMarketVolume")
        regular_change_pct = info.get("regularMarketChangePercent")

        volume_usd = None
        if regular_price is not None and regular_volume is not None:
            try:
                volume_usd = float(regular_price) * float(regular_volume)
            except Exception:
                volume_usd = None

        return_24h = None
        if regular_change_pct is not None:
            try:
                return_24h = float(regular_change_pct) / 100.0
            except Exception:
                return_24h = None

        result.append(
            {
                "symbol": symbol,
                "name": name,
                "datetime": ts.isoformat(),
                "open": float(last_row["Open"]),
                "high": float(last_row["High"]),
                "low": float(last_row["Low"]),
                "close": float(last_row["Close"]),
                "volume": int(last_row["Volume"]),
                "volume_usd": volume_usd,
                "return_24h": return_24h,
            }
        )

    return result


def main():
    top_n = get_top_n_from_args()
    stocks = get_stocks_from_db(top_n)
    data = fetch_ohlcv(stocks)
    json.dump(data, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
