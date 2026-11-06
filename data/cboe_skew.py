"""
CBOE SKEW Index data fetcher.
Downloads directly from CBOE's public CSV endpoint (updated daily after market close).
Used in place of yfinance ^SKEW which has frequent gaps.

CBOE endpoint: https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv
Format: DATE (MM/DD/YYYY), SKEW (float)
"""
import io
import warnings
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests

CBOE_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv"
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_FILE = CACHE_DIR / "CBOE_SKEW_full.parquet"

# Re-download if cache is older than this many hours
CACHE_TTL_HOURS = 18


def _cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
    return age < timedelta(hours=CACHE_TTL_HOURS)


def _download() -> pd.DataFrame:
    """Fetch full SKEW history from CBOE and return as a clean DataFrame."""
    resp = requests.get(CBOE_URL, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    df.columns = df.columns.str.strip()
    df["DATE"] = pd.to_datetime(df["DATE"], format="%m/%d/%Y")
    df = df.set_index("DATE").sort_index()
    df = df.rename(columns={"SKEW": "Close"})
    df["Open"] = df["Close"]
    df["High"] = df["Close"]
    df["Low"]  = df["Close"]
    df["Volume"] = 0
    return df[["Open", "High", "Low", "Close", "Volume"]]


def load_skew(start: str = None, end: str = None) -> pd.DataFrame:
    """
    Return CBOE SKEW as a daily OHLCV DataFrame, optionally sliced to [start, end].
    Uses a local cache with an 18-hour TTL so backtest runs don't hammer the endpoint.
    Falls back to cached data if the download fails.
    """
    if not _cache_is_fresh():
        try:
            df = _download()
            CACHE_DIR.mkdir(exist_ok=True)
            df.to_parquet(CACHE_FILE)
            print("    SKEW (CBOE)... downloaded OK")
        except Exception as e:
            if CACHE_FILE.exists():
                warnings.warn(f"CBOE SKEW download failed ({e}), using cached data.")
                df = pd.read_parquet(CACHE_FILE)
            else:
                raise RuntimeError(f"CBOE SKEW download failed and no cache exists: {e}") from e
    else:
        df = pd.read_parquet(CACHE_FILE)
        print("    SKEW (CBOE)... from cache")

    if start:
        df = df.loc[start:]
    if end:
        df = df.loc[:end]
    return df
