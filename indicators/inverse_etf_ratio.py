"""
Inverse ETF dollar-volume share — a speculative-positioning capitulation gauge.

ratio = inverse ETF $ volume / (inverse + leveraged-long ETF $ volume)

Spikes mean traders are rushing into downside bets (fear). Readings are ranked
against a trailing window only (no look-ahead), so a signal on day t uses data
available at day t's close.
"""
import sys
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).parent.parent))

INVERSE = ["SH", "SDS", "SPXU", "SPXS", "PSQ", "QID", "SQQQ", "DOG", "DXD",
           "SDOW", "RWM", "TWM", "TZA", "SRTY", "SOXS", "FAZ"]
LEVERAGED = ["SSO", "UPRO", "SPXL", "QLD", "TQQQ", "DDM", "UDOW", "UWM",
             "TNA", "URTY", "SOXL", "FAS"]

SMOOTH = 10          # trading days for the moving average
RANK_WINDOW = 756    # ~3 years of trailing history for the percentile
CACHE = Path(__file__).parent.parent / "data" / "cache" / "inverse_etf_dollar_volume.parquet"


def download_dollar_volume(tickers: list[str] | None = None, retries: int = 3,
                           **kw) -> tuple[pd.DataFrame, list[str]]:
    """Daily dollar volume (unadjusted close x shares) per ETF -> (frame, still_missing).

    Parallel yfinance downloads sometimes drop a ticker ('database is locked' from its
    cache), which skews the share badly: losing SOXL alone moves it from ~40% to ~60%.
    Tickers with no recent or no latest-day data are retried one at a time; whatever is
    still missing after `retries` is returned so the caller can refuse to use the data.
    kw goes to yf.download (start= or period=)."""
    tickers = tickers or INVERSE + LEVERAGED
    d = yf.download(tickers, auto_adjust=False, progress=False, threads=True, **kw)
    dv = (d["Close"] * d["Volume"]).reindex(columns=tickers)

    def gaps():
        return [t for t in tickers if dv[t].iloc[-30:].fillna(0).sum() == 0
                or pd.isna(dv[t].iloc[-1]) or dv[t].iloc[-1] == 0]

    for attempt in range(1, retries + 1):
        missing = gaps()
        if not missing:
            break
        print(f"  ETF volume retry {attempt}/{retries}: {missing}", file=sys.stderr)
        time.sleep(3 * attempt)
        for t in missing:
            one = yf.download(t, auto_adjust=False, progress=False, threads=False, **kw)
            if one.empty:
                continue
            col = (one["Close"] * one["Volume"]).squeeze()
            dv = dv.reindex(dv.index.union(col.index))
            dv[t] = col
    return dv, gaps()


def load_dollar_volume(start: str = "2010-01-01", refresh: bool = False) -> pd.DataFrame:
    """Daily dollar volume (unadjusted close x shares) per ETF, cached to parquet."""
    if CACHE.exists() and not refresh:
        cached = pd.read_parquet(CACHE)
        if (pd.Timestamp.today().normalize() - cached.index.max()).days <= 3:
            return cached
    dv, missing = download_dollar_volume(start=start)
    if missing:
        raise RuntimeError(f"no ETF volume for {missing}; not caching a partial basket")
    dv = dv.fillna(0.0)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    dv.to_parquet(CACHE)
    return dv


def compute(dv: pd.DataFrame) -> pd.DataFrame:
    inv = dv[[t for t in INVERSE if t in dv]].sum(axis=1)
    lev = dv[[t for t in LEVERAGED if t in dv]].sum(axis=1)
    total = inv + lev
    out = pd.DataFrame({"inverse_dv": inv, "leveraged_dv": lev})
    out = out[total > 0]
    out["ratio"] = out["inverse_dv"] / (out["inverse_dv"] + out["leveraged_dv"])
    out["ratio_sm"] = out["ratio"].rolling(SMOOTH).mean()
    # trailing percentile: share of the past RANK_WINDOW days at or below today
    out["pctile"] = out["ratio_sm"].rolling(RANK_WINDOW, min_periods=252).apply(
        lambda w: (w <= w[-1]).mean(), raw=True)
    return out


def spike_signal(pctile: pd.Series, threshold: float = 0.90) -> pd.Series:
    """Fires on the day the smoothed ratio first crosses into the top decile."""
    return (pctile >= threshold) & (pctile.shift(1) < threshold)
