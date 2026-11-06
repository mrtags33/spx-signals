"""
Data for the positioning / breadth indicators.

Each loader returns a daily Series built only from data up to that day
(rolling windows look backward), so it can be precomputed over the full
history and sliced by the backtest without look-ahead.

- inverse_share_5d : inverse ETF $ volume / (inverse + leveraged-long) $ volume, 5-day sums
- vol_target_frame : modeled exposure of 10%-vol-target funds and its z-score vs 1 year
- breadth_50dma    : % of current S&P 500 members closing above their 50-day average
"""
import io
import sys
import urllib.request
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).parent.parent))
from indicators.inverse_etf_ratio import INVERSE, LEVERAGED, load_dollar_volume

CACHE = Path(__file__).parent / "cache"
CACHE.mkdir(exist_ok=True)

VOL_TARGET = 0.10      # annualised volatility the modeled funds target
MAX_LEVERAGE = 1.5     # exposure cap
Z_WINDOW = 252         # 1 year for the exposure z-score


# ── Inverse ETF share ─────────────────────────────────────────────────────────

def inverse_share_5d(refresh: bool = False) -> pd.Series:
    dv = load_dollar_volume(refresh=refresh)
    inv = dv[[t for t in INVERSE if t in dv]].sum(axis=1).rolling(5).sum()
    lev = dv[[t for t in LEVERAGED if t in dv]].sum(axis=1).rolling(5).sum()
    return (inv / (inv + lev)).rename("inverse_share_5d")


# ── Vol-target fund exposure ──────────────────────────────────────────────────

def vol_target_frame(close: pd.Series) -> pd.DataFrame:
    """Exposure = target vol / max(1-month, 3-month realised vol), capped."""
    r = np.log(close).diff()
    rv = pd.concat([r.rolling(21).std(), r.rolling(63).std()], axis=1).max(axis=1) * np.sqrt(252)
    exposure = (VOL_TARGET / rv).clip(upper=MAX_LEVERAGE)
    mean = exposure.rolling(Z_WINDOW, min_periods=126).mean()
    sd = exposure.rolling(Z_WINDOW, min_periods=126).std()
    return pd.DataFrame({"exposure": exposure, "z": (exposure - mean) / sd, "realized_vol": rv})


# ── Breadth ───────────────────────────────────────────────────────────────────

def sp500_members() -> list[str]:
    path = CACHE / "sp500_members.csv"
    if path.exists() and (pd.Timestamp.now() - pd.Timestamp(path.stat().st_mtime, unit="s")).days < 30:
        return pd.read_csv(path)["Symbol"].tolist()
    req = urllib.request.Request("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                                 headers={"User-Agent": "spx-backtest/1.0"})
    html = urllib.request.urlopen(req, timeout=30).read().decode()
    syms = pd.read_html(io.StringIO(html))[0]["Symbol"].str.replace(".", "-", regex=False)
    syms.to_frame().to_csv(path, index=False)
    return syms.tolist()


def _download_closes(tickers: list[str], **kw) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        d = yf.download(tickers, auto_adjust=True, progress=False, threads=True, **kw)
    return d["Close"]


def breadth_50dma(start: str = "2009-06-01", refresh: bool = False, recent_days: int = 0) -> pd.Series:
    """
    % of members above their 50-day average. Full history is cached; with
    recent_days > 0 only that many days are downloaded (for the live dashboard).
    """
    if recent_days:
        closes = _download_closes(sp500_members(), period=f"{recent_days}d")
    else:
        path = CACHE / "sp500_member_closes.parquet"
        if path.exists() and not refresh:
            closes = pd.read_parquet(path)
        else:
            closes = _download_closes(sp500_members(), start=start)
            closes.to_parquet(path)
    ma = closes.rolling(50, min_periods=50).mean()
    valid = ma.notna() & closes.notna()
    above = (closes > ma) & valid
    n = valid.sum(axis=1)
    pct = above.sum(axis=1) / n
    return pct.where(n >= 100).rename("breadth_50dma")
