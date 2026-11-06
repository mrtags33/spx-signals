"""
SPX Live Signal Dashboard Backend
Polls yfinance every 60s, computes all 11 CV-validated indicators, broadcasts via WebSocket.

Start:     python dashboard_backend.py
Dashboard: http://localhost:8000
"""
import asyncio
import json
import sys
import warnings
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Set

import numpy as np
import pandas as pd
import uvicorn
import yfinance as yf
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

sys.path.insert(0, str(Path(__file__).parent))
import config
from data.cboe_skew import load_skew
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD, SMAIndicator
from ta.volatility import BollingerBands
from ta.volume import OnBalanceVolumeIndicator

app = FastAPI()

clients: Set[WebSocket] = set()

dashboard_state: dict = {
    "spx_price": None, "spx_change": None, "spx_change_pct": None,
    "vix": None, "vix3m": None, "vix_ratio": None,
    "last_updated": None, "market_open": False,
    "signals": {}, "alerts": [],
}

alert_log: list = []

# ── Backtest stats (fallback; signal_stats.json overrides — 7 two-year periods 2012–2026) ─────────────────────
SIGNAL_META = {
    "vix_vix3m_backwardation": {
        "label": "VIX BACKWARDATION", "rank": 1,
        "win_rate_1d": "55.8%", "win_rate_1d_std": "±10.7%",
        "win_rate": "70.6%", "win_rate_std": "±18.8%",
        "win_rate_30d": "81.0%", "avg_ret_1d": "+0.30%", "avg_ret_5d": "+1.13%", "avg_ret_30d": "+2.81%",
        "sharpe": "8.30", "cv_score": "0.601",
        "description": "VIX/VIX3M ratio crosses below 1.0. When spot VIX exceeds 3-month VIX the term structure inverts (backwardation), signaling panic. Resolution signals fear exhaustion.",
        "trigger_desc": "VIX/VIX3M ratio: ≥1.0 → <1.0",
    },
    "skew_tail_risk_resolving": {
        "label": "SKEW RESOLVING", "rank": 2,
        "win_rate_1d": "53.0%", "win_rate_1d_std": "±11.2%",
        "win_rate": "71.5%", "win_rate_std": "±18.9%",
        "win_rate_30d": "72.3%", "avg_ret_1d": "+0.10%", "avg_ret_5d": "+0.33%", "avg_ret_30d": "+1.48%",
        "sharpe": "7.61", "cv_score": "0.559",
        "description": "CBOE SKEW Index was above 130 and drops more than 4% in a single session. High SKEW means institutions are paying elevated premiums for OTM put protection (tail-risk hedging). A sharp SKEW collapse marks the moment that fear premium evaporates — institutional panic is resolved.",
        "trigger_desc": "SKEW_prev > 130 AND single-day SKEW drop > 4%",
    },
    "bollinger_lower": {
        "label": "BOLLINGER LOWER", "rank": 3,
        "win_rate_1d": "57.3%", "win_rate_1d_std": "±13.1%",
        "win_rate": "65.7%", "win_rate_std": "±13.3%",
        "win_rate_30d": "67.7%", "avg_ret_1d": "+0.13%", "avg_ret_5d": "-0.05%", "avg_ret_30d": "+1.42%",
        "sharpe": "5.00", "cv_score": "0.457",
        "description": "SPX closes at or below the lower Bollinger Band (20-day SMA − 2σ). Price is statistically extended — more than 2 standard deviations below its 20-day mean. Historical mean-reversion edge.",
        "trigger_desc": "Daily close ≤ lower BB after being above it",
    },
    "vix_fear_capitulation": {
        "label": "VIX FEAR CAP", "rank": 4,
        "win_rate_1d": "51.0%", "win_rate_1d_std": "±19.9%",
        "win_rate": "61.0%", "win_rate_std": "±13.9%",
        "win_rate_30d": "70.7%", "avg_ret_1d": "-0.44%", "avg_ret_5d": "+0.13%", "avg_ret_30d": "+1.87%",
        "sharpe": "5.59", "cv_score": "0.354",
        "description": "VIX was above 20 (sustained fear) and drops >5% in a single day. A sharp VIX collapse from elevated levels signals that sellers have exhausted themselves — the fear premium evaporates rapidly.",
        "trigger_desc": "VIX_prev > 20 AND single-day VIX drop > 5%",
    },
    "fib_retracement_bounce": {
        "label": "FIB BOUNCE", "rank": 5,
        "win_rate_1d": "55.4%", "win_rate_1d_std": "±7.4%",
        "win_rate": "62.1%", "win_rate_std": "±11.4%",
        "win_rate_30d": "71.7%", "avg_ret_1d": "+0.06%", "avg_ret_5d": "+0.23%", "avg_ret_30d": "+1.94%",
        "sharpe": "3.99", "cv_score": "0.326",
        "description": "SPX bounces from a Fibonacci retracement level (38.2%, 50%, or 61.8% of the prior 63-bar swing range). Fib levels act as natural support clusters. Signal confirms: price was at the level yesterday, closes above it today.",
        "trigger_desc": "Yesterday ±0.3% of fib level, today close > fib level",
    },
    "skew_tail_risk_spike": {
        "label": "SKEW SPIKE", "rank": 6,
        "win_rate_1d": "53.2%", "win_rate_1d_std": "±12.1%",
        "win_rate": "63.5%", "win_rate_std": "±14.3%",
        "win_rate_30d": "77.3%", "avg_ret_1d": "+0.05%", "avg_ret_5d": "-0.06%", "avg_ret_30d": "+1.84%",
        "sharpe": "4.52", "cv_score": "0.282",
        "description": "CBOE SKEW Index crosses above 130. SKEW measures the implied cost of OTM put options relative to ATM options. When SKEW crosses above 130, the market is paying a significant premium for tail-risk protection — a surge in institutional demand for downside protection that historically precedes market stabilization.",
        "trigger_desc": "SKEW crosses from ≤130 to >130 (daily close)",
    },
    "macd_line_cross": {
        "label": "MACD CROSS", "rank": 7,
        "win_rate_1d": "48.3%", "win_rate_1d_std": "±3.3%",
        "win_rate": "58.8%", "win_rate_std": "±6.0%",
        "win_rate_30d": "67.7%", "avg_ret_1d": "-0.04%", "avg_ret_5d": "+0.35%", "avg_ret_30d": "+1.94%",
        "sharpe": "2.72", "cv_score": "0.210",
        "description": "MACD line (12-day EMA minus 26-day EMA) crosses above its 9-day signal line. Momentum inflection — the short-term average is accelerating faster than the long-term average, signaling building buy pressure.",
        "trigger_desc": "MACD line crosses above signal line from below",
    },
    "macd_histogram_pos": {
        "label": "MACD HIST+", "rank": 8,
        "win_rate_1d": "48.3%", "win_rate_1d_std": "±3.3%",
        "win_rate": "58.8%", "win_rate_std": "±6.0%",
        "win_rate_30d": "67.7%", "avg_ret_1d": "-0.04%", "avg_ret_5d": "+0.35%", "avg_ret_30d": "+1.94%",
        "sharpe": "2.72", "cv_score": "0.210",
        "description": "MACD histogram (MACD minus signal) crosses zero from negative to positive. Slightly earlier than the line cross — histogram turning positive means the gap is actively expanding in the bullish direction.",
        "trigger_desc": "Histogram crosses from negative to positive",
    },
    "rsi_cross_40": {
        "label": "RSI CROSS 40", "rank": 9,
        "win_rate_1d": "56.0%", "win_rate_1d_std": "±12.9%",
        "win_rate": "55.0%", "win_rate_std": "±5.6%",
        "win_rate_30d": "69.4%", "avg_ret_1d": "-0.32%", "avg_ret_5d": "-0.50%", "avg_ret_30d": "+1.89%",
        "sharpe": "2.61", "cv_score": "0.139",
        "description": "RSI(14) was below 40 (sustained bearish momentum) and crosses back above 40. Catches recovery confirmation — RSI above 40 indicates momentum has shifted from bearish to neutral, often the early stage of a recovery.",
        "trigger_desc": "RSI(14) crosses from ≤40 to >40",
    },
    "price_cross_50ma": {
        "label": "50MA CROSS", "rank": 10,
        "win_rate_1d": "54.5%", "win_rate_1d_std": "±15.0%",
        "win_rate": "56.9%", "win_rate_std": "±6.7%",
        "win_rate_30d": "75.0%", "avg_ret_1d": "+0.24%", "avg_ret_5d": "+0.41%", "avg_ret_30d": "+1.53%",
        "sharpe": "0.75", "cv_score": "0.030",
        "description": "SPX daily close crosses above its 50-day simple moving average from below. The 50-day MA is one of the most widely watched medium-term trend indicators. A cross above triggers systematic buying from trend-following funds.",
        "trigger_desc": "Daily close crosses above 50-day SMA",
    },
    "obv_divergence": {
        "label": "OBV DIVERGE", "rank": 11,
        "win_rate_1d": "47.3%", "win_rate_1d_std": "±12.9%",
        "win_rate": "57.4%", "win_rate_std": "±23.8%",
        "win_rate_30d": "65.7%", "avg_ret_1d": "-0.25%", "avg_ret_5d": "-0.92%", "avg_ret_30d": "+1.37%",
        "sharpe": "3.75", "cv_score": "0.015",
        "description": "Bullish OBV divergence: SPX makes a 10-day lower low but SPY On-Balance Volume makes a 10-day higher low. Volume pressure is holding up even as price declines — signals institutional accumulation during the selloff.",
        "trigger_desc": "Price 10d lower low + OBV 10d higher low (new crossover)",
    },
    # Positioning / breadth signals — zone triggers (see run_threshold_report.py).
    # Stats from run_positioning_backtest.py: same folds, score on the pool above's scale,
    # 30d = fold-averaged 30-day hold. Zones fire at most once per 5 days while they last.
    "inverse_etf_fear": {
        "label": "INVERSE ETF FEAR", "rank": 0,
        "win_rate_1d": "46.1%", "win_rate_1d_std": "±8.2%",
        "win_rate": "57.9%", "win_rate_std": "±5.5%",
        "win_rate_30d": "75.6%", "avg_ret_1d": "-0.10%", "avg_ret_5d": "+0.18%", "avg_ret_30d": "+2.03%",
        "sharpe": "1.77", "cv_score": "0.132",
        "description": "Inverse ETFs' share of leveraged-plus-inverse ETF dollar volume, summed over 5 days. At 40% or more, traders are crowding into downside bets; Warren Pies uses 40% as a tactical-bottom level. Returns get better the higher it goes, and 45%+ is the strongest zone.",
        "trigger_desc": "Triggered while the 5-day share is ≥40% (strong ≥45%); armed ≥35%",
    },
    "vol_target_washout": {
        "label": "VOL-TARGET WASHOUT", "rank": 0,
        "win_rate_1d": "55.1%", "win_rate_1d_std": "±15.7%",
        "win_rate": "63.0%", "win_rate_std": "±27.7%",
        "win_rate_30d": "71.4%", "avg_ret_1d": "-0.07%", "avg_ret_5d": "+0.58%", "avg_ret_30d": "+3.21%",
        "sharpe": "2.01", "cv_score": "0.211",
        "description": "Models funds that target 10% volatility: exposure = 10% ÷ the higher of 1-month and 3-month realised volatility, capped at 1.5×. Low exposure means their forced selling has mostly happened. The edge is weak except at the extreme (z below −2).",
        "trigger_desc": "Triggered while exposure z-score ≤ −1.0 (strong ≤ −2.0); armed ≤ −0.5",
    },
    "breadth_washout": {
        "label": "BREADTH WASHOUT", "rank": 0,
        "win_rate_1d": "53.7%", "win_rate_1d_std": "±12.8%",
        "win_rate": "69.8%", "win_rate_std": "±17.6%",
        "win_rate_30d": "84.9%", "avg_ret_1d": "-0.03%", "avg_ret_5d": "+0.60%", "avg_ret_30d": "+4.15%",
        "sharpe": "5.97", "cv_score": "0.473",
        "description": "Share of S&P 500 stocks closing above their 50-day average. At 30% or less the sell-off is broad; the lower it goes, the better the next 30 days have been. Uses today's index members, so stocks that left the index are missing from the history.",
        "trigger_desc": "Triggered while ≤30% of stocks are above their 50-day (strong ≤20%); armed ≤40%",
    },
}

# Backtest stats, score and rank come from signal_stats.json (refresh_dashboard_stats.py);
# the numbers typed above are only a fallback if that file is missing.
_STATS_FILE = Path(__file__).parent / "signal_stats.json"
STATS_INFO: dict = {}
if _STATS_FILE.exists():
    _stats = json.loads(_STATS_FILE.read_text())
    STATS_INFO = {k: _stats.get(k) for k in ("generated", "data_through", "periods")}
    for _k, _s in _stats["signals"].items():
        if _k in SIGNAL_META:
            SIGNAL_META[_k].update(_s)
else:
    for _i, _k in enumerate(sorted(SIGNAL_META, key=lambda k: -float(SIGNAL_META[k]["cv_score"])), 1):
        SIGNAL_META[_k]["rank"] = _i

POSITIONING_KEYS = ["inverse_etf_fear", "vol_target_washout", "breadth_washout"]
POSITIONING_TTL = 30 * 60   # seconds between refreshes (these are daily-close signals)
positioning_cache: dict = {"at": 0.0, "values": None}


def compute_positioning() -> dict | None:
    """Download ETF volume, member closes and SPX history; evaluate the 3 signals."""
    from indicators.inverse_etf_ratio import INVERSE, LEVERAGED
    from data.positioning import breadth_50dma, vol_target_frame
    import indicators.positioning as P
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        etf = yf.download(INVERSE + LEVERAGED, period="40d", auto_adjust=False, progress=False, threads=True)
        spx = yf.download(config.SPX_TICKER, period="2y", auto_adjust=True, progress=False)["Close"].squeeze()
    dv = (etf["Close"] * etf["Volume"]).fillna(0.0)
    inv_d = dv[[t for t in INVERSE if t in dv]].sum(axis=1)
    lev_d = dv[[t for t in LEVERAGED if t in dv]].sum(axis=1)
    inv5, lev5 = inv_d.rolling(5).sum(), lev_d.rolling(5).sum()
    share = (inv5 / (inv5 + lev5)).dropna()
    share_1d = float(inv_d.iloc[-1] / (inv_d.iloc[-1] + lev_d.iloc[-1]))

    vt = vol_target_frame(spx).dropna()
    br = breadth_50dma(recent_days=130).dropna()

    s_now, s_prev = float(share.iloc[-1]), float(share.iloc[-2])
    z_now, z_prev = float(vt["z"].iloc[-1]), float(vt["z"].iloc[-2])
    b_now, b_prev = float(br.iloc[-1]), float(br.iloc[-2])
    b_min10 = float(br.iloc[-10:].min())
    ia, it, istrong = P.INV_ZONE
    va, vt_, vstrong = P.VOLT_ZONE
    ba, bt, bstrong = P.BREADTH_ZONE
    return {
        "inverse_etf_fear": {
            "fired": s_now >= it, "armed": ia <= s_now < it, "new_entry": s_now >= it > s_prev,
            "strong": s_now >= istrong,
            "share_5d": s_now, "share_prev": s_prev, "share_1d": share_1d,
            "inv_bn": float(inv_d.iloc[-1]) / 1e9, "lev_bn": float(lev_d.iloc[-1]) / 1e9,
        },
        "vol_target_washout": {
            "fired": z_now <= vt_, "armed": vt_ < z_now <= va, "new_entry": z_now <= vt_ < z_prev,
            "strong": z_now <= vstrong,
            "exposure": float(vt["exposure"].iloc[-1]), "exposure_prev": float(vt["exposure"].iloc[-2]),
            "z": z_now, "z_prev": z_prev, "realized_vol": float(vt["realized_vol"].iloc[-1]),
        },
        "breadth_washout": {
            "fired": b_now <= bt, "armed": bt < b_now <= ba, "new_entry": b_now <= bt < b_prev,
            "strong": b_now <= bstrong,
            "pct": b_now, "pct_prev": b_prev, "min_10d": b_min10,
        },
    }


async def positioning_loop() -> None:
    """Refresh the positioning signals in a worker thread so the 60s loop never waits."""
    import time
    while True:
        try:
            vals = await asyncio.to_thread(compute_positioning)
            if vals:
                positioning_cache.update(at=time.time(), values=vals)
                print("  Positioning signals refreshed")
                await update_state()
                await broadcast(dashboard_state)
        except Exception as e:
            print(f"  Warning: positioning refresh failed: {e}")
        await asyncio.sleep(POSITIONING_TTL)


# ── Helpers ────────────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    return dt_time(9, 30) <= now.time() <= dt_time(16, 0)


def fetch_latest_data(days: int = 300) -> dict[str, pd.DataFrame]:
    yf_tickers = [config.SPX_TICKER, config.SPY_TICKER,
                  config.VIX_TICKER, config.VIX3M_TICKER]
    data = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for t in yf_tickers:
            try:
                df = yf.download(t, period=f"{days}d", auto_adjust=True, progress=False)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                if not df.empty:
                    data[t] = df[["Open", "High", "Low", "Close", "Volume"]]
            except Exception as e:
                print(f"  Warning: could not fetch {t}: {e}")

    # SKEW: fetch from CBOE directly (18h cache, falls back to cache on failure)
    try:
        skew_df = load_skew()
        if not skew_df.empty:
            data[config.SKEW_TICKER] = skew_df
    except Exception as e:
        print(f"  Warning: could not fetch CBOE SKEW: {e}")

    if config.SPX_TICKER in data:
        idx = data[config.SPX_TICKER].index
        for t in list(data.keys()):
            data[t] = data[t].reindex(idx).ffill(limit=5)
    return data


# ── Signal computations ────────────────────────────────────────────────────────

def signal_vix_ratio(data: dict) -> dict:
    vix   = data[config.VIX_TICKER]["Close"]
    vix3m = data[config.VIX3M_TICKER]["Close"]
    ratio      = vix / vix3m.replace(0, float("nan"))
    ratio_now  = float(ratio.iloc[-1])
    ratio_prev = float(ratio.iloc[-2])
    vix_now    = float(vix.iloc[-1])
    vix3m_now  = float(vix3m.iloc[-1])
    vix_prev   = float(vix.iloc[-2])

    fired = (ratio_now < 1.0) and (ratio_prev >= 1.0)
    armed = (not fired) and (ratio_now >= 1.0) and (ratio_now < 1.06)

    return {
        "fired": fired, "armed": armed,
        "vix": round(vix_now, 2), "vix3m": round(vix3m_now, 2),
        "ratio": round(ratio_now, 4), "prev_ratio": round(ratio_prev, 4),
        "vix_chg": round(vix_now - vix_prev, 2),
    }


def signal_bollinger_lower(close: pd.Series) -> dict:
    bb     = BollingerBands(close, window=config.BB_PERIOD, window_dev=config.BB_STD)
    lower  = bb.bollinger_lband()
    middle = bb.bollinger_mavg()
    upper  = bb.bollinger_hband()

    close_now  = float(close.iloc[-1])
    close_prev = float(close.iloc[-2])
    lower_now  = float(lower.iloc[-1])
    lower_prev = float(lower.iloc[-2])
    mid_now    = float(middle.iloc[-1])
    upper_now  = float(upper.iloc[-1])

    fired    = (close_now <= lower_now) and (close_prev > lower_prev)
    dist_pct = (close_now - lower_now) / lower_now * 100
    armed    = (not fired) and (0 <= dist_pct <= 1.0)

    band_width = upper_now - lower_now
    pct_b = (close_now - lower_now) / band_width if band_width > 0 else 0.5

    return {
        "fired": fired, "armed": armed,
        "close": round(close_now, 2),
        "lower_band": round(lower_now, 2),
        "middle_band": round(mid_now, 2),
        "dist_pct": round(dist_pct, 2),
        "pct_b": round(pct_b, 3),
    }


def signal_vix_fear_cap(data: dict) -> dict:
    vix      = data[config.VIX_TICKER]["Close"]
    vix_now  = float(vix.iloc[-1])
    vix_prev = float(vix.iloc[-2])
    chg_pct  = (vix_now - vix_prev) / vix_prev * 100

    fired = (vix_prev > 20.0) and (chg_pct < -5.0)
    armed = (not fired) and (vix_prev > 20.0) and (-5.0 <= chg_pct < -2.0)

    return {
        "fired": fired, "armed": armed,
        "vix": round(vix_now, 2),
        "vix_prev": round(vix_prev, 2),
        "vix_chg_pct": round(chg_pct, 2),
        "above_20": vix_prev > 20.0,
    }


def signal_fib_bounce(close: pd.Series) -> dict:
    LOOKBACK   = 63
    FIB_LEVELS = [0.382, 0.500, 0.618]
    TOL        = 0.003

    close_now  = float(close.iloc[-1])
    close_prev = float(close.iloc[-2])

    if len(close) < LOOKBACK + 2:
        return {"fired": False, "armed": False, "close": round(close_now, 2)}

    swing_high = float(close.iloc[-LOOKBACK - 1: -1].max())
    swing_low  = float(close.iloc[-LOOKBACK - 1: -1].min())
    rng = swing_high - swing_low

    if rng == 0:
        return {"fired": False, "armed": False, "close": round(close_now, 2)}

    fib_prices = {f: swing_high - f * rng for f in FIB_LEVELS}
    fib_names  = {0.382: "38.2%", 0.500: "50.0%", 0.618: "61.8%"}

    fired = False
    armed = False
    nearest_fib   = None
    nearest_dist  = float("inf")
    nearest_price = None

    for fib, fp in fib_prices.items():
        dist_pct = abs(close_now - fp) / fp * 100
        if dist_pct < nearest_dist:
            nearest_dist  = dist_pct
            nearest_fib   = fib_names[fib]
            nearest_price = fp

        at_yesterday  = (close_prev >= fp * (1 - TOL)) and (close_prev <= fp * (1 + TOL))
        bounced_today = close_now > fp
        if at_yesterday and bounced_today:
            fired = True

        if not fired and dist_pct <= 0.7:
            armed = True

    return {
        "fired": fired, "armed": armed,
        "close": round(close_now, 2),
        "fib_382": round(fib_prices[0.382], 2),
        "fib_500": round(fib_prices[0.500], 2),
        "fib_618": round(fib_prices[0.618], 2),
        "nearest_fib": nearest_fib or "—",
        "nearest_price": round(nearest_price, 2) if nearest_price else None,
        "nearest_dist_pct": round(nearest_dist, 2),
        "swing_high": round(swing_high, 2),
        "swing_low": round(swing_low, 2),
    }


def signal_macd(close: pd.Series) -> dict:
    m    = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    line = m.macd()
    sig  = m.macd_signal()
    hist = m.macd_diff()

    l_now, l_prev = float(line.iloc[-1]), float(line.iloc[-2])
    s_now, s_prev = float(sig.iloc[-1]),  float(sig.iloc[-2])
    h_now         = float(hist.iloc[-1])

    fired = (l_now > s_now) and (l_prev <= s_prev)
    armed = (not fired) and (h_now < 0) and (h_now > -5) and (l_now > l_prev)

    return {
        "fired": fired, "armed": armed,
        "macd": round(l_now, 3), "signal_line": round(s_now, 3),
        "histogram": round(h_now, 3),
        "gap": round(l_now - s_now, 3),
    }


def signal_macd_histogram(close: pd.Series) -> dict:
    hist  = MACD(close, window_slow=26, window_fast=12, window_sign=9).macd_diff()
    h_now = float(hist.iloc[-1])
    h_prev = float(hist.iloc[-2])

    fired = (h_now > 0) and (h_prev <= 0)
    armed = (not fired) and (h_now < 0) and (h_now > -5) and (h_now > h_prev)

    return {
        "fired": fired, "armed": armed,
        "histogram": round(h_now, 3),
        "hist_prev": round(h_prev, 3),
        "rising": h_now > h_prev,
    }


def signal_rsi_cross_40(close: pd.Series) -> dict:
    rsi      = RSIIndicator(close, window=config.RSI_PERIOD).rsi()
    rsi_now  = float(rsi.iloc[-1])
    rsi_prev = float(rsi.iloc[-2])

    fired = (rsi_now > 40) and (rsi_prev <= 40)
    armed = (not fired) and (36 <= rsi_now <= 42) and (rsi_now > rsi_prev)

    return {
        "fired": fired, "armed": armed,
        "rsi": round(rsi_now, 2),
        "rsi_prev": round(rsi_prev, 2),
        "rising": rsi_now > rsi_prev,
    }


def signal_obv_divergence(data: dict) -> dict:
    spx_close  = data[config.SPX_TICKER]["Close"]
    spy_close  = data[config.SPY_TICKER]["Close"]
    spy_volume = data[config.SPY_TICKER]["Volume"]
    obv_series = OnBalanceVolumeIndicator(spy_close, spy_volume).on_balance_volume()

    w = config.OBV_WINDOW

    spx_now       = float(spx_close.iloc[-1])
    spx_10d_min   = float(spx_close.iloc[-w - 1: -1].min())
    obv_now       = float(obv_series.iloc[-1])
    obv_10d_min   = float(obv_series.iloc[-w - 1: -1].min())

    spx_prev      = float(spx_close.iloc[-2])
    spx_10d_min_p = float(spx_close.iloc[-w - 2: -2].min())
    obv_prev      = float(obv_series.iloc[-2])
    obv_10d_min_p = float(obv_series.iloc[-w - 2: -2].min())

    price_ll      = spx_now  < spx_10d_min
    obv_hl        = obv_now  > obv_10d_min
    price_ll_prev = spx_prev < spx_10d_min_p
    obv_hl_prev   = obv_prev > obv_10d_min_p

    fired = price_ll and obv_hl and not (price_ll_prev and obv_hl_prev)
    armed = (not fired) and price_ll and not obv_hl

    return {
        "fired": fired, "armed": armed,
        "price_lower_low": price_ll,
        "obv_higher_low": obv_hl,
        "spx": round(spx_now, 2),
        "spx_10d_min": round(spx_10d_min, 2),
        "obv_now": int(obv_now),
        "obv_10d_min": int(obv_10d_min),
    }


def signal_price_cross_50ma(close: pd.Series) -> dict:
    ma50       = SMAIndicator(close, window=config.MA_LONG).sma_indicator()
    close_now  = float(close.iloc[-1])
    close_prev = float(close.iloc[-2])
    ma50_now   = float(ma50.iloc[-1])
    ma50_prev  = float(ma50.iloc[-2])

    fired   = (close_now > ma50_now) and (close_prev <= ma50_prev)
    gap_pct = (close_now - ma50_now) / ma50_now * 100
    armed   = (not fired) and (-0.5 <= gap_pct <= 0)

    return {
        "fired": fired, "armed": armed,
        "close": round(close_now, 2),
        "ma50": round(ma50_now, 2),
        "gap_pct": round(gap_pct, 2),
        "above_ma": close_now > ma50_now,
    }


def signal_skew_resolving(data: dict) -> dict:
    """SKEW was above 130 and drops >4% in one session — tail-risk unwind."""
    try:
        skew = data[config.SKEW_TICKER]["Close"]
        if len(skew.dropna()) < 2:
            return {"fired": False, "armed": False, "skew": None, "skew_prev": None}
        skew_now  = float(skew.iloc[-1])
        skew_prev = float(skew.iloc[-2])
        chg_pct   = (skew_now - skew_prev) / skew_prev * 100

        fired = (skew_prev > config.SKEW_HIGH_THRESHOLD) and (chg_pct < -config.SKEW_DROP_THRESHOLD * 100)
        armed = (not fired) and (skew_prev > config.SKEW_HIGH_THRESHOLD) and (-config.SKEW_DROP_THRESHOLD * 100 <= chg_pct < -1.0)

        return {
            "fired": fired, "armed": armed,
            "skew": round(skew_now, 2),
            "skew_prev": round(skew_prev, 2),
            "skew_chg_pct": round(chg_pct, 2),
            "was_elevated": skew_prev > config.SKEW_HIGH_THRESHOLD,
        }
    except Exception:
        return {"fired": False, "armed": False, "skew": None, "skew_prev": None}


def signal_skew_spike(data: dict) -> dict:
    """SKEW crosses above 130 — elevated institutional tail-risk demand."""
    try:
        skew = data[config.SKEW_TICKER]["Close"]
        if len(skew.dropna()) < 2:
            return {"fired": False, "armed": False, "skew": None, "skew_prev": None}
        skew_now  = float(skew.iloc[-1])
        skew_prev = float(skew.iloc[-2])
        dist_pct  = (skew_now - config.SKEW_HIGH_THRESHOLD) / config.SKEW_HIGH_THRESHOLD * 100

        fired = (skew_now > config.SKEW_HIGH_THRESHOLD) and (skew_prev <= config.SKEW_HIGH_THRESHOLD)
        armed = (not fired) and (-2.0 <= dist_pct <= 0)

        return {
            "fired": fired, "armed": armed,
            "skew": round(skew_now, 2),
            "skew_prev": round(skew_prev, 2),
            "dist_pct": round(dist_pct, 2),
            "above_130": skew_now > config.SKEW_HIGH_THRESHOLD,
        }
    except Exception:
        return {"fired": False, "armed": False, "skew": None, "skew_prev": None}

# ── Alert message builders ─────────────────────────────────────────────────────

def alert_message(key: str, v: dict) -> str:
    if key == "vix_vix3m_backwardation":
        return (f"VIX/VIX3M ratio resolved to {v['ratio']:.4f} — "
                f"term structure normalized, acute fear resolved")
    if key == "bollinger_lower":
        return (f"SPX closed at {v['close']:.0f} below lower BB ({v['lower_band']:.0f}), "
                f"%B={v['pct_b']:.2f} — >2σ extension, mean-reversion setup")
    if key == "vix_fear_capitulation":
        return (f"VIX dropped {v['vix_chg_pct']:.1f}% from {v['vix_prev']:.1f} to {v['vix']:.1f} — "
                f"panic sellers exhausted, fear premium collapsing")
    if key == "fib_retracement_bounce":
        np_ = v.get('nearest_price')
        np_str = f"{np_:.0f}" if np_ else "—"
        return (f"SPX bounced from {v['nearest_fib']} Fibonacci level ({np_str}) — "
                f"confluence support held, bounce confirmed")
    if key == "macd_line_cross":
        return (f"MACD ({v['macd']:.2f}) crossed above signal ({v['signal_line']:.2f}), "
                f"histogram {v['histogram']:+.3f} — momentum turning bullish")
    if key == "macd_histogram_pos":
        return (f"MACD histogram crossed zero (now {v['histogram']:+.3f}) — "
                f"short-term momentum inflection bullish")
    if key == "rsi_cross_40":
        return (f"RSI(14) crossed above 40 ({v['rsi_prev']:.1f} → {v['rsi']:.1f}) — "
                f"bearish momentum unwinding, recovery phase begins")
    if key == "obv_divergence":
        return ("Bullish OBV divergence — SPX at 10-day low but volume pressure (OBV) "
                "making higher low: institutional accumulation during selloff")
    if key == "price_cross_50ma":
        return (f"SPX ({v['close']:.0f}) crossed above 50-day SMA ({v['ma50']:.0f}) — "
                f"trend resumption, trend-following funds triggered")
    if key == "skew_tail_risk_resolving":
        return (f"SKEW dropped {v['skew_chg_pct']:.1f}% from {v['skew_prev']:.1f} to {v['skew']:.1f} — "
                f"institutional tail-risk hedging collapsing, fear premium unwinding")
    if key == "skew_tail_risk_spike":
        return (f"SKEW crossed above 130 ({v['skew_prev']:.1f} → {v['skew']:.1f}) — "
                f"elevated institutional OTM put demand, market pricing significant tail risk")
    if key == "inverse_etf_fear":
        return f"5-day inverse ETF share at {v['share_5d']:.1%} (40%+ zone) — downside bets crowded"
    if key == "vol_target_washout":
        return (f"Modeled vol-target exposure at {v['exposure']:.2f}× (z {v['z']:.2f}) — "
                f"systematic de-risking largely done")
    if key == "breadth_washout":
        return f"Only {v['pct']:.0%} of S&P 500 stocks are above their 50-day average — broad wash-out"
    return f"{key} fired"


# ── Core update loop ───────────────────────────────────────────────────────────

async def update_state() -> None:
    global dashboard_state, alert_log, bearish_alert_log

    try:
        data = fetch_latest_data()
        if config.SPX_TICKER not in data or data[config.SPX_TICKER].empty:
            print("  No SPX data — skipping update")
            return

        spx        = data[config.SPX_TICKER]
        close      = spx["Close"]
        price_now  = float(close.iloc[-1])
        price_prev = float(close.iloc[-2])
        chg        = price_now - price_prev
        chg_pct    = chg / price_prev * 100

        vix_now   = float(data[config.VIX_TICKER]["Close"].iloc[-1])   if config.VIX_TICKER   in data else None
        vix3m_now = float(data[config.VIX3M_TICKER]["Close"].iloc[-1]) if config.VIX3M_TICKER in data else None
        skew_now  = float(data[config.SKEW_TICKER]["Close"].iloc[-1]) if config.SKEW_TICKER in data else None

        computed = {
            "vix_vix3m_backwardation":  signal_vix_ratio(data),
            "skew_tail_risk_resolving": signal_skew_resolving(data),
            "bollinger_lower":          signal_bollinger_lower(close),
            "vix_fear_capitulation":    signal_vix_fear_cap(data),
            "fib_retracement_bounce":   signal_fib_bounce(close),
            "skew_tail_risk_spike":     signal_skew_spike(data),
            "macd_line_cross":          signal_macd(close),
            "macd_histogram_pos":       signal_macd_histogram(close),
            "rsi_cross_40":             signal_rsi_cross_40(close),
            "obv_divergence":           signal_obv_divergence(data),
            "price_cross_50ma":         signal_price_cross_50ma(close),
        }
        if positioning_cache["values"]:
            computed.update(positioning_cache["values"])

        today       = close.index[-1].strftime("%Y-%m-%d")   # date of the bar that fired, not the clock
        fired_today = {a["indicator"] for a in alert_log if a.get("date") == today}

        new_alerts = []
        for key, v in computed.items():
            # zone signals stay on for days; only alert on the day they enter the zone
            entered = v.get("new_entry", True) if key in POSITIONING_KEYS else True
            if v.get("fired") and entered and key not in fired_today:
                new_alerts.append({
                    "time":      datetime.now().strftime("%H:%M:%S"),
                    "date":      today,
                    "indicator": key,
                    "message":   alert_message(key, v),
                })

        alert_log = new_alerts + alert_log

        def status(v):
            return "TRIGGERED" if v["fired"] else ("ARMED" if v["armed"] else "WATCHING")

        signals_out = {}
        for key, v in computed.items():
            meta = SIGNAL_META[key]
            signals_out[key] = {
                "label":           meta["label"],
                "rank":            meta["rank"],
                "status":          status(v),
                "win_rate_1d":     meta["win_rate_1d"],
                "win_rate_1d_std": meta["win_rate_1d_std"],
                "win_rate":        meta["win_rate"],
                "win_rate_std":    meta["win_rate_std"],
                "win_rate_30d":    meta.get("win_rate_30d", "—"),
                "avg_ret_1d":      meta.get("avg_ret_1d",  "—"),
                "avg_ret_5d":      meta.get("avg_ret_5d",  "—"),
                "avg_ret_30d":     meta.get("avg_ret_30d", "—"),
                "sharpe":          meta["sharpe"],
                "cv_score":        meta["cv_score"],
                "description":     meta["description"],
                "trigger_desc":    meta["trigger_desc"],
                "values":          v,
            }

        vix_v = computed["vix_vix3m_backwardation"]
        dashboard_state = {
            "spx_price":      round(price_now, 2),
            "spx_change":     round(chg, 2),
            "spx_change_pct": round(chg_pct, 2),
            "vix":            round(vix_now, 2)   if vix_now   else None,
            "vix3m":          round(vix3m_now, 2) if vix3m_now else None,
            "vix_ratio":      round(vix_v["ratio"], 4),
            "skew":           round(skew_now, 2) if skew_now else None,
            "last_updated":   datetime.now().strftime("%H:%M:%S"),
            "market_open":    is_market_open(),
            "stats_info":     STATS_INFO,
            "signals": signals_out,
            "alerts":  alert_log[:100],
        }

    except Exception as e:
        print(f"  Error in update_state: {e}")
        import traceback; traceback.print_exc()


async def broadcast(msg: dict) -> None:
    if not clients:
        return
    txt = json.dumps(msg)
    dead = set()
    for ws in list(clients):
        try:
            await ws.send_text(txt)
        except Exception:
            dead.add(ws)
    clients.difference_update(dead)


async def polling_loop() -> None:
    while True:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Updating market data...")
        await update_state()
        await broadcast(dashboard_state)
        spx         = dashboard_state.get("spx_price")
        ratio       = dashboard_state.get("vix_ratio")
        n_triggered = sum(1 for s in dashboard_state.get("signals", {}).values()
                          if s.get("status") == "TRIGGERED")
        n_armed     = sum(1 for s in dashboard_state.get("signals", {}).values()
                          if s.get("status") == "ARMED")
        print(f"  SPX {spx} | VIX/VIX3M {ratio} | "
              f"TRIGGERED={n_triggered} ARMED={n_armed} | clients={len(clients)}")
        await asyncio.sleep(60)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    asyncio.create_task(polling_loop())
    asyncio.create_task(positioning_loop())


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(Path(__file__).parent.joinpath("dashboard.html").read_text())


@app.get("/state")
async def get_state():
    return dashboard_state


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    await ws.send_text(json.dumps(dashboard_state))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)


if __name__ == "__main__":
    print("SPX Signal Dashboard — http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
