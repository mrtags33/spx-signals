"""
Positioning and breadth signals (bullish / bottom-finding).

They read precomputed daily series that run_positioning_backtest.py adds to the
data dict (see data/positioning.py).

These are zone signals: they are on for every day the indicator sits in its fear
zone, and the engine's 5-day cooldown turns that into one entry every 5 trading
days while the zone lasts. run_threshold_report.py showed that this keeps the edge
while giving several times more signals than a single crossing:
- inverse ETF share >= 40%            (Pies's tactical level; edge grows with the level)
- vol-target exposure z <= -1.0       (weak: only the <= -2 extreme stands out)
- % of stocks above 50-day <= 30%     (strongest and most consistent across periods)
"""
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from indicators.registry import register

INV_KEY, VOLT_KEY, BREADTH_KEY = "INV_SHARE", "VOL_TARGET", "BREADTH"

# zone edges: (armed, triggered, strong)
INV_ZONE = (0.35, 0.40, 0.45)        # higher = more fear
VOLT_ZONE = (-0.5, -1.0, -2.0)       # lower = more de-risked
BREADTH_ZONE = (0.40, 0.30, 0.20)    # lower = broader wash-out


@register("inverse_etf_fear")
def inverse_etf_fear(data: dict) -> pd.Series:
    """5-day inverse ETF share of leveraged+inverse $ volume is at or above 40%."""
    return data[INV_KEY]["Close"] >= INV_ZONE[1]


@register("vol_target_washout")
def vol_target_washout(data: dict) -> pd.Series:
    """Modeled vol-target fund exposure is 1 std or more below its 1-year average."""
    return data[VOLT_KEY]["z"] <= VOLT_ZONE[1]


@register("breadth_washout")
def breadth_washout(data: dict) -> pd.Series:
    """30% or fewer S&P 500 stocks are above their 50-day average."""
    return data[BREADTH_KEY]["Close"] <= BREADTH_ZONE[1]
