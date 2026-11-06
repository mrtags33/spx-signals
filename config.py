"""Central configuration — all constants live here. Never scatter magic numbers in other modules."""

# ── Tickers ──────────────────────────────────────────────────────────────────
SPX_TICKER   = "^GSPC"
SPY_TICKER   = "SPY"       # used for OBV (has real volume)
VIX_TICKER   = "^VIX"
VIX3M_TICKER = "^VIX3M"
PCCE_TICKER  = "^PCCE"    # CBOE equity-only put/call ratio
PCALL_TICKER = "^PCALL"   # CBOE total put/call ratio
SKEW_TICKER  = "^SKEW"    # CBOE Skew Index (OTM put demand / tail risk premium)
VXST_TICKER  = "^VXST"   # CBOE 9-day VIX (short-term implied volatility)

# ── Walk-forward windows (legacy single split — kept for equity curve charting) ─
TRAIN_START = "2010-01-01"
TRAIN_END   = "2019-12-31"
VAL_START   = "2020-01-01"
VAL_END     = "2026-11-06"

# ── Time-series test periods ──────────────────────────────────────────────────
# Seven 2-year blocks (the last runs to the latest data). The train columns are kept
# for the expanding-window layout, but signal thresholds are fixed, so nothing is fitted
# on them: each block is simply tested separately and the results are averaged.
CV_FOLDS = [
    # (train_start, train_end, val_start, val_end)
    ("2010-01-01", "2011-12-31", "2012-01-01", "2013-12-31"),  # 1
    ("2010-01-01", "2013-12-31", "2014-01-01", "2015-12-31"),  # 2
    ("2010-01-01", "2015-12-31", "2016-01-01", "2017-12-31"),  # 3
    ("2010-01-01", "2017-12-31", "2018-01-01", "2019-12-31"),  # 4
    ("2010-01-01", "2019-12-31", "2020-01-01", "2021-12-31"),  # 5
    ("2010-01-01", "2021-12-31", "2022-01-01", "2023-12-31"),  # 6
    ("2010-01-01", "2023-12-31", "2024-01-01", "2026-11-06"),  # 7 (latest; end is exclusive for downloads)
]

# ── Backtest params ───────────────────────────────────────────────────────────
HOLD_PERIODS     = [1, 2, 3, 5]   # days to measure forward return
PRIMARY_HOLD     = 5              # hold period used for headline win rate
SECONDARY_HOLD   = 1              # additional hold period for 1-day win rate
SLIPPAGE         = 0.0005         # 0.05% one-way (applied to each trade)
CLUSTER_COOLDOWN = 5              # bars to suppress after a signal fires
MIN_SIGNALS      = 7              # min average signals per test period (~3.4 a year) to join the scoring pool

# ── Indicator parameters ──────────────────────────────────────────────────────
RSI_PERIOD       = 14
BB_PERIOD        = 20
BB_STD           = 2.0
STOCH_K          = 14
STOCH_D          = 3
STOCH_SMOOTH     = 3
MA_SHORT         = 10
MA_LONG          = 50
OBV_WINDOW       = 10
PCR_MA_WINDOW    = 10

# VIX thresholds
VIX_HIGH         = 25.0
VIX_MED          = 20.0
VIX_SPIKE_DROP   = 0.05    # 5% single-day VIX decline triggers fear-capitulation signal

# PCR thresholds
PCR_EQUITY_HIGH  = 0.80
PCR_TOTAL_HIGH   = 1.00
PCR_MA_ELEVATED  = 0.90

# Options activity thresholds
PCR_ZSCORE_WINDOW    = 60     # rolling window for z-score normalization
PCR_ZSCORE_SPIKE     = 2.0    # std deviations above mean = abnormal put buying
PCR_ZSCORE_EXTREME   = 3.0    # extreme put buying (rare, very contrarian)
PCR_SUSTAINED_DAYS   = 5      # days PCR must stay elevated before peak counts
SKEW_HIGH_THRESHOLD  = 130.0  # CBOE SKEW above this = elevated tail risk demand
SKEW_DROP_THRESHOLD  = 0.04   # 4% single-day SKEW drop = tail risk unwind signal
VXST_VIX_PANIC       = 1.15   # VXST/VIX ratio above this = acute short-term panic

# Composite: signals must all fire within this many bars of each other
COMPOSITE_WINDOW = 3

# ── Ranking weights (negative = lower is better; max_drawdown is stored negative, so higher is better) ──────────────────────────────
RANKING_WEIGHTS = {
    "win_rate_5d":          0.28,
    "win_rate_1d":          0.12,
    "sharpe":               0.30,
    "false_positive_rate": -0.15,
    "max_drawdown":         0.10,   # worst 5d return (negative); closer to 0 = better, so positive weight
    "n_signals":            0.05,
}
TOP_N_CURVES = 5

# ── Output ────────────────────────────────────────────────────────────────────
REPORT_PATH = "output/report.html"
