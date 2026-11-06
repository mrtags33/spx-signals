# SPX Signals

Auto-updating dashboard of S&P 500 bottom-finding signals: volatility (VIX term
structure, SKEW), price (Bollinger, MACD, RSI, Fibonacci, 50-day) and positioning
(inverse ETF share, vol-target exposure, breadth). Backtest stats come from a separate
private project; this repo only holds what the live page needs.

- `build_state.py` computes every signal and writes `state.json`.
- `.github/workflows/update.yml` runs it every 15 minutes during US market hours and
  once after the close (which also refreshes `positioning.json`).
- `index.html` reads `state.json`; GitHub Pages serves it.

Data: Yahoo Finance (via yfinance), CBOE (SKEW), Wikipedia (S&P 500 members).
Not investment advice.
