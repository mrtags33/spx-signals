"""
Indicator registry. Each indicator is a plain function with the signature:
    fn(data: dict[str, pd.DataFrame]) -> pd.Series[bool]

Signal contract:
- Input: dict keyed by ticker symbol, values are daily OHLCV DataFrames
- Output: boolean Series with the same DatetimeIndex as the SPX DataFrame
- True = signal fires on that bar (observable at that bar's close)
- Must NOT access any future data (no negative shifts)
"""
from typing import Callable
import pandas as pd

SignalFn = Callable[[dict[str, pd.DataFrame]], pd.Series]

_REGISTRY: dict[str, SignalFn] = {}


def register(name: str) -> Callable:
    """Decorator: registers a signal function under the given name."""
    def decorator(fn: SignalFn) -> SignalFn:
        _REGISTRY[name] = fn
        return fn
    return decorator


def get_all() -> dict[str, SignalFn]:
    return dict(_REGISTRY)


def get(name: str) -> SignalFn:
    if name not in _REGISTRY:
        raise KeyError(f"Indicator '{name}' not registered. Available: {list(_REGISTRY)}")
    return _REGISTRY[name]
