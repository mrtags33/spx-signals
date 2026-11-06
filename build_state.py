"""
Build state.json for the published (static) copy of the signal dashboard.

    python build_state.py               # prices + the 11 price/volatility signals (intraday)
    python build_state.py --positioning # also refresh positioning.json (daily, ~1 min)

Runs the same signal code as dashboard_backend.py, once, instead of the 60-second loop.
Alerts are carried over from the previous state.json so the alert list persists.
"""
import asyncio
import json
import os
import sys
import time
import warnings
from pathlib import Path

os.environ["TZ"] = "America/New_York"   # market-hours check and timestamps in ET
time.tzset()
warnings.filterwarnings("ignore")

HERE = Path(__file__).resolve().parent
ROOT = HERE if (HERE / "dashboard_backend.py").exists() else HERE.parent
sys.path.insert(0, str(ROOT))
import dashboard_backend as b  # noqa: E402

STATE = HERE / "state.json"
POSITIONING = HERE / "positioning.json"


def main() -> int:
    if "--positioning" in sys.argv:
        vals = b.compute_positioning()
        POSITIONING.write_text(json.dumps({"updated": time.strftime("%Y-%m-%d %H:%M"), "values": vals}))
        print("positioning.json refreshed")

    if POSITIONING.exists():
        b.positioning_cache["values"] = json.loads(POSITIONING.read_text())["values"]
    if STATE.exists():
        b.alert_log = json.loads(STATE.read_text()).get("alerts", [])

    asyncio.run(b.update_state())
    state = dict(b.dashboard_state)
    if not state.get("signals"):
        print("update failed; keeping the previous state.json", file=sys.stderr)
        return 1
    state["tz"] = "ET"
    state["last_updated"] = time.strftime("%b %-d, %-I:%M %p")
    state["alerts"] = state.get("alerts", [])[:50]
    STATE.write_text(json.dumps(state, default=float))
    print(f"state.json: SPX {state['spx_price']} | {len(state['signals'])} signals")
    return 0


if __name__ == "__main__":
    sys.exit(main())
