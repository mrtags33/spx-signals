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
NOTIFY = HERE / "notify.md"        # written when a watched signal fires; the workflow emails it
NOTIFY_KEYS = {"vix_vix3m_backwardation"}
POSITIONING = HERE / "positioning.json"


def write_notification(alerts: list[dict], test: bool = False) -> None:
    """notify.md: first line is the issue title, the rest its body."""
    NOTIFY.unlink(missing_ok=True)
    if test:
        alerts = [{"indicator": "vix_vix3m_backwardation", "date": time.strftime("%Y-%m-%d"),
                   "time": time.strftime("%H:%M:%S"), "message": "TEST alert: nothing fired."}]
    if not alerts:
        return
    s = b.dashboard_state
    lines = [f"{'TEST: ' if test else ''}VIX/VIX3M signal fired {alerts[0]['date']}", ""]
    for a in alerts:
        lines.append(f"**{a['date']} {a['time']} ET**: {a['message']}")
    lines += ["", f"SPX {s.get('spx_price')} · VIX {s.get('vix')} · VIX3M {s.get('vix3m')} · "
                  f"ratio {s.get('vix_ratio')}", "",
              "Intraday check: if this came during market hours, the cross is not final until the close.",
              "", "Dashboard: https://mrtags33.github.io/spx-signals/"]
    NOTIFY.write_text("\n".join(lines) + "\n")
    print(f"notify.md written ({len(alerts)} alert(s))")


def main() -> int:
    if "--positioning" in sys.argv:
        prev = json.loads(POSITIONING.read_text())["values"] if POSITIONING.exists() else None
        vals = b.compute_positioning(previous=prev)
        POSITIONING.write_text(json.dumps({"updated": time.strftime("%Y-%m-%d %H:%M"), "values": vals}))
        print("positioning.json refreshed")

    if POSITIONING.exists():
        b.positioning_cache["values"] = json.loads(POSITIONING.read_text())["values"]
    if STATE.exists():
        b.alert_log = json.loads(STATE.read_text()).get("alerts", [])

    n_before = len(b.alert_log)
    asyncio.run(b.update_state())
    new_alerts = b.alert_log[:len(b.alert_log) - n_before]
    write_notification([a for a in new_alerts if a["indicator"] in NOTIFY_KEYS],
                       test="--test-alert" in sys.argv)
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
