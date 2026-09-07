# AlgoBot Pro v6 — V10.2 Exit Watchdog

This build is for 24×7 PAPER testing only.

## Exit fixes
- Auto-entry remains 75% confidence or higher.
- Backend ticker watchdog checks open paper positions about every 5 seconds.
- A second recent-1m OHLC verifier catches short TP/SL touches that occur between ticker samples.
- If both TP and SL are touched inside the same 1m candle, V10.2 records Stop Loss (conservative) because OHLC cannot prove which touched first.
- Dashboard open positions now show Entry, Live, SL, TP, watchdog state, and seconds since last check.
- Dashboard has a self-healing fail-safe: if a stored position is already beyond TP/SL or watchdog data is stale, it asks the backend to run an immediate PAPER-only exit check.
- `/api/autobot/check-exits` can be called manually for diagnostics; it never opens positions and never sends live orders.

## Recommended validation
Keep the bot in PAPER mode. Confirm that open positions show Watch: ACTIVE and a watchdog age normally below about 10 seconds. If SL/TP is crossed, the position should disappear from Open Positions and an EXIT row should be written to the Trade Journal.
