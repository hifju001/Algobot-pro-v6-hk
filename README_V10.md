# AlgoBot Pro v6 — V10 24×7 Backend Auto-Paper

This build moves automatic paper execution from the browser into the Flask backend.

## What is new

- 24×7 backend scheduler/worker; browser may be closed.
- Auto paper entries remain hard-blocked below 75% confidence.
- Multi-timeframe confirmation is still required.
- CoinDCX public market data with Binance public fallback; no API key needed for paper crypto feed.
- Open paper positions are stored in the database and recovered after process restart.
- Real observed public prices monitor SL/TP and close positions automatically.
- Signal Journal and Trade Journal remain downloadable as CSV.
- Start/Stop button now controls the server-side worker configuration, not a browser timer.
- Dashboard polls backend status every 15 seconds and shows last successful scan, counts, source, errors, and open positions.
- 24×7 worker contains a hard safety boundary: if account mode is changed away from PAPER it disables itself and will not place live-money orders.

## Required for a real 15–30 day test

### 1. Persistent PostgreSQL
Set `DATABASE_URL` in Render. Do not rely on local SQLite for a long cloud test because filesystem data may be lost on redeploy/restart.

### 2. Always-on hosting
The worker runs only while the Render service process is awake. A service that spins down from inactivity is not true 24×7. Use an always-on Render instance/background-worker-capable plan (or another always-on host).

### 3. Keep one Gunicorn worker
The supplied `render.yaml` uses one Gunicorn worker. This is intentional so there is exactly one scheduler thread. Do not increase Gunicorn workers without moving scheduling into a dedicated worker/queue system, otherwise duplicate schedulers could run.

## Usage

1. Deploy this ZIP/repository to Render.
2. Ensure account mode = PAPER.
3. Select Scalping / Intraday / Options / Swing.
4. Select strategy.
5. Keep Auto Paper = ON.
6. Choose Scan Interval.
7. Press **Start 24×7 Bot**.
8. Wait for the status line to show `24×7 Backend Worker: RUNNING` and a recent `Last success` time.
9. You may close the browser. The backend continues as long as the host remains awake.
10. Reopen later to monitor or download Signal/Trade journals.

## Safety / scope

- This build does **not** auto-place live-money orders.
- India 24×7 paper worker remains disabled because a reliable Indian live market-data feed has not yet been connected.
- The legacy Sentiment/Liquidity/Options presentation modules may still contain simulator/demo logic. The 24×7 auto-paper engine uses the real public crypto scan path only.
