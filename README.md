# AlgoBot Pro v6 — V9 Auto-80 + Journals + Selectable Live Strategies

This build extends the stable V8/V7 live-paper core.

## New in V9
- Auto Paper entries are hard-gated at **75% confidence or higher**. The Signal Display filter cannot lower the auto-entry threshold.
- Every live scan result (BUY / SELL / WAIT / blocked reason) is stored in a persistent **Signal Journal** table.
- Every paper trade ENTRY and EXIT is stored in a **Trade Journal** table.
- Signals screen has one-click CSV downloads for both journals.
- Live strategy selector works with every crypto trading mode (Scalping / Intraday / Options / Swing).
- Four real-candle strategies are available: Confluence MTF, Trend + Momentum, Volume Breakout, Trend Pullback.
- Primary + confirmation timeframe use the same selected strategy.
- No strategy is advertised as a guaranteed 75%+ winner. V9 is intentionally built to measure the real win rate over your 15–30 day paper test.

## Important for a 15–30 day journal
Set a persistent `DATABASE_URL` (for example managed PostgreSQL) in Render. If Render falls back to local SQLite, the service filesystem can be replaced on redeploy/restart and journal history may be lost.

## India paper trading
This build still blocks synthetic India signals. Official NSE real-time data is a licensed product; for reliable scalping/options paper validation, use a broker/data-vendor market-data session (market data only is enough; live order execution can stay disabled). Do not judge Indian scalping from an unofficial delayed webpage feed.

# AlgoBot Pro v6 — V7 Stable Core / Live Paper Fix

This build fixes the frontend freeze and backend list/DataFrame mismatch found in the deployed V6 repository.

- Fixes fatal `x is not defined` error in `CPAIRS` initialization that prevented mode switching, sentiment, liquidity and signal initialization.
- Fixes `/api/scan`, `/api/execute` and `/api/debug-candles` to use the list-of-dicts candle format returned by the V6 lightweight feed layer.
- Preserves CoinDCX primary + Binance public fallback without API keys.
- Reports the actual public source used for a signal.
- Adds fault-isolated UI startup so one optional module cannot freeze the entire dashboard.
- Changes the service worker to a new cache version and network-first app shell, preventing old dashboard code from surviving a new Render deployment.
- India synthetic signals remain disabled for live-paper validation.

# AlgoBot Pro v6 — V6 Render-safe dual public feed

This build addresses repeated Render HTTP 502 errors during live crypto scans.

- Removes pandas from the live scan path to reduce Render memory use.
- CoinDCX remains the primary no-key public source.
- If CoinDCX is unreachable or rejects the Render server, Binance public `data-api.binance.vision` automatically supplies live ticker/candles.
- No API key is required for either public feed.
- Real MTF mapping remains: Scalping 1m+5m, Intraday 15m+1h, Options 5m+15m, Swing 4h+1d.
- Signal calculations use only genuine OHLCV candles.
- Gunicorn is reduced to 2 threads and a 90s timeout for stability on small Render instances.
- `/api/feed-test` remains available for diagnostics.

# AlgoBot Pro v6 — V4 no-key live-paper feed fix

This build fixes the V3 live crypto scanner.

- Uses documented CoinDCX Spot candles endpoint: `https://api.coindcx.com/market_data/candles`.
- Native Spot candle intervals used directly: 1m, 15m, 1h, 1d.
- 5m, 30m and 4h are resampled from real lower-timeframe candles (no random/synthetic prices).
- Mode mapping remains: Scalping 1m+5m, Intraday 15m+1h, Options 5m+15m, Swing 4h+1d.
- Frontend now displays network/server/data errors in the Signals grid instead of failing silently.
- Paper/live signal scan does not execute a trade unless real candle + ticker data is available.

Note: the dedicated legacy Options/Backtest/Liquidity simulator sections still contain synthetic logic and must not be used to judge live-strategy performance. Use the Signals tab for V4 live-paper validation.

---

# AlgoBot Pro v6 — Backend + Website + PWA

This folder is a complete deployable package:
- Flask backend (`app.py`) — login system, database, API
- Login page (`static/login.html`)
- Dashboard (`static/dashboard.html`) — your existing trading UI, now auth-gated
- PWA files (`manifest.json`, `sw.js`, icons) — makes it installable on phones

## Run locally first (recommended before deploying)

```bash
cd algobot-backend
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Open http://localhost:5000 — you'll land on the login page. Create an account, log in, and you'll be redirected to the dashboard.

## Deploy to Render

1. Push this whole folder to a GitHub repo (see steps below)
2. On Render: New + → Web Service (NOT Static Site this time — this has a backend)
3. Connect your GitHub repo
4. Render will auto-detect `render.yaml` and configure itself
5. Click Create Web Service
6. Wait ~2 minutes for first deploy — you'll get a URL like `https://algobot-pro-v6.onrender.com`

## Push to GitHub

```bash
cd algobot-backend
git init
git add .
git commit -m "AlgoBot Pro v6 with login"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/algobot-pro-v6.git
git push -u origin main
```

## Install as a mobile app (PWA)

Once deployed on Render:
- **Android (Chrome)**: open the site → menu (⋮) → "Add to Home screen"
- **iPhone (Safari)**: open the site → Share button → "Add to Home Screen"

It will appear on the home screen with an icon and open full-screen like a native app.

## What's real vs simulated right now

- ✅ Real: login/register, password hashing, sessions, database storage of trades
- ✅ Real: API key storage (server-side, never exposed to browser)
- ⚠️ Still simulated: the actual trading logic in dashboard.html still uses JavaScript
  `Math.random()` for signals — it is NOT yet wired to your Python bot's real exchange
  connections. That's the next stage (connecting algobot_v6.py's engines to this backend
  as real API endpoints instead of a standalone script).

## Security notes for going further

- Change `SECRET_KEY` — Render's `render.yaml` auto-generates one, don't reuse the dev default
- SQLite is fine for one person testing; for real multi-user production, migrate to Render's
  managed PostgreSQL (a few line changes in `app.py`'s `SQLALCHEMY_DATABASE_URI`)
- Add rate-limiting on `/api/login` before this is public, to prevent brute-force attempts

## V8 Auto Paper Trading
- Start Bot + Auto Paper ON automatically opens PAPER positions from real public-feed BUY/SELL signals that meet Min Confidence and filters.
- Automatic execution is deliberately PAPER ONLY. Switching to LIVE requires stopping the bot and no automatic real-money orders are placed by this V8 path.
- One open position per pair, max-position and daily-loss guards, plus cooldown prevent duplicate entries.
- Open paper positions are monitored on each fresh market scan and close only when observed real public-feed price reaches SL or TP.
- Random win/loss closing is removed from the main Signals paper-trading path.
- Closed paper trades are persisted through /api/trades; open positions remain browser-session state in this version.
