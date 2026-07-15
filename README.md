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
