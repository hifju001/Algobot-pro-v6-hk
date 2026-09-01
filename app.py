"""
AlgoBot Pro v6 — Backend Server
Flask + SQLite + Login System (username/password) + JWT sessions
Deploy target: Render (Web Service)
"""

import os
import jwt
import hmac
import hashlib
import json
import time
import datetime
import requests
import pandas as pd
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from flask_cors import CORS

# ─────────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────────
app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)  # Allows the frontend (even on a different domain) to call this API

# SECRET_KEY signs the login tokens — MUST be set as an env var in production.
# Locally it falls back to a dev key so you can test without setting anything.
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me-in-render")

_db_url = os.getenv("DATABASE_URL", "sqlite:///algobot.db")
# Render gives postgres:// but SQLAlchemy 1.4+/2.x requires postgresql://
if _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_DATABASE_URI"] = _db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

# ─────────────────────────────────────────────
# DATABASE MODELS
# ─────────────────────────────────────────────
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    # Per-user trading settings (kept simple — one row per user)
    capital = db.Column(db.Float, default=100000)
    portfolio = db.Column(db.Float, default=100000)
    leverage_enabled = db.Column(db.Boolean, default=False)
    leverage_mode = db.Column(db.String(10), default="ai")
    strategy = db.Column(db.String(30), default="confluence")
    mode = db.Column(db.String(10), default="paper")  # paper / live

    # Encrypted-at-rest would be better; for now these are plain columns.
    # NEVER return these fields in any API response.
    coindcx_key = db.Column(db.String(255), default="")
    coindcx_secret = db.Column(db.String(255), default="")
    delta_key = db.Column(db.String(255), default="")
    delta_secret = db.Column(db.String(255), default="")

    trades = db.relationship("Trade", backref="user", lazy=True)

    def to_public_dict(self):
        """Only ever send this shape back to the frontend — never raw model."""
        return {
            "id": self.id,
            "username": self.username,
            "capital": self.capital,
            "portfolio": self.portfolio,
            "leverage_enabled": self.leverage_enabled,
            "leverage_mode": self.leverage_mode,
            "strategy": self.strategy,
            "mode": self.mode,
            "has_coindcx_keys": bool(self.coindcx_key),
            "has_delta_keys": bool(self.delta_key),
        }


class Trade(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    pair = db.Column(db.String(30))
    side = db.Column(db.String(10))
    entry = db.Column(db.Float)
    exit_price = db.Column(db.Float)
    leverage = db.Column(db.Integer, default=1)
    pnl = db.Column(db.Float)
    reason = db.Column(db.String(50))
    trade_mode = db.Column(db.String(20))
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "pair": self.pair,
            "side": self.side,
            "entry": self.entry,
            "exit": self.exit_price,
            "leverage": self.leverage,
            "pnl": self.pnl,
            "reason": self.reason,
            "trade_mode": self.trade_mode,
            "time": self.created_at.isoformat(),
        }


# ─────────────────────────────────────────────
# REAL MARKET DATA — CoinDCX public candles
# This is the SAME data source used whether you're in paper or live mode.
# Only the execution step at the end differs between the two.
# ─────────────────────────────────────────────
COINDCX_PUBLIC = "https://public.coindcx.com"


def fetch_candles(pair, interval="5m", limit=200):
    """
    pair must be CoinDCX format, e.g. 'B-BTC_USDT' (NOT 'BTCUSDT').
    Returns a pandas DataFrame with open/high/low/close/volume, or None on failure.
    """
    try:
        r = requests.get(
            f"{COINDCX_PUBLIC}/market_data/candles",
            params={"pair": pair, "interval": interval, "limit": limit},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        df = pd.DataFrame(data)
        # CoinDCX returns keys: time, open, high, low, close, volume
        df = df.rename(columns=str.lower)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col])
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        return df.sort_values("time").reset_index(drop=True)
    except Exception as e:
        print(f"[fetch_candles] {pair} error: {e}")
        return None


# ─────────────────────────────────────────────
# REAL INDICATORS — pure pandas, no external TA library needed
# ─────────────────────────────────────────────
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))


def macd(series, fast=12, slow=26, signal=9):
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line, macd_line - signal_line


# ─────────────────────────────────────────────
# REAL SIGNAL ENGINE — Confluence strategy on real prices
# This is what makes paper trading trustworthy: same math, real inputs.
# ─────────────────────────────────────────────
def compute_signal(df):
    if df is None or len(df) < 55:
        return {"action": "WAIT", "confidence": 0, "reason": "Insufficient real data yet"}

    close = df["close"]
    e9, e21, e50 = ema(close, 9), ema(close, 21), ema(close, 50)
    r = rsi(close, 14)
    macd_line, signal_line, hist = macd(close)

    last_e9, last_e21, last_e50 = e9.iloc[-1], e21.iloc[-1], e50.iloc[-1]
    last_r = r.iloc[-1]
    last_hist = hist.iloc[-1]
    price = close.iloc[-1]

    bull, bear, total = 0, 0, 4
    reasons = []

    if last_e9 > last_e21 > last_e50:
        bull += 1; reasons.append("EMA uptrend")
    elif last_e9 < last_e21 < last_e50:
        bear += 1; reasons.append("EMA downtrend")

    if last_hist > 0:
        bull += 1; reasons.append("MACD bullish")
    elif last_hist < 0:
        bear += 1; reasons.append("MACD bearish")

    if 45 < last_r < 65:
        bull += 1; reasons.append(f"RSI {last_r:.0f} healthy")
    elif last_r <= 35:
        bull += 0.5; reasons.append(f"RSI {last_r:.0f} oversold")
    elif last_r >= 70:
        bear += 0.5; reasons.append(f"RSI {last_r:.0f} overbought")

    if price > last_e9:
        bull += 1
    else:
        bear += 1

    max_score = max(bull, bear)
    confidence = min(95, int((max_score / total) * 100))
    action = "BUY" if bull > bear else "SELL" if bear > bull else "WAIT"
    if confidence < 50:
        action = "WAIT"

    return {
        "action": action,
        "confidence": confidence,
        "price": float(price),
        "reason": ", ".join(reasons) if reasons else "No clear setup",
        "rsi": round(float(last_r), 1),
    }


# ─────────────────────────────────────────────
# COINDCX ORDER EXECUTION (LIVE MODE ONLY)
# ─────────────────────────────────────────────
def coindcx_place_order(api_key, api_secret, pair, side, quantity):
    """Places a REAL market order. Only called when user.mode == 'live'."""
    body = {
        "side": side.lower(),
        "order_type": "market_order",
        "market": pair,
        "total_quantity": quantity,
        "timestamp": int(time.time() * 1000),
    }
    json_body = json.dumps(body, separators=(",", ":"))
    signature = hmac.new(
        api_secret.encode(), json_body.encode(), hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": signature,
    }
    try:
        r = requests.post(
            "https://api.coindcx.com/exchange/v1/orders/create",
            data=json_body, headers=headers, timeout=10,
        )
        return r.json()
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────
# AUTH HELPERS
# ─────────────────────────────────────────────
def create_token(user_id):
    payload = {
        "user_id": user_id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7),
        "iat": datetime.datetime.utcnow(),
    }
    return jwt.encode(payload, app.config["SECRET_KEY"], algorithm="HS256")


def token_required(f):
    """Decorator — protects any route that needs a logged-in user."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
        if not token:
            return jsonify({"error": "Missing token — please log in"}), 401
        try:
            data = jwt.decode(
                token, app.config["SECRET_KEY"], algorithms=["HS256"]
            )
            current_user = User.query.get(data["user_id"])
            if not current_user:
                return jsonify({"error": "User not found"}), 401
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Session expired — please log in again"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401
        return f(current_user, *args, **kwargs)
    return decorated


# ─────────────────────────────────────────────
# AUTH ROUTES
# ─────────────────────────────────────────────
@app.route("/api/register", methods=["POST"])
def register():
    data = request.get_json(force=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    if User.query.filter_by(username=username).first():
        return jsonify({"error": "Username already taken"}), 409

    user = User(
        username=username,
        password_hash=generate_password_hash(password),
    )
    db.session.add(user)
    db.session.commit()

    token = create_token(user.id)
    return jsonify({"token": token, "user": user.to_public_dict()}), 201


@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json(force=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    user = User.query.filter_by(username=username).first()
    if not user or not check_password_hash(user.password_hash, password):
        return jsonify({"error": "Invalid username or password"}), 401

    token = create_token(user.id)
    return jsonify({"token": token, "user": user.to_public_dict()}), 200


@app.route("/api/me", methods=["GET"])
@token_required
def me(current_user):
    return jsonify({"user": current_user.to_public_dict()}), 200


# ─────────────────────────────────────────────
# USER SETTINGS
# ─────────────────────────────────────────────
@app.route("/api/settings", methods=["POST"])
@token_required
def update_settings(current_user):
    data = request.get_json(force=True) or {}
    if "capital" in data:
        current_user.capital = float(data["capital"])
    if "leverage_enabled" in data:
        current_user.leverage_enabled = bool(data["leverage_enabled"])
    if "leverage_mode" in data:
        current_user.leverage_mode = str(data["leverage_mode"])
    if "strategy" in data:
        current_user.strategy = str(data["strategy"])
    if "mode" in data:
        # Extra guard: don't silently let someone flip to live without keys
        if data["mode"] == "live" and not current_user.coindcx_key:
            return jsonify({"error": "Add API keys before enabling live mode"}), 400
        current_user.mode = str(data["mode"])
    db.session.commit()
    return jsonify({"user": current_user.to_public_dict()}), 200


@app.route("/api/keys", methods=["POST"])
@token_required
def update_keys(current_user):
    """Store exchange API keys — server-side only, never echoed back raw."""
    data = request.get_json(force=True) or {}
    current_user.coindcx_key = data.get("coindcx_key", current_user.coindcx_key)
    current_user.coindcx_secret = data.get("coindcx_secret", current_user.coindcx_secret)
    current_user.delta_key = data.get("delta_key", current_user.delta_key)
    current_user.delta_secret = data.get("delta_secret", current_user.delta_secret)
    db.session.commit()
    return jsonify({"message": "Keys saved securely on server"}), 200


# ─────────────────────────────────────────────
# LIVE MARKET SCAN — real prices, real indicators
# Both paper and live users hit this same endpoint. It never places an order.
# ─────────────────────────────────────────────
WATCHLIST = ["B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT", "B-BNB_USDT"]


@app.route("/api/scan", methods=["GET"])
@token_required
def scan(current_user):
    results = []
    for pair in WATCHLIST:
        df = fetch_candles(pair, interval="5m", limit=200)
        sig = compute_signal(df)
        sig["pair"] = pair
        sig["data_source"] = "real" if df is not None else "unavailable"
        results.append(sig)
    return jsonify({"signals": results, "mode": current_user.mode}), 200


# ─────────────────────────────────────────────
# EXECUTE A TRADE — this is the ONLY place paper and live diverge
# ─────────────────────────────────────────────
@app.route("/api/execute", methods=["POST"])
@token_required
def execute(current_user):
    data = request.get_json(force=True) or {}
    pair = data.get("pair")
    side = data.get("side")  # BUY / SELL
    confidence = data.get("confidence", 0)

    if side not in ("BUY", "SELL"):
        return jsonify({"error": "side must be BUY or SELL"}), 400

    df = fetch_candles(pair, interval="5m", limit=5)
    if df is None:
        return jsonify({"error": "Could not fetch live price — try again"}), 502
    price = float(df["close"].iloc[-1])

    risk_amount = current_user.portfolio * 0.015  # 1.5% risk per trade
    quantity = round(risk_amount / price, 6)

    if current_user.mode == "live":
        if not current_user.coindcx_key:
            return jsonify({"error": "No API keys saved — cannot trade live"}), 400
        result = coindcx_place_order(
            current_user.coindcx_key, current_user.coindcx_secret,
            pair, side, quantity,
        )
        executed = "error" not in result
        note = "LIVE order placed on CoinDCX" if executed else f"LIVE order FAILED: {result}"
    else:
        # Paper mode: no order sent anywhere. We just record the intent
        # against the REAL price we just fetched — that's what makes it trustworthy.
        result = {"simulated": True}
        executed = True
        note = "PAPER trade logged against real market price (no real order sent)"

    trade = Trade(
        user_id=current_user.id,
        pair=pair,
        side=side,
        entry=price,
        exit_price=None,
        leverage=1,
        pnl=0,
        reason=f"{note} | confidence {confidence}%",
        trade_mode=current_user.mode,
    )
    db.session.add(trade)
    db.session.commit()

    return jsonify({
        "executed": executed,
        "mode": current_user.mode,
        "price": price,
        "quantity": quantity,
        "note": note,
        "exchange_response": result,
    }), 200 if executed else 502


# ─────────────────────────────────────────────
# TRADES
# ─────────────────────────────────────────────
@app.route("/api/trades", methods=["GET"])
@token_required
def get_trades(current_user):
    trades = (
        Trade.query.filter_by(user_id=current_user.id)
        .order_by(Trade.created_at.desc())
        .limit(100)
        .all()
    )
    return jsonify({"trades": [t.to_dict() for t in trades]}), 200


@app.route("/api/trades", methods=["POST"])
@token_required
def log_trade(current_user):
    """
    Called by the frontend after a paper trade closes, OR by the Python bot
    backend logic (Stage 3) after a real trade closes.
    """
    data = request.get_json(force=True) or {}
    trade = Trade(
        user_id=current_user.id,
        pair=data.get("pair"),
        side=data.get("side"),
        entry=data.get("entry"),
        exit_price=data.get("exit"),
        leverage=data.get("leverage", 1),
        pnl=data.get("pnl", 0),
        reason=data.get("reason", ""),
        trade_mode=data.get("trade_mode", "scalp"),
    )
    current_user.portfolio += float(data.get("pnl", 0))
    db.session.add(trade)
    db.session.commit()
    return jsonify({"trade": trade.to_dict(),
                     "portfolio": current_user.portfolio}), 201


# ─────────────────────────────────────────────
# HEALTH CHECK (Render uses this to confirm the service is alive)
# ─────────────────────────────────────────────
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.datetime.utcnow().isoformat()})


# ─────────────────────────────────────────────
# SERVE FRONTEND (so ONE Render service can host both API + website)
# ─────────────────────────────────────────────
@app.route("/")
def serve_index():
    return send_from_directory(app.static_folder, "login.html")


@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory(app.static_folder, path)


# ─────────────────────────────────────────────
# DB INIT
# ─────────────────────────────────────────────
with app.app_context():
    db.create_all()


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
