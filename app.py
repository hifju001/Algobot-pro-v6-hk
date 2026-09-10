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
import threading
import requests
import traceback
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory, Response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import OperationalError
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

# ── PERMANENT FIX for "SSL error: unexpected eof while reading" ──
# Render's Postgres (and most managed Postgres) silently kills idle
# connections after a few minutes. Without these settings, SQLAlchemy
# keeps trying to reuse a connection that the server already closed,
# causing intermittent OperationalError crashes on real requests.
#
# pool_pre_ping   -> tests each connection with a cheap query before use;
#                    if it's dead, transparently opens a new one instead
#                    of surfacing the error to your request.
# pool_recycle    -> forces connections older than this many seconds to
#                    be discarded and reopened, well before Render's own
#                    idle timeout has a chance to kill them first.
# Only applied for Postgres — SQLite (local dev) doesn't need or support this.
if _db_url.startswith("postgresql://"):
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "pool_pre_ping": True,
        "pool_recycle": 280,   # seconds; stay under typical 5min idle cutoffs
        "pool_size": 5,
        "max_overflow": 2,
    }

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


class SignalLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    pair = db.Column(db.String(30))
    action = db.Column(db.String(10))
    confidence = db.Column(db.Float)
    price = db.Column(db.Float)
    strategy = db.Column(db.String(40))
    trade_mode = db.Column(db.String(20))
    primary_tf = db.Column(db.String(10))
    confirm_tf = db.Column(db.String(10))
    confirm_action = db.Column(db.String(10))
    data_source = db.Column(db.String(80))
    reason = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, index=True)


class TradeEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    event = db.Column(db.String(10))  # ENTRY / EXIT
    pair = db.Column(db.String(30))
    side = db.Column(db.String(10))
    price = db.Column(db.Float)
    entry = db.Column(db.Float)
    exit_price = db.Column(db.Float)
    pnl = db.Column(db.Float, default=0)
    leverage = db.Column(db.Float, default=1)
    confidence = db.Column(db.Float)
    strategy = db.Column(db.String(40))
    trade_mode = db.Column(db.String(20))
    reason = db.Column(db.String(120))
    source = db.Column(db.String(80))
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, index=True)


class AutoBotConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), unique=True, nullable=False, index=True)
    enabled = db.Column(db.Boolean, default=False, nullable=False)
    trade_mode = db.Column(db.String(20), default="scalp")
    strategy = db.Column(db.String(40), default="confluence")
    primary_tf = db.Column(db.String(10), default="1m")
    confirm_tf = db.Column(db.String(10), default="5m")
    scan_interval_sec = db.Column(db.Integer, default=60)
    min_confidence = db.Column(db.Float, default=75.0)
    sl_pct = db.Column(db.Float, default=0.5)
    tp_pct = db.Column(db.Float, default=1.0)
    max_positions = db.Column(db.Integer, default=5)
    daily_loss_limit = db.Column(db.Float, default=3000.0)
    risk_pct = db.Column(db.Float, default=1.5)
    last_scan_at = db.Column(db.DateTime)
    last_success_at = db.Column(db.DateTime)
    last_error = db.Column(db.String(500), default="")
    last_source = db.Column(db.String(100), default="")
    scans_count = db.Column(db.Integer, default=0)
    entries_count = db.Column(db.Integer, default=0)
    exits_count = db.Column(db.Integer, default=0)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


class PaperPosition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    pair = db.Column(db.String(30), nullable=False, index=True)
    display_pair = db.Column(db.String(30))
    side = db.Column(db.String(10), nullable=False)
    entry = db.Column(db.Float, nullable=False)
    sl = db.Column(db.Float, nullable=False)
    tp = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    notional = db.Column(db.Float, nullable=False)
    leverage = db.Column(db.Float, default=1.0)
    confidence = db.Column(db.Float)
    strategy = db.Column(db.String(40))
    trade_mode = db.Column(db.String(20))
    source = db.Column(db.String(80))
    status = db.Column(db.String(12), default="OPEN", index=True)
    opened_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, index=True)
    closed_at = db.Column(db.DateTime)
    exit_price = db.Column(db.Float)
    pnl = db.Column(db.Float, default=0.0)
    exit_reason = db.Column(db.String(120))
    last_price = db.Column(db.Float)
    last_checked_at = db.Column(db.DateTime)

    def to_dict(self):
        return {
            "id": self.id, "pair": self.pair, "display_pair": self.display_pair,
            "side": self.side, "entry": self.entry, "sl": self.sl, "tp": self.tp,
            "quantity": self.quantity, "notional": self.notional, "leverage": self.leverage,
            "confidence": self.confidence, "strategy": self.strategy, "trade_mode": self.trade_mode,
            "source": self.source, "status": self.status, "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "exit_price": self.exit_price, "pnl": self.pnl, "exit_reason": self.exit_reason,
            "last_price": self.last_price, "last_checked_at": self.last_checked_at.isoformat() if self.last_checked_at else None,
        }


# ─────────────────────────────────────────────
# REAL MARKET DATA — CoinDCX public candles
# This is the SAME data source used whether you're in paper or live mode.
# Only the execution step at the end differs between the two.
# ─────────────────────────────────────────────
COINDCX_API = "https://api.coindcx.com"



def _aggregate_candles(rows, target_interval):
    """Aggregate genuine lower-timeframe OHLCV candles using pure Python."""
    bucket_ms = {"5m": 5*60_000, "30m": 30*60_000, "4h": 4*60*60_000}.get(target_interval)
    if not bucket_ms or not rows:
        return rows
    out, cur = [], None
    source = rows[0].get("_source", "unknown")
    for r in sorted(rows, key=lambda x: x["time"]):
        b = (int(r["time"]) // bucket_ms) * bucket_ms
        if cur is None or cur["time"] != b:
            if cur is not None:
                out.append(cur)
            cur = {
                "time": b, "open": float(r["open"]), "high": float(r["high"]),
                "low": float(r["low"]), "close": float(r["close"]),
                "volume": float(r.get("volume", 0) or 0), "_source": source
            }
        else:
            cur["high"] = max(cur["high"], float(r["high"]))
            cur["low"] = min(cur["low"], float(r["low"]))
            cur["close"] = float(r["close"])
            cur["volume"] += float(r.get("volume", 0) or 0)
    if cur is not None:
        out.append(cur)
    return out


def _parse_coindcx_candles(data):
    rows = []
    if not isinstance(data, list):
        return rows
    for x in data:
        try:
            rows.append({
                "open": float(x["open"]), "high": float(x["high"]),
                "low": float(x["low"]), "close": float(x["close"]),
                "volume": float(x.get("volume", 0) or 0),
                "time": int(float(x["time"])), "_source": "CoinDCX"
            })
        except Exception:
            continue
    return sorted(rows, key=lambda x: x["time"])


def _fetch_binance_candles(pair, interval, limit):
    """No-key fallback market data from Binance public data API."""
    symbol = pair.replace("B-", "").replace("_", "").upper()
    url = "https://data-api.binance.vision/api/v3/klines"
    r = requests.get(
        url,
        params={"symbol": symbol, "interval": interval, "limit": min(1000, max(60, limit))},
        timeout=8,
        headers={"Accept": "application/json", "User-Agent": "AlgoBot-Pro-v6/6.0"},
    )
    if r.status_code != 200:
        return None, f"Binance candles HTTP {r.status_code}: {r.text[:120]}"
    data = r.json()
    if not isinstance(data, list) or not data:
        return None, f"Binance returned no candles for {symbol} {interval}"
    rows = []
    for x in data:
        try:
            rows.append({
                "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
                "close": float(x[4]), "volume": float(x[5]),
                "time": int(x[0]), "_source": "Binance"
            })
        except Exception:
            continue
    return rows, None


def fetch_candles(pair, interval="1h", limit=200):
    """
    Public no-key candles. CoinDCX is primary.
    If CoinDCX is unreachable from Render, automatically falls back to Binance.
    CoinDCX-native: 1m,15m,1h,1d. 5m/30m/4h are aggregated from genuine lower bars.
    """
    source_map = {
        "1m": ("1m", min(1000, max(limit, 200))),
        "5m": ("1m", min(1000, max(limit * 5, 300))),
        "15m": ("15m", min(1000, max(limit, 200))),
        "30m": ("15m", min(1000, max(limit * 2, 300))),
        "1h": ("1h", min(1000, max(limit, 200))),
        "4h": ("1h", min(1000, max(limit * 4, 400))),
        "1d": ("1d", min(1000, max(limit, 200))),
    }
    if interval not in source_map:
        return None, f"Unsupported requested interval: {interval}"
    source_interval, source_limit = source_map[interval]
    cd_err = None
    try:
        r = requests.get(
            f"{COINDCX_API}/market_data/candles",
            params={"pair": pair, "interval": source_interval, "limit": source_limit},
            timeout=7,
            headers={"Accept": "application/json", "User-Agent": "AlgoBot-Pro-v6/6.0"},
        )
        if r.status_code == 200:
            rows = _parse_coindcx_candles(r.json())
            if rows:
                if interval in ("5m", "30m", "4h"):
                    rows = _aggregate_candles(rows, interval)
                rows = rows[-limit:]
                if len(rows) >= 55:
                    return rows, None
                cd_err = f"CoinDCX only returned {len(rows)} usable {interval} candles"
            else:
                cd_err = f"CoinDCX returned no usable candles for {pair} {source_interval}"
        else:
            cd_err = f"CoinDCX candles HTTP {r.status_code}: {r.text[:100]}"
    except Exception as e:
        cd_err = f"CoinDCX {type(e).__name__}: {e}"

    # Fallback supports these requested intervals directly.
    try:
        rows, b_err = _fetch_binance_candles(pair, interval, limit)
        if rows and len(rows) >= 55:
            return rows, None
        return rows, f"{cd_err}; fallback: {b_err or 'insufficient Binance candles'}"
    except Exception as e:
        return None, f"{cd_err}; Binance fallback {type(e).__name__}: {e}"


def fetch_all_tickers():
    """CoinDCX ticker primary; no-key Binance ticker fallback."""
    cd_err = None
    try:
        r = requests.get(
            f"{COINDCX_API}/exchange/ticker", timeout=7,
            headers={"Accept": "application/json", "User-Agent": "AlgoBot-Pro-v6/6.0"}
        )
        if r.status_code == 200:
            rows = r.json()
            out = {"__SOURCE__": "CoinDCX"}
            if isinstance(rows, list):
                for row in rows:
                    market = str(row.get("market", "")).upper()
                    if not market:
                        continue
                    try:
                        price = float(row.get("last_price"))
                    except Exception:
                        continue
                    ts = None
                    try:
                        v = float(row.get("timestamp"))
                        if v > 1e12: v /= 1000.0
                        ts = datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc)
                    except Exception:
                        pass
                    out[market] = (price, ts)
                if len(out) > 1:
                    return out, None
            cd_err = "CoinDCX ticker returned invalid data"
        else:
            cd_err = f"CoinDCX ticker HTTP {r.status_code}"
    except Exception as e:
        cd_err = f"CoinDCX ticker {type(e).__name__}: {e}"

    try:
        r = requests.get(
            "https://data-api.binance.vision/api/v3/ticker/price",
            timeout=7, headers={"Accept":"application/json","User-Agent":"AlgoBot-Pro-v6/6.0"}
        )
        if r.status_code != 200:
            return {}, f"{cd_err}; Binance ticker HTTP {r.status_code}"
        data = r.json()
        out = {"__SOURCE__": "Binance"}
        now = datetime.datetime.now(datetime.timezone.utc)
        for row in data if isinstance(data, list) else []:
            try:
                out[str(row["symbol"]).upper()] = (float(row["price"]), now)
            except Exception:
                continue
        if len(out) > 1:
            return out, None
        return {}, f"{cd_err}; Binance ticker returned invalid data"
    except Exception as e:
        return {}, f"{cd_err}; Binance ticker {type(e).__name__}: {e}"


def fetch_live_ticker_price(pair, ticker_map=None):
    market = pair.replace("B-", "").replace("_", "").upper()
    if ticker_map is None:
        ticker_map, err = fetch_all_tickers()
        if err:
            return None, None, err
    row = ticker_map.get(market)
    if not row:
        return None, None, f"Ticker {market} not found"
    return row[0], row[1], None


def _ema_values(values, period):
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(float(v) * k + out[-1] * (1.0 - k))
    return out


def _rsi_last(values, period=14):
    if len(values) <= period:
        return 50.0
    diffs = [values[i] - values[i-1] for i in range(1, len(values))]
    recent = diffs[-period:]
    gain = sum(max(d, 0) for d in recent) / period
    loss = sum(max(-d, 0) for d in recent) / period
    if loss <= 1e-12:
        return 100.0 if gain > 0 else 50.0
    rs = gain / loss
    return 100.0 - (100.0 / (1.0 + rs))


def _sma(values, period):
    if len(values) < period:
        return sum(values) / max(1, len(values))
    return sum(values[-period:]) / period


def _vwap_last(rows, period=30):
    subset = rows[-period:]
    pv = 0.0
    vol = 0.0
    for r in subset:
        v = float(r.get("volume", 0) or 0)
        typical = (float(r["high"]) + float(r["low"]) + float(r["close"])) / 3.0
        pv += typical * v
        vol += v
    return pv / vol if vol > 1e-12 else float(subset[-1]["close"])


MODE_STRATEGIES = {
    "scalp": {"scalp_vwap_momentum", "scalp_ema_pullback", "scalp_breakout"},
    "intraday": {"intraday_trend", "intraday_vwap_pullback", "intraday_breakout"},
    "options": {"options_momentum", "options_breakout", "options_reversal"},
    "swing": {"swing_trend", "swing_pullback", "swing_breakout"},
}
ALL_LIVE_STRATEGIES = set().union(*MODE_STRATEGIES.values())

def compute_signal(rows, strategy="scalp_vwap_momentum"):
    """Mode-specific live-candle signal engine. No random inputs.

    Each strategy is intentionally matched to its trading horizon rather than
    reusing one generic scoring model for every mode.
    """
    if rows is None or len(rows) < 55:
        return {"action": "WAIT", "confidence": 0, "reason": "Insufficient real data yet", "strategy": strategy}

    close = [float(x["close"]) for x in rows]
    high = [float(x["high"]) for x in rows]
    low = [float(x["low"]) for x in rows]
    volume = [float(x.get("volume", 0) or 0) for x in rows]
    e9 = _ema_values(close, 9); e21 = _ema_values(close, 21); e50 = _ema_values(close, 50)
    ef = _ema_values(close, 12); es = _ema_values(close, 26)
    macd_line = [a-b for a,b in zip(ef, es)]
    signal_line = _ema_values(macd_line, 9)
    hist = [a-b for a,b in zip(macd_line, signal_line)]

    price = close[-1]; prev_price = close[-2]
    last_e9, last_e21, last_e50 = e9[-1], e21[-1], e50[-1]
    prev_e9, prev_e21 = e9[-2], e21[-2]
    last_r = _rsi_last(close, 14)
    last_hist, prev_hist = hist[-1], hist[-2]
    avg_vol = _sma(volume[:-1] if len(volume)>1 else volume, 20)
    vol_ratio = volume[-1] / avg_vol if avg_vol > 1e-12 else 1.0
    vwap = _vwap_last(rows, 30)
    prev20_high = max(high[-21:-1]); prev20_low = min(low[-21:-1])
    prev10_high = max(high[-11:-1]); prev10_low = min(low[-11:-1])
    recent_range = max(high[-10:]) - min(low[-10:])
    range_pct = recent_range / price * 100 if price else 0

    bull = bear = 0.0
    reasons=[]
    total=6.0

    # ── SCALPING: fast confirmation, VWAP/EMA9/EMA21 and immediate momentum ──
    if strategy == "scalp_vwap_momentum":
        total=7.0
        if last_e9 > last_e21: bull+=1.5; reasons.append("EMA9>21")
        elif last_e9 < last_e21: bear+=1.5; reasons.append("EMA9<21")
        if price > vwap: bull+=1.5; reasons.append("above VWAP")
        elif price < vwap: bear+=1.5; reasons.append("below VWAP")
        if last_hist > 0 and last_hist >= prev_hist: bull+=1.5; reasons.append("MACD accelerating +")
        elif last_hist < 0 and last_hist <= prev_hist: bear+=1.5; reasons.append("MACD accelerating -")
        if 52 <= last_r <= 68: bull+=1; reasons.append(f"RSI {last_r:.0f}")
        elif 32 <= last_r <= 48: bear+=1; reasons.append(f"RSI {last_r:.0f}")
        if vol_ratio >= 1.15: bull += .75 if price>last_e9 else 0; bear += .75 if price<last_e9 else 0; reasons.append(f"vol {vol_ratio:.1f}x")
        if price > prev10_high: bull+=.75; reasons.append("micro breakout")
        elif price < prev10_low: bear+=.75; reasons.append("micro breakdown")

    elif strategy == "scalp_ema_pullback":
        total=6.5
        up=last_e9>last_e21; dn=last_e9<last_e21
        near9=abs(price-last_e9)/price <= .0025
        if up: bull+=2; reasons.append("fast trend up")
        elif dn: bear+=2; reasons.append("fast trend down")
        if near9 and up and price>=prev_price: bull+=1.5; reasons.append("EMA9 bounce")
        elif near9 and dn and price<=prev_price: bear+=1.5; reasons.append("EMA9 rejection")
        if last_hist>0: bull+=1; reasons.append("MACD+")
        elif last_hist<0: bear+=1; reasons.append("MACD-")
        if price>vwap: bull+=1
        elif price<vwap: bear+=1
        if 45<=last_r<=65 and up: bull+=1
        elif 35<=last_r<=55 and dn: bear+=1

    elif strategy == "scalp_breakout":
        total=6.0
        if price>prev10_high: bull+=2; reasons.append("10-bar breakout")
        elif price<prev10_low: bear+=2; reasons.append("10-bar breakdown")
        if vol_ratio>=1.25: bull += 1 if price>prev_price else 0; bear += 1 if price<prev_price else 0; reasons.append(f"vol {vol_ratio:.1f}x")
        if last_e9>last_e21: bull+=1
        elif last_e9<last_e21: bear+=1
        if last_hist>0: bull+=1
        elif last_hist<0: bear+=1
        if price>vwap: bull+=1
        elif price<vwap: bear+=1

    # ── INTRADAY: slower trend structure and stronger confirmation ──
    elif strategy == "intraday_trend":
        total=7.0
        if last_e9>last_e21>last_e50: bull+=2; reasons.append("EMA trend up")
        elif last_e9<last_e21<last_e50: bear+=2; reasons.append("EMA trend down")
        if last_hist>0: bull+=1.5; reasons.append("MACD+")
        elif last_hist<0: bear+=1.5; reasons.append("MACD-")
        if 52<=last_r<=68: bull+=1; reasons.append(f"RSI {last_r:.0f}")
        elif 32<=last_r<=48: bear+=1; reasons.append(f"RSI {last_r:.0f}")
        if price>vwap: bull+=1
        elif price<vwap: bear+=1
        if vol_ratio>=1.10: bull += .75 if price>last_e21 else 0; bear += .75 if price<last_e21 else 0
        if last_e21>last_e50 and price>last_e9: bull+=.75
        elif last_e21<last_e50 and price<last_e9: bear+=.75

    elif strategy == "intraday_vwap_pullback":
        total=6.5
        up=last_e21>last_e50; dn=last_e21<last_e50
        near=abs(price-vwap)/price<=.005 or abs(price-last_e21)/price<=.006
        if up: bull+=2; reasons.append("trend up")
        elif dn: bear+=2; reasons.append("trend down")
        if near and up and price>prev_price: bull+=1.5; reasons.append("pullback recovery")
        elif near and dn and price<prev_price: bear+=1.5; reasons.append("pullback rejection")
        if last_hist>0: bull+=1
        elif last_hist<0: bear+=1
        if 42<=last_r<=62 and up: bull+=1
        elif 38<=last_r<=58 and dn: bear+=1
        if vol_ratio>=1.05: bull += .5 if up else 0; bear += .5 if dn else 0
        if price>vwap and up: bull+=.5
        elif price<vwap and dn: bear+=.5

    elif strategy == "intraday_breakout":
        total=7.0
        if price>prev20_high: bull+=2.5; reasons.append("20-bar breakout")
        elif price<prev20_low: bear+=2.5; reasons.append("20-bar breakdown")
        if vol_ratio>=1.25: bull += 1 if price>prev_price else 0; bear += 1 if price<prev_price else 0; reasons.append(f"vol {vol_ratio:.1f}x")
        if last_e21>last_e50: bull+=1
        elif last_e21<last_e50: bear+=1
        if last_hist>0: bull+=1
        elif last_hist<0: bear+=1
        if 50<=last_r<72: bull+=1
        elif 28<last_r<=50: bear+=1
        if price>vwap: bull+=.5
        elif price<vwap: bear+=.5

    # ── OPTIONS MODE: signal on underlying; favors impulse/volatility setups ──
    elif strategy == "options_momentum":
        total=7.0
        if last_e9>last_e21: bull+=1.5; reasons.append("fast trend up")
        elif last_e9<last_e21: bear+=1.5; reasons.append("fast trend down")
        if last_hist>0 and last_hist>prev_hist: bull+=2; reasons.append("momentum expansion +")
        elif last_hist<0 and last_hist<prev_hist: bear+=2; reasons.append("momentum expansion -")
        if vol_ratio>=1.25: bull += 1 if price>prev_price else 0; bear += 1 if price<prev_price else 0; reasons.append(f"vol {vol_ratio:.1f}x")
        if 55<=last_r<=72: bull+=1
        elif 28<=last_r<=45: bear+=1
        if price>vwap: bull+=1
        elif price<vwap: bear+=1
        if range_pct>=0.4: bull += .5 if price>prev_price else 0; bear += .5 if price<prev_price else 0

    elif strategy == "options_breakout":
        total=7.0
        if price>prev20_high: bull+=2.5; reasons.append("breakout")
        elif price<prev20_low: bear+=2.5; reasons.append("breakdown")
        if vol_ratio>=1.35: bull += 1.5 if price>prev_price else 0; bear += 1.5 if price<prev_price else 0; reasons.append("volume expansion")
        if last_hist>0: bull+=1
        elif last_hist<0: bear+=1
        if last_e9>last_e21: bull+=1
        elif last_e9<last_e21: bear+=1
        if price>vwap: bull+=1
        elif price<vwap: bear+=1

    elif strategy == "options_reversal":
        total=6.0
        # Mean-reversion signal after extended RSI, but only after momentum turns.
        if last_r<=30 and last_hist>prev_hist and price>prev_price: bull+=2.5; reasons.append("oversold reversal")
        elif last_r>=70 and last_hist<prev_hist and price<prev_price: bear+=2.5; reasons.append("overbought reversal")
        if price>last_e9 and prev_price<=prev_e9: bull+=1.5; reasons.append("EMA9 reclaim")
        elif price<last_e9 and prev_price>=prev_e9: bear+=1.5; reasons.append("EMA9 rejection")
        if vol_ratio>=1.15: bull += 1 if bull>bear else 0; bear += 1 if bear>bull else 0
        if price>vwap and bull>bear: bull+=1
        elif price<vwap and bear>bull: bear+=1

    # ── SWING: broad trend / pullback / breakout structure ──
    elif strategy == "swing_trend":
        total=7.0
        if last_e9>last_e21>last_e50: bull+=2.5; reasons.append("major trend up")
        elif last_e9<last_e21<last_e50: bear+=2.5; reasons.append("major trend down")
        if last_hist>0: bull+=1.5
        elif last_hist<0: bear+=1.5
        if 50<=last_r<=67: bull+=1
        elif 33<=last_r<=50: bear+=1
        if price>last_e21: bull+=1
        elif price<last_e21: bear+=1
        if vol_ratio>=1.05: bull += .5 if price>last_e21 else 0; bear += .5 if price<last_e21 else 0
        if price>vwap: bull+=.5
        elif price<vwap: bear+=.5

    elif strategy == "swing_pullback":
        total=6.5
        up=last_e21>last_e50; dn=last_e21<last_e50
        near21=abs(price-last_e21)/price<=.015
        if up: bull+=2.5; reasons.append("major trend up")
        elif dn: bear+=2.5; reasons.append("major trend down")
        if near21 and up and last_hist>prev_hist: bull+=1.5; reasons.append("pullback turning up")
        elif near21 and dn and last_hist<prev_hist: bear+=1.5; reasons.append("pullback turning down")
        if 40<=last_r<=60 and up: bull+=1
        elif 40<=last_r<=60 and dn: bear+=1
        if price>vwap and up: bull+=.75
        elif price<vwap and dn: bear+=.75
        if vol_ratio>=1.0: bull += .75 if up else 0; bear += .75 if dn else 0

    elif strategy == "swing_breakout":
        total=7.0
        if price>prev20_high: bull+=2.5; reasons.append("multi-bar breakout")
        elif price<prev20_low: bear+=2.5; reasons.append("multi-bar breakdown")
        if last_e21>last_e50: bull+=1.5
        elif last_e21<last_e50: bear+=1.5
        if last_hist>0: bull+=1
        elif last_hist<0: bear+=1
        if vol_ratio>=1.20: bull += 1 if price>prev_price else 0; bear += 1 if price<prev_price else 0
        if 50<=last_r<75: bull+=1
        elif 25<last_r<=50: bear+=1

    else:
        # Safe fallback: no trade instead of silently running a strategy from a
        # different mode.
        return {"action":"WAIT", "confidence":0, "price":float(price), "reason":f"Strategy {strategy} is not valid for this mode", "rsi":round(float(last_r),1), "strategy":strategy, "volume_ratio":round(float(vol_ratio),2), "vwap":round(float(vwap),8)}

    max_score=max(bull,bear)
    confidence=min(95, int(round((max_score/max(total,1e-9))*100)))
    action="BUY" if bull>bear else "SELL" if bear>bull else "WAIT"
    if confidence < 55:
        action="WAIT"
    return {"action":action,"confidence":confidence,"price":float(price),"reason":", ".join(reasons) if reasons else "No clear setup","rsi":round(float(last_r),1),"strategy":strategy,"volume_ratio":round(float(vol_ratio),2),"vwap":round(float(vwap),8)}


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
            # One retry on transient DB connection errors (belt-and-braces
            # alongside pool_pre_ping above — covers the rare race where a
            # connection dies between the ping and the actual query).
            try:
                current_user = User.query.get(data["user_id"])
            except OperationalError:
                db.session.rollback()
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


def perform_market_scan(current_user, requested_interval="15m", requested_confirm="1h", strategy="confluence", trade_mode="scalp", log_signals=True):
    """Shared real-market scan used by both HTTP requests and the 24x7 backend worker."""
    requested_interval = (requested_interval or "15m").lower()
    requested_confirm = (requested_confirm or "1h").lower()
    trade_mode = (trade_mode or "scalp").lower()
    rules = MODE_RULES.get(trade_mode, MODE_RULES["scalp"])
    strategy = (strategy or rules["default_strategy"]).lower()
    allowed_strategies = MODE_STRATEGIES.get(trade_mode, MODE_STRATEGIES["scalp"])
    if strategy not in allowed_strategies:
        strategy = rules["default_strategy"]
    interval_map = {"1m":"1m", "5m":"5m", "15m":"15m", "30m":"30m", "1h":"1h", "4h":"4h", "1d":"1d"}
    interval = interval_map.get(requested_interval, "15m")
    confirm_interval = interval_map.get(requested_confirm, "1h")

    ticker_map, ticker_global_err = fetch_all_tickers()
    candle_results = {}
    jobs = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        for pair in WATCHLIST:
            jobs[pool.submit(fetch_candles, pair, interval, 200)] = (pair, "primary")
            jobs[pool.submit(fetch_candles, pair, confirm_interval, 200)] = (pair, "confirm")
        for fut in as_completed(jobs):
            pair, kind = jobs[fut]
            try:
                candle_results[(pair, kind)] = fut.result()
            except Exception as e:
                candle_results[(pair, kind)] = (None, f"{type(e).__name__}: {e}")

    results = []
    for pair in WATCHLIST:
        df, err = candle_results.get((pair, "primary"), (None, "Primary candle request missing"))
        cdf, cerr = candle_results.get((pair, "confirm"), (None, "Confirmation candle request missing"))
        primary = compute_signal(df, strategy)
        confirm = compute_signal(cdf, strategy)
        sig = dict(primary)

        p_action = primary.get("action", "WAIT")
        c_action = confirm.get("action", "WAIT")
        confirmation_ok = p_action in ("BUY", "SELL") and p_action == c_action
        if p_action in ("BUY", "SELL") and not confirmation_ok:
            sig["action"] = "WAIT"
            sig["confidence"] = min(int(primary.get("confidence", 0)), 49)
            sig["reason"] = f"Primary {p_action} not confirmed by {confirm_interval} ({c_action}) | " + primary.get("reason", "")
        elif confirmation_ok:
            sig["confidence"] = min(95, round((int(primary.get("confidence",0))*0.65) + (int(confirm.get("confidence",0))*0.35)))
            sig["reason"] = f"MTF confirmed {p_action} ({interval}+{confirm_interval}) | " + primary.get("reason", "")

        live_price, ticker_ts, ticker_err = fetch_live_ticker_price(pair, ticker_map)
        if ticker_global_err and not ticker_err:
            ticker_err = ticker_global_err
        if live_price is not None:
            sig["price"] = live_price
        sig["pair"] = pair
        sig["strategy"] = strategy
        sig["trade_mode"] = trade_mode
        sig["display_pair"] = pair.replace("B-", "").replace("_", "/")
        sig["currency"] = "USDT"
        candle_sources = {
            row.get("_source") for rows in (df, cdf) if rows for row in rows[-1:] if row.get("_source")
        }
        ticker_source = ticker_map.get("__SOURCE__") if isinstance(ticker_map, dict) else None
        if ticker_source:
            candle_sources.add(ticker_source)
        if df and cdf and live_price is not None and candle_sources:
            sig["data_source"] = "/".join(sorted(candle_sources)) + " public data"
        else:
            sig["data_source"] = "partial/unavailable"
        sig["requested_interval"] = requested_interval
        sig["actual_interval"] = interval
        sig["confirm_requested_interval"] = requested_confirm
        sig["confirm_actual_interval"] = confirm_interval
        sig["confirmation_action"] = c_action
        sig["confirmation_confidence"] = confirm.get("confidence", 0)
        sig["confirmation_ok"] = confirmation_ok
        if df:
            candle_ms = int(df[-1].get("time", 0) or 0)
            sig["candle_time"] = datetime.datetime.fromtimestamp(candle_ms/1000.0, tz=datetime.timezone.utc).isoformat() if candle_ms else None
        if cdf:
            ccandle_ms = int(cdf[-1].get("time", 0) or 0)
            sig["confirm_candle_time"] = datetime.datetime.fromtimestamp(ccandle_ms/1000.0, tz=datetime.timezone.utc).isoformat() if ccandle_ms else None
        if ticker_ts is not None:
            sig["quote_time"] = ticker_ts.isoformat()
            sig["feed_age_seconds"] = max(0, int((datetime.datetime.now(datetime.timezone.utc) - ticker_ts).total_seconds()))
        else:
            sig["quote_time"] = None
            sig["feed_age_seconds"] = None
        errors = [x for x in (err, cerr, ticker_err) if x]
        if errors:
            sig["reason"] = (sig.get("reason", "") + " | " + " | ".join(errors)).strip(" |")
        results.append(sig)
        if log_signals:
            db.session.add(SignalLog(
                user_id=current_user.id, pair=sig.get("display_pair") or pair,
                action=sig.get("action"), confidence=float(sig.get("confidence", 0) or 0),
                price=float(sig.get("price", 0) or 0), strategy=strategy, trade_mode=trade_mode,
                primary_tf=interval, confirm_tf=confirm_interval, confirm_action=c_action,
                data_source=sig.get("data_source", ""), reason=(sig.get("reason", "") or "")[:500]
            ))

    if log_signals:
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            app.logger.exception("Signal journal commit failed")

    return {
        "signals": results, "mode": current_user.mode,
        "requested_interval": requested_interval, "actual_interval": interval,
        "confirm_requested_interval": requested_confirm, "confirm_actual_interval": confirm_interval,
        "feed_status": "ok" if any("public data" in x.get("data_source", "") for x in results) else "unavailable",
        "strategy": strategy, "trade_mode": trade_mode,
    }


@app.route("/api/scan", methods=["GET"])
@token_required
def scan(current_user):
    try:
        payload = perform_market_scan(
            current_user,
            request.args.get("interval") or "15m",
            request.args.get("confirm_interval") or "1h",
            request.args.get("strategy") or "confluence",
            request.args.get("trade_mode") or "scalp",
            log_signals=True,
        )
        return jsonify(payload), 200
    except Exception as e:
        app.logger.exception("/api/scan crashed")
        return jsonify({"error": f"SCAN_ENGINE_ERROR: {type(e).__name__}: {e}", "hint": "Check Render logs for the full traceback."}), 500


@app.route("/api/feed-test", methods=["GET"])
@token_required
def feed_test(current_user):
    """Fast diagnostics for CoinDCX public ticker/candle connectivity."""
    out = {"ticker": {}, "candles": {}}
    ticker_map, terr = fetch_all_tickers()
    out["ticker"]["ok"] = not bool(terr)
    out["ticker"]["error"] = terr
    for pair in WATCHLIST:
        market = pair.replace("B-", "").replace("_", "").upper()
        item = ticker_map.get(market)
        out["ticker"][pair] = item[0] if item else None
    # One pair is enough to verify each native timeframe quickly.
    for tf in ("1m", "15m", "1h", "1d"):
        df, err = fetch_candles("B-BTC_USDT", tf, 60)
        out["candles"][tf] = {"ok": df is not None and len(df) > 0, "rows": 0 if df is None else len(df), "error": err}
    return jsonify(out), 200


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

    df, err = fetch_candles(pair, interval="1m", limit=5)
    if df is None:
        return jsonify({"error": f"Could not fetch live price: {err}"}), 502
    price, _, ticker_err = fetch_live_ticker_price(pair)
    if price is None:
        price = float(df[-1]["close"])

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
    try:
        db.session.add(TradeEvent(
            user_id=current_user.id, event="ENTRY", pair=pair, side=side,
            price=price, entry=price, leverage=1, confidence=float(confidence or 0),
            strategy=current_user.strategy or "confluence", trade_mode=current_user.mode,
            reason=note[:120], source="CoinDCX" if current_user.mode == "live" else "Public Feed"
        ))
    except Exception:
        app.logger.exception("Could not queue execute trade journal")
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
# SIGNAL / TRADE JOURNALS + CSV EXPORT
# ─────────────────────────────────────────────
@app.route("/api/trade-events", methods=["POST"])
@token_required
def log_trade_event(current_user):
    data = request.get_json(force=True) or {}
    ev = TradeEvent(
        user_id=current_user.id, event=(data.get("event") or "ENTRY")[:10],
        pair=data.get("pair"), side=data.get("side"), price=data.get("price"),
        entry=data.get("entry"), exit_price=data.get("exit"), pnl=data.get("pnl", 0),
        leverage=data.get("leverage", 1), confidence=data.get("confidence"),
        strategy=(data.get("strategy") or "confluence")[:40],
        trade_mode=(data.get("trade_mode") or "scalp")[:20],
        reason=(data.get("reason") or "")[:120], source=(data.get("source") or "")[:80],
    )
    db.session.add(ev); db.session.commit()
    return jsonify({"ok": True, "id": ev.id}), 201


def _csv_response(filename, headers, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for row in rows: w.writerow(row)
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.route("/api/export/signals.csv", methods=["GET"])
@token_required
def export_signals(current_user):
    q = SignalLog.query.filter_by(user_id=current_user.id).order_by(SignalLog.created_at.asc()).all()
    return _csv_response("algobot_signal_journal.csv",
        ["time_utc","mode","strategy","pair","action","confidence","price","primary_tf","confirm_tf","confirm_action","data_source","reason"],
        [[x.created_at.isoformat(),x.trade_mode,x.strategy,x.pair,x.action,x.confidence,x.price,x.primary_tf,x.confirm_tf,x.confirm_action,x.data_source,x.reason] for x in q])


@app.route("/api/export/trades.csv", methods=["GET"])
@token_required
def export_trade_events(current_user):
    q = TradeEvent.query.filter_by(user_id=current_user.id).order_by(TradeEvent.created_at.asc()).all()
    return _csv_response("algobot_trade_journal.csv",
        ["time_utc","event","mode","strategy","pair","side","price","entry","exit","pnl","leverage","confidence","source","reason"],
        [[x.created_at.isoformat(),x.event,x.trade_mode,x.strategy,x.pair,x.side,x.price,x.entry,x.exit_price,x.pnl,x.leverage,x.confidence,x.source,x.reason] for x in q])


# ─────────────────────────────────────────────
# 24x7 BACKEND AUTO-PAPER WORKER
# Runs independently of the browser. State and open positions are persisted
# in the database, so a Render process restart can resume the paper test.
# This worker NEVER places live-money orders.
# ─────────────────────────────────────────────
MODE_RULES = {
    # primary/confirm timeframes, default SL/TP, max positions, maximum holding time.
    # Max-hold is a safety/time-style exit; TP/SL can still close earlier.
    "scalp":    {"primary":"1m",  "confirm":"5m",  "sl":0.35, "tp":0.55, "max":5, "max_hold_min":30,    "default_strategy":"scalp_vwap_momentum"},
    "intraday": {"primary":"15m", "confirm":"1h",  "sl":0.80, "tp":1.60, "max":3, "max_hold_min":720,   "default_strategy":"intraday_trend"},
    "options":  {"primary":"5m",  "confirm":"15m", "sl":0.70, "tp":1.40, "max":3, "max_hold_min":240,   "default_strategy":"options_momentum"},
    "swing":    {"primary":"4h",  "confirm":"1d",  "sl":3.00, "tp":6.00, "max":3, "max_hold_min":10080, "default_strategy":"swing_trend"},
}
AUTO_ENTRY_MIN_CONFIDENCE = 75.0
_worker_stop = threading.Event()
_worker_started = False
_worker_lock = threading.Lock()


def _utcnow():
    return datetime.datetime.utcnow()


def _daily_realized_loss(user_id):
    today = _utcnow().date()
    start = datetime.datetime.combine(today, datetime.time.min)
    rows = PaperPosition.query.filter(
        PaperPosition.user_id == user_id,
        PaperPosition.status == "CLOSED",
        PaperPosition.closed_at >= start,
        PaperPosition.pnl < 0,
    ).all()
    return sum(abs(float(x.pnl or 0)) for x in rows)


def _close_position(pos, price, reason, user):
    price = float(price)
    if pos.side == "BUY":
        pnl = (price - pos.entry) * pos.quantity
    else:
        pnl = (pos.entry - price) * pos.quantity
    pos.status = "CLOSED"
    pos.exit_price = price
    pos.pnl = pnl
    pos.exit_reason = reason
    pos.closed_at = _utcnow()
    pos.last_price = price
    pos.last_checked_at = _utcnow()
    user.portfolio = float(user.portfolio or user.capital or 0) + pnl
    db.session.add(TradeEvent(
        user_id=user.id, event="EXIT", pair=pos.pair, side=pos.side,
        price=price, entry=pos.entry, exit_price=price, pnl=pnl,
        leverage=pos.leverage or 1, confidence=pos.confidence,
        strategy=pos.strategy or "confluence", trade_mode=pos.trade_mode or "scalp",
        reason=reason[:120], source=(pos.source or "Public Feed")[:80],
    ))
    db.session.add(Trade(
        user_id=user.id, pair=pos.pair, side=pos.side, entry=pos.entry,
        exit_price=price, leverage=int(pos.leverage or 1), pnl=pnl,
        reason=reason[:50], trade_mode=pos.trade_mode or "scalp",
    ))
    return pnl


def _recent_1m_candles_light(pair, limit=3):
    """Fetch only a few genuine 1-minute candles for exit-touch verification.

    Unlike fetch_candles(), this helper intentionally does not require 55+ bars.
    CoinDCX is primary; Binance public data is the no-key fallback.
    """
    limit = max(2, min(10, int(limit or 3)))
    cd_err = None
    try:
        r = requests.get(
            f"{COINDCX_API}/market_data/candles",
            params={"pair": pair, "interval": "1m", "limit": limit},
            timeout=5,
            headers={"Accept": "application/json", "User-Agent": "AlgoBot-Pro-v6/10.2"},
        )
        if r.status_code == 200:
            rows = _parse_coindcx_candles(r.json())
            if rows:
                return rows[-limit:], None
            cd_err = "CoinDCX returned no recent 1m candles"
        else:
            cd_err = f"CoinDCX recent candle HTTP {r.status_code}"
    except Exception as e:
        cd_err = f"CoinDCX recent candle {type(e).__name__}: {e}"
    try:
        rows, b_err = _fetch_binance_candles(pair, "1m", limit)
        if rows:
            return rows[-limit:], None
        return None, f"{cd_err}; Binance: {b_err or 'no recent candles'}"
    except Exception as e:
        return None, f"{cd_err}; Binance recent candle {type(e).__name__}: {e}"


def _position_trigger_from_price(pos, px):
    """Return Stop Loss / Take Profit if a current price has crossed the stored level."""
    px = float(px)
    sl = float(pos.sl)
    tp = float(pos.tp)
    if pos.side == "BUY":
        if px <= sl:
            return "Stop Loss"
        if px >= tp:
            return "Take Profit"
    else:
        if px >= sl:
            return "Stop Loss"
        if px <= tp:
            return "Take Profit"
    return None


def _position_trigger_from_candles(pos, rows):
    """Detect a TP/SL touch inside recent 1m OHLC bars.

    If both TP and SL were touched in the same 1m candle, the order of touches is
    unknowable from OHLC alone, so use the conservative paper-testing assumption:
    Stop Loss wins. This avoids overstating strategy performance.
    """
    if not rows:
        return None
    opened_ms = int(pos.opened_at.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000) if pos.opened_at else 0
    sl = float(pos.sl)
    tp = float(pos.tp)
    for row in sorted(rows, key=lambda x: x.get("time", 0)):
        # Keep the entry minute because the trade may have opened during that bar.
        if opened_ms and int(row.get("time", 0)) + 60_000 < opened_ms:
            continue
        hi = float(row.get("high", 0) or 0)
        lo = float(row.get("low", 0) or 0)
        if hi <= 0 or lo <= 0:
            continue
        if pos.side == "BUY":
            sl_hit = lo <= sl
            tp_hit = hi >= tp
        else:
            sl_hit = hi >= sl
            tp_hit = lo <= tp
        if sl_hit and tp_hit:
            return "Stop Loss (same-candle conservative)"
        if sl_hit:
            return "Stop Loss (1m touch)"
        if tp_hit:
            return "Take Profit (1m touch)"
    return None


def _monitor_open_positions_fast(cfg, user, now):
    """Robust PAPER exit watchdog.

    1) Check a fresh all-market public ticker snapshot every heartbeat (~5s).
    2) For positions not closed by the ticker, verify recent 1m candle high/low so
       a brief TP/SL touch between ticker polls is not missed.
    3) Persist last price/check time even when no exit occurs, so the dashboard
       can prove that the watchdog is alive.
    """
    positions = PaperPosition.query.filter_by(user_id=user.id, status="OPEN").all()
    if not positions:
        return 0

    ticker_map, ticker_err = fetch_all_tickers()
    exits = 0
    survivors = []

    for pos in positions:
        px = None
        if ticker_map:
            px, _, _ = fetch_live_ticker_price(pos.pair, ticker_map=ticker_map)
        if px is not None:
            px = float(px)
            pos.last_price = px
            pos.last_checked_at = now
            mode_rule = MODE_RULES.get((pos.trade_mode or "scalp").lower(), MODE_RULES["scalp"])
            max_hold_min = int(mode_rule.get("max_hold_min", 0) or 0)
            held_min = ((now - pos.opened_at).total_seconds() / 60.0) if pos.opened_at else 0
            if max_hold_min and held_min >= max_hold_min:
                _close_position(pos, px, f"Time Exit ({max_hold_min}m max hold)", user)
                exits += 1
                continue
            reason = _position_trigger_from_price(pos, px)
            if reason:
                _close_position(pos, px, reason, user)
                exits += 1
                continue
        survivors.append(pos)

    # OHLC touch verification runs in parallel for the remaining positions.
    # This catches short-lived target/SL touches that a 5s ticker sample can miss.
    if survivors:
        def get_touch(pos):
            rows, err = _recent_1m_candles_light(pos.pair, 3)
            return pos.id, rows, err
        results = {}
        with ThreadPoolExecutor(max_workers=min(4, len(survivors))) as ex:
            futs = [ex.submit(get_touch, p) for p in survivors]
            for fut in as_completed(futs):
                try:
                    pid, rows, err = fut.result()
                    results[pid] = (rows, err)
                except Exception as e:
                    app.logger.warning("Exit candle verifier failed: %s", e)

        for pos in survivors:
            if pos.status != "OPEN":
                continue
            rows, err = results.get(pos.id, (None, None))
            if rows:
                # Use latest close as a fallback last_price when ticker is missing.
                try:
                    pos.last_price = float(rows[-1]["close"])
                    pos.last_checked_at = now
                except Exception:
                    pass
                mode_rule = MODE_RULES.get((pos.trade_mode or "scalp").lower(), MODE_RULES["scalp"])
                max_hold_min = int(mode_rule.get("max_hold_min", 0) or 0)
                held_min = ((now - pos.opened_at).total_seconds() / 60.0) if pos.opened_at else 0
                if max_hold_min and held_min >= max_hold_min:
                    _close_position(pos, float(pos.last_price or rows[-1]["close"]), f"Time Exit ({max_hold_min}m max hold)", user)
                    exits += 1
                    continue
                reason = _position_trigger_from_candles(pos, rows)
                if reason:
                    exit_px = float(pos.last_price or rows[-1]["close"])
                    # For a detected historical touch, record the actual trigger level
                    # rather than a later price, making paper P&L deterministic.
                    if reason.startswith("Stop Loss"):
                        exit_px = float(pos.sl)
                    elif reason.startswith("Take Profit"):
                        exit_px = float(pos.tp)
                    _close_position(pos, exit_px, reason, user)
                    exits += 1
            elif err and not ticker_map:
                cfg.last_error = ("Exit watchdog data unavailable: " + err)[:500]

    if not ticker_map and ticker_err and not cfg.last_error:
        cfg.last_error = ("Exit watchdog ticker: " + ticker_err)[:500]
    if exits:
        cfg.exits_count = int(cfg.exits_count or 0) + exits
    db.session.flush()
    return exits

def _process_auto_config(cfg):
    user = db.session.get(User, cfg.user_id)
    if not user or not cfg.enabled:
        return
    # Hard safety boundary: the 24x7 worker is paper-only.
    if user.mode != "paper":
        cfg.enabled = False
        cfg.last_error = "Auto worker stopped because account mode is not PAPER"
        db.session.commit()
        return

    now = _utcnow()

    # TP/SL watchdog runs on every ~5s worker heartbeat, independently of the
    # slower signal scan cadence. This prevents a target touch being missed.
    try:
        _monitor_open_positions_fast(cfg, user, now)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        cfg = db.session.get(AutoBotConfig, cfg.id)
        if cfg:
            cfg.last_error = f"Exit watchdog {type(e).__name__}: {e}"[:500]
            try: db.session.commit()
            except Exception: db.session.rollback()
        app.logger.exception("24x7 TP/SL watchdog failed for user %s", user.id)

    # Full indicator/signal scan respects the configured interval.
    if cfg.last_scan_at and (now - cfg.last_scan_at).total_seconds() < max(30, int(cfg.scan_interval_sec or 60)):
        return
    cfg.last_scan_at = now
    cfg.scans_count = int(cfg.scans_count or 0) + 1

    try:
        payload = perform_market_scan(
            user, cfg.primary_tf, cfg.confirm_tf, cfg.strategy, cfg.trade_mode, log_signals=True
        )
        signals = payload.get("signals", [])
        cfg.last_source = ", ".join(sorted({s.get("data_source", "") for s in signals if "public data" in s.get("data_source", "")}))[:100]
        price_map = {s.get("pair"): float(s.get("price", 0) or 0) for s in signals if float(s.get("price", 0) or 0) > 0}

        # Existing positions are monitored by the independent ~5s TP/SL watchdog above.


        # Enforce daily-loss and max-position controls before new entries.
        if _daily_realized_loss(user.id) >= float(cfg.daily_loss_limit or 3000):
            cfg.last_error = "Daily loss limit reached; entries blocked"
            cfg.last_success_at = now
            db.session.commit()
            return

        active = PaperPosition.query.filter_by(user_id=user.id, status="OPEN").count()
        max_pos = max(1, int(cfg.max_positions or 1))
        for sig in signals:
            if active >= max_pos:
                break
            action = sig.get("action")
            conf = float(sig.get("confidence", 0) or 0)
            source = sig.get("data_source", "")
            price = float(sig.get("price", 0) or 0)
            if action not in ("BUY", "SELL"):
                continue
            if conf < max(AUTO_ENTRY_MIN_CONFIDENCE, float(cfg.min_confidence or 75)):
                continue
            if not sig.get("confirmation_ok") or "public data" not in source or price <= 0:
                continue
            if PaperPosition.query.filter_by(user_id=user.id, pair=sig.get("pair"), status="OPEN").first():
                continue
            # Cooldown: do not re-enter same symbol/mode inside two scan intervals.
            recent = PaperPosition.query.filter_by(user_id=user.id, pair=sig.get("pair"), trade_mode=cfg.trade_mode).order_by(PaperPosition.opened_at.desc()).first()
            if recent and recent.opened_at and (now - recent.opened_at).total_seconds() < max(120, int(cfg.scan_interval_sec or 60) * 2):
                continue

            sl_pct = float(cfg.sl_pct or 0.5) / 100.0
            tp_pct = float(cfg.tp_pct or 1.0) / 100.0
            sl = price * (1 - sl_pct) if action == "BUY" else price * (1 + sl_pct)
            tp = price * (1 + tp_pct) if action == "BUY" else price * (1 - tp_pct)
            # Conservative paper sizing: risk_pct is the capital allocated, not leveraged account risk.
            notional = max(0.0, float(user.portfolio or user.capital or 0) * float(cfg.risk_pct or 1.5) / 100.0)
            if notional <= 0:
                continue
            qty = notional / price
            pos = PaperPosition(
                user_id=user.id, pair=sig.get("pair"), display_pair=sig.get("display_pair"),
                side=action, entry=price, sl=sl, tp=tp, quantity=qty, notional=notional,
                leverage=1, confidence=conf, strategy=cfg.strategy, trade_mode=cfg.trade_mode,
                source=source[:80], status="OPEN", last_price=price, last_checked_at=now,
            )
            db.session.add(pos)
            db.session.add(TradeEvent(
                user_id=user.id, event="ENTRY", pair=sig.get("pair"), side=action,
                price=price, entry=price, pnl=0, leverage=1, confidence=conf,
                strategy=cfg.strategy, trade_mode=cfg.trade_mode,
                reason=f"24x7 AUTO PAPER >= {cfg.min_confidence:.0f}%"[:120], source=source[:80],
            ))
            cfg.entries_count = int(cfg.entries_count or 0) + 1
            active += 1

        cfg.last_success_at = now
        cfg.last_error = ""
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        cfg = db.session.get(AutoBotConfig, cfg.id)
        if cfg:
            cfg.last_error = f"{type(e).__name__}: {e}"[:500]
            cfg.last_scan_at = now
            try: db.session.commit()
            except Exception: db.session.rollback()
        app.logger.exception("24x7 auto-paper worker scan failed for user %s", user.id if user else "?")


def _autobot_worker_loop():
    # Short heartbeat; each config independently respects its scan interval.
    while not _worker_stop.wait(5):
        try:
            with app.app_context():
                enabled_ids = [x.id for x in AutoBotConfig.query.filter_by(enabled=True).all()]
                for cfg_id in enabled_ids:
                    cfg = db.session.get(AutoBotConfig, cfg_id)
                    if cfg and cfg.enabled:
                        _process_auto_config(cfg)
                    db.session.remove()
        except Exception:
            app.logger.exception("24x7 worker loop error")
            try:
                with app.app_context(): db.session.remove()
            except Exception:
                pass


def start_autobot_worker_once():
    global _worker_started
    if os.getenv("DISABLE_AUTOBOT_WORKER", "0") == "1":
        return
    with _worker_lock:
        if _worker_started:
            return
        t = threading.Thread(target=_autobot_worker_loop, name="algobot-auto-paper", daemon=True)
        t.start()
        _worker_started = True
        app.logger.info("24x7 auto-paper backend worker started")


@app.route("/api/autobot", methods=["GET"])
@token_required
def autobot_status(current_user):
    cfg = AutoBotConfig.query.filter_by(user_id=current_user.id).first()
    positions = PaperPosition.query.filter_by(user_id=current_user.id, status="OPEN").order_by(PaperPosition.opened_at.asc()).all()
    if not cfg:
        return jsonify({"enabled": False, "worker_started": _worker_started, "positions": [], "message": "Not configured"}), 200
    return jsonify({
        "enabled": bool(cfg.enabled), "worker_started": _worker_started,
        "trade_mode": cfg.trade_mode, "strategy": cfg.strategy,
        "primary_tf": cfg.primary_tf, "confirm_tf": cfg.confirm_tf,
        "scan_interval_sec": cfg.scan_interval_sec, "min_confidence": cfg.min_confidence,
        "sl_pct": cfg.sl_pct, "tp_pct": cfg.tp_pct,
        "risk_reward": (round(float(cfg.tp_pct or 0) / float(cfg.sl_pct or 1), 2) if float(cfg.sl_pct or 0) > 0 else None),
        "max_hold_min": MODE_RULES.get((cfg.trade_mode or "scalp").lower(), MODE_RULES["scalp"]).get("max_hold_min"),
        "last_scan_at": cfg.last_scan_at.isoformat() if cfg.last_scan_at else None,
        "last_success_at": cfg.last_success_at.isoformat() if cfg.last_success_at else None,
        "last_error": cfg.last_error or "", "last_source": cfg.last_source or "",
        "scans_count": cfg.scans_count or 0, "entries_count": cfg.entries_count or 0, "exits_count": cfg.exits_count or 0,
        "positions": [dict(p.to_dict(),
            watchdog_age_sec=(round((_utcnow() - p.last_checked_at).total_seconds(), 1) if p.last_checked_at else None),
            held_minutes=(round((_utcnow() - p.opened_at).total_seconds()/60.0, 1) if p.opened_at else None),
            max_hold_minutes=MODE_RULES.get((p.trade_mode or "scalp").lower(), MODE_RULES["scalp"]).get("max_hold_min"),
            exit_state=(
                "SL_DUE" if p.last_price is not None and _position_trigger_from_price(p, p.last_price) and str(_position_trigger_from_price(p, p.last_price)).startswith("Stop") else
                "TP_DUE" if p.last_price is not None and _position_trigger_from_price(p, p.last_price) and str(_position_trigger_from_price(p, p.last_price)).startswith("Take") else
                "ACTIVE" if p.last_checked_at else "NOT_CHECKED"
            )
        ) for p in positions],
        "watchdog_last_check_at": max([p.last_checked_at for p in positions if p.last_checked_at] or [None]).isoformat() if any(p.last_checked_at for p in positions) else None,
        "portfolio": current_user.portfolio,
        "account_mode": current_user.mode,
    }), 200


@app.route("/api/autobot", methods=["POST"])
@token_required
def autobot_update(current_user):
    data = request.get_json(force=True) or {}
    enabled = bool(data.get("enabled", False))
    if enabled and current_user.mode != "paper":
        return jsonify({"error": "24x7 Auto Paper can only run while account mode is PAPER"}), 400
    mode = (data.get("trade_mode") or "scalp").lower()
    if mode == "india":
        return jsonify({"error": "India 24x7 paper worker is disabled until a reliable Indian live-data feed is connected"}), 400
    rules = MODE_RULES.get(mode, MODE_RULES["scalp"])
    strategy = (data.get("strategy") or current_user.strategy or rules["default_strategy"]).lower()
    allowed = MODE_STRATEGIES.get(mode, MODE_STRATEGIES["scalp"])
    if strategy not in allowed:
        strategy = rules["default_strategy"]
    cfg = AutoBotConfig.query.filter_by(user_id=current_user.id).first()
    if not cfg:
        cfg = AutoBotConfig(user_id=current_user.id)
        db.session.add(cfg)
    cfg.enabled = enabled
    cfg.trade_mode = mode
    cfg.strategy = strategy
    cfg.primary_tf = (data.get("primary_tf") or rules["primary"]).lower()
    cfg.confirm_tf = (data.get("confirm_tf") or rules["confirm"]).lower()
    cfg.scan_interval_sec = max(30, min(3600, int(data.get("scan_interval_sec") or 60)))
    cfg.min_confidence = max(AUTO_ENTRY_MIN_CONFIDENCE, float(data.get("min_confidence") or AUTO_ENTRY_MIN_CONFIDENCE))
    cfg.sl_pct = max(0.05, float(data.get("sl_pct") or rules["sl"]))
    cfg.tp_pct = max(0.05, float(data.get("tp_pct") or rules["tp"]))
    cfg.max_positions = max(1, min(20, int(data.get("max_positions") or rules["max"])))
    cfg.daily_loss_limit = max(0.0, float(data.get("daily_loss_limit") or 3000))
    cfg.risk_pct = max(0.1, min(10.0, float(data.get("risk_pct") or 1.5)))
    cfg.last_error = ""
    current_user.strategy = strategy
    db.session.commit()
    if enabled:
        start_autobot_worker_once()
    return jsonify({"ok": True, "enabled": cfg.enabled, "message": "24x7 backend Auto Paper started" if enabled else "24x7 backend Auto Paper stopped"}), 200


@app.route("/api/autobot/check-exits", methods=["POST"])
@token_required
def autobot_check_exits(current_user):
    """Manual diagnostic/repair trigger for PAPER exits.

    Safe to call from the dashboard: it never creates a new position and never
    places a live-money order. It only checks existing paper positions.
    """
    if current_user.mode != "paper":
        return jsonify({"error": "Exit checker is PAPER-only"}), 400
    cfg = AutoBotConfig.query.filter_by(user_id=current_user.id).first()
    if not cfg:
        return jsonify({"error": "AutoBot is not configured"}), 400
    try:
        exits = _monitor_open_positions_fast(cfg, current_user, _utcnow())
        db.session.commit()
        remaining = PaperPosition.query.filter_by(user_id=current_user.id, status="OPEN").all()
        return jsonify({"ok": True, "exits": exits, "remaining": [p.to_dict() for p in remaining]}), 200
    except Exception as e:
        db.session.rollback()
        app.logger.exception("Manual exit check failed")
        return jsonify({"error": f"EXIT_CHECK_ERROR: {type(e).__name__}: {e}"}), 500


@app.route("/api/autobot/positions/<int:position_id>/close", methods=["POST"])
@token_required
def autobot_manual_close_position(current_user, position_id):
    """Manually close one OPEN paper position at the latest real public market price.

    Safety boundary: PAPER mode only. This endpoint never sends a broker/exchange order.
    """
    if current_user.mode != "paper":
        return jsonify({"error": "Manual close is PAPER-only"}), 400

    pos = PaperPosition.query.filter_by(
        id=position_id, user_id=current_user.id, status="OPEN"
    ).first()
    if not pos:
        return jsonify({"error": "Open paper position not found"}), 404

    try:
        ticker_map, ticker_err = fetch_all_tickers()
        price = None
        source = None
        if ticker_map:
            price, _, px_err = fetch_live_ticker_price(pos.pair, ticker_map=ticker_map)
            if not px_err:
                source = ticker_map.get("__SOURCE__") if isinstance(ticker_map, dict) else None

        # A manual exit should still use a real observed market price. If the
        # live ticker is temporarily unavailable, allow only a very recent
        # watchdog price rather than inventing/fabricating an execution price.
        if price is None and pos.last_price is not None and pos.last_checked_at is not None:
            age = (_utcnow() - pos.last_checked_at).total_seconds()
            if age <= 30:
                price = float(pos.last_price)
                source = pos.source or "recent watchdog price"

        if price is None:
            return jsonify({
                "error": "Live price unavailable; manual paper exit was NOT executed",
                "detail": ticker_err or "No fresh watchdog price"
            }), 503

        reason = "Manual Exit"
        pnl = _close_position(pos, float(price), reason, current_user)
        cfg = AutoBotConfig.query.filter_by(user_id=current_user.id).first()
        if cfg:
            cfg.exits_count = int(cfg.exits_count or 0) + 1
            cfg.last_source = str(source or pos.source or "Public Feed")[:100]
        db.session.commit()
        return jsonify({
            "ok": True,
            "message": "Paper position closed manually",
            "position_id": pos.id,
            "pair": pos.display_pair or pos.pair,
            "side": pos.side,
            "exit_price": pos.exit_price,
            "pnl": pnl,
            "reason": reason,
            "source": source or pos.source or "Public Feed",
            "portfolio": current_user.portfolio,
        }), 200
    except Exception as e:
        db.session.rollback()
        app.logger.exception("Manual paper close failed for position %s", position_id)
        return jsonify({"error": f"MANUAL_CLOSE_ERROR: {type(e).__name__}: {e}"}), 500


@app.route("/api/autobot/positions", methods=["GET"])
@token_required
def autobot_positions(current_user):
    rows = PaperPosition.query.filter_by(user_id=current_user.id).order_by(PaperPosition.opened_at.desc()).limit(100).all()
    return jsonify({"positions": [p.to_dict() for p in rows]}), 200


# ─────────────────────────────────────────────
# HEALTH CHECK (Render uses this to confirm the service is alive)
# ─────────────────────────────────────────────
@app.route("/api/health", methods=["GET"])
def health():
    enabled = 0
    try:
        enabled = AutoBotConfig.query.filter_by(enabled=True).count()
    except Exception:
        pass
    return jsonify({
        "status": "ok", "time": datetime.datetime.utcnow().isoformat(),
        "autobot_worker_started": _worker_started, "autobot_enabled_users": enabled
    })


@app.route("/api/debug-candles", methods=["GET"])
def debug_candles():
    """Visit this in your browser to see the raw fetch attempt and error, if any."""
    pair = request.args.get("pair", "B-BTC_USDT")
    interval = request.args.get("interval", "1h")
    df, err = fetch_candles(pair, interval=interval, limit=5)
    if df is None:
        return jsonify({"pair": pair, "interval": interval, "success": False, "error": err}), 200
    return jsonify({
        "pair": pair, "interval": interval, "success": True,
        "rows_fetched": len(df),
        "latest_close": float(df[-1]["close"]),
        "latest_time": str(df[-1].get("time")),
    }), 200


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

start_autobot_worker_once()


@app.errorhandler(Exception)
def json_unhandled_error(e):
    # Keep API failures machine-readable instead of Flask's HTML 500 page.
    if request.path.startswith("/api/"):
        app.logger.error("Unhandled API exception: %s\n%s", e, traceback.format_exc())
        return jsonify({"error": f"UNHANDLED_SERVER_ERROR: {type(e).__name__}: {e}"}), 500
    raise e

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
