# ============================================================
# BINANCE EARLY-PUMP SCANNER V4.3 — دمج V4.1 + ميزات V4.2
# ============================================================
# V4.3 (2026-10-11) — دمج كامل:
# [جديد من V4.2] BTC Market Guard: إيقاف الإشارات عند انهيار السوق
# [جديد من V4.2] Funding Rate (bulk) + Short Squeeze lane
# [جديد من V4.2] Cumulative Taker Delta (buy_pressure محسّن)
# [إصلاح V4.2] Funding bulk fetch: 1 طلب بدل 400
# [إصلاح V4.2] ATR/RSI: نسخة Wilder صحيحة
# [إصلاح V4.2] SHARIAH_COMPLIANT_BASES: 30+ عملة
# [إصلاح V4.2] MAX_24H_GAIN_PCT: 50
# [إصلاح V4.2] MIN_24H_QUOTE_VOLUME: 2M
# [إصلاح V4.2] MIN_CANDIDATE_SCORE: 68
# [محتفظ من V4.1] كل الـlanes + تأكيد + cooldown + tracking
# ============================================================

import requests, time, math, csv, os, json, sys, logging, re, html
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scanner.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("scanner")

# ---------- Binance APIs ----------
BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]
FUTURES_BASE_URL = "https://fapi.binance.com"
STATE_FILE = "scanner_state.json"
INTERVAL = "5m"
KLINES_LIMIT = 180

# ---------- Scan ----------
SCAN_EVERY_SECONDS = 300
MIN_24H_QUOTE_VOLUME = 2_000_000   # ← V4.3: كان 3M
MAX_24H_GAIN_PCT = 50.0            # ← V4.3: كان 35
MIN_15M_GAIN = -3.0                # ← V4.3: كان -2
MAX_15M_GAIN = 12.0                # ← V4.3: كان 10
MIN_1H_GAIN = -12.0
MAX_1H_GAIN = 20.0                 # ← V4.3: كان 18

# ---------- BTC Market Guard ----------
BTC_CRASH_THRESHOLD_15M = -1.2
BTC_CRASH_THRESHOLD_1H = -2.5

# ---------- Wash Trading Detector ----------
WASH_MAX_PRICE_MOVE_FOR_BIG_VOL = 0.5
WASH_ACCEL_THRESHOLD = 5.0
WASH_VOL_1H_THRESHOLD = 6.0
WASH_VOL_15M_HARD_CAP = 18.0

# ---------- FINAL gate ----------
FINAL_MIN_15M_MOVE = 1.25
FINAL_MIN_5M_MOVE = 0.25
FINAL_MIN_VOLUME_15M = 4.0
FINAL_MIN_VOLUME_1H = 2.0
FINAL_MIN_ACCELERATION = 1.2
FINAL_MAX_ACCELERATION = 4.5
FINAL_MIN_24H_VOLUME = 2_000_000
FINAL_MAX_24H_GAIN = 22.0          # ← V4.3: كان 12
FINAL_MAX_RSI = 78.0
FINAL_MIN_BUY_PRESSURE = 0.55
FINAL_MIN_GREEN_STREAK = 2
FINAL_MIN_TREND_CONTINUITY = 3

# ---------- EXPLOSIVE EARLY ----------
EXPLOSIVE_MIN_5M_MOVE = 1.0
EXPLOSIVE_MIN_15M_MOVE = 0.8
EXPLOSIVE_MIN_VOLUME_15M = 4.0
EXPLOSIVE_MAX_VOLUME_15M = 12.0
EXPLOSIVE_MIN_VOLUME_1H = 2.0
EXPLOSIVE_MIN_ACCELERATION = 1.5
EXPLOSIVE_MAX_ACCELERATION = 4.0
EXPLOSIVE_MIN_BUY_PRESSURE = 0.58
EXPLOSIVE_MIN_24H_VOLUME = 2_000_000
EXPLOSIVE_MAX_24H_GAIN = 25.0      # ← V4.3: كان 15
EXPLOSIVE_MAX_1H_GAIN = 10.0       # ← V4.3: كان 8
EXPLOSIVE_MAX_RSI = 76.0
EXPLOSIVE_MAX_UPPER_WICK = 0.40
EXPLOSIVE_MIN_BREAKOUT = -1.50
EXPLOSIVE_MIN_GREEN_STREAK = 3
EXPLOSIVE_MIN_TREND_CONTINUITY = 4

# ---------- Persistent lane ----------
PERSISTENT_FINAL_SCORE = 70
PERSISTENT_MIN_5M_MOVE = 0.70
PERSISTENT_MIN_15M_MOVE = 1.25
PERSISTENT_MIN_VOLUME_15M = 2.50
PERSISTENT_MIN_VOLUME_1H = 1.50
PERSISTENT_MIN_PERSISTENCE = 2.00
PERSISTENT_MIN_BUY_PRESSURE = 0.54
PERSISTENT_MAX_1H_GAIN = 18.0
PERSISTENT_MAX_24H_GAIN = 30.0     # ← V4.3: كان 15
PERSISTENT_MAX_RSI = 80.0
PERSISTENT_MAX_UPPER_WICK = 0.45
PERSISTENT_MIN_BREAKOUT = -1.25
PERSISTENT_MIN_GREEN_STREAK = 2
PERSISTENT_MIN_TREND_CONTINUITY = 3

# ---------- Reversal lane ----------
REVERSAL_FINAL_SCORE = 72
REVERSAL_MIN_5M_MOVE = 0.50
REVERSAL_MIN_15M_MOVE = 1.25
REVERSAL_MIN_VOLUME_15M = 3.00
REVERSAL_MIN_VOLUME_1H = 1.70
REVERSAL_MIN_BUY_PRESSURE = 0.54
REVERSAL_MIN_24H_VOLUME = 2_000_000
REVERSAL_MAX_24H_GAIN = 22.0       # ← V4.3: كان 12
REVERSAL_MAX_RSI = 78.0
REVERSAL_MAX_UPPER_WICK = 0.45
REVERSAL_MIN_BREAKOUT = -1.50
REVERSAL_MIN_GREEN_STREAK = 2
REVERSAL_MIN_TREND_CONTINUITY = 3

# ---------- Short Squeeze lane (جديد من V4.2) ----------
SQUEEZE_FINAL_SCORE = 70
SQUEEZE_MAX_FUNDING = -0.0005      # funding سالب (short crowded)
SQUEEZE_MIN_15M_MOVE = 1.0
SQUEEZE_MIN_VOLUME_15M = 2.5
SQUEEZE_MIN_BUY_PRESSURE = 0.55
SQUEEZE_MAX_24H_GAIN = 20.0

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI",
    "USDE", "USD1", "RLUSD", "USDD", "EUR", "EURI", "USTC",
}
MEGA_CAP_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE"}

# ---------- Risk model ----------
RISK_MIN_SL_PCT = 3.5
RISK_MAX_SL_PCT = 12.0
RISK_ATR_MULT_SL = 1.5
RISK_RR_TP1 = 1.5
RISK_RR_TP2 = 3.0
RISK_HIGH_ATR_THRESHOLD = 6.0
RISK_HIGH_ATR_TP1_RR = 1.2
RISK_HIGH_RSI_THRESHOLD = 76.0
RISK_HIGH_RSI_TP1_RR = 1.0
RISK_LOW_LIQ_TP1_RR = 1.0

# ---------- Scores ----------
WATCH_SCORE = 65
FINAL_SCORE = 80
EXCEPTIONAL_SCORE = 88
CONFIRMATIONS_REQUIRED = 2
CANDIDATE_EXPIRY_MINUTES = 45
FINAL_COOLDOWN_HOURS = 12
MIN_CANDIDATE_SCORE = 68           # ← V4.3: كان 70

# ---------- Cross-Exchange ----------
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
CROSS_CONFIRM_AGREE_PCT = 1.0
CROSS_CONFIRM_DISAGREE_PCT = -1.0
SUPPRESS_ON_DIVERGENCE = False
CROSS_CACHE_TTL_SECONDS = 300

SCAN_DIAGNOSTICS_FILE = "early_scan_diagnostics.csv"

# ---------- OKX Lead Watcher ----------
OKX_LEAD_MIN_1H_CHG = 2.5
OKX_LEAD_MIN_4H_CHG = 5.0
OKX_LEAD_MIN_PREMIUM = 1.0
OKX_LEAD_MAX_24H_GAIN = 20.0       # ← V4.3: كان 15
OKX_LEAD_MIN_24H_VOLUME = 2_000_000
OKX_LEAD_COOLDOWN_HOURS = 4

# ---------- Pre-Pump Compressed ----------
COMPRESSED_MAX_BREAKOUT = -6.0
COMPRESSED_MIN_GAIN_4H = 0.25
COMPRESSED_MAX_GAIN_4H = 3.0
COMPRESSED_MIN_VOL_1H = 1.4
COMPRESSED_MIN_VOL_15M = 1.2
COMPRESSED_MAX_15M_GAIN = 2.5
COMPRESSED_MAX_24H_GAIN = 20.0     # ← V4.3: كان 15
COMPRESSED_MAX_RSI = 55.0
COMPRESSED_COOLDOWN_HOURS = 4

# ---------- Tracking ----------
TRACK_MINUTES = [5, 15, 30, 60, 240, 1440]
TRACK_FILE = "early_pump_signal_results.csv"
EARLY_WATCH_LOG_FILE = "early_watch_log.csv"
EARLY_WATCH_COOLDOWN_HOURS = 2

# ---------- CSV retention ----------
LOG_RETENTION_DAYS = 4
LOG_CLEANUP_INTERVAL_HOURS = 6

# ---------- Shariah ----------
SHARIAH_CACHE_FILE = "shariah_compliant_bases.json"
SHARIAH_UPDATE_HOURS = 24
SHARIAH_SOURCE_URL = "https://cryptoummah.com/halal-crypto-list"
SHARIAH_SOURCE_API = "https://gethalalcrypto.com/api/datasets/halal-coins?status=halal"

# ---------- Telegram ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PROGRAM_TAG = "🤖 [V4]"
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    log.warning("TELEGRAM_BOT_TOKEN أو TELEGRAM_CHAT_ID غير موجودين — سيعمل بدون إرسال.")

session = requests.Session()
session.headers.update({"User-Agent": "Binance-Early-Pump-Scanner-V4.3/1.0"})

# ---------- Global state ----------
candidates = {}
final_cooldown = {}
active_tracks = {}
early_watch_logged = {}
cross_cache = {}
okx_lead_alerted = {}
compressed_alerted = {}
diagnostics_last_logged = {}
funding_rates_cache = {}          # ← V4.3: cache للـfunding
DIAGNOSTIC_COOLDOWN_MINUTES = 15
SHARIAH_DIAGNOSTIC_COOLDOWN_HOURS = 24


# ============================================================
# HTTP
# ============================================================

def get_json(path, params=None, retries=3):
    last_err = None
    for base in BASE_URLS:
        for attempt in range(retries):
            try:
                r = session.get(base + path, params=params, timeout=15)
                if r.status_code == 451:
                    last_err = requests.HTTPError(f"451 from {base}", response=r)
                    break
                if r.status_code in (418, 429):
                    wait = 2 ** attempt
                    log.warning(f"Rate limit {r.status_code} من {base} — انتظار {wait}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.HTTPError:
                raise
            except requests.RequestException as e:
                last_err = e
                time.sleep(1 + attempt)
                continue
    raise last_err if last_err else RuntimeError("no base url worked")


def telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ============================================================
# BTC Market Guard + Funding Rate (جديد من V4.2)
# ============================================================

def check_btc_market_health():
    """فحص اتجاه البيتكوين لمنع إعطاء إشارات في حالة الانهيار العام."""
    try:
        klines = get_json("/api/v3/klines", {"symbol": "BTCUSDT", "interval": "15m", "limit": 8})
        if not klines or len(klines) < 5:
            return True, 0.0, 0.0
        closes = [float(x[4]) for x in klines]
        btc_15m = (closes[-1] / closes[-2] - 1.0) * 100.0
        btc_1h = (closes[-1] / closes[-5] - 1.0) * 100.0
        is_safe = not (btc_15m <= BTC_CRASH_THRESHOLD_15M or btc_1h <= BTC_CRASH_THRESHOLD_1H)
        return is_safe, btc_15m, btc_1h
    except Exception as e:
        log.debug(f"BTC health check failed: {e}")
        return True, 0.0, 0.0


def fetch_funding_rates_bulk():
    """جلب كل معدلات التمويل في طلب واحد (بدل 400 طلب)."""
    global funding_rates_cache
    try:
        r = session.get(f"{FUTURES_BASE_URL}/fapi/v1/premiumIndex", timeout=10)
        r.raise_for_status()
        data = r.json()
        rates = {}
        for x in data:
            sym = x.get("symbol", "")
            if sym.endswith("USDT"):
                try:
                    rates[sym] = float(x.get("lastFundingRate", 0.0))
                except (TypeError, ValueError):
                    pass
        funding_rates_cache = rates
        log.info(f"Funding rates fetched: {len(rates)} symbols")
        return rates
    except Exception as e:
        log.warning(f"Funding rates bulk fetch failed: {e}")
        return funding_rates_cache


# ============================================================
# Shariah list
# ============================================================

STABLECOIN_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "BFUSD",
    "USDE", "PYUSD", "EUR", "GBP", "AEUR", "USD1", "WBETH",
}
SHARIAH_COMPLIANT_BASES = {
    "BTC", "BNB", "ADA", "ETH", "XRP", "XLM", "ALGO", "AVAX",
    "LTC", "DOT", "MATIC", "XTZ", "SOL", "LINK", "ETC", "UNI",
    "ATOM", "FIL", "HNT", "ICP", "XMR", "NEAR", "THETA", "TON",
    "TRX", "SUI", "CTSI", "STRK", "W", "OGN", "MET",
}
SHARIAH_USER_APPROVED_BASES = {
    "W", "CTSI", "STRK", "OGN", "MET",
    # V4.3: قائمة موسّعة مقترحة (يمكن تعديلها)
    "KAIA", "MAGIC", "BAT", "S", "CHIP", "CKB",
    "RLC", "NIL", "ERA", "MANTA", "PHA", "DIA", "BEAMX",
    "APT", "SEI", "TIA", "INJ", "FET", "RENDER",
    "ARB", "OP", "LDO", "ENS", "GRT", "SAND", "MANA", "GALA",
    "AXS", "CHZ", "ENJ", "IMX", "FLOW", "ROSE", "ONE",
    "KAVA", "CRV", "SNX", "COMP", "AAVE", "SUSHI",
    "OCEAN", "ANKR", "STORJ", "RSR", "CVC", "LRC",
}
SHARIAH_FILTER_ENABLED = False     # ← V4.3: معطّل مؤقتًا للتشخيص
def _extract_symbols_from_halalcrypto(payload):
    rows = payload
    if isinstance(payload, dict):
        for key in ("data", "results", "assets", "coins", "rows"):
            if isinstance(payload.get(key), list):
                rows = payload[key]
                break
    if not isinstance(rows, list):
        return set()
    out = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", row.get("verdict", ""))).strip().lower()
        if status and "halal" not in status:
            continue
        sym = row.get("symbol") or row.get("ticker") or row.get("base")
        if isinstance(sym, str):
            sym = sym.strip().upper()
            if sym and sym.isascii() and 1 <= len(sym) <= 20:
                out.add(sym)
    return out


def _extract_symbols_from_crypto_ummmah(text):
    out = set()
    clean = html.unescape(re.sub(r"<[^>]+>", " ", text))
    clean = re.sub(r"\s+", " ", clean)
    pat = re.compile(r"\b([A-Z][A-Z0-9]{1,14})\s*\|\s*[0-9]+(?:\.[0-9]+)?\s*\|\s*HALAL\b")
    for m in pat.finditer(clean):
        out.add(m.group(1).upper())
    pat2 = re.compile(r"\b([A-Z][A-Z0-9]{1,14})\s+Same category:.*?\|\s*Halal\b", re.S)
    for m in pat2.finditer(clean):
        out.add(m.group(1).upper())
    return out


def load_user_shariah_overrides():
    filename = "shariah_user_overrides.json"
    if not os.path.exists(filename):
        return set(SHARIAH_USER_APPROVED_BASES)
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        vals = data.get("approved_symbols", []) if isinstance(data, dict) else []
        return {str(x).strip().upper() for x in vals if isinstance(x, str) and x.strip()}
    except Exception as e:
        log.warning(f"Shariah overrides load failed: {e}")
        return set(SHARIAH_USER_APPROVED_BASES)


def load_shariah_cache():
    if not os.path.exists(SHARIAH_CACHE_FILE):
        return set()
    try:
        with open(SHARIAH_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(str(x).upper() for x in data.get("symbols", []) if isinstance(x, str))
    except Exception as e:
        log.warning(f"Shariah cache load failed: {e}")
        return set()


def save_shariah_cache(symbols, source):
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "symbols": sorted(symbols),
    }
    with open(SHARIAH_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def update_shariah_list(force=False):
    global SHARIAH_COMPLIANT_BASES
    cached = load_shariah_cache()
    now = datetime.now(timezone.utc)
    should_update = force or not os.path.exists(SHARIAH_CACHE_FILE)
    if not should_update:
        try:
            with open(SHARIAH_CACHE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            ts = pd.to_datetime(d.get("updated_at_utc"), utc=True, errors="coerce")
            should_update = pd.isna(ts) or (now - ts.to_pydatetime()).total_seconds() >= SHARIAH_UPDATE_HOURS * 3600
        except Exception:
            should_update = True

    discovered = set()
    source = "cache/fallback"
    if should_update:
        try:
            r = session.get(SHARIAH_SOURCE_API, timeout=20)
            r.raise_for_status()
            discovered = _extract_symbols_from_halalcrypto(r.json())
            if discovered:
                source = SHARIAH_SOURCE_API
        except Exception as e:
            log.warning(f"Shariah API update failed: {e}")

        if not discovered:
            try:
                r = session.get(SHARIAH_SOURCE_URL, timeout=20)
                r.raise_for_status()
                discovered = _extract_symbols_from_crypto_ummmah(r.text)
                if discovered:
                    source = SHARIAH_SOURCE_URL
            except Exception as e:
                log.warning(f"Shariah web update failed: {e}")

        if discovered:
            save_shariah_cache(discovered, source)
            cached = discovered
            log.info(f"Shariah list updated: {len(discovered)} halal symbols from {source}")
        else:
            log.warning("Shariah update unavailable — using last cached/fallback list")

    if cached:
        SHARIAH_COMPLIANT_BASES = set(cached)
    SHARIAH_COMPLIANT_BASES.update(load_user_shariah_overrides())
    log.info(f"Shariah scan universe: {len(SHARIAH_COMPLIANT_BASES)} symbols (including user-approved overrides)")


def get_symbols():
    data = get_json("/api/v3/exchangeInfo")
    out = []
    shariah_exclusions = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for s in data["symbols"]:
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("isSpotTradingAllowed") is False:
            continue
        sym = s["symbol"]
        if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        base = sym[:-4]
        if base in STABLECOIN_BASES or base in MEGA_CAP_BASES:
            continue
        if SHARIAH_FILTER_ENABLED and base not in SHARIAH_COMPLIANT_BASES:
            shariah_exclusions.append({
                "logged_time_utc": now_iso, "symbol": sym,
                "stage": "SHARIAH_FILTER", "reason": "NOT_IN_COMPLIANT_LIST",
            })
            continue
        out.append(sym)
    append_scan_diagnostics(shariah_exclusions)
    return out


def get_tickers():
    data = get_json("/api/v3/ticker/24hr")
    out = {}
    for x in data:
        sym = x["symbol"]
        if not sym.endswith("USDT"):
            continue
        try:
            out[sym] = {
                "price": float(x["lastPrice"]),
                "change": float(x["priceChangePercent"]),
                "quote_volume": float(x["quoteVolume"]),
            }
        except Exception:
            pass
    return out


# ============================================================
# Math (Wilder صحيحة — إصلاح V4.2)
# ============================================================

def pct(a, b):
    if b == 0:
        return 0.0
    return (a / b - 1) * 100.0


def mean(v):
    v = [x for x in v if math.isfinite(x)]
    return sum(v) / len(v) if v else 0.0


def ema(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return 100.0 - (100.0 / (1.0 + rs))


def atr_pct(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return (atr / closes[-1] * 100.0) if closes[-1] else None


def pct_slope(values):
    if len(values) < 2 or values[0] == 0:
        return 0.0
    return (values[-1] / values[0] - 1.0) * 100.0


# ============================================================
# Trend continuity + wash detector
# ============================================================

def trend_continuity_score(closes_5m, volumes_5m):
    if len(closes_5m) < 6:
        return 0, 0

    last_closes = closes_5m[-6:]
    last_vols = volumes_5m[-6:]

    changes = [pct(last_closes[i], last_closes[i - 1]) for i in range(1, len(last_closes))]
    green_streak = 0
    for c in reversed(changes):
        if c > 0:
            green_streak += 1
        else:
            break

    green_count = sum(1 for c in changes if c > 0)
    higher_lows = sum(
        1 for i in range(2, len(last_closes))
        if last_closes[i] > last_closes[i - 2]
    )

    vol_increasing = 0
    for i in range(1, len(last_vols)):
        if last_vols[i] >= last_vols[i - 1] * 0.85:
            vol_increasing += 1

    score = 0
    if green_count >= 4:
        score += 3
    elif green_count >= 3:
        score += 2
    if green_streak >= 3:
        score += 2
    if higher_lows >= 3:
        score += 2
    if vol_increasing >= 4:
        score += 1

    return min(score, 8), green_streak


def is_likely_wash_trading(x):
    v15 = x.get("volume_15m", 0)
    v1h = x.get("volume_1h", 0)
    accel = x.get("acceleration", 0)
    g5 = x.get("5m", 0)
    g15 = x.get("15m", 0)
    g1h = x.get("1h", 0)

    if v15 > WASH_VOL_15M_HARD_CAP:
        return True, "vol_15m شاذ (>18x)"

    if v15 > 10 and abs(g15) < WASH_MAX_PRICE_MOVE_FOR_BIG_VOL:
        return True, f"vol_15m={v15:.1f}x مع 15m={g15:+.2f}%"

    if accel > WASH_ACCEL_THRESHOLD and abs(g15) < 1.0:
        return True, f"accel={accel:.1f}x مع 15m={g15:+.2f}%"

    if v1h > WASH_VOL_1H_THRESHOLD and abs(g1h) < 1.0:
        return True, f"vol_1h={v1h:.1f}x مع 1h={g1h:+.2f}%"

    return False, ""


# ============================================================
# Risk model
# ============================================================

def compute_risk_levels(entry, atr_val, rsi_val, liquidity):
    if not entry or entry <= 0:
        return None
    atr_use = atr_val if (atr_val and atr_val > 0) else 4.0

    sl_pct_raw = atr_use * RISK_ATR_MULT_SL
    sl_pct = max(RISK_MIN_SL_PCT, min(sl_pct_raw, RISK_MAX_SL_PCT))

    tp1_rr = RISK_RR_TP1
    notes = []
    if atr_use > RISK_HIGH_ATR_THRESHOLD:
        tp1_rr = min(tp1_rr, RISK_HIGH_ATR_TP1_RR)
        notes.append(f"ATR مرتفع ({atr_use:.1f}%) → TP1 أقرب")
    if rsi_val is not None and rsi_val > RISK_HIGH_RSI_THRESHOLD:
        tp1_rr = min(tp1_rr, RISK_HIGH_RSI_TP1_RR)
        notes.append(f"RSI مرتفع ({rsi_val:.0f}) → TP1 أقرب")
    if liquidity == "LOW":
        tp1_rr = min(tp1_rr, RISK_LOW_LIQ_TP1_RR)
        notes.append("سيولة منخفضة → TP1 أقرب")

    tp2
