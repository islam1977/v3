"""
market_features.py
==================
تجميع 3 مجموعات features لكل رمز وتسجيلها في CSV، بدون أي تنفيذ صفقات:

  1) Open Interest   (Binance Futures - بيانات عامة، ولها تاريخ ~30 يوم)
  2) Order Book Imbalance (Spot depth - لقطة لحظية، مفيش تاريخ لها)
  3) Whale trades    (Spot aggTrades في آخر N دقايق)

الاستخدام من السكريبت الأساسي (سطر واحد):

    from market_features import collect_features, log_features
    feats = collect_features("PEPEUSDT")
    log_features("PEPEUSDT", feats, extra={"signal_id": sid, "score": score})

أي فشل في API بيرجّع None للـ feature دي، ومش بيوقف المسح.
"""
import csv
import os
import statistics
import time

import requests

SPOT = "https://api.binance.com"
FUTURES = "https://fapi.binance.com"
TIMEOUT = 8
LOG_PATH = os.getenv("FEATURES_LOG", "market_features_log.csv")

# حدود قابلة للتعديل
OBI_LEVELS = 20            # عدد مستويات الـ order book
OBI_BAND_PCT = 1.0         # نطاق % حول السعر الأوسط
WHALE_MIN_USD = 20_000     # أقل قيمة مطلقة للصفقة الحوتية
WHALE_MEDIAN_MULT = 30     # أو مضاعف لوسيط حجم الصفقة، الأكبر منهم هو المعتمد
WHALE_WINDOW_MIN = 5

FIELDS = [
    "ts", "symbol",
    "oi_value_usd", "oi_chg_5m_pct", "oi_chg_1h_pct",
    "obi_levels", "obi_band", "spread_pct",
    "whale_count", "whale_buy_usd", "whale_sell_usd", "whale_net_usd",
    "trades_count",
]


def _get(url, params=None):
    """GET مع محاولة إعادة واحدة. بيرجّع None لو فشل."""
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):   # rate limit / ban: ما نكررش
                return None
        except requests.RequestException:
            pass
        time.sleep(0.3)
    return None


# ---------------------------------------------------------------- Open Interest
def open_interest_features(symbol):
    out = {"oi_value_usd": None, "oi_chg_5m_pct": None, "oi_chg_1h_pct": None}
    data = _get(f"{FUTURES}/futures/data/openInterestHist",
                {"symbol": symbol, "period": "5m", "limit": 13})
    if not data or len(data) < 2:
        return out   # الرمز غالبًا ملوش عقد futures
    vals = [float(x["sumOpenInterestValue"]) for x in data]
    out["oi_value_usd"] = vals[-1]
    if vals[-2] > 0:
        out["oi_chg_5m_pct"] = (vals[-1] / vals[-2] - 1) * 100
    if len(vals) >= 13 and vals[0] > 0:
        out["oi_chg_1h_pct"] = (vals[-1] / vals[0] - 1) * 100
    return out


# ---------------------------------------------------------- Order Book Imbalance
def _imbalance(bid_usd, ask_usd):
    tot = bid_usd + ask_usd
    return (bid_usd - ask_usd) / tot if tot > 0 else None


def orderbook_features(symbol):
    out = {"obi_levels": None, "obi_band": None, "spread_pct": None}
    book = _get(f"{SPOT}/api/v3/depth", {"symbol": symbol, "limit": 100})
    if not book or not book.get("bids") or not book.get("asks"):
        return out
    bids = [(float(p), float(q)) for p, q in book["bids"]]
    asks = [(float(p), float(q)) for p, q in book["asks"]]
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask) / 2
    out["spread_pct"] = (best_ask - best_bid) / mid * 100

    # 1) أعلى N مستوى
    b_usd = sum(p * q for p, q in bids[:OBI_LEVELS])
    a_usd = sum(p * q for p, q in asks[:OBI_LEVELS])
    out["obi_levels"] = _imbalance(b_usd, a_usd)

    # 2) داخل نطاق %  من السعر الأوسط (أقل حساسية لجدران بعيدة)
    lo, hi = mid * (1 - OBI_BAND_PCT / 100), mid * (1 + OBI_BAND_PCT / 100)
    b_band = sum(p * q for p, q in bids if p >= lo)
    a_band = sum(p * q for p, q in asks if p <= hi)
    out["obi_band"] = _imbalance(b_band, a_band)
    return out


# --------------------------------------------------------------------- Whales
def whale_features(symbol, window_min=WHALE_WINDOW_MIN):
    out = {"whale_count": None, "whale_buy_usd": None, "whale_sell_usd": None,
           "whale_net_usd": None, "trades_count": None}
    end = int(time.time() * 1000)
    start = end - window_min * 60_000
    trades = _get(f"{SPOT}/api/v3/aggTrades",
                  {"symbol": symbol, "startTime": start, "endTime": end, "limit": 1000})
    if trades is None:
        return out
    notional = [(float(t["p"]) * float(t["q"]), t["m"]) for t in trades]
    out["trades_count"] = len(notional)
    if not notional:
        out.update(whale_count=0, whale_buy_usd=0.0, whale_sell_usd=0.0, whale_net_usd=0.0)
        return out
    med = statistics.median(n for n, _ in notional)
    threshold = max(WHALE_MIN_USD, med * WHALE_MEDIAN_MULT)
    buy = sell = 0.0
    cnt = 0
    for usd, buyer_is_maker in notional:
        if usd >= threshold:
            cnt += 1
            if buyer_is_maker:   # المشتري صانع سوق => المهاجم بائع
                sell += usd
            else:
                buy += usd
    out.update(whale_count=cnt, whale_buy_usd=buy, whale_sell_usd=sell,
               whale_net_usd=buy - sell)
    return out


# ------------------------------------------------------------------- واجهة عامة
def collect_features(symbol):
    feats = {}
    feats.update(open_interest_features(symbol))
    feats.update(orderbook_features(symbol))
    feats.update(whale_features(symbol))
    return feats


def log_features(symbol, feats, extra=None, path=None):
    """بيضيف صف في CSV. extra: أعمدة إضافية (signal_id, score...) تتضاف بعد الأساسية."""
    path = path or LOG_PATH
    row = {"ts": int(time.time()), "symbol": symbol, **feats, **(extra or {})}
    cols = FIELDS + [k for k in (extra or {}) if k not in FIELDS]
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow(row)
    return row


if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    print(log_features(sym, collect_features(sym)))
