#!/usr/bin/env python3
"""
backtest_5m.py — الخطوة 2: backtest تاريخي لمنطق الماسح على شموع 5m
=====================================================================
بيجاوب: هل بوابات الماسح (STRICT / EXPLOSIVE / PERSISTENT) فيها edge حقيقي على 90 يوم؟
وهل فيه قاعدة بديلة بتكسب بعد الرسوم وبشكل ثابت خارج العينة؟

الكون: نفس كون الماسح (USDT، من غير مستقرة/عملات كبيرة، وضمن قائمة الحلال shariah_compliant_bases.json).

منهجية (عشان نتجنب خداع النفس):
  - الإشارة تتحسب من شمعة 5m مقفولة، والدخول بعد --lag شمعة (افتراضي 2 ≈ تأخير cron + GitHub + تنفيذك).
  - رسوم ذهاب+إياب (--fee) + انزلاق عند الخروج بالـ stop (--slip).
  - لو SL وTP في نفس شمعة 5m -> SL (متحفظ؛ شموع 5m أخشن من 1m فالتقدير متشائم شوية للستوبات الضيقة).
  - تقسيم زمني: اختيار القاعدة على أول 2/3، والقياس على آخر 1/3 (مع embargo 4 ساعات).
  - فاصل ثقة بـ bootstrap على مستوى اليوم (الإشارات متجمعة في نفس الأيام).
  - بنعرض عدد التركيبات المختبرة (كل ما زاد، زاد احتمال صدفة).

اللي مش مطابق للماسح 100%: مفيش score ولا تأكيد مسحتين، وحساب الحجم تقريبي. الـ BTC guard مطبّق.

التشغيل:  python backtest_5m.py [--days 90 --symbols 100 --lag 2]
اختبار ذاتي (بدون شبكة):  python backtest_5m.py --selftest   |   --selftest-edge (بيزرع edge للتأكد إن الأداة بتكتشفه)
"""
import argparse
import json
import os
import pickle
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

BASES = ["https://data-api.binance.vision", "https://api.binance.com"]
OUT_DIR = "bt_out"
STEP_MS = 300_000
W = 48  # أطول نافذة متابعة = 48 شمعة 5m = 4 ساعات
STABLES = {"USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDE", "USD1", "RLUSD", "USDD", "EUR", "EURI",
           "USTC", "BUSD", "AEUR", "XUSD"}
MEGA = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE"}

session = requests.Session()
session.headers.update({"User-Agent": "bt-5m/1.0"})


# ============================================================ جلب البيانات
def api_get(path, params=None):
    for base in BASES:
        for attempt in range(4):
            try:
                r = session.get(base + path, params=params, timeout=20)
                if r.status_code in (418, 429):
                    time.sleep(2 ** attempt + 1)
                    continue
                if r.status_code == 451:
                    break
                if r.status_code == 400:
                    return None
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                time.sleep(1 + attempt)
    return None


def get_universe(n, min_qv, shariah_file, use_shariah):
    data = api_get("/api/v3/ticker/24hr")
    if not data:
        raise SystemExit("مقدرتش أجيب قائمة الأزواج من Binance (شبكة/حظر؟)")
    halal = None
    if use_shariah and os.path.exists(shariah_file):
        halal = set(json.load(open(shariah_file, encoding="utf-8")).get("symbols", []))
    rows = []
    for t in data:
        s = t["symbol"]
        if not s.endswith("USDT") or any(x in s for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        base = s[:-4]
        if base in STABLES or base in MEGA:
            continue
        if halal is not None and base not in halal:
            continue
        qv = float(t["quoteVolume"])
        if qv >= min_qv:
            rows.append((s, qv))
    rows.sort(key=lambda x: -x[1])
    return [s for s, _ in rows[:n]], (halal is not None)


def fetch_symbol(sym, start_ms, now_ms, old=None):
    cur = start_ms
    if old is not None and len(old):
        cur = max(start_ms, int(old["t"].iloc[-1]) + STEP_MS)
    rows = []
    while cur < now_ms:
        data = api_get("/api/v3/klines", {"symbol": sym, "interval": "5m", "startTime": int(cur), "limit": 1000})
        if not data:
            break
        rows += [r for r in data if int(r[6]) < now_ms - 2000]
        last_open = int(data[-1][0])
        if len(data) < 1000 or last_open + STEP_MS <= cur:
            break
        cur = last_open + STEP_MS
        time.sleep(0.04)
    if rows:
        new = pd.DataFrame({"t": [int(r[0]) for r in rows], "o": [float(r[1]) for r in rows],
                            "h": [float(r[2]) for r in rows], "l": [float(r[3]) for r in rows],
                            "c": [float(r[4]) for r in rows], "qv": [float(r[7]) for r in rows],
                            "tbq": [float(r[10]) for r in rows]})
        df = pd.concat([old, new]) if old is not None and len(old) else new
    else:
        df = old
    if df is None or df.empty:
        return None
    df = df.drop_duplicates("t").sort_values("t")
    return df[df["t"] >= start_ms].reset_index(drop=True)


def load_data(syms, days, workers, cache_path):
    now_ms = int(time.time() * 1000)
    now_ms -= now_ms % STEP_MS
    start_ms = now_ms - days * 86_400_000
    cache = {}
    if cache_path and os.path.exists(cache_path):
        try:
            cache = pickle.load(open(cache_path, "rb"))
            print(f"كاش موجود: {len(cache)} رمز")
        except Exception:
            cache = {}
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_symbol, s, start_ms, now_ms, cache.get(s)): s for s in syms}
        for n, f in enumerate(as_completed(futs), 1):
            s = futs[f]
            try:
                d = f.result()
            except Exception:
                d = None
            if d is not None and len(d) >= 600:
                out[s] = d
            if n % 20 == 0:
                print(f"  ... {n}/{len(syms)}")
    if cache_path:
        try:
            pickle.dump(out, open(cache_path, "wb"))
        except Exception:
            pass
    return out, start_ms, now_ms


# ============================================================ المؤشرات (vectorized)
def streak_of(g):
    idx = np.arange(len(g))
    last_false = np.maximum.accumulate(np.where(~g, idx, 0))
    return np.where(g, idx - last_false, 0)


def features(d):
    """d: dict of numpy arrays على الشبكة الزمنية الكاملة (NaN قبل الإدراج). كل القيم عند إغلاق الشمعة i."""
    c, o, h, l = (pd.Series(d[k]) for k in ("c", "o", "h", "l"))
    qv, tb = pd.Series(d["qv"]), pd.Series(d["tbq"])
    x = {}
    x["g5"] = ((c / c.shift(1) - 1) * 100).to_numpy()
    x["g15"] = ((c / c.shift(3) - 1) * 100).to_numpy()
    x["g1h"] = ((c / c.shift(12) - 1) * 100).to_numpy()
    x["g4h"] = ((c / c.shift(48) - 1) * 100).to_numpy()
    x["g24h"] = ((c / c.shift(288) - 1) * 100).to_numpy()
    v15, v1h = qv.rolling(3).sum(), qv.rolling(12).sum()
    b15 = qv.shift(3).rolling(144, min_periods=96).mean()
    b1h = qv.shift(12).rolling(144, min_periods=96).mean()
    x["vol15"] = (v15 / (3 * b15)).replace([np.inf, -np.inf], np.nan).to_numpy()
    x["vol1h"] = (v1h / (12 * b1h)).replace([np.inf, -np.inf], np.nan).to_numpy()
    prev15 = qv.shift(3).rolling(3).sum()
    x["accel"] = (v15 / prev15.replace(0, np.nan)).to_numpy()
    x["buy"] = (tb.rolling(3).sum() / v15.replace(0, np.nan)).to_numpy()
    x["brk"] = ((c / h.shift(1).rolling(48).max() - 1) * 100).to_numpy()
    rng = (h - l).replace(0, np.nan)
    x["wick"] = ((h - np.maximum(o, c)) / rng).fillna(0).to_numpy()
    x["cloc"] = ((c - l) / rng).fillna(0.5).to_numpy()
    dlt = c.diff()
    up = dlt.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-dlt.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = 100 - 100 / (1 + up / dn)
    x["rsi"] = np.where(dn.to_numpy() == 0, 100.0, rsi.to_numpy())
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    x["atr"] = (tr.ewm(alpha=1 / 14, adjust=False).mean() / c * 100).to_numpy()
    # استمرارية الترند (نفس منطق trend_continuity_score في الماسح)
    g = (dlt > 0).to_numpy()
    gs = pd.Series(g.astype(int))
    green5 = gs.rolling(5).sum().to_numpy()
    streak = streak_of(g)
    hl = (c > c.shift(2)).astype(int).rolling(4).sum().to_numpy()
    vi = (qv >= qv.shift(1) * 0.85).astype(int).rolling(5).sum().to_numpy()
    tc = np.where(green5 >= 4, 3, np.where(green5 >= 3, 2, 0)) + (streak >= 3) * 2 + (hl >= 3) * 2 + (vi >= 4) * 1
    x["streak"], x["tc"] = np.minimum(streak, 5), np.minimum(tc, 8)  # الماسح بيعدّ آخر 5 تغييرات بس
    x["qv24"] = qv.rolling(288).sum().to_numpy()
    x["wash"] = ((x["vol15"] > 18) | ((x["vol15"] > 10) & (np.abs(x["g15"]) < 0.5))
                 | ((x["accel"] > 5) & (np.abs(x["g15"]) < 1.0)) | ((x["vol1h"] > 6) & (np.abs(x["g1h"]) < 1.0)))
    x["valid"] = np.isfinite(x["g24h"]) & np.isfinite(x["vol1h"]) & np.isfinite(x["buy"]) & np.isfinite(x["brk"])
    return x


def rule_masks(x, btc_ok):
    """يرجّع dict اسم القاعدة -> mask. بوابات الماسح بالضبط (من غير score/تأكيد مسحتين) + عائلة قواعد زخم."""
    m = {}
    ok = x["valid"] & ~x["wash"] & btc_ok & (x["qv24"] >= 2e6)
    cq = (x["buy"] >= 0.52) & (x["rsi"] < 80) & (x["wick"] < 0.5) & ~((x["g15"] > 8) & (x["g1h"] > 12))
    pconf = (((x["g15"] >= 1.0) & (x["g5"] >= 0.3)) | ((x["g1h"] >= 2.0) & (x["g5"] >= 0.2))
             | ((x["brk"] >= 0) & (x["cloc"] >= 0.9) & (x["vol1h"] >= 4.0) & (x["g15"] >= 0.6)))
    strict = (ok & (x["vol15"] <= 18) & (x["accel"] <= 4.5) & (x["rsi"] < 78) & (x["buy"] >= 0.55)
              & (x["streak"] >= 2) & (x["tc"] >= 3)
              & (x["g15"] >= 1.25) & ((x["g5"] >= 0.25) | (x["brk"] >= 0))
              & (x["vol1h"] >= 2) & (x["vol15"] >= 4) & (x["accel"] >= 1.2)
              & (x["wick"] < 0.45) & (x["brk"] >= -0.75)
              & (x["g24h"] <= 22) & (x["g15"] <= 6) & (x["g1h"] <= 10))
    explosive = (ok & (x["vol15"] <= 12) & (x["accel"] <= 4) & (x["rsi"] < 76)
                 & (x["g5"] >= 1.0) & (x["g15"] >= 0.8)
                 & (x["vol15"] >= 4) & (x["vol1h"] >= 2) & (x["accel"] >= 1.5)
                 & (x["buy"] >= 0.58) & (x["wick"] < 0.40) & (x["brk"] >= -1.5)
                 & (x["g24h"] <= 25) & (x["g1h"] <= 10) & (x["g15"] <= 4)
                 & (x["streak"] >= 3) & (x["tc"] >= 4))
    persistent = (ok & (x["g5"] >= 0.7) & (x["g15"] >= 1.25) & (x["vol15"] >= 2.5) & (x["vol1h"] >= 1.5)
                  & (np.minimum(x["vol15"], x["vol1h"]) >= 2.0) & (x["buy"] >= 0.54) & (x["wick"] < 0.45)
                  & (x["brk"] >= -1.25) & (x["rsi"] < 80) & (x["g1h"] <= 18) & (x["g24h"] <= 30)
                  & (x["streak"] >= 2) & (x["tc"] >= 3))
    m["GATE:STRICT"] = strict & pconf & cq
    m["GATE:EXPLOSIVE"] = explosive & pconf & cq
    m["GATE:PERSISTENT"] = persistent & cq
    m["GATE:ANY"] = m["GATE:STRICT"] | m["GATE:EXPLOSIVE"] | m["GATE:PERSISTENT"]
    base = (ok & (x["g5"] >= 0.25) & (x["g15"] >= 1.0) & (x["vol15"] >= 2) & (x["rsi"] < 78)
            & (x["buy"] >= 0.54) & (x["g24h"] <= 25))
    m["MOM:BASE"] = base
    for st in (0, 2, 4):
        for v1 in (0, 2.5):
            for br in (None, 0.0):
                for ac in (None, 3.0):
                    if st == 0 and v1 == 0 and br is None and ac is None:
                        continue  # مطابقة لـ MOM:BASE
                    mk = base & (x["streak"] >= st) & (x["vol1h"] >= v1)
                    if br is not None:
                        mk = mk & (x["brk"] >= br)
                    if ac is not None:
                        mk = mk & (x["accel"] <= ac)
                    m[f"MOM:streak{st}|v1h{v1}|brk{'-' if br is None else br}|acc{'-' if ac is None else ac}"] = mk
    return m


def cooldown(idx, cd):
    keep, last = [], -10 ** 9
    for i in idx:
        if i - last >= cd:
            keep.append(i)
            last = i
    return np.array(keep, dtype=np.int64)


# ============================================================ المحاكاة (vectorized على كل الإشارات)
def sim_fixed(H, L, C, E, sl, tp, n, slip):
    Hn, Ln = H[:, :n], L[:, :n]
    hs, ht = Ln <= (E * (1 - sl / 100))[:, None], Hn >= (E * (1 + tp / 100))[:, None]
    i_s = np.where(hs.any(1), hs.argmax(1), n)
    i_t = np.where(ht.any(1), ht.argmax(1), n)
    none = (i_s == n) & (i_t == n)
    return np.where(none, (C[:, n - 1] / E - 1) * 100, np.where(i_s <= i_t, -sl - slip, tp))


def sim_dyn(H, L, C, E, sl, tp, n, slip, be=None, tt=None, tr=None):
    N = len(E)
    stop, tpp, peak = E * (1 - sl / 100), E * (1 + tp / 100), E.copy()
    done, ex = np.zeros(N, bool), np.zeros(N)
    for j in range(n):
        hit_s = ~done & (L[:, j] <= stop)
        ex[hit_s] = (stop[hit_s] / E[hit_s] - 1) * 100 - slip
        done |= hit_s
        hit_t = ~done & (H[:, j] >= tpp)
        ex[hit_t] = tp
        done |= hit_t
        act = ~done
        peak = np.where(act, np.maximum(peak, H[:, j]), peak)
        if be is not None:
            stop = np.where(act & (peak >= E * (1 + be / 100)), np.maximum(stop, E), stop)
        if tr is not None:
            stop = np.where(act & (peak >= E * (1 + tt / 100)), np.maximum(stop, peak * (1 - tr / 100)), stop)
    rest = ~done
    ex[rest] = (C[rest, n - 1] / E[rest] - 1) * 100
    return ex


def exit_variants():
    v = []
    for T in (12, 24, 48):
        for sl in (2, 3.5, 5):
            for tp in (2.5, 5.25, 8):
                v.append({"name": f"SL{sl}/TP{tp}/{T * 5}د", "kind": "fixed", "sl": sl, "tp": tp, "n": T})
    for sl, tp, be in ((3.5, 8, 2.5), (5, 8, 3)):
        v.append({"name": f"BE SL{sl}/TP{tp}/تعادل@{be}/240د", "kind": "dyn", "sl": sl, "tp": tp, "n": 48, "be": be})
    for sl, tp, tt, tr in ((3.5, 12, 2.5, 1.5), (5, 12, 3, 2)):
        v.append({"name": f"Trail SL{sl}/TP{tp}/بعد{tt}/تتبع{tr}/240د", "kind": "dyn", "sl": sl, "tp": tp, "n": 48,
                  "tt": tt, "tr": tr})
    return v


CURRENT_EXIT = "SL3.5/TP5.25/240د"


def run_exit(v, H, L, C, E, fee, slip):
    if v["kind"] == "fixed":
        ex = sim_fixed(H, L, C, E, v["sl"], v["tp"], v["n"], slip)
    else:
        ex = sim_dyn(H, L, C, E, v["sl"], v["tp"], v["n"], slip, v.get("be"), v.get("tt"), v.get("tr"))
    return ex - fee


# ============================================================ إحصاء
def stats(a):
    if len(a) == 0:
        return {"n": 0, "mean": np.nan, "win": np.nan, "pf": np.nan}
    neg = -a[a < 0].sum()
    return {"n": len(a), "mean": float(a.mean()), "win": float((a > 0).mean()),
            "pf": float(a[a > 0].sum() / neg) if neg > 0 else np.inf}


def cluster_ci(a, day, n_boot=2000, seed=11):
    """bootstrap على مستوى اليوم. يرجّع (2.5%, 97.5%, 0.5%, 99.5%)."""
    if len(a) < 10:
        return (np.nan,) * 4
    _, inv = np.unique(day, return_inverse=True)
    sums, cnts = np.bincount(inv, weights=a), np.bincount(inv)
    r = np.random.default_rng(seed)
    idx = r.integers(0, len(sums), (n_boot, len(sums)))
    m = sums[idx].sum(1) / cnts[idx].sum(1)
    return tuple(np.percentile(m, [2.5, 97.5, 0.5, 99.5]))


OUT = []


def emit(x=""):
    print(x)
    OUT.append(str(x))


def fs(s):
    return f"n={s['n']:>5} | متوسط {s['mean']:+6.2f}% | فوز {s['win']:>4.0%} | PF {s['pf']:.2f}"


# ============================================================ التشغيل
def build_grid(data, start_ms, now_ms):
    t_all = np.arange(start_ms - start_ms % STEP_MS, now_ms, STEP_MS)
    T = len(t_all)
    syms = sorted(data)
    arr = {k: np.full((len(syms), T), np.nan) for k in ("o", "h", "l", "c", "qv", "tbq")}
    for si, s in enumerate(syms):
        d = data[s]
        pos = ((d["t"].to_numpy() - t_all[0]) // STEP_MS).astype(int)
        ok = (pos >= 0) & (pos < T)
        for k in arr:
            arr[k][si, pos[ok]] = d[k].to_numpy()[ok]
        c = pd.Series(arr["c"][si]).ffill().to_numpy()
        first = np.argmax(np.isfinite(c))
        arr["c"][si] = c
        for k in ("o", "h", "l"):
            col = arr[k][si]
            col[np.isnan(col) & np.isfinite(c)] = c[np.isnan(col) & np.isfinite(c)]
        for k in ("qv", "tbq"):
            col = arr[k][si]
            col[np.isnan(col)] = 0.0
            col[:first] = np.nan
    return syms, t_all, arr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--symbols", type=int, default=100)
    ap.add_argument("--min-vol", type=float, default=2_000_000)
    ap.add_argument("--lag", type=int, default=2, help="عدد الشموع بين شمعة الإشارة وشمعة الدخول")
    ap.add_argument("--fee", type=float, default=0.2)
    ap.add_argument("--slip", type=float, default=0.1)
    ap.add_argument("--cooldown", type=int, default=48, help="شموع 5m بين إشارتين لنفس العملة")
    ap.add_argument("--control", type=int, default=20000)
    ap.add_argument("--train-frac", type=float, default=0.67)
    ap.add_argument("--min-n", type=int, default=150)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--no-shariah", action="store_true")
    ap.add_argument("--shariah-file", default="shariah_compliant_bases.json")
    ap.add_argument("--cache", default="")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--selftest-edge", action="store_true")
    a = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)
    rng = np.random.default_rng(5)

    if a.selftest or a.selftest_edge:
        data, start_ms, now_ms = synth(a.selftest_edge)
        halal = None
    else:
        syms, halal = get_universe(a.symbols, a.min_vol, a.shariah_file, not a.no_shariah)
        print(f"الكون: {len(syms)} رمز | فلتر الحلال: {'نعم' if halal else 'لا'} | جلب {a.days} يوم شموع 5m ...")
        data, start_ms, now_ms = load_data(syms + ["BTCUSDT"], a.days, a.workers, a.cache)
    if "BTCUSDT" not in data:
        print("تحذير: مفيش BTC — الـ BTC guard مش هيتطبق.")
    btc_d = data.pop("BTCUSDT", None)
    if len(data) < 3:
        raise SystemExit("بيانات غير كافية.")

    syms, t_all, arr = build_grid(data, start_ms, now_ms)
    S, T = len(syms), len(t_all)
    # سياق BTC
    btc_ok = np.ones(T, bool)
    btc4h = np.full(T, np.nan)
    if btc_d is not None:
        _, _, ba = build_grid({"BTC": btc_d}, start_ms, now_ms)
        bc = pd.Series(ba["c"][0])
        b15, b1h = (bc / bc.shift(3) - 1) * 100, (bc / bc.shift(12) - 1) * 100
        btc_ok = ~((b15 < -1.2) | (b1h < -2.5)).fillna(False).to_numpy()
        btc4h = ((bc / bc.shift(48) - 1) * 100).to_numpy()

    print(f"حساب المؤشرات والإشارات لـ {S} عملة × {T} شمعة ...")
    feats, rule_keys = [], {}
    g1h_wide = np.full((S, T), np.nan)
    for si, s in enumerate(syms):
        x = features({k: arr[k][si] for k in ("o", "h", "l", "c", "qv", "tbq")})
        feats.append(x)
        g1h_wide[si] = x["g1h"]
        for name, mk in rule_masks(x, btc_ok).items():
            mk = mk.copy()
            mk[:300] = False
            idx = cooldown(np.flatnonzero(mk), a.cooldown)
            if len(idx):
                rule_keys.setdefault(name, []).append(si * T + idx)
    with np.errstate(invalid="ignore"):
        breadth = np.nanmean(np.where(np.isfinite(g1h_wide), (g1h_wide > 2).astype(float), np.nan), axis=0)

    # عينة ضابطة عشوائية
    cand = []
    for si in range(S):
        v = np.flatnonzero(feats[si]["valid"])
        v = v[(v >= 300) & (v < T - a.lag - W)]
        if len(v):
            cand.append(si * T + v)
    cand = np.concatenate(cand)
    rule_keys["CONTROL"] = [rng.choice(cand, size=min(a.control, len(cand)), replace=False)]
    keys = {k: np.unique(np.concatenate(v)) for k, v in rule_keys.items()}

    # جمع نوافذ الأسعار لكل الإشارات مرة واحدة
    union = np.unique(np.concatenate(list(keys.values())))
    s_i, i_i = union // T, union % T
    e_i = i_i + a.lag
    okw = e_i + W <= T
    union, s_i, i_i, e_i = union[okw], s_i[okw], i_i[okw], e_i[okw]
    cols = e_i[:, None] + np.arange(W)[None, :]
    Hh, Ll, Cc = arr["h"][s_i[:, None], cols], arr["l"][s_i[:, None], cols], arr["c"][s_i[:, None], cols]
    E = arr["o"][s_i, e_i]
    good = np.isfinite(Hh).all(1) & np.isfinite(Ll).all(1) & np.isfinite(Cc).all(1) & np.isfinite(E) & (E > 0)
    union, s_i, i_i, e_i, Hh, Ll, Cc, E = (z[good] for z in (union, s_i, i_i, e_i, Hh, Ll, Cc, E))
    N = len(union)
    t_sig = t_all[i_i]
    day = t_sig // 86_400_000
    print(f"إشارات فريدة بنوافذ كاملة: {N:,}")

    rules = sorted(keys)
    memb = {}
    for r in rules:
        k = keys[r]
        p = np.clip(np.searchsorted(union, k), 0, N - 1)
        memb[r] = p[union[p] == k]  # الإشارات اللي نافذتها كاملة بس

    variants = exit_variants()
    NET = np.column_stack([run_exit(v, Hh, Ll, Cc, E, a.fee, a.slip) for v in variants]).astype(np.float32)
    vnames = [v["name"] for v in variants]
    vcur = vnames.index(CURRENT_EXIT)

    cutoff = t_all[0] + (t_all[-1] - t_all[0]) * a.train_frac
    embargo = 4 * 3600_000
    is_train, is_test = t_sig <= cutoff - embargo, t_sig >= cutoff
    edges = np.quantile(t_sig, np.linspace(0, 1, 7)[1:-1])
    fold = np.digitize(t_sig, edges)

    emit("=" * 78)
    emit(f"Backtest 5m | {S} عملة | {pd.to_datetime(t_all[0], unit='ms'):%Y-%m-%d} → {pd.to_datetime(t_all[-1], unit='ms'):%Y-%m-%d}"
         f" | فلتر الحلال: {'نعم' if halal else 'لا/تجريبي'}")
    emit(f"رسوم={a.fee}% انزلاق={a.slip}% | دخول بعد {a.lag} شمعة | cooldown={a.cooldown} شمعة | تدريب حتى {pd.to_datetime(cutoff, unit='ms'):%Y-%m-%d}")
    emit("=" * 78)
    emit("عدد الإشارات (بعد cooldown): " + " | ".join(f"{r}={len(memb[r])}" for r in rules if r.startswith(('GATE', 'CONTROL', 'MOM:BASE'))))
    days_span = max((t_all[-1] - t_all[0]) / 86_400_000, 1)
    emit(f"كثافة GATE:ANY ≈ {len(memb.get('GATE:ANY', [])) / days_span:.1f} إشارة/يوم على {S} عملة")

    # [1] بوابات الماسح مقابل العشوائي
    emit(f"\n[1] بوابات الماسح بالإعداد الحالي ({CURRENT_EXIT}) مقابل دخول عشوائي — الفترة كلها | آخر 1/3 فقط")
    for r in ("CONTROL", "GATE:STRICT", "GATE:EXPLOSIVE", "GATE:PERSISTENT", "GATE:ANY", "MOM:BASE"):
        if r not in memb or len(memb[r]) == 0:
            continue
        ix = memb[r]
        allv, tst = NET[ix, vcur], NET[ix[is_test[ix]], vcur]
        ci = cluster_ci(allv.astype(float), day[ix])
        emit(f"  {r:<16} كله: {fs(stats(allv))} [95% CI {ci[0]:+.2f},{ci[1]:+.2f}] || اختبار: n={len(tst)} متوسط {np.mean(tst) if len(tst) else np.nan:+.2f}%")

    # [2] أفضل خروج لبوابات الماسح
    emit("\n[2] أفضل 5 إعدادات خروج لـ GATE:ANY (معلومة، مش اختيار نهائي)")
    if len(memb.get("GATE:ANY", [])) >= 30:
        ix = memb["GATE:ANY"]
        order = np.argsort(-NET[ix].mean(0))[:5]
        for j in order:
            emit(f"  {vnames[j]:<34} {fs(stats(NET[ix, j].astype(float)))}")

    # [3] بحث walk-forward
    emit(f"\n[3] بحث walk-forward: اختيار على التدريب، قياس على الاختبار")
    combos, seen = [], set()
    for r in rules:
        if r == "CONTROL":
            continue
        ix = memb[r]
        sig = ix.tobytes()
        if sig in seen:  # قاعدة بنفس الإشارات بالظبط = مش اختبار جديد
            continue
        seen.add(sig)
        tr_ix, te_ix = ix[is_train[ix]], ix[is_test[ix]]
        if len(tr_ix) < a.min_n or len(te_ix) < 40:
            continue
        for j in range(len(vnames)):
            combos.append((r, j, NET[tr_ix, j].mean(), len(tr_ix), NET[te_ix, j].mean(), len(te_ix)))
    emit(f"  تركيبات مختبرة (قاعدة × خروج): {len(combos)} — توقّع ~{0.025 * len(combos):.0f} 'نجاح' بالصدفة عند 95%")
    cdf = pd.DataFrame(combos, columns=["rule", "j", "m_train", "n_train", "m_test", "n_test"])
    cdf["exit"] = [vnames[j] for j in cdf["j"]] if len(cdf) else []
    candidates = []
    if len(cdf):
        best = cdf.loc[cdf.groupby("rule")["m_train"].idxmax()].sort_values("m_train", ascending=False)
        top = best.head(10)
        emit("  أفضل 10 قواعد (بأفضل خروج لكل واحدة على التدريب) → أداؤهم على الاختبار:")
        for _, rw in top.iterrows():
            ix = memb[rw["rule"]]
            te = ix[is_test[ix]]
            vals = NET[te, int(rw["j"])].astype(float)
            ci = cluster_ci(vals, day[te])
            fm = [NET[ix[fold[ix] == k], int(rw["j"])].mean() if (fold[ix] == k).sum() >= 10 else np.nan for k in range(6)]
            pos = int(np.nansum(np.array(fm) > 0))
            emit(f"  {rw['rule'][:36]:<36} {rw['exit']:<26} تدريب {rw['m_train']:+5.2f}% ({int(rw['n_train'])}) → اختبار {rw['m_test']:+5.2f}% ({int(rw['n_test'])})"
                 f" [95% {ci[0]:+.2f},{ci[1]:+.2f}] فترات موجبة {pos}/6")
            if rw["m_train"] > 0 and rw["m_test"] > 0 and ci[2] > 0 and pos >= 4 and rw["n_test"] >= 100:
                candidates.append((rw["rule"], rw["exit"], rw["m_test"], ci[2]))
        both = cdf[cdf["m_train"] > 0]
        emit(f"  تركيبات رابحة في التدريب: {len(both)} من {len(cdf)} | منها رابحة في الاختبار: {int((both['m_test'] > 0).sum())}"
             f" | ارتباط (رتب) تدريب↔اختبار: {cdf['m_train'].rank().corr(cdf['m_test'].rank()):+.2f}")
        cdf.drop(columns=["j"]).to_csv(os.path.join(OUT_DIR, "bt_combos.csv"), index=False, encoding="utf-8-sig")

    # [4] سياق السوق لبوابات الماسح
    emit("\n[4] GATE:ANY حسب حالة السوق (الإعداد الحالي، الفترة كلها)")
    ix = memb.get("GATE:ANY", np.array([], dtype=int))
    if len(ix) >= 60:
        br, b4 = breadth[i_i[ix]], btc4h[i_i[ix]]
        vals = NET[ix, vcur].astype(float)
        if np.isfinite(br).sum() > 30:
            q = np.nanquantile(br, [1 / 3, 2 / 3])
            for lab, mk in (("breadth منخفض", br <= q[0]), ("breadth متوسط", (br > q[0]) & (br <= q[1])), ("breadth عالي", br > q[1])):
                emit(f"  {lab:<14} {fs(stats(vals[mk]))}")
        for lab, mk in (("BTC 4h هابط", b4 < -0.5), ("BTC 4h هادئ", (b4 >= -0.5) & (b4 <= 0.5)), ("BTC 4h صاعد", b4 > 0.5)):
            if mk.sum() >= 15:
                emit(f"  {lab:<14} {fs(stats(vals[mk]))}")

    # الحكم
    emit("\n" + "=" * 78)
    if candidates:
        emit("الحكم: فيه مرشحين اجتازوا الاختبار (متوسط اختبار موجب + حد أدنى CI 99% موجب + 4/6 فترات موجبة):")
        for r, e, mt, lo in candidates:
            emit(f"   • {r} | {e} | متوسط اختبار {mt:+.2f}% | أدنى CI99 {lo:+.2f}%")
        emit("   حتى كده: ده مرشح مش دليل. لازم يتأكد على فترة جديدة (forward) قبل أي فلوس حقيقية.")
    else:
        emit("الحكم: مفيش قاعدة اجتازت كل الاختبارات (موجبة خارج العينة + CI99 موجب + ثبات 4/6 فترات).")
        emit("   يعني: على البيانات دي، مفيش edge ثابت بعد الرسوم للقواعد المختبرة.")
    emit("تنبيه: شموع 5m أخشن من 1m (تقدير متحفظ للستوبات)، والكون = عملات متداولة النهارده (تحيز بقاء)، وفترة واحدة من السوق.")

    # حفظ
    with open(os.path.join(OUT_DIR, "bt_report.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(OUT))
    ix = memb.get("GATE:ANY", np.array([], dtype=int))
    if len(ix):
        rows = []
        for k in ix:
            x = feats[s_i[k]]
            r = {"symbol": syms[s_i[k]], "time_utc": pd.to_datetime(t_sig[k], unit="ms"), "net_current_exit": float(NET[k, vcur])}
            for c in ("g5", "g15", "g1h", "g24h", "vol15", "vol1h", "accel", "buy", "brk", "rsi", "streak", "tc"):
                r[c] = float(x[c][i_i[k]])
            rows.append(r)
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, "bt_gate_any_signals.csv"), index=False, encoding="utf-8-sig")
    print(f"\nتم: {OUT_DIR}/bt_report.txt , bt_combos.csv , bt_gate_any_signals.csv")


# ============================================================ بيانات اصطناعية للاختبار الذاتي
def synth(plant_edge, n_sym=16, days=60):
    r = np.random.default_rng(21)
    now_ms = int(time.time() * 1000)
    now_ms -= now_ms % STEP_MS
    start_ms = now_ms - days * 86_400_000
    T = days * 288
    data = {}
    for k in range(n_sym + 1):
        ret = r.normal(0, 0.0035, T)
        qv = r.lognormal(np.log(2e5), 0.5, T)
        buy = r.uniform(0.42, 0.58, T)
        spikes = np.flatnonzero(r.random(T) < 0.004)
        for s in spikes:
            if s < 320 or s > T - 60:
                continue
            ret[s - 3:s + 1] += 0.004          # 3 شموع صاعدة قبل الإشارة (بتظهر بشكل عشوائي)
            qv[s - 2:s + 1] *= r.uniform(4, 7)
            buy[s - 2:s + 1] = r.uniform(0.6, 0.7)
            if plant_edge and k % 2 == 0:
                ret[s + 2:s + 26] += 0.0012    # edge مزروع: تكملة صعود بعد النمط
        c = 10 * np.cumprod(1 + ret)
        o = np.r_[c[0], c[:-1]]
        hi, lo = np.maximum(o, c) * (1 + np.abs(r.normal(0, 0.0012, T))), np.minimum(o, c) * (1 - np.abs(r.normal(0, 0.0012, T)))
        t = start_ms + np.arange(T) * STEP_MS
        name = "BTCUSDT" if k == n_sym else f"S{k}USDT"
        data[name] = pd.DataFrame({"t": t, "o": o, "h": hi, "l": lo, "c": c, "qv": qv, "tbq": qv * buy})
    return data, start_ms, now_ms


if __name__ == "__main__":
    main()
