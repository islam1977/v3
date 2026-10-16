#!/usr/bin/env python3
"""
label_watch_log.py  —  الخطوة 1: قياس بدون ما نلمس الماسح
=========================================================
بياخد كل حالة في early_watch_log.csv (881+ حالة بدل 25 إشارة بس)، ويجيب شموع 1m
بعد لحظة الرصد، ويحسب النتيجة الفعلية تحت عدة إعدادات SL/TP/وقف زمني.

بيجاوب على 3 أسئلة:
  1) هل الحالات اللي الماسح بيلتقطها أحسن من دخول عشوائي في نفس العملات؟ (مجموعة ضابطة)
  2) أنهي SL/TP/مدة بيدّوا أحسن توقّع (وهل ده ثابت في فترة الاختبار؟)
  3) أنهي مؤشرات (حجم، ضغط شراء...) بيفرّقوا الرابح من الخاسر؟

افتراضات واقعية:
  - الدخول = افتتاح أول شمعة 1m بعد (لحظة الرصد + --delay دقيقة) عشان تأخير GitHub/تليجرام/التنفيذ.
  - لو SL وTP في نفس الشمعة -> SL (متحفظ، زي الماسح).
  - رسوم ذهاب+إياب (--fee) وانزلاق إضافي عند الخروج بالـSL (--slip).

التشغيل:
  python label_watch_log.py                 # على early_watch_log.csv
  python label_watch_log.py --selftest      # اختبار على بيانات اصطناعية (بدون شبكة)
المخرجات في label_out/ :  labeled_watch.csv , label_report.txt
"""
import argparse
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

BASES = ["https://data-api.binance.vision", "https://api.binance.com"]
WATCH_FILE = "early_watch_log.csv"
OUT_DIR = "label_out"
MAX_T = 240  # أطول مدة بنختبرها (دقيقة)

session = requests.Session()
session.headers.update({"User-Agent": "watch-labeler/1.0"})


# ============================================================ جلب الشموع
def fetch_1m(symbol, start_ms, limit):
    """يرجّع قائمة شموع 1m أو None لو الرمز مش موجود/فشل."""
    for base in BASES:
        for attempt in range(3):
            try:
                r = session.get(base + "/api/v3/klines",
                                params={"symbol": symbol, "interval": "1m",
                                        "startTime": int(start_ms), "limit": limit}, timeout=15)
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


def to_arrays(rows, start_ms, now_ms):
    """شموع مقفولة بس + فحص إن أول شمعة قريبة من وقت الدخول. يرجّع (arrays|None, سبب الفشل)."""
    if rows is None:
        return None, "فشل الجلب/الرمز غير موجود"
    rows = [r for r in rows if int(r[6]) < now_ms - 2000]
    if not rows:
        return None, "مفيش شموع مقفولة بعد الوقت ده"
    if abs(int(rows[0][0]) - start_ms) > 180_000:
        return None, "أول شمعة بعيدة عن وقت الدخول (رمز اتدرج بعدها/فجوة)"
    a = np.array([[float(r[1]), float(r[2]), float(r[3]), float(r[4])] for r in rows])
    return (a[:, 0], a[:, 1], a[:, 2], a[:, 3]), "ok"


# ============================================================ المحاكاة
def sim_fixed(h, l, c, entry, sl, tp, T):
    """SL/TP ثابتين + وقف زمني. يرجّع (نتيجة, خروج%, دقيقة) أو None لو لسه مخلصش."""
    n = min(len(h), T)
    if n == 0:
        return None
    sl_p, tp_p = entry * (1 - sl / 100), entry * (1 + tp / 100)
    hs, ht = l[:n] <= sl_p, h[:n] >= tp_p
    i_s = int(hs.argmax()) if hs.any() else n
    i_t = int(ht.argmax()) if ht.any() else n
    if i_s == n and i_t == n:
        if len(h) < T:
            return None
        return "TO", (c[n - 1] / entry - 1) * 100, n
    if i_s <= i_t:
        return "SL", -float(sl), i_s + 1
    return "TP", float(tp), i_t + 1


def sim_dynamic(h, l, c, entry, sl, tp, T, be_trig=None, trail=None, trail_trig=None):
    """SL يتحرك: breakeven بعد be_trig% أو trailing بعد trail_trig% (الفحص قبل التحديث = متحفظ)."""
    n = min(len(h), T)
    if n == 0:
        return None
    stop, tp_p, peak = entry * (1 - sl / 100), entry * (1 + tp / 100), entry
    for i in range(n):
        if l[i] <= stop:
            ex = (stop / entry - 1) * 100
            return ("SL" if ex < -0.01 else ("BE" if ex < 0.01 else "TRL")), ex, i + 1
        if h[i] >= tp_p:
            return "TP", float(tp), i + 1
        peak = max(peak, h[i])
        if be_trig is not None and peak >= entry * (1 + be_trig / 100):
            stop = max(stop, entry)
        if trail is not None and peak >= entry * (1 + trail_trig / 100):
            stop = max(stop, peak * (1 - trail / 100))
    if len(h) < T:
        return None
    return "TO", (c[n - 1] / entry - 1) * 100, n


def build_variants():
    v = []
    for T in (60, 120, 240):
        for sl in (2, 3, 3.5, 5, 7):
            for tp in (1.5, 2.5, 3.5, 5.25, 7, 10):
                v.append({"name": f"ثابت SL{sl}/TP{tp}/{T}د", "kind": "fixed", "sl": sl, "tp": tp, "T": T})
    for sl, tp, be in ((3.5, 7, 2.5), (5, 7, 3), (3.5, 10, 2.5), (5, 10, 3)):
        v.append({"name": f"BE SL{sl}/TP{tp}/تعادل@{be}/240د", "kind": "dyn", "sl": sl, "tp": tp, "T": 240, "be": be})
    for sl, tp, tt, tr in ((3.5, 10, 2.5, 1.5), (5, 10, 3, 2), (3.5, 15, 3, 2)):
        v.append({"name": f"Trail SL{sl}/TP{tp}/بعد{tt}/تتبع{tr}/240د", "kind": "dyn", "sl": sl, "tp": tp,
                  "T": 240, "tt": tt, "tr": tr})
    return v


CURRENT = "ثابت SL3.5/TP5.25/240د"  # الإعداد الحالي في الماسح (TP1 الأساسي)


def run_variant(v, samples, fee, slip):
    """يرجّع (net% لكل عينة مكتملة, نتائج, أرقام العينات)."""
    nets, outs, idx = [], [], []
    for k, s in enumerate(samples):
        o, h, l, c = s["arr"]
        entry = o[0]
        if v["kind"] == "fixed":
            r = sim_fixed(h, l, c, entry, v["sl"], v["tp"], v["T"])
        else:
            r = sim_dynamic(h, l, c, entry, v["sl"], v["tp"], v["T"], v.get("be"), v.get("tr"), v.get("tt"))
        if r is None:
            continue
        out, ex, _ = r
        if out in ("SL", "BE", "TRL"):
            ex -= slip
        nets.append(ex - fee)
        outs.append(out)
        idx.append(k)
    return np.array(nets), outs, idx


def stats(nets, outs=None):
    if len(nets) == 0:
        return {"n": 0, "avg": np.nan, "med": np.nan, "win": np.nan, "pf": np.nan}
    pos, neg = nets[nets > 0].sum(), -nets[nets < 0].sum()
    return {"n": len(nets), "avg": nets.mean(), "med": float(np.median(nets)),
            "win": float((nets > 0).mean()), "pf": (pos / neg) if neg > 0 else np.inf}


# ============================================================ التقرير
OUT = []


def emit(x=""):
    print(x)
    OUT.append(str(x))


def fmt_row(name, a, b=None):
    s = (f"{name:<42} n={a['n']:>4} | فوز {a['win']:>4.0%} | متوسط {a['avg']:+6.2f}% | "
         f"وسيط {a['med']:+6.2f}% | PF {a['pf']:.2f}")
    if b is not None:
        s += f" || ضابطة: n={b['n']:>4} متوسط {b['avg']:+6.2f}%"
    return s


def mfe_mae(arr, k):
    o, h, l, c = arr
    kk = min(k, len(h))
    return (h[:kk].max() / o[0] - 1) * 100, (l[:kk].min() / o[0] - 1) * 100


# ============================================================ التشغيل
def load_watch(path):
    w = pd.read_csv(path)
    w["ts"] = pd.to_datetime(w["logged_time_utc"], utc=True, errors="coerce")
    w = w.dropna(subset=["ts", "symbol"]).sort_values("ts").reset_index(drop=True)
    # مستقل عن دقة الوقت في pandas (ns/us) — الطريقة القديمة (astype int64) كانت بتفشل مع pandas 3
    w["ts_ms"] = ((w["ts"] - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(milliseconds=1)).astype("int64")
    if len(w) and not (1.5e12 < w["ts_ms"].median() < 4e12):
        raise SystemExit(f"خطأ: تحويل الوقت غلط (median ts_ms={w['ts_ms'].median()}). راجع صيغة logged_time_utc.")
    w["hour_utc"] = w["ts"].dt.hour
    return w


def collect(w, delay, workers, rng, now_ms, fetch):
    """يجيب شموع الحالة الحقيقية + شمعة ضابطة عشوائية (6-60 ساعة قبلها، نفس العملة)."""
    jobs = []
    for i, r in w.iterrows():
        real_start = ((int(r.ts_ms) // 60000) + 1 + delay) * 60000
        ctrl_ms = int(r.ts_ms - rng.uniform(6, 60) * 3600_000)
        ctrl_start = ((ctrl_ms // 60000) + 1) * 60000
        jobs.append((i, "watch", r.symbol, real_start))
        jobs.append((i, "control", r.symbol, ctrl_start))
    res, reasons = {}, {}
    limit = MAX_T + delay + 15

    def work(job):
        i, kind, sym, start = job
        rows = fetch(sym, start, limit)
        arr, why = to_arrays(rows, start, now_ms)
        return job, arr, why

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, j) for j in jobs]
        for n, f in enumerate(as_completed(futs), 1):
            job, arr, why = f.result()
            if arr is not None:
                res[(job[0], job[1])] = arr
            else:
                reasons[why] = reasons.get(why, 0) + 1
            if n % 300 == 0:
                print(f"  ... {n}/{len(jobs)}")
    return res, reasons


def report(w, res, fee, slip, delay, reasons=None):
    variants = build_variants()
    watch = [{"i": i, "arr": res[(i, "watch")]} for i in range(len(w)) if (i, "watch") in res]
    ctrl = [{"i": i, "arr": res[(i, "control")]} for i in range(len(w)) if (i, "control") in res]
    emit("=" * 78)
    emit(f"تقرير القياس | حالات watch: {len(w)} | شموع متاحة: watch={len(watch)} control={len(ctrl)}")
    emit(f"رسوم={fee}% | انزلاق SL={slip}% | تأخير الدخول={delay}د | الفترة: {w.ts.min():%Y-%m-%d %H:%M} → {w.ts.max():%Y-%m-%d %H:%M}")
    emit("=" * 78)
    if reasons:
        emit("طلبات فشلت: " + " | ".join(f"{k}: {v}" for k, v in reasons.items()))
    if not watch:
        emit("\nمفيش ولا شمعة اتجابت. راجع سبب الفشل فوق (الشبكة/حظر 451 من Binance؟ أو وقت غلط؟) وابعتهولي.")
        return pd.DataFrame(), None
    emit("ملاحظة: الحالات متجمعة زمنياً (نفس السوق في نفس الوقت) فالعينة الفعلية المستقلة أقل من n.")

    # [1] حركة السعر بعد الرصد مقابل عشوائي
    emit("\n[1] هل الحالات أحسن من دخول عشوائي؟ (حركة السعر الخام بعد الدخول)")
    for lab, S in (("watch", watch), ("ضابطة", ctrl)):
        if not S:
            continue
        m = np.array([mfe_mae(s["arr"], 240) for s in S])
        emit(f"  {lab:<6} n={len(S):>4} | وسيط أقصى صعود 4س={np.median(m[:, 0]):+.2f}% | وسيط أقصى نزول 4س={np.median(m[:, 1]):+.2f}% | "
             f"وصلت +3.5%={np.mean(m[:, 0] >= 3.5):.0%} | نزلت -3.5%={np.mean(m[:, 1] <= -3.5):.0%}")

    # [2] الإعداد الحالي
    base = next(v for v in variants if v["name"] == CURRENT)
    nw, ow, iw = run_variant(base, watch, fee, slip)
    nc, oc, _ = run_variant(base, ctrl, fee, slip)
    emit("\n[2] الإعداد الحالي في الماسح (SL 3.5% / TP 5.25% / 4 ساعات)")
    emit("  " + fmt_row("watch", stats(nw), stats(nc)))
    if ow:
        emit("  توزيع نتائج watch: " + ", ".join(f"{k}={ow.count(k) / len(ow):.0%}" for k in ("TP", "SL", "TO")))

    # [3] شبكة المتغيرات
    emit("\n[3] كل المتغيرات (مرتبة بمتوسط الربح الصافي على حالات watch) — أفضل 12 + أسوأ 3")
    rows = []
    cache = {}
    for v in variants:
        a, _, ia = run_variant(v, watch, fee, slip)
        b, _, _ = run_variant(v, ctrl, fee, slip)
        cache[v["name"]] = (a, ia)
        if len(a) >= 50:
            rows.append((v["name"], stats(a), stats(b)))
    rows.sort(key=lambda x: x[1]["avg"], reverse=True)
    for name, a, b in rows[:12]:
        emit("  " + fmt_row(name, a, b))
    emit("  ...")
    for name, a, b in rows[-3:]:
        emit("  " + fmt_row(name, a, b))
    pos = sum(1 for r in rows if r[1]["avg"] > 0)
    emit(f"  عدد المتغيرات الرابحة صافي: {pos} من {len(rows)}")

    # [4] اختبار خارج العينة
    emit("\n[4] اختبار خارج العينة: أختار الأفضل على أول 60% زمنياً، وأقيسه على آخر 40%")
    cut = int(len(w) * 0.6)
    tr_rows = []
    for v in variants:
        a, ia = cache[v["name"]]
        if len(a) < 100:
            continue
        mask_tr = np.array([watch[k]["i"] < cut for k in ia])
        if mask_tr.sum() < 60 or (~mask_tr).sum() < 40:
            continue
        tr_rows.append((v["name"], a[mask_tr].mean(), stats(a[~mask_tr])))
    tr_rows.sort(key=lambda x: x[1], reverse=True)
    if not tr_rows:
        emit("  عينة غير كافية للتقسيم.")
    for name, tr_avg, te in tr_rows[:5]:
        emit(f"  {name:<42} تدريب {tr_avg:+6.2f}% → اختبار n={te['n']:>3} متوسط {te['avg']:+6.2f}% فوز {te['win']:.0%}")
    if tr_rows:
        emit("  (لو الاختبار سالب فالأفضلية في التدريب غالباً صدفة)")

    # [5] المؤشرات
    emit("\n[5] هل مؤشر معيّن بيفرّق؟ (على الإعداد الحالي، ارتباط Spearman + أدنى/أعلى ربع)")
    df = w.loc[[watch[k]["i"] for k in iw]].copy()
    df["net"] = nw
    df["log_qv"] = np.log10(df["quote_volume"].clip(lower=1))
    for f in ("score", "gain_15m", "gain_1h", "gain_24h", "volume_1h", "volume_15m", "acceleration",
              "breakout", "trend_continuity", "green_streak", "log_qv", "hour_utc"):
        if f not in df or df[f].nunique() < 4 or len(df) < 80:
            continue
        rho = df[f].corr(df["net"], method="spearman")
        try:
            q = pd.qcut(df[f], 4, duplicates="drop")
            g = df.groupby(q, observed=True)["net"].agg(["count", "mean"])
            lo, hi = g.iloc[0], g.iloc[-1]
            emit(f"  {f:<17} rho={rho:+.2f} | أدنى ربع: n={int(lo['count']):>3} متوسط {lo['mean']:+5.2f}% | "
                 f"أعلى ربع: n={int(hi['count']):>3} متوسط {hi['mean']:+5.2f}%")
        except ValueError:
            continue
    if "setup" in df:
        emit("  حسب نوع الـsetup:")
        for s, g in df.groupby("setup"):
            if len(g) >= 15:
                emit(f"    {s:<32} n={len(g):>3} متوسط {g['net'].mean():+5.2f}% فوز {(g['net'] > 0).mean():.0%}")
    emit("\nتنبيه: اختبار 12+ مؤشر على عينة صغيرة = احتمال صدفة عالي. مؤشر واحد بارتباط |rho|<0.1 مش دليل.")
    return df, base


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watch-file", default=WATCH_FILE)
    ap.add_argument("--delay", type=int, default=2, help="دقائق بين لحظة الرصد والدخول الفعلي")
    ap.add_argument("--fee", type=float, default=0.2, help="رسوم ذهاب+إياب %%")
    ap.add_argument("--slip", type=float, default=0.1, help="انزلاق إضافي عند الخروج بالـSL %%")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--max-rows", type=int, default=0)
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    now_ms = int(time.time() * 1000)
    fetch = fetch_1m
    rng = random.Random(7)

    if a.selftest:
        np_rng = np.random.default_rng(3)
        syms = [f"T{i}USDT" for i in range(40)]
        ts = pd.Timestamp.now(tz="UTC") - pd.to_timedelta(np_rng.uniform(8, 90, 400), unit="h")
        w = pd.DataFrame({"logged_time_utc": ts.astype(str), "symbol": np_rng.choice(syms, 400),
                          "setup": np_rng.choice(["A", "B"], 400), "score": np_rng.integers(65, 95, 400),
                          "gain_15m": np_rng.normal(2, 1, 400), "gain_1h": np_rng.normal(3, 2, 400),
                          "gain_24h": np_rng.normal(5, 4, 400), "volume_1h": np_rng.uniform(1, 6, 400),
                          "volume_15m": np_rng.uniform(2, 9, 400), "acceleration": np_rng.uniform(1, 4, 400),
                          "breakout": np_rng.normal(0, 1, 400), "quote_volume": np_rng.uniform(2e6, 5e7, 400),
                          "trend_continuity": np_rng.integers(3, 8, 400), "green_streak": np_rng.integers(2, 5, 400)})
        w.to_csv(os.path.join(OUT_DIR, "_selftest_watch.csv"), index=False)

        def fetch(sym, start, limit):  # شموع اصطناعية (مشي عشوائي)
            r = np.random.default_rng(abs(hash((sym, start))) % (2 ** 32))
            p = 100 * np.cumprod(1 + r.normal(0, 0.0015, limit + 1))
            return [[start + k * 60000, p[k], max(p[k], p[k + 1]) * 1.0004, min(p[k], p[k + 1]) * 0.9996, p[k + 1],
                     1, start + k * 60000 + 59999] for k in range(limit)]
        w = load_watch(os.path.join(OUT_DIR, "_selftest_watch.csv"))
    else:
        if not os.path.exists(a.watch_file):
            print(f"مش لاقي {a.watch_file}")
            sys.exit(1)
        w = load_watch(a.watch_file)
    if a.max_rows:
        w = w.tail(a.max_rows).reset_index(drop=True)

    print(f"حالات: {len(w)} | جلب الشموع (watch + ضابطة = {2 * len(w)} طلب)...")
    res, reasons = collect(w, a.delay, a.workers, rng, now_ms, fetch)
    df, base = report(w, res, a.fee, a.slip, a.delay, reasons)
    if df.empty:
        with open(os.path.join(OUT_DIR, "label_report.txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(OUT))
        sys.exit(1)

    # CSV: مؤشرات + النتيجة على الإعداد الحالي + MFE/MAE
    extra = {"mfe_60": [], "mae_60": [], "mfe_240": [], "mae_240": []}
    for i in df.index:
        arr = res[(i, "watch")]
        for k in (60, 240):
            m1, m2 = mfe_mae(arr, k)
            extra[f"mfe_{k}"].append(m1)
            extra[f"mae_{k}"].append(m2)
    for k, v in extra.items():
        df[k] = v
    df.drop(columns=["ts_ms"], errors="ignore").to_csv(os.path.join(OUT_DIR, "labeled_watch.csv"),
                                                     index=False, encoding="utf-8-sig")
    with open(os.path.join(OUT_DIR, "label_report.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(OUT))
    print(f"\nتم: {OUT_DIR}/labeled_watch.csv , {OUT_DIR}/label_report.txt")


if __name__ == "__main__":
    main()
