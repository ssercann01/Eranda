"""
ERANDA V2 — consumer.py  (Katman 3 — Consumer worker, ayrı process)
================================================================
RAW Queue'dan ham mesaj alır → Parse + merge → indikatör → KNS/Stoch/Fib →
gap check → Redis (Katman 4) + Pub/Sub ("updates"). TÜM CPU-ağır iş burada.

Her handler'ın gövdesi collector.py'deki ESKİ inline handler'lardan BİREBİR
taşındı; sadece "cache'deki son df'i al → işle → cache'e yaz" mantığı artık
producer yerine bu ayrı process'te çalışıyor. İş mantığı değişmedi.

Eşleme (hangi handler nereden geldi):
  _apply_bnc_kline   ← collector _ws_on_message      (3300–3360)
  _apply_mexc_deal   ← collector _mexc_tick_update   (3957–4011)
  _apply_mexc_kline  ← collector _mexc_process_kline (4120–4169)
  _apply_bnc_rest    ← fetch_kline_data parse + fetch_and_process_data store
  _apply_mexc_rest   ← _fetch_kline_mexc parse + _mexc_poll_loop merge + derived
  _apply_price_rest  ← fast_poll_task                (3598–3616)

Redis yazımı SADECE snapshot/refresh (poll=False) ve derived pass'te yapılır —
WS tick'leri ve MEXC poll yalnızca in-memory cache'i günceller. (collector ile
birebir aynı davranış: "WS Redis'e yazmaz, periyodik fetch yazar".)
"""

import json
import time
import traceback

import numpy as np
import pandas as pd

from shared import (
    redis_cache, redis_client, SYMBOL_SOURCE,
    _derive_interval, _MEXC_DERIVED_INTERVALS,
    COLLECTOR_SYMBOLS, COLLECTOR_INTERVALS, COLLECTOR_INDICATORS,
    deque,
)
from bus import timestamp_to_datetime, KNS_PRESETS, kns_col_prefix
import indicators
from indicators import (
    calculate_indicators, update_indicators,
    calculate_consensus_signal, calculate_support_resistance, get_sr_signal,
    cache, cache_lock, _sr_cache, _TPD_WINDOW, CACHE_SIZE,
)

# ── Consumer-owned config/state (collector global'lerinden taşındı) ──────────
selected_symbols   = list(COLLECTOR_SYMBOLS)
selected_intervals = list(COLLECTOR_INTERVALS)
selected_indicators = list(COLLECTOR_INDICATORS)
kns_indicators_global = ['VOL', 'CVD', 'VWAP']

_THROTTLE_SECONDS = {
    '1m': 0.5, '3m': 1.0, '5m': 1.5, '15m': 3.0, '30m': 5.0, '1h': 8.0,
    '2h': 12.0, '4h': 20.0, '6h': 30.0, '8h': 30.0, '12h': 45.0, '1d': 60.0, '3d': 90.0,
}
_DEFAULT_THROTTLE = 2.0
_last_update_time: dict = {}

_TPD_THROTTLE = {
    '1m': 1.0, '3m': 2.0, '5m': 3.0, '15m': 5.0, '30m': 8.0, '1h': 12.0,
    '2h': 20.0, '4h': 30.0, '6h': 45.0, '8h': 45.0, '12h': 60.0, '1d': 90.0, '3d': 120.0,
}
_last_tpd_time: dict = {}

_mexc_last_price: dict = {}
_mexc_tick_last_run: dict = {}
_MEXC_TICK_THROTTLE = 1.0

_OHLCV = ('open', 'high', 'low', 'close', 'volume')


# ── KNS + SR'yi consumer'da hesapla, df'e kolon yaz (dashboard SADECE okur) ───
# Eski dashboard.py bunları render sırasında hesaplıyordu (Katman 5 ihlali).
# Artık Katman 3 hesaplar, Redis df'ine gömer; dashboard kolonları okur.
def _enrich_kns_sr(df: pd.DataFrame, symbol: str, interval: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    idx = df.index[-1]
    # KNS artık dashboard'da CANLI hesaplanıyor (kullanıcının seçtiği kombinasyon
    # son-satır kolonlarından oylanıyor). Burada sabit preset hesaplamaya gerek
    # YOK → consumer hafifler, Redis daralır. Sadece SR önceden hesaplanır.
    try:
        sup, res = calculate_support_resistance(df)
        close_v = float(df['close'].iloc[-1])
        df.loc[idx, 'SR_SUPPORT']    = sup if sup is not None else np.nan
        df.loc[idx, 'SR_RESISTANCE'] = res if res is not None else np.nan
        df.loc[idx, 'SR_SIGNAL']     = get_sr_signal(close_v, sup, res)
    except Exception as e:
        print(f"[SR] {symbol}-{interval}: {e}")
    return df


# ── Redis yazımı + Pub/Sub (fetch_and_process_data store guard — BİREBİR) ─────
def _clean_row(row) -> dict:
    """Bir df satırını JSON-uyumlu dict'e çevir (NaN→None, numpy→python)."""
    clean = {}
    for col, vv in row.items():
        try:
            if pd.isna(vv):
                clean[col] = None
                continue
        except (TypeError, ValueError):
            pass
        if isinstance(vv, np.floating):
            clean[col] = float(vv)
        elif isinstance(vv, np.integer):
            clean[col] = int(vv)
        elif isinstance(vv, (int, float, str, bool)):
            clean[col] = vv
        else:
            clean[col] = str(vv)
    return clean


def _store_and_publish(symbol: str, interval: str, processed_df: pd.DataFrame):
    try:
        ind_cols = [c for c in processed_df.columns if c not in _OHLCV]
        if not ind_cols:
            print(f"[SKIP] {symbol}-{interval}: indikatör yok, Redis korundu")
            return
        try:
            existing = redis_cache.client.get(f"market_data:{symbol}:{interval}:rows")
            existing_rows = int(existing) if existing else 0
        except Exception:
            existing_rows = 0
        n = len(processed_df)
        if n < 5:
            print(f"[SKIP] {symbol}-{interval}: {n} row çok kısa")
            return
        if existing_rows > 0 and n < existing_rows * 0.5:
            print(f"[SKIP] {symbol}-{interval}: {n} row < mevcut {existing_rows}, korundu")
            return
        redis_cache.store_dataframe(symbol, interval, processed_df)
        # >>> snap: dashboard için KÜÇÜK son-satır JSON'u. Dashboard 500 satırlık
        # df'i hiç açmaz, sadece bu son satırı + live: overlay'i okur (büyük hız).
        try:
            redis_client.set(f"snap:{symbol}:{interval}",
                             json.dumps(_clean_row(processed_df.iloc[-1])))
        except Exception:
            pass
        try:
            redis_client.publish("updates", f"{symbol}:{interval}")
        except Exception:
            pass
    except Exception as e:
        print(f"[ERROR] Redis write {symbol}-{interval}: {e}")


def _cache_append(cache_key: str, df: pd.DataFrame):
    with cache_lock:
        if cache_key not in cache:
            cache[cache_key] = deque(maxlen=CACHE_SIZE)
        cache[cache_key].append(df)


# ── Hafif canlı yazım: forming mumun SON SATIRINI küçük Redis key'ine ─────────
# Tam df YAZMAZ (o yavaştı + bozuyordu). Sadece son barın skalerlerini JSON
# olarak 'live:SEMBOL:interval' key'ine yazar (TTL'li). Dashboard snapshot
# df'inin üstüne bu son barı bindirir → forming mum taze, geçmiş snapshot'tan.
#
# TÜM interval'lar canlı — ama her birine UYGUN throttle: kısa interval hızlı,
# uzun interval seyrek (uzun mum saniyede kıpırdamaz → seyrek yeterli, consumer
# boğulmaz). TTL de interval'a göre (uzun mum live key'i daha uzun yaşar).
# Canlı tick yolundan ÇIKARILAN ağır indikatörler (snapshot zaten hesaplar;
# forming mumda her tick yeniden hesaplamak gereksiz + pahalı).
_LIVE_SKIP = {'SR', 'Fibonacci', 'TRD', 'DIV', 'DIVERGENCE'}

_LIVE_THROTTLE_BY_IV = {
    '1m': 0.4, '3m': 0.5, '5m': 0.7, '15m': 1.0, '30m': 1.5,
    '1h': 2.0, '2h': 3.0, '4h': 5.0, '6h': 8.0,
    '8h': 8.0, '12h': 12.0, '1d': 20.0, '3d': 30.0,
}
_LIVE_TTL_BY_IV = {
    '1m': 10, '3m': 12, '5m': 15, '15m': 30, '30m': 45,
    '1h': 60, '2h': 90, '4h': 120, '6h': 180,
    '8h': 180, '12h': 240, '1d': 300, '3d': 300,
}
_live_gate: dict = {}

def _live_due(symbol: str, interval: str) -> bool:
    """Bu (symbol, interval) için canlı hesap+yazım zamanı geldi mi?
    Per-interval throttle → kısa hızlı, uzun seyrek. O(1) sözlük kontrolü."""
    thr = _LIVE_THROTTLE_BY_IV.get(interval, 2.0)
    k = f"{symbol}-{interval}"
    now = time.monotonic()
    if now - _live_gate.get(k, 0.0) < thr:
        return False
    _live_gate[k] = now
    return True


def _store_live(symbol: str, interval: str, df: pd.DataFrame):
    if df is None or df.empty:
        return
    try:
        df = _enrich_kns_sr(df, symbol, interval)  # son bar KNS/SR (ucuz)
        clean = _clean_row(df.iloc[-1])
        ttl = _LIVE_TTL_BY_IV.get(interval, 30)
        redis_client.set(f"live:{symbol}:{interval}", json.dumps(clean), ex=ttl)
    except Exception as e:
        print(f"[LIVE] {symbol}-{interval}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  INCREMENTAL — Binance canlı mum  (eski _ws_on_message gövdesi)
# ═══════════════════════════════════════════════════════════════════════════
def _apply_bnc_kline(k: dict):
    if not _live_due(k['s'], k['i']):
        return  # bu interval için throttle dolmadı → ucuz çık
    new_data = {
        'timestamp': timestamp_to_datetime(k['t']),
        'open':  float(k['o']), 'high': float(k['h']), 'low': float(k['l']),
        'close': float(k['c']), 'volume': float(k['v']),
    }
    cache_key = f"{k['s']}-{k['i']}"
    with cache_lock:
        if cache_key in cache and cache[cache_key]:
            df = cache[cache_key][-1].copy()
        else:
            return  # cache henüz dolmamış → snapshot bekleniyor
    _win = _TPD_WINDOW.get(k['i'], 200)
    df_win = df.tail(_win).copy() if len(df) > _win else df.copy()
    df_win.loc[new_data['timestamp']] = new_data
    if len(df_win) > _win:
        df_win = df_win.iloc[-_win:]

    if 'SR' in selected_indicators:
        tail14 = df_win.tail(14)
        _sr_cache[cache_key] = (
            float(min(tail14['low'].min(),  new_data['low'])),
            float(max(tail14['high'].max(), new_data['high'])),
        )

    _now = time.monotonic()
    _run_tpd = (_now - _last_tpd_time.get(cache_key, 0.0)) >= _TPD_THROTTLE.get(k['i'], 2.0)
    if _run_tpd:
        _last_tpd_time[cache_key] = _now

    _fast = [i for i in selected_indicators   if not i.startswith('TPD') and i not in _LIVE_SKIP]
    _kns  = [i for i in kns_indicators_global if not i.startswith('TPD') and i not in _LIVE_SKIP]
    _comb = list(set(_fast) | set(_kns))

    r = update_indicators(df_win, _comb, k['i'], k['s']) if _comb else df_win
    if _run_tpd and any(x.startswith('TPD') for x in selected_indicators):
        r = update_indicators(r, [x for x in selected_indicators if x.startswith('TPD')], k['i'], k['s'])
    _cache_append(cache_key, r)
    _store_live(k['s'], k['i'], r)  # throttle'lı canlı Redis yazımı


# ═══════════════════════════════════════════════════════════════════════════
#  INCREMENTAL — MEXC deal tick  (eski _mexc_tick_update gövdesi)
# ═══════════════════════════════════════════════════════════════════════════
def _apply_mexc_deal(symbol: str, price: float):
    # mexc_deal mesajı yalnızca MEXC WS'ten gelir → kaynak zaten mexc.
    ivs = [iv for iv in (selected_intervals or []) if _live_due(symbol, iv)]
    if not ivs:
        return
    _fast = [i for i in selected_indicators   if not i.startswith("TPD") and i not in _LIVE_SKIP]
    _kns  = [i for i in kns_indicators_global if not i.startswith("TPD") and i not in _LIVE_SKIP]
    _comb = list(set(_fast) | set(_kns))

    for iv in ivs:
        cache_key = f"{symbol}-{iv}"
        _win = _TPD_WINDOW.get(iv, 200)
        with cache_lock:
            q = cache.get(cache_key)
            if not q:
                continue
            df = q[-1].copy()
        if df.empty:
            continue
        df.iat[-1, df.columns.get_loc("close")] = price
        if price > df.iat[-1, df.columns.get_loc("high")]:
            df.iat[-1, df.columns.get_loc("high")] = price
        if price < df.iat[-1, df.columns.get_loc("low")]:
            df.iat[-1, df.columns.get_loc("low")] = price
        df_win = df.tail(_win) if len(df) > _win else df
        if "SR" in selected_indicators:
            tail14 = df_win.tail(14)
            _sr_cache[cache_key] = (float(tail14["low"].min()), float(tail14["high"].max()))
        processed = update_indicators(df_win, _comb, iv, symbol) if _comb else df_win
        _cache_append(cache_key, processed)
        _store_live(symbol, iv, processed)  # throttle'lı canlı Redis yazımı


# ═══════════════════════════════════════════════════════════════════════════
#  INCREMENTAL — MEXC push.kline  (eski _mexc_process_kline gövdesi)
# ═══════════════════════════════════════════════════════════════════════════
def _apply_mexc_kline(symbol: str, interval: str, kd: dict):
    if not _live_due(symbol, interval):
        return
    cache_key = f"{symbol}-{interval}"
    _win = _TPD_WINDOW.get(interval, 200)
    with cache_lock:
        if cache_key not in cache or not cache[cache_key]:
            return
        df = cache[cache_key][-1].copy()
    try:
        current_close = float(kd.get("c", 0))
        if current_close == 0:
            return
        current_ts = kd.get("t", 0)
        ts_ms = int(current_ts) * 1000 if int(current_ts) < 1_000_000_000_000 else int(current_ts)
        new_data = {
            "timestamp": timestamp_to_datetime(ts_ms),
            "open":  float(kd.get("o", current_close)), "high": float(kd.get("h", current_close)),
            "low":   float(kd.get("l", current_close)), "close": current_close,
            "volume": float(kd.get("v", kd.get("vol", 0))),
        }
    except Exception as ex:
        print(f"[MEXC-KLINE] parse hata {symbol}-{interval}: {ex}")
        return
    df_win = df.tail(_win).copy() if len(df) > _win else df.copy()
    df_win.loc[new_data["timestamp"]] = new_data
    if len(df_win) > _win:
        df_win = df_win.iloc[-_win:]
    if "SR" in selected_indicators:
        tail14 = df_win.tail(14)
        _sr_cache[cache_key] = (
            float(min(tail14["low"].min(),  new_data["low"])),
            float(max(tail14["high"].max(), new_data["high"])),
        )
    _now = time.monotonic()
    _run_tpd = (_now - _last_tpd_time.get(cache_key, 0.0)) >= _TPD_THROTTLE.get(interval, 2.0)
    if _run_tpd:
        _last_tpd_time[cache_key] = _now
    _fast = [i for i in selected_indicators   if not i.startswith("TPD") and i not in _LIVE_SKIP]
    _kns  = [i for i in kns_indicators_global if not i.startswith("TPD") and i not in _LIVE_SKIP]
    _comb = list(set(_fast) | set(_kns))
    processed_df = update_indicators(df_win, _comb, interval, symbol) if _comb else df_win
    if _run_tpd and any(i.startswith("TPD") for i in selected_indicators):
        processed_df = update_indicators(processed_df, [i for i in selected_indicators if i.startswith("TPD")], interval, symbol)
    _cache_append(cache_key, processed_df)
    _store_live(symbol, interval, processed_df)  # throttle'lı canlı Redis yazımı


# ═══════════════════════════════════════════════════════════════════════════
#  SNAPSHOT — Binance REST  (parse + calculate_indicators + Redis)
# ═══════════════════════════════════════════════════════════════════════════
def _parse_bnc(data) -> pd.DataFrame:
    df = pd.DataFrame(np.array(data)[:, :6],
                      columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    num_cols = ['open', 'high', 'low', 'close', 'volume']
    df[num_cols] = df[num_cols].astype(np.float64)
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms')
    df.set_index('timestamp', inplace=True)
    return df


def _apply_bnc_rest(symbol: str, interval: str, data):
    if not data:
        return
    try:
        df = _parse_bnc(data)
    except Exception as e:
        print(f"[BNC-REST] parse hata {symbol}-{interval}: {e}")
        return
    if df.empty:
        return
    processed = calculate_indicators(df, selected_indicators, interval, symbol)
    processed = _enrich_kns_sr(processed, symbol, interval)
    _cache_append(f"{symbol}-{interval}", processed)
    _store_and_publish(symbol, interval, processed)


# ═══════════════════════════════════════════════════════════════════════════
#  MEXC REST  — snapshot (poll=False) veya poll (poll=True, merge + cache-only)
# ═══════════════════════════════════════════════════════════════════════════
def _parse_mexc(data) -> pd.DataFrame:
    times  = data.get("time",  [])
    if not times:
        return pd.DataFrame()
    opens  = data.get("open",  []); highs = data.get("high", [])
    lows   = data.get("low",   []); closes = data.get("close", [])
    vols   = data.get("vol",   data.get("volume", [0] * len(times)))
    ts_ms  = [int(t) * 1000 if int(t) < 1_000_000_000_000 else int(t) for t in times]
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(ts_ms, unit="ms"),
        "open":   np.array(opens,  dtype=np.float64),
        "high":   np.array(highs,  dtype=np.float64),
        "low":    np.array(lows,   dtype=np.float64),
        "close":  np.array(closes, dtype=np.float64),
        "volume": np.array(vols,   dtype=np.float64),
    }).set_index("timestamp")
    return df


def _derived_pass(symbol: str, src_iv: str, src_df: pd.DataFrame, poll: bool):
    """MEXC türetilmiş interval (2h/6h/12h/3d/3m) — resample + hesap.
    (collector fetch_and_process_data derived pass + _mexc_poll_loop derived)"""
    for derived_iv, (s_iv, _n) in _MEXC_DERIVED_INTERVALS.items():
        if s_iv != src_iv:
            continue
        if selected_intervals and derived_iv not in selected_intervals:
            continue
        derived_df = _derive_interval(src_df, derived_iv)
        if derived_df is None or derived_df.empty or len(derived_df) < 5:
            continue
        if len(derived_df) < 30:
            _inds = [i for i in selected_indicators if i in ["RSI","CMF","CCI","WIL","ADO","+DI","MFI"]]
        else:
            _inds = selected_indicators
        try:
            if poll:
                d_proc = update_indicators(derived_df, list(set(selected_indicators) | set(kns_indicators_global)), derived_iv, symbol)
            else:
                d_proc = calculate_indicators(derived_df, _inds, derived_iv, symbol)
        except Exception as ce:
            print(f"[DERIVED-ERR] {symbol}-{derived_iv}: {ce}")
            d_proc = derived_df
        _cache_append(f"{symbol}-{derived_iv}", d_proc)
        if not poll:  # poll cache-only; snapshot Redis'e de yazar
            d_proc = _enrich_kns_sr(d_proc, symbol, derived_iv)
            _store_and_publish(symbol, derived_iv, d_proc)


def _apply_mexc_rest(symbol: str, interval: str, data, poll: bool):
    try:
        df = _parse_mexc(data)
    except Exception as e:
        print(f"[MEXC-REST] parse hata {symbol}-{interval}: {e}")
        return
    if df.empty:
        return
    combined = list(set(selected_indicators) | set(kns_indicators_global))
    cache_key = f"{symbol}-{interval}"

    if poll:
        # _mexc_poll_loop davranışı: 1m'de mevcut cache ile merge, update_indicators, cache-only
        with cache_lock:
            existing = cache.get(cache_key)
            if existing and interval == "1m":
                base_df = existing[-1].copy()
                merged = pd.concat([base_df, df[~df.index.isin(base_df.index)]])
                merged = merged[~merged.index.duplicated(keep="last")]
                merged.update(df)
                _win = _TPD_WINDOW.get(interval, 200)
                df = merged.tail(_win)
        processed = update_indicators(df, combined, interval, symbol)
        _cache_append(cache_key, processed)
        _store_live(symbol, interval, processed)  # throttle'lı canlı Redis yazımı
        _derived_pass(symbol, interval, df, poll=True)
    else:
        # snapshot: calculate_indicators (full) + Redis
        processed = calculate_indicators(df, selected_indicators, interval, symbol)
        processed = _enrich_kns_sr(processed, symbol, interval)
        _cache_append(cache_key, processed)
        _store_and_publish(symbol, interval, processed)
        _derived_pass(symbol, interval, df, poll=False)


# ═══════════════════════════════════════════════════════════════════════════
#  fast-poll fiyat patch  (eski fast_poll_task gövdesi)
# ═══════════════════════════════════════════════════════════════════════════
def _apply_price_rest(symbol: str, interval: str, price: float):
    if not _live_due(symbol, interval):
        return
    cache_key = f"{symbol}-{interval}"
    with cache_lock:
        if cache_key not in cache or not cache[cache_key]:
            return
        df = cache[cache_key][-1].copy()
    df.iloc[-1, df.columns.get_loc('close')] = price
    _win = _TPD_WINDOW.get(interval, 200)
    df_win = df.tail(_win).copy() if len(df) > _win else df
    combined = list(set(selected_indicators) | set(kns_indicators_global))
    processed_win = update_indicators(df_win, combined, interval, symbol)
    df.loc[processed_win.index[-1]] = processed_win.iloc[-1]
    _cache_append(cache_key, df)
    _store_live(symbol, interval, df)  # throttle'lı canlı Redis yazımı


# ═══════════════════════════════════════════════════════════════════════════
#  Dispatch + ana döngü
# ═══════════════════════════════════════════════════════════════════════════
def _dispatch(msg: dict):
    t = msg.get("t")
    # TICK → forming mum canlı yolu. Her handler kendi _live_due (interval-bazlı
    # throttle) gate'ini en başta yapar; due değilse O(1) çıkar → consumer boğulmaz.
    if t == "bnc_kline":
        _apply_bnc_kline(msg["k"])
    elif t == "mexc_deal":
        _apply_mexc_deal(msg["symbol"], msg["price"])
    elif t == "mexc_kline":
        _apply_mexc_kline(msg["symbol"], msg["interval"], msg["kd"])
    elif t == "price_rest":
        _apply_price_rest(msg["symbol"], msg["interval"], msg["price"])
    elif t == "bnc_rest":
        _apply_bnc_rest(msg["symbol"], msg["interval"], msg["data"])
    elif t == "mexc_rest":
        _apply_mexc_rest(msg["symbol"], msg["interval"], msg["data"], msg.get("poll", False))


def _resource_monitor(tag: str, q, get_count, interval_s: int = 10, put_counter=None):
    """Her interval_s saniyede CPU%, RAM, işlem hızı ve QUEUE DERİNLİĞİ yazar.
    Derinlik = router'ın bu queue'ya koyduğu toplam (put_counter) − consumer'ın
    aldığı (get_count). macOS'ta qsize() yok; bu sayaç yöntemi her yerde çalışır."""
    import psutil
    proc = psutil.Process()
    proc.cpu_percent(None)  # ilk çağrı 0 döner, kalibre et
    last_n = get_count()
    last_t = time.monotonic()
    while True:
        time.sleep(interval_s)
        now = time.monotonic()
        n = get_count()
        rate = (n - last_n) / max(1e-6, (now - last_t))
        last_n, last_t = n, now
        cpu = proc.cpu_percent(None)
        ram = proc.memory_info().rss / (1024 * 1024)
        depth_s = "queue=?"
        if put_counter is not None:
            try:
                depth = max(0, int(put_counter.value) - n)
                depth_s = f"queue={depth}"
            except Exception:
                pass
        else:
            try:
                depth_s = f"queue≈{q.qsize()}"
            except Exception:
                pass
        try:
            keys = len(cache)
        except Exception:
            keys = -1
        print(f"[{tag}] CPU {cpu:4.1f}% | RAM {ram:6.1f} MB | "
              f"{rate:6.1f} msg/s | toplam {n} | {depth_s} | cache_keys={keys}")


def run_consumer(q, shard_id: int = 0, put_counter=None):
    """Consumer process girişi. q = bu shard'ın RAW Queue'su.
    put_counter = router'ın bu queue'ya koyduğu toplam (queue derinliği için)."""
    print(f"[CONSUMER-{shard_id}] Başladı — {len(selected_symbols)} sembol, "
          f"{len(selected_indicators)} indikatör hesaplanacak")
    import threading as _th
    _state = {"n": 0}
    _th.Thread(target=_resource_monitor,
               args=(f"CONSUMER-{shard_id}", q, lambda: _state["n"], 10, put_counter),
               daemon=True).start()
    while True:
        try:
            msg = q.get()
        except (KeyboardInterrupt, EOFError):
            break
        if msg is None:  # poison pill → kapan
            break
        try:
            _dispatch(msg)
            _state["n"] += 1
        except Exception as e:
            print(f"[CONSUMER-{shard_id}] dispatch hata ({msg.get('t')}): "
                  f"{type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"[CONSUMER-{shard_id}] Durdu — toplam {_state['n']} mesaj")