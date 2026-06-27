"""
ERANDA V2 — producer.py  (Katman 1 — Veri kaynağı)
================================================================
SADECE veri toplar. Binance WS + MEXC WS(deal+depth) + REST poll.
Her ham mesajı RAW Queue'ya (bus) koyar. BURADA:
  - talib / ta / update_indicators / calculate_indicators  YOK
  - in-memory cache                                        YOK
  - DataFrame indikatör hesabı                              YOK
  - event loop içinde CPU-ağır iş                           YOK
Producer kullanıcı sayısından habersizdir, tek instance çalışır.

Sorumluluk: ham JSON → Queue. Parse + hesap Katman 3'te (consumer).
"""

import asyncio
import json
import ssl
import threading
import time
import traceback
from typing import List

import aiohttp
import websockets
import websocket as websocket_client  # websocket-client

from shared import SYMBOL_SOURCE, MEXC_WS_URI, MEXC_FUTURES_BASE, redis_client
import bus

# ── MEXC sabit + helper'lar — async thread scope garantisi (collector ile aynı) ─
_MEXC_INTERVAL_MAP = {
    "1m": "Min1", "3m": "Min3", "5m": "Min5", "15m": "Min15",
    "30m": "Min30", "1h": "Min60", "4h": "Hour4", "8h": "Hour8", "1d": "Day1",
}
_MEXC_DERIVED_INTERVALS = {
    "3m": ("1m", 3), "2h": ("1h", 2), "6h": ("1h", 6),
    "12h": ("1h", 12), "3d": ("1d", 3),
}
_MEXC_SKIP_INTERVALS: set = set()
_RUNTIME_MEXC_FALLBACK: set = set()
_MEXC_SYMBOL_OVERRIDE = {
    "NAS100USDT": "NAS100_USDT", "US30USDT": "US30_USDT",
    "SP500USDT": "SPX500_USDT", "SPX500USDT": "SPX500_USDT",
}
_MEXC_SYMBOL_REVERSE = {"SPX500USDT": "SP500USDT"}
_MEXC_POLL_INTERVALS = {
    "1m": 60, "5m": 60, "15m": 60, "30m": 60,
    "1h": 120, "4h": 300, "8h": 300, "1d": 600,
}
_MEXC_POLL_LIMIT = {
    "1m": 100, "5m": 200, "15m": 300, "30m": 300,
    "1h": 500, "4h": 500, "8h": 500, "1d": 500,
}

def _mexc_format(symbol: str) -> str:
    if symbol in _MEXC_SYMBOL_OVERRIDE:
        return _MEXC_SYMBOL_OVERRIDE[symbol]
    if "_" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return symbol[:-4] + "_USDT"
    return symbol

def _mexc_interval(binance_iv: str) -> str:
    return _MEXC_INTERVAL_MAP.get(binance_iv, "Min1")

def _mexc_to_dashboard(mexc_sym_raw: str) -> str:
    return _MEXC_SYMBOL_REVERSE.get(mexc_sym_raw, mexc_sym_raw)

# ── Producer global state ─────────────────────────────────────────────────────
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

_Q: "bus._mp.Queue" = None          # run_producer içinde set edilir
selected_symbols: List[str]   = []
selected_intervals: List[str] = []

FAST_POLL_SYMBOLS = {'BTCDOMUSDT', 'DEFIUSDT'}
_ws_msg_counter = 0
_enq_count = 0  # Queue'ya konan toplam mesaj (monitör için)
_drop_count = 0  # Queue dolu olduğu için DÜŞÜRÜLEN mesaj (backpressure görünürlüğü)


def _put(msg: dict):
    """Queue'ya koy — dolu ise mesajı düşür (producer ASLA bloke olmaz)."""
    global _enq_count, _drop_count
    try:
        _Q.put_nowait(msg)
        _enq_count += 1
    except Exception:
        # Queue full → consumer geride. Ham tick düşmek, WS'i bloke etmekten iyidir.
        # Sessizce yutmuyoruz: sayıyoruz ki monitörde görünür olsun.
        _drop_count += 1


# ── Dinamik sembol kaydı (kullanıcı ekleyince büyür) ─────────────────────────
# Kullanıcı dashboard'dan sembol ekleyince Redis 'collector:symbols' set'ine
# yazılır. Kontrol thread'i bunu izler, kaynağı (binance/mexc) çözer ve REST
# toplama listesine (_active) ekler → yeni sembol ≤refresh süresi içinde gelir.
# (v1: REST ile toplanır; WS canlılık v2'de eklenecek.)
_reg_lock = threading.Lock()
_active: set = set()   # şu an REST ile toplanan semboller
_known: set = set()    # kaynağı çözülmüş (geçerli/geçersiz) semboller

# ── Dinamik WS havuzu (v2 — eklenen sembollere canlı subscribe) ──────────────
_ws_lock = threading.Lock()
_ws_intervals: list = []           # _ws_task set eder
_bnc_conns: list = []              # [{conn_id, symbols:set, ws, lock}]
_mexc_added: set = set()           # WS thread'i açılmış MEXC sembolleri
_MAX_STREAMS_PER_CONN = 190        # Binance bağlantı başına stream tavanı (<200)

def _active_symbols():
    with _reg_lock:
        return list(_active)


async def _resolve_source(session, symbol):
    """Sembolün kaynağını çöz: Binance mı MEXC mi? Geçersizse None."""
    try:
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=1"
        async with session.get(url) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                if d:
                    return "binance"
    except Exception:
        pass
    try:
        msym = _mexc_format(symbol)
        url = f"{MEXC_FUTURES_BASE}/kline/{msym}?interval=Min1&limit=1"
        async with session.get(url) as r:
            if r.status == 200:
                raw = await r.json(content_type=None)
                if raw.get("success"):
                    return "mexc"
    except Exception:
        pass
    return None


def _control_loop(intervals, cancel):
    """Redis 'collector:symbols' set'ini izle; yeni sembolleri çöz + _active'e ekle."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _probe_add(new_syms):
        async with aiohttp.ClientSession(headers={"Accept-Encoding": "gzip, deflate"}) as session:
            for sym in new_syms:
                if cancel.is_set():
                    break
                src = await _resolve_source(session, sym)
                with _reg_lock:
                    _known.add(sym)
                    if src:
                        _active.add(sym)
                        SYMBOL_SOURCE[sym] = src
                try:
                    redis_client.hset("collector:symbol_status", sym, src or "invalid")
                except Exception:
                    pass
                # Canlılık: kaynağa göre WS'e de subscribe et (v2)
                if src == "binance":
                    _binance_add(sym, cancel)
                elif src == "mexc":
                    _mexc_add(sym, intervals, cancel)
                print(f"[PROD-CTRL] {sym} → {src or 'GEÇERSİZ'}")

    while not cancel.is_set():
        try:
            members = redis_client.smembers("collector:symbols")
            members = {m.decode() if isinstance(m, bytes) else m for m in members}
        except Exception:
            members = set()
        with _reg_lock:
            new_syms = [m for m in members if m not in _known]
        if new_syms:
            try:
                loop.run_until_complete(_probe_add(new_syms))
            except Exception as e:
                print(f"[PROD-CTRL] probe hata: {e}")
        time.sleep(3)


# ═══════════════════════════════════════════════════════════════════════════
#  BİNANCE WS  (eski _ws_on_message — artık SADECE forward eder)
# ═══════════════════════════════════════════════════════════════════════════
def _ws_on_message(ws, message):
    global _ws_msg_counter
    try:
        data = json.loads(message)
        _ws_msg_counter += 1
        inner = data['data'] if 'data' in data else data
        if inner.get('e') != 'kline':
            return
        kline = inner['k']
        # Filtre: aktif (toplanan) sembol değilse forward etme.
        if kline['s'] not in _active:
            return
        if not selected_intervals or kline['i'] not in selected_intervals:
            return
        # >>> tek iş: ham kline'ı Queue'ya koy. Hesap consumer'da.
        _put(bus.msg_bnc_kline(kline))
    except Exception as e:
        print(f"[PROD-BNC-WS] EXCEPTION: {type(e).__name__}: {e}")


def _ws_on_error(ws, error):
    print(f"[PROD-BNC-WS] HATA: {error}")


def _start_single_ws_connection(conn_rec, cancel):
    """Bir Binance bağlantısı. conn_rec['symbols'] MUTABLE — kopma/yeniden
    bağlanmada GÜNCEL set (dinamik eklenenler dahil) yeniden subscribe edilir."""
    url = "wss://fstream.binance.com/market/stream"
    cid = conn_rec["conn_id"]
    while not cancel.is_set():
        try:
            def _on_open(ws):
                with conn_rec["lock"]:
                    conn_rec["ws"] = ws
                    syms = list(conn_rec["symbols"])
                params = [f"{s.lower()}@kline_{iv}" for s in syms for iv in _ws_intervals]
                for i in range(0, len(params), 200):
                    try:
                        ws.send(json.dumps({"method": "SUBSCRIBE",
                                            "params": params[i:i + 200], "id": i // 200 + 1}))
                    except Exception:
                        break
                    time.sleep(0.3)
                print(f"[PROD-BNC-WS-{cid}] BAGLANDI — {len(syms)} sembol subscribe")

            ws = websocket_client.WebSocketApp(
                url,
                on_message=_ws_on_message,
                on_error=_ws_on_error,
                on_close=lambda ws, c, m: print(f"[PROD-BNC-WS-{cid}] KAPANDI: {c} {m}"),
                on_open=_on_open,
            )
            conn_rec["ws"] = ws
            ws.run_forever(ping_interval=30, ping_timeout=20)
            conn_rec["ws"] = None
            if not cancel.is_set():
                print(f"[PROD-BNC-WS-{cid}] Koptu, 5sn sonra yeniden...")
                time.sleep(5)
        except Exception as e:
            conn_rec["ws"] = None
            print(f"[PROD-BNC-WS-{cid}] HATA: {type(e).__name__}: {e}")
            time.sleep(5)


def _binance_add(symbol, cancel):
    """Dinamik: sembolü uygun bağlantıya canlı subscribe et; yer yoksa yeni
    bağlantı aç. Kopmada on_open zaten güncel seti yeniden subscribe eder."""
    n_iv = max(1, len(_ws_intervals))
    with _ws_lock:
        for c in _bnc_conns:
            if symbol in c["symbols"]:
                return  # zaten var
        target = None
        for c in _bnc_conns:
            if (len(c["symbols"]) + 1) * n_iv <= _MAX_STREAMS_PER_CONN:
                target = c
                break
        if target is None:
            cid = len(_bnc_conns)
            target = {"conn_id": cid, "symbols": {symbol}, "ws": None, "lock": threading.Lock()}
            _bnc_conns.append(target)
            threading.Thread(target=_start_single_ws_connection,
                             args=(target, cancel), daemon=True).start()
            print(f"[PROD-BNC-WS] yeni bağlantı {cid} + {symbol}")
            return
        target["symbols"].add(symbol)
        ws = target["ws"]
        cid = target["conn_id"]
    # Canlı bağlantıya anlık SUBSCRIBE (ws None ise on_open zaten ekleyecek)
    if ws is not None:
        try:
            params = [f"{symbol.lower()}@kline_{iv}" for iv in _ws_intervals]
            ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 99999}))
            print(f"[PROD-BNC-WS-{cid}] +{symbol} canlı subscribe")
        except Exception as e:
            print(f"[PROD-BNC-WS-{cid}] subscribe hata {symbol}: {e}")


def _mexc_add(symbol, intervals, cancel):
    """Dinamik: MEXC sembolü için yeni WS thread aç."""
    with _ws_lock:
        if symbol in _mexc_added:
            return
        _mexc_added.add(symbol)
        cid = 1000 + len(_mexc_added)
    threading.Thread(target=_start_mexc_ws_thread,
                     args=([symbol], intervals, cid, cancel), daemon=True).start()
    print(f"[PROD-MEXC-WS] +{symbol} yeni thread")


# ═══════════════════════════════════════════════════════════════════════════
#  MEXC WS  (push.deal → tick forward) + MEXC poll (REST → forward)
# ═══════════════════════════════════════════════════════════════════════════
async def _mexc_poll_loop(symbol_batch, intervals, cancel):
    """MEXC native interval'ları HTTP poll ile çek → HAM JSON'u forward et.
    DİKKAT: update_indicators ARTIK YOK — event loop CPU'suz. (eski sürüm
    burada talib çağırıp loop'u bloke ediyordu; düzeltilen ana nokta budur.)"""
    _last_poll = {}
    async with aiohttp.ClientSession(headers={"Accept-Encoding": "gzip, deflate"}) as session:
        while not cancel.is_set():
            now = time.monotonic()
            for symbol in symbol_batch:
                if symbol not in _active:
                    continue
                native_ivs = [iv for iv in intervals
                              if iv in _MEXC_INTERVAL_MAP
                              and iv not in _MEXC_DERIVED_INTERVALS
                              and (not selected_intervals or iv in selected_intervals)]
                for iv in native_ivs:
                    gap = _MEXC_POLL_INTERVALS.get(iv, 60)
                    key = (symbol, iv)
                    if now - _last_poll.get(key, 0) < gap:
                        continue
                    _last_poll[key] = now
                    mexc_sym = _mexc_format(symbol)
                    mexc_iv  = _mexc_interval(iv)
                    _limit   = _MEXC_POLL_LIMIT.get(iv, 500)
                    url = f"{MEXC_FUTURES_BASE}/kline/{mexc_sym}?interval={mexc_iv}&limit={_limit}"
                    try:
                        async with session.get(url) as response:
                            if response.status != 200:
                                continue
                            raw = await response.json(content_type=None)
                    except Exception:
                        continue
                    if not raw.get("success"):
                        _last_poll[key] = now + 300
                        continue
                    # >>> tek iş: ham data'yı forward et (parse+hesap consumer'da)
                    _put(bus.msg_mexc_rest(symbol, iv, raw.get("data", {}), poll=True))
            await asyncio.sleep(1)


async def _mexc_ws_loop(symbol_batch, intervals, conn_id, cancel):
    """MEXC WS — push.deal tick'i forward eder. push.depth şimdilik yok sayılır
    (collector ile aynı). Poll loop paralelde çalışır."""
    uri = MEXC_WS_URI
    poll_task = asyncio.ensure_future(_mexc_poll_loop(symbol_batch, intervals, cancel))
    pong_cache: dict = {}
    while not cancel.is_set():
        try:
            async with websockets.connect(
                uri, ping_interval=30, ping_timeout=20, close_timeout=5, max_size=2**20,
            ) as ws:
                print(f"[PROD-MEXC-WS-{conn_id}] Baglandi — {len(symbol_batch)} sembol")
                for sym in symbol_batch:
                    sym_f = _mexc_format(sym)
                    await ws.send(json.dumps({"method": "sub.deal",  "param": {"symbol": sym_f}}))
                    # sub.depth KALDIRILDI: depth verisi hiçbir yerde kullanılmıyordu (push.depth
                    # zaten atılıyordu). Abone olmak reconnect anında recv'i gereksiz selliyordu.
                print(f"[PROD-MEXC-WS-{conn_id}] Subscribe OK (deal)")

                async for raw in ws:
                    if cancel.is_set():
                        break
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    ping_val = data.get("ping")
                    if ping_val is not None:
                        pong_str = pong_cache.get(ping_val)
                        if pong_str is None:
                            pong_str = json.dumps({"pong": ping_val})
                            pong_cache[ping_val] = pong_str
                        try:
                            await ws.send(pong_str)
                        except Exception:
                            pass
                        continue
                    channel = data.get("channel", "")
                    sym_raw = data.get("symbol", "").replace("_", "")
                    dashboard_sym = _mexc_to_dashboard(sym_raw)
                    if dashboard_sym not in _active:
                        continue
                    if channel == "push.depth":
                        continue  # şimdilik sadece yok say (collector ile aynı)
                    if channel != "push.deal":
                        continue
                    deals = data.get("data")
                    if not deals:
                        continue
                    last = deals[-1] if isinstance(deals, list) else deals
                    try:
                        price = float(last.get("p", last.get("price", 0)))
                    except Exception:
                        continue
                    if price <= 0:
                        continue
                    # >>> tek iş: fiyat tick'ini forward et (tick→indikatör consumer'da)
                    _put(bus.msg_mexc_deal(dashboard_sym, price))
        except Exception as e:
            if not cancel.is_set():
                print(f"[PROD-MEXC-WS-{conn_id}] Hata: {e} — 3sn sonra...")
                await asyncio.sleep(3)
    poll_task.cancel()


def _start_mexc_ws_thread(symbol_batch, intervals, conn_id, cancel):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_mexc_ws_loop(symbol_batch, intervals, conn_id, cancel))
    finally:
        loop.close()


# ═══════════════════════════════════════════════════════════════════════════
#  REST — snapshot / gap recovery  (eski fetch_kline_data — artık RAW forward)
# ═══════════════════════════════════════════════════════════════════════════
async def _fetch_bnc_raw(session, symbol, interval):
    """Binance REST → ham klines list. 400 → MEXC fallback (kontrol producer'da)."""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit=500"
    try:
        async with session.get(url) as response:
            if response.status != 200:
                if response.status == 400:
                    if symbol not in _RUNTIME_MEXC_FALLBACK:
                        _RUNTIME_MEXC_FALLBACK.add(symbol)
                        SYMBOL_SOURCE[symbol] = "mexc"
                        print(f"[PROD-FALLBACK] {symbol} Binance 400 → MEXC")
                    return ("mexc", await _fetch_mexc_raw(session, symbol, interval))
                return None
            data = await response.json(content_type=None)
            return ("bnc", data) if data else None
    except Exception as e:
        print(f"[PROD-BNC-REST] {symbol}:{interval}: {e}")
        return None


async def _fetch_mexc_raw(session, symbol, interval):
    """MEXC REST → ham data dict."""
    if interval in _MEXC_SKIP_INTERVALS or interval not in _MEXC_INTERVAL_MAP:
        return None
    mexc_sym = _mexc_format(symbol)
    mexc_iv  = _mexc_interval(interval)
    url = f"{MEXC_FUTURES_BASE}/kline/{mexc_sym}?interval={mexc_iv}&limit=500"
    try:
        async with session.get(url) as response:
            if response.status != 200:
                return None
            raw = await response.json(content_type=None)
            if not raw.get("success"):
                return None
            return raw.get("data", {})
    except Exception as e:
        print(f"[PROD-MEXC-REST] {symbol}:{interval}: {e}")
        return None


async def _snapshot_fetch(symbols, intervals):
    """Tüm (symbol, interval) için REST snapshot çek → ham forward et.
    (eski fetch_and_process_data'nın SADECE fetch yarısı; hesap+Redis consumer'da.)"""
    derived_set = set(_MEXC_DERIVED_INTERVALS.keys())
    def _eff_ivs(sym):
        if SYMBOL_SOURCE.get(sym, "binance") == "mexc":
            return [iv for iv in intervals if iv not in derived_set]
        return intervals
    pairs = [(s, iv) for s in symbols for iv in _eff_ivs(s)]
    binance_pairs = [(s, i) for s, i in pairs if SYMBOL_SOURCE.get(s, "binance") == "binance"]
    mexc_pairs    = [(s, i) for s, i in pairs if SYMBOL_SOURCE.get(s, "binance") == "mexc"]

    async with aiohttp.ClientSession(headers={"Accept-Encoding": "gzip, deflate"}) as session:
        # Binance: 20 paralel
        for i in range(0, len(binance_pairs), 20):
            chunk = binance_pairs[i:i + 20]
            res = await asyncio.gather(*[_fetch_bnc_raw(session, s, iv) for s, iv in chunk],
                                       return_exceptions=True)
            for (s, iv), r in zip(chunk, res):
                if not r or isinstance(r, Exception):
                    continue
                kind, payload = r
                if not payload:
                    continue
                if kind == "bnc":
                    _put(bus.msg_bnc_rest(s, iv, payload))
                else:
                    _put(bus.msg_mexc_rest(s, iv, payload, poll=False))
            await asyncio.sleep(0.3)
        # MEXC: 3 paralel, 1sn ara
        for i in range(0, len(mexc_pairs), 3):
            chunk = mexc_pairs[i:i + 3]
            res = await asyncio.gather(*[_fetch_mexc_raw(session, s, iv) for s, iv in chunk],
                                       return_exceptions=True)
            for (s, iv), payload in zip(chunk, res):
                if not payload or isinstance(payload, Exception):
                    continue
                _put(bus.msg_mexc_rest(s, iv, payload, poll=False))
            await asyncio.sleep(1.0)


async def _fast_poll_loop(symbols, intervals, cancel):
    """fast-poll: BTCDOMUSDT gibi semboller — sadece FİYAT patch'i forward eder."""
    poll_symbols = [s for s in symbols if s in FAST_POLL_SYMBOLS]
    if not poll_symbols:
        return
    POLL_INTERVALS = {'1m', '3m', '5m', '15m'}
    print(f"[PROD-FAST-POLL] {poll_symbols}")
    while not cancel.is_set():
        try:
            async with aiohttp.ClientSession(headers={"Accept-Encoding": "gzip, deflate"}) as session:
                for symbol in poll_symbols:
                    src = SYMBOL_SOURCE.get(symbol, "binance")
                    if src == "binance":
                        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                continue
                            price = float((await resp.json(content_type=None))['price'])
                    else:
                        url = f"{MEXC_FUTURES_BASE}/ticker?symbol={_mexc_format(symbol)}"
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                continue
                            price = float(((await resp.json(content_type=None)).get("data") or {}).get("lastPrice", 0))
                            if price <= 0:
                                continue
                    for iv in [x for x in intervals if x in POLL_INTERVALS]:
                        _put(bus.msg_price_rest(symbol, iv, price))
        except Exception as e:
            print(f"[PROD-FAST-POLL] HATA: {e}")
        await asyncio.sleep(2)


# ═══════════════════════════════════════════════════════════════════════════
#  Orkestrasyon
# ═══════════════════════════════════════════════════════════════════════════
def _ws_task(symbols, intervals, cancel):
    """Binance + MEXC WS bağlantılarını başlat. Bağlantılar conn-record tabanlı
    (mutable sembol seti) → dinamik eklemeler aynı havuza katılır."""
    global _ws_intervals
    _ws_intervals = list(intervals)
    binance_syms = [s for s in symbols if SYMBOL_SOURCE.get(s, "binance") == "binance"]
    mexc_syms    = [s for s in symbols if SYMBOL_SOURCE.get(s, "binance") == "mexc"]
    print(f"[PROD-WS] Binance: {len(binance_syms)} | MEXC: {len(mexc_syms)}")

    if binance_syms:
        per_conn = max(1, _MAX_STREAMS_PER_CONN // max(1, len(intervals)))
        batches = [binance_syms[i:i + per_conn] for i in range(0, len(binance_syms), per_conn)]
        with _ws_lock:
            for cid, batch in enumerate(batches):
                rec = {"conn_id": cid, "symbols": set(batch), "ws": None, "lock": threading.Lock()}
                _bnc_conns.append(rec)
        for rec in list(_bnc_conns):
            threading.Thread(target=_start_single_ws_connection,
                             args=(rec, cancel), daemon=True).start()
            time.sleep(1)

    if mexc_syms:
        PER = 2
        batches = [mexc_syms[i:i + PER] for i in range(0, len(mexc_syms), PER)]
        with _ws_lock:
            for s in mexc_syms:
                _mexc_added.add(s)
        for i, batch in enumerate(batches):
            threading.Thread(target=_start_mexc_ws_thread,
                             args=(batch, intervals, 100 + i, cancel), daemon=True).start()
            time.sleep(0.5)

    while not cancel.is_set():
        time.sleep(1)


def _rest_task(symbols, intervals, cancel):
    """İlk snapshot + periyodik refresh. Dinamik _active listesini kullanır →
    kullanıcı sonradan eklediği semboller otomatik snapshot'a girer."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        short_ivs = [iv for iv in intervals if iv in ["1m","3m","5m","15m","30m","1h","4h"]]
        _syms = _active_symbols() or list(symbols)
        print(f"[PROD-REST] Snapshot aşama 1: {len(_syms)} x {len(short_ivs)}")
        loop.run_until_complete(_snapshot_fetch(_syms, short_ivs))
        long_ivs = [iv for iv in intervals if iv not in short_ivs]
        if long_ivs:
            print(f"[PROD-REST] Snapshot aşama 2: {len(long_ivs)} uzun interval")
            loop.run_until_complete(_snapshot_fetch(_syms, long_ivs))
    except Exception as e:
        print(f"[PROD-REST] İlk snapshot hatası: {e}")

    # ── Interval'a göre tazeleme hızı ──────────────────────────────────────────
    _REFRESH_CADENCE = {
        '1m': 5, '3m': 8, '5m': 12, '15m': 25, '30m': 40,
        '1h': 60, '2h': 90, '4h': 120, '6h': 180,
        '8h': 180, '12h': 240, '1d': 300, '3d': 300,
    }
    _last_refresh = {iv: 0.0 for iv in intervals}
    while not cancel.is_set():
        time.sleep(2)
        now = time.monotonic()
        due = [iv for iv in intervals
               if now - _last_refresh.get(iv, 0.0) >= _REFRESH_CADENCE.get(iv, 60)]
        if not due:
            continue
        try:
            loop.run_until_complete(_snapshot_fetch(_active_symbols(), due))
            for iv in due:
                _last_refresh[iv] = now
        except Exception as e:
            print(f"[PROD-REST] Refresh hatası: {e}")


def _fast_poll_thread(symbols, intervals, cancel):
    if not any(s in FAST_POLL_SYMBOLS for s in symbols):
        return
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_fast_poll_loop(symbols, intervals, cancel))


def run_producer(q, symbols: List[str], intervals: List[str]):
    """Producer process girişi. q = bus RAW Queue. Bloke eder (Ctrl+C ile çıkar)."""
    global _Q, selected_symbols, selected_intervals
    _Q = q
    selected_symbols   = list(symbols)
    selected_intervals = list(intervals)

    cancel = threading.Event()
    print(f"[PRODUCER] Başlıyor — {len(symbols)} sembol x {len(intervals)} interval")

    # Base sembolleri dinamik kayda seed et (kaynakları shared.SYMBOL_SOURCE'ta hazır)
    with _reg_lock:
        for s in symbols:
            _active.add(s); _known.add(s)
    try:
        if symbols:
            redis_client.sadd("collector:symbols", *symbols)
    except Exception as e:
        print(f"[PRODUCER] seed sinyali: {e}")

    # CPU/RAM + enqueue hızı monitörü
    def _monitor():
        import psutil
        proc = psutil.Process()
        proc.cpu_percent(None)
        last_n, last_t = _enq_count, time.monotonic()
        while not cancel.is_set():
            time.sleep(10)
            now = time.monotonic()
            rate = (_enq_count - last_n) / max(1e-6, now - last_t)
            last_n, last_t = _enq_count, now
            cpu = proc.cpu_percent(None)
            ram = proc.memory_info().rss / (1024 * 1024)
            drop_s = f" | DROP {_drop_count}" if _drop_count else ""
            print(f"[PRODUCER] CPU {cpu:4.1f}% | RAM {ram:6.1f} MB | "
                  f"{rate:6.1f} enq/s | toplam {_enq_count}{drop_s}")
    threading.Thread(target=_monitor, daemon=True).start()

    threading.Thread(target=_rest_task,       args=(symbols, intervals, cancel), daemon=True).start()
    threading.Thread(target=_ws_task,         args=(symbols, intervals, cancel), daemon=True).start()
    threading.Thread(target=_fast_poll_thread,args=(symbols, intervals, cancel), daemon=True).start()
    threading.Thread(target=_control_loop,    args=(intervals, cancel), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[PRODUCER] Durduruluyor...")
        cancel.set()