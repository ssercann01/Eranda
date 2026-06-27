"""
ERANDA V2 — bus.py  (Katman 2 — RAW Queue)
================================================================
Producer ↔ Consumer arasındaki TEK sınır. multiprocessing.Queue.
Producer sadece buraya ham mesaj koyar (put), Consumer buradan alır (get).
Birbirlerini bloke etmezler.

Mesaj şeması (hepsi pickle'lanabilir saf dict — DataFrame TAŞINMAZ):

  {"t": "bnc_kline",  "k": <binance kline 'k' dict>}        # Binance WS canlı mum
  {"t": "mexc_deal",  "symbol": str, "price": float}        # MEXC WS trade tick
  {"t": "mexc_kline", "symbol": str, "interval": str,
                       "kd": dict}                           # MEXC push.kline (opsiyonel)
  {"t": "bnc_rest",   "symbol": str, "interval": str,
                       "data": <ham binance klines list>}    # REST snapshot/gap
  {"t": "mexc_rest",  "symbol": str, "interval": str,
                       "data": <ham mexc data dict>,
                       "poll": bool}                          # REST snapshot/poll
  {"t": "price_rest", "symbol": str, "interval": str,
                       "price": float}                        # fast-poll fiyat patch

Not: Ham JSON taşınır, "Parse + merge" Katman 3'te (consumer) yapılır —
diyagramdaki sorumluluk dağılımı budur.
"""

import multiprocessing as _mp
import threading
import pandas as pd

# macOS POSIX semafor üst sınırı (SEM_VALUE_MAX) = 32767.
# multiprocessing.Queue(maxsize=N) içeride N değerli bir semafor kurar;
# N bu sınırı aşarsa "OSError: [Errno 22] Invalid argument" alınır.
_SAFE_QUEUE_MAX = 30_000


# ── RAW Queue fabrikası ──────────────────────────────────────────────────────
# maxsize: producer consumer'dan hızlı üretirse geri-basınç (backpressure) sağlar.
# 0 = sınırsız; >0 ise macOS koruması için 30000 ile sınırlanır.
def make_raw_queue(maxsize: int = _SAFE_QUEUE_MAX, ctx=None) -> "_mp.Queue":
    if maxsize and maxsize > _SAFE_QUEUE_MAX:
        maxsize = _SAFE_QUEUE_MAX  # macOS EINVAL koruması
    if ctx is not None:
        return ctx.Queue(maxsize=maxsize)
    return _mp.Queue(maxsize=maxsize)


# ── Mesaj kurucular (producer kullanır) ──────────────────────────────────────
def msg_bnc_kline(k: dict) -> dict:
    return {"t": "bnc_kline", "k": k}

def msg_mexc_deal(symbol: str, price: float) -> dict:
    return {"t": "mexc_deal", "symbol": symbol, "price": price}

def msg_mexc_kline(symbol: str, interval: str, kd: dict) -> dict:
    return {"t": "mexc_kline", "symbol": symbol, "interval": interval, "kd": kd}

def msg_bnc_rest(symbol: str, interval: str, data) -> dict:
    return {"t": "bnc_rest", "symbol": symbol, "interval": interval, "data": data}

def msg_mexc_rest(symbol: str, interval: str, data, poll: bool = False) -> dict:
    return {"t": "mexc_rest", "symbol": symbol, "interval": interval,
            "data": data, "poll": poll}

def msg_price_rest(symbol: str, interval: str, price: float) -> dict:
    return {"t": "price_rest", "symbol": symbol, "interval": interval, "price": price}


# ── KNS preset kombinasyonları ───────────────────────────────────────────────
# Consumer her preset'i hesaplar ve AYRI kolonlara yazar (KNS_<AD>_*).
# Dashboard kullanıcı seçimini bir preset'e eşleyip o kolonları okur.
# Düzenlemek için TEK yer burası → consumer & dashboard otomatik senkron.
#
# DİKKAT (Redis genişliği): her preset 6 kolon ekler. Çok preset = geniş df.
# 600MB bütçesini korumak için preset sayısını makul tut (öneri ≤4).
KNS_PRESETS = {
    "DEFAULT":  ["VOL", "CVD", "VWAP"],
    "MOMENTUM": ["RSI", "MFI", "SMI", "CCI", "WIL"],
    "VOLUME":   ["VOL", "CVD", "OBV", "CMF", "ADO"],
    "TREND":    ["TRD", "ADX", "+DI", "EMA", "MCD"],
}

def kns_col_prefix(name: str) -> str:
    return f"KNS_{name}"

def match_kns_preset(selected_inds) -> str:
    """Kullanıcının KNS seçimini bir preset ADINA eşle.
    Tam küme eşleşmesi yoksa 'DEFAULT' döner (dashboard hesap yapmaz)."""
    sel = frozenset(selected_inds or [])
    for name, inds in KNS_PRESETS.items():
        if frozenset(inds) == sel:
            return name
    return "DEFAULT"


# ── Canlı KNS konsensüs (talib YOK — sadece son-satır kolonlarını oylar) ──────
# Dashboard kullanıcının seçtiği HERHANGİ bir KNS kombinasyonu için bunu canlı
# çağırır → dropdown'a indikatör ekleyince anında tepki verir. Ucuz (sadece
# sözlük okuma + eşik). Gövde collector calculate_consensus_signal/_indicator_vote
# ile aynı mantık; df yerine doğrudan last_row dict alır.
INDICATOR_WEIGHTS = {
    'TRD': 2.0, 'ADX': 1.0, 'MCD': 1.5, 'EMA': 1.0,
    'SMA_20': 0.5, 'SMA_50': 0.5, 'SMA_200': 0.5,
    'RSI': 1.5, 'SMI': 1.5, 'WIL': 1.0, 'CCI': 1.0, 'RSM': 1.0, 'TRP': 1.0,
    'MFI': 1.5, 'CMF': 1.5, 'ADO': 1.0, 'OBV': 0.8, 'CVD': 1.8,
    'VOL': 2.0, 'VWAP': 1.5, 'VRT': 0.8, 'FRC': 0.8, 'BOL': 1.0,
}


def _kns_vote(indicator: str, last_row: dict):
    w = INDICATOR_WEIGHTS.get(indicator, 1.0)
    g = last_row.get
    try:
        if indicator == 'RSI':
            v = g('RSI', 50);  return (1, w) if v > 55 else (-1, w) if v < 45 else (0, w)
        if indicator == 'MFI':
            v = g('MFI', 50);  return (1, w) if v > 55 else (-1, w) if v < 45 else (0, w)
        if indicator == 'RSM':
            v = g('RSM', 50);  return (1, w) if v > 55 else (-1, w) if v < 45 else (0, w)
        if indicator == 'SMI':
            v = g('SMI', 0);   return (1, w) if v > 20 else (-1, w) if v < -20 else (0, w)
        if indicator == 'WIL':
            v = g('WIL', -50); return (1, w) if v > -30 else (-1, w) if v < -80 else (0, w)
        if indicator == 'CCI':
            v = g('CCI', 0);   return (1, w) if v > 100 else (-1, w) if v < -100 else (0, w)
        if indicator == 'CMF':
            v = g('CMF', 0);   return (1, w) if v > 0.05 else (-1, w) if v < -0.05 else (0, w)
        if indicator == 'ADO':
            v = g('ADO', 0);   return (1, w) if v > 0 else (-1, w) if v < 0 else (0, w)
        if indicator == 'TRP':
            v = g('TRP', 0);   return (1, w) if v > 0 else (-1, w) if v < 0 else (0, w)
        if indicator == 'FRC':
            v = g('FRC', 0);   return (1, w) if v > 0 else (-1, w) if v < 0 else (0, w)
        if indicator == 'CVD':
            mom, slope = g('CVD_Mom'), g('CVD_Slope')
            try:
                mom = float(mom) if mom is not None and not pd.isna(mom) else None
                slope = float(slope) if slope is not None and not pd.isna(slope) else None
            except (TypeError, ValueError):
                mom = slope = None
            if mom is not None and slope is not None:
                if mom > 0 and slope > 0: return 1, w
                if mom < 0 and slope < 0: return -1, w
                if mom > 0: return 0.5, w
                if mom < 0: return -0.5, w
            elif mom is not None:
                return (1, w) if mom > 0 else (-1, w) if mom < 0 else (0, w)
            return 0, w
        if indicator == 'VOL':
            z = g('Net_Volume_Z')
            try:
                z = float(z) if z is not None and not pd.isna(z) else None
            except (TypeError, ValueError):
                z = None
            if z is not None:
                if z >= 2.0: return 1, w
                if z <= -2.0: return -1, w
                if z >= 0.5: return 0.5, w
                if z <= -0.5: return -0.5, w
                return 0, w
            v = g('Net_Volume', 0)
            return (1, w) if v > 0 else (-1, w) if v < 0 else (0, w)
        if indicator == 'VWAP':
            close, vwap = g('close', 0), g('VWAP', 0)
            if close > vwap * 1.001: return 1, w
            if close < vwap * 0.999: return -1, w
            return 0, w
        if indicator == 'TRD':
            sig = g('COMBINED_TREND_SIGNAL', 'WAIT'); adx = g('TRD_ADX', 0)
            ww = w * (1.5 if adx >= 40 else 1.0)
            if sig == 'BUY': return 1, ww
            if sig == 'SELL': return -1, ww
            return 0, w
        if indicator == 'ADX':
            adx, p, m = g('ADX', 0), g('+DI', 0), g('-DI', 0)
            if adx >= 25:
                if p > m: return 1, w
                if m > p: return -1, w
            return 0, w
        if indicator == 'MCD':
            h = g('MCD_Hist', 0); return (1, w) if h > 0 else (-1, w) if h < 0 else (0, w)
        if indicator == 'EMA':
            s = g('EMA_Signal', 0); return (1, w) if s > 0.1 else (-1, w) if s < -0.1 else (0, w)
        if indicator in ('SMA_20', 'SMA_50', 'SMA_200'):
            close, sma = g('close', 0), g(indicator, 0)
            if close > sma * 1.001: return 1, w
            if close < sma * 0.999: return -1, w
            return 0, w
        if indicator == 'BOL':
            close, mid = g('close', 0), g('BB_Middle', 0)
            return (1, w) if close > mid else (-1, w) if close < mid else (0, w)
        if indicator == 'VRT':
            p, m = g('VI+', 1), g('VI-', 1)
            if p > m * 1.05: return 1, w
            if m > p * 1.05: return -1, w
            return 0, w
        if indicator == 'OBV':
            return 0, w
    except Exception:
        pass
    return 0, w


def consensus_from_row(last_row: dict, indicators) -> dict:
    """last_row (df.iloc[-1].to_dict()) + seçili indikatörler → KNS sonucu.
    Döndürür: signal, score, strength, votes_buy/sell/neutral."""
    if not last_row or not indicators:
        return {'signal': 'NO_DATA', 'score': 0, 'strength': 0,
                'votes_buy': 0, 'votes_sell': 0, 'votes_neutral': 0}
    try:
        last_row = dict(last_row)
        last_row['close'] = float(last_row.get('close', 0) or 0)
    except (TypeError, ValueError):
        pass
    _SKIP = {'DIV', 'DIVERGENCE', 'TPD', 'Fibonacci',
             'TPD_SIGNAL', 'TPD_RELIABILITY', 'TPD_MOMENTUM', 'TPD_RISK',
             'TPD_CONFLUENCE', 'TPD_DIVERGENCE', 'TPD_STRENGTH', 'TPD_TREND'}
    tw = sc = 0.0
    vb = vs = vn = 0
    for ind in indicators:
        if ind in _SKIP:
            continue
        vote, weight = _kns_vote(ind, last_row)
        sc += vote * weight
        tw += weight
        if vote > 0: vb += 1
        elif vote < 0: vs += 1
        else: vn += 1
    if tw == 0:
        return {'signal': 'NO_DATA', 'score': 0, 'strength': 0,
                'votes_buy': 0, 'votes_sell': 0, 'votes_neutral': 0}
    norm = sc / tw
    if norm >= 0.35:   signal = 'STRONG_BUY'
    elif norm >= 0.15: signal = 'BUY'
    elif norm <= -0.35: signal = 'STRONG_SELL'
    elif norm <= -0.15: signal = 'SELL'
    else:               signal = 'NEUTRAL'
    return {'signal': signal, 'score': round(norm, 3), 'strength': int(abs(norm) * 100),
            'votes_buy': vb, 'votes_sell': vs, 'votes_neutral': vn}


# ── Sembol çıkarımı + shard yönlendirme ──────────────────────────────────────
def msg_symbol(msg: dict) -> str:
    """Mesajın ait olduğu sembolü döndür (shard routing için)."""
    if msg.get("t") == "bnc_kline":
        return msg["k"].get("s", "")
    return msg.get("symbol", "")


class ShardRouter:
    """Çok-consumer ölçeklemede sembolü DAİMA aynı consumer'a yönlendirir.
    Böylece bir sembolün incremental WS güncellemeleri tek bir consumer'ın
    in-memory cache'inde tutarlı kalır. N=1 iken tek queue gibi davranır.

    Producer tarafında tek bir `.put_nowait(msg)` arayüzü sunar — producer
    kodu queue sayısından habersizdir.
    """
    def __init__(self, queues, counters=None):
        self.queues = list(queues)
        self.n = len(self.queues)
        self.counters = list(counters) if counters else None
        # Sembol→shard SABIT atama (cache tutarlılığı). Hash yerine ilk-görülme
        # sırasına göre round-robin → semboller eşit dağılır VE liste başındaki
        # yüksek-hacimli coinler (BTC/ETH/SOL...) farklı consumer'lara serpilir
        # (tek shard'a yığılmaz → CPU dengeli).
        self._sym_shard = {}
        self._rr = 0
        self._assign_lock = threading.Lock()

    # Lock pickle'lanamaz → process'e geçerken düşür, karşı tarafta yeniden kur.
    def __getstate__(self):
        state = self.__dict__.copy()
        state['_assign_lock'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._assign_lock = threading.Lock()

    def _shard_for(self, sym: str) -> int:
        idx = self._sym_shard.get(sym)
        if idx is None:
            with self._assign_lock:
                idx = self._sym_shard.get(sym)
                if idx is None:
                    idx = self._rr % self.n
                    self._sym_shard[sym] = idx
                    self._rr += 1
        return idx

    def _bump(self, idx):
        if self.counters is not None:
            c = self.counters[idx]
            try:
                with c.get_lock():
                    c.value += 1
            except Exception:
                pass

    def put_nowait(self, msg: dict):
        if self.n == 1:
            self.queues[0].put_nowait(msg)
            self._bump(0)
            return
        sym = msg_symbol(msg)
        idx = self._shard_for(sym) if sym else 0
        self.queues[idx].put_nowait(msg)
        self._bump(idx)


# ── Ortak util (collector.py timestamp_to_datetime — BİREBİR) ─────────────────
def timestamp_to_datetime(timestamp):
    if isinstance(timestamp, (int, float)):
        return pd.to_datetime(timestamp, unit='ms')
    elif isinstance(timestamp, str):
        return pd.to_datetime(timestamp)
    elif isinstance(timestamp, pd.Timestamp):
        return timestamp
    else:
        raise ValueError(f"Unexpected timestamp format: {type(timestamp)}")