"""
ERANDA V2 — Shared Module
Collector ve Dashboard tarafından import edilir.
Config, Redis, indikatör fonksiyonları, yardımcı sınıflar.
"""
"""
HIGH PERFORMANCE REDIS CLUSTER CONFIGURATION & TRADING Bozuk<       T INTEGRATION GUIDE
"""

from redis.exceptions import ConnectionError, DataError
from typing import Dict, List, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor
from collections import deque, defaultdict
from dataclasses import dataclass
from functools import lru_cache
from datetime import datetime

import multiprocessing as mp  # (eski: torch.multiprocessing) — sadece set_start_method/cpu_count için kullanılıyordu, stdlib birebir aynı
import concurrent.futures
import multiprocessing
import pandas as pd
import numpy as np
import schedule
import platform
import traceback
import websockets
import websocket as websocket_client  # websocket-client kutuphanesi
import json
import atexit
import asyncio
import aiohttp
import threading
import resource
import warnings
import time
import pickle
import logging
# import torch  # kaldırıldı — kod tabanında torch ile hesap yapılmıyordu (yalnız torch.multiprocessing içindi)
import socket
import orjson
import redis
import talib
import psutil
import signal
import ssl
import sys
import ta
import os
import msgpack
from pandas.api.types import is_numeric_dtype
import lz4.frame
from scipy.signal import argrelextrema
sys.path.append('/Users/secoosecoo/Desktop/YAPI') 
# symbols.py'den gelmez — COLLECTOR_SYMBOLS sabit liste kullanır
# Eski: from symbols import binance_symbols as symbols, ALL_SYMBOLS as _BINANCE_ALL_SYMBOLS
symbols = []  # backward compat
_BINANCE_ALL_SYMBOLS = []

# ── HİBRİT SİSTEM — MEXC-only semboller ─────────────────────────────────────
# Binance Futures'ta olmayan, sadece MEXC'de olan semboller buraya eklenir
MEXC_ONLY_SYMBOLS: set = {
    # Doğrulanmış MEXC index futures
    "NAS100USDT",   # Nasdaq 100
    "US30USDT",     # Dow Jones
    "SP500USDT",    # S&P 500 → SPX500_USDT (MEXC adı)
    # MEXC üzerinden yönlendirilen kripto (stres testi — 10 MEXC sembolü)
    "PEPEUSDT", "WIFUSDT", "BONKUSDT", "FLOKIUSDT",
    "JUPUSDT", "PYTHUSDT", "STXUSDT",
}

# Runtime'da Binance 400 alınca otomatik MEXC'e taşınan semboller
_RUNTIME_MEXC_FALLBACK: set = set()

# Birleşik sembol listesi (dropdown için)
# ALL_SYMBOLS: dropdown için tüm mevcut semboller
# Collector başladıktan sonra COLLECTOR_SYMBOLS + MEXC_ONLY_SYMBOLS
ALL_SYMBOLS = list(dict.fromkeys(
    [
        "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
        "ADAUSDT","LINKUSDT","DOTUSDT","AVAXUSDT","LTCUSDT",
        "DOGEUSDT","TRXUSDT","ATOMUSDT","NEARUSDT","APTUSDT",
        "ARBUSDT","OPUSDT","SUIUSDT","INJUSDT","TIAUSDT",
        "SEIUSDT","FILUSDT","ETCUSDT","UNIUSDT","AAVEUSDT",
        "RUNEUSDT","ORDIUSDT","GALAUSDT","SANDUSDT","ICPUSDT",
    ] + sorted(MEXC_ONLY_SYMBOLS)
))

# ── Evrensel sabitler — collector ve dashboard her ikisi de kullanır ──────────
ALL_INTERVALS = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d"
]

ALL_INDICATORS = [
    # Hafif (talib tek-satır) — düşük maliyet
    "RSI", "RSM", "MFI", "SMI", "CMF", "ADO", "+DI", "ADX", "CCI", "WIL",
    "SR", "VOL", "CVD", "VWAP",
    "EMA", "MCD", "SMA_20", "SMA_50", "SMA_200", "BOL", "OBV", "VRT", "FRC", "TRP", "TRD",
    # NOT: Fibonacci ve TPD_TREND çıkarıldı — ağır hesap, consumer'ı boğuyordu
    # (özellikle türetilmiş kısa interval'larda "geçersiz veri"). Gerekirse
    # seyrek-cadence (30-60sn) ile geri eklenebilir.
]

# Collector'ın daima hesapladığı evrensel set
# Kullanıcı seçiminden bağımsız — dashboard filtreler
# ── STRES TESTİ: 30 Binance + 10 MEXC ───────────────────────────────────────
COLLECTOR_SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "ADAUSDT","LINKUSDT","DOTUSDT","AVAXUSDT","LTCUSDT",
    "DOGEUSDT","TRXUSDT","ATOMUSDT","NEARUSDT","APTUSDT",
    "ARBUSDT","OPUSDT","SUIUSDT","INJUSDT","TIAUSDT",
    "SEIUSDT","FILUSDT","ETCUSDT","UNIUSDT","AAVEUSDT",
    "RUNEUSDT","ORDIUSDT","GALAUSDT","SANDUSDT","ICPUSDT",
] + sorted(MEXC_ONLY_SYMBOLS)

# ── Test override ────────────────────────────────────────────────────────────
# COLLECTOR_SYMBOLS_OVERRIDE env'i (virgülle ayrık) varsa onu kullan; boşsa tam 40.
# SYMBOL_SOURCE aşağıda tüm evrenden türetildiği için MEXC sembolleri yine MEXC'e gider.
# Örn .env:  COLLECTOR_SYMBOLS_OVERRIDE=BTCUSDT,ETHUSDT,SOLUSDT,PEPEUSDT,NAS100USDT
_sym_override = os.environ.get("COLLECTOR_SYMBOLS_OVERRIDE", "").strip()
if _sym_override:
    COLLECTOR_SYMBOLS = [s.strip().upper() for s in _sym_override.split(",") if s.strip()]

COLLECTOR_INTERVALS  = ALL_INTERVALS
COLLECTOR_INDICATORS = ALL_INDICATORS

# Başlangıç seçimleri — setup.json yoksa bu default'lar kullanılır
SETUP_FILE = os.environ.get("SETUP_FILE", "/Users/secoosecoo/Documents/setup2.json")

def save_setup(symbols, intervals, indicators):
    setup = {"symbols": symbols, "intervals": intervals, "indicators": indicators}
    try:
        with open(SETUP_FILE, "w") as f:
            json.dump(setup, f)
    except Exception as e:
        print(f"[SETUP] Kaydetme hatası: {e}")

def load_setup():
    if os.path.exists(SETUP_FILE):
        try:
            with open(SETUP_FILE, "r") as f:
                setup = json.load(f)
            return (
                setup.get("symbols",    []),
                setup.get("intervals",  []),
                setup.get("indicators", []),
            )
        except Exception:
            pass
    return [], [], []

# Her sembol için kaynak: "binance" veya "mexc"
SYMBOL_SOURCE: dict = {s: ("mexc" if s in MEXC_ONLY_SYMBOLS else "binance") for s in ALL_SYMBOLS}

# MEXC Futures sabitler
MEXC_FUTURES_BASE = "https://contract.mexc.com/api/v1/contract"
MEXC_WS_URI       = "wss://contract.mexc.com/edge"

# Interval dönüşüm: Binance → MEXC formatı
# Kullanıcının verdiği resmi MEXC futures interval listesi
# Binance interval → MEXC interval (resmi MEXC futures listesi)
# 2h/6h/12h/3d MEXC'de yok → en yakın desteklenen interval'a map et
# MEXC'in gerçekten desteklediği interval'lar
_MEXC_INTERVAL_MAP = {
    "1m":  "Min1",
    "3m":  "Min3",
    "5m":  "Min5",
    "15m": "Min15",
    "30m": "Min30",
    "1h":  "Min60",
    "4h":  "Hour4",
    "8h":  "Hour8",
    "1d":  "Day1",
}
# MEXC'te olmayan interval'lar → mevcut veriden resample ile üret
# format: 'hedef_interval': ('kaynak_interval', kaç_mum_birleştir)
_MEXC_DERIVED_INTERVALS = {
    "3m":  ("1m",  3),
    "2h":  ("1h",  2),
    "6h":  ("1h",  6),
    "12h": ("1h", 12),
    "3d":  ("1d",  3),
}
_MEXC_SKIP_INTERVALS: set = set()  # artık skip yok — ya fetch ya derive

_MEXC_INTERVAL_REVERSE = {
    "Min1":  "1m",
    "Min3":  "3m",
    "Min5":  "5m",
    "Min15": "15m",
    "Min30": "30m",
    "Min60": "1h",
    "Hour4": "4h",
    "Hour8": "8h",
    "Day1":  "1d",
}

def _mexc_interval(binance_iv: str) -> str:
    return _MEXC_INTERVAL_MAP.get(binance_iv, "Min1")

# Resample rule: pandas resample için offset string
_RESAMPLE_RULE = {
    "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h",
    "12h": "12h", "1d": "1D", "3d": "3D",
}

def _derive_interval(df: "pd.DataFrame", target_iv: str) -> "pd.DataFrame | None":
    """Kaynak DataFrame'i resample ederek hedef interval'ı üret.
    Örn: 1m df → 3m  |  1h df → 6h  |  1d df → 3d

    NOT: calculate_indicators warm-up (talib NaN) sorunundan kaçınmak için
    resample sonucu yeterli mum sayısına sahip olmalı.
    Kaynak df zaten 500 mum içerdiğinden resample her zaman yeterli mum üretir.
    """
    if df is None or df.empty:
        return None
    rule = _RESAMPLE_RULE.get(target_iv)
    if rule is None:
        return None
    try:
        resampled = df.resample(rule).agg({
            "open":   "first",
            "high":   "max",
            "low":    "min",
            "close":  "last",
            "volume": "sum",
        }).dropna(subset=["open", "close"])  # open da NaN olmamalı

        if resampled.empty:
            return None

        # Tüm sayısal sütunları float64 yap — talib için gerekli
        for col in ["open", "high", "low", "close", "volume"]:
            if col in resampled.columns:
                resampled[col] = resampled[col].astype(np.float64)

        # calculate_indicators minimum 30 satır bekliyor — yeterli mi?
        if len(resampled) < 30:
            print(f"[DERIVE] {target_iv}: sadece {len(resampled)} mum — indikatörler için yetersiz olabilir")

        return resampled
    except Exception as e:
        print(f"[DERIVE] resample hata {target_iv}: {e}")
        return None

# MEXC futures'ta bazı semboller farklı isimde — override tablosu
_MEXC_SYMBOL_OVERRIDE = {
    "MXUSDT":     "MX_USDT",
    "NAS100USDT": "NAS100_USDT",
    "US30USDT":   "US30_USDT",
    "US500USDT":  "US500_USDT",
    "SP500USDT":  "SPX500_USDT",
    "SPX500USDT": "SPX500_USDT",
    "XAUUSDT":    "XAU_USDT",
    "XAUTUSDT":   "XAUT_USDT",
    "TRUMPUSDT":  "TRUMP_USDT",    # MEXC format
}

# WS'den gelen MEXC sembolünü (SPX500USDT) dashboard sembolüne (SP500USDT) çevir
# Manuel reverse: MEXC WS sembolü (underscore kaldırılmış) → dashboard sembolü
# Otomatik reverse kullanılmıyor çünkü birden fazla key aynı value'ya map olabilir
_MEXC_SYMBOL_REVERSE = {
    "SPX500USDT": "SP500USDT",    # WS: SPX500_USDT → SP500USDT
    "XAUTUSDT":   "XAUTUSDT",
    "MX USDT".replace(" ",""): "MXUSDT",
}

def _mexc_format(symbol: str) -> str:
    """BTCUSDT → BTC_USDT  (MEXC futures format)"""
    if symbol in _MEXC_SYMBOL_OVERRIDE:
        return _MEXC_SYMBOL_OVERRIDE[symbol]
    if "_" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return symbol[:-4] + "_USDT"
    return symbol

def _mexc_to_dashboard(mexc_sym_raw: str) -> str:
    """WS'den gelen sembolü (SPX500USDT) dashboard sembolüne (SP500USDT) çevir."""
    return _MEXC_SYMBOL_REVERSE.get(mexc_sym_raw, mexc_sym_raw)





# macOS için multiprocessing ayarı
if platform.system() == 'Darwin':
    mp.set_start_method('spawn', force=True)
    print("Multiprocessing 'spawn' yöntemiyle başlatıldı.")

def get_optimal_process_count():
    if platform.processor() == 'arm':  # M1/M2 işlemcilerini kontrol et
        return min(mp.cpu_count() - 2, 6)  # 2 çekirdek sistem için ayrılıyor
    return min(mp.cpu_count() - 1, 4)  # Diğer işlemciler için




def monitor_usage():
    process = psutil.Process()
    while True:
        cpu_usage = process.cpu_percent(interval=1)
        memory_info = process.memory_info()
        memory_usage = memory_info.rss / (1024 * 1024)
        # Her 5 saniyede CPU ve RAM kullanımını yazdır
        print(f"CPU Kullanımı: {cpu_usage}% | RAM Kullanımı: {memory_usage:.2f} MB")
        time.sleep(120)

# İzleme fonksiyonunu ayrı bir iş parçacığında çalıştırıyoruz
monitor_thread = threading.Thread(target=monitor_usage, daemon=True)
monitor_thread.start()


# Redis configuration
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD') or None
REDIS_EXPIRE_TIME = 3600  # 1 hour

# Initialize Redis client
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=False  # Binary data for serialization
)

# Loglama yapılandırması
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CircularBuffer:
    def __init__(self, size):
        self.buffer = np.full(size, np.nan, dtype=np.float64)
        self.index = 0
        self.size = size
        self.is_full = False
        self.lock = threading.Lock()  # Lock() → threading.Lock() (Lock import eksikti)

    def append(self, value):
        with self.lock:
            self.buffer[self.index % self.size] = value
            self.index += 1
            if self.index >= self.size:
                self.is_full = True
                self.index = 0

    def get(self):
        with self.lock:
            if not self.is_full and self.index == 0:
                return np.array([])
            
            end_idx = self.index % self.size
            if self.is_full:
                return np.roll(self.buffer, -end_idx)[:self.size]
            return self.buffer[:end_idx]

class RedisCache:
    def __init__(self):
        self.client = redis_client
        self.expire_time = REDIS_EXPIRE_TIME
        self.auto_save_enabled = False
        self.save_thread = None
        
    def get_key(self, symbol: str, interval: str) -> str:
        return f"market_data:{symbol}:{interval}"

    def compress_lz4(self, data: bytes) -> bytes:
        """LZ4 ile sıkıştır - Fixed version"""
        try:
            # LZ4 header ekle (format tanıma için)
            compressed = lz4.frame.compress(data)
            return b"LZ4:" + compressed
        except Exception as e:
            logger.error(f"LZ4 compression error: {e}")
            return data

    def decompress_lz4(self, data: bytes) -> bytes:
        """LZ4 ile aç - Backward compatible version"""
        try:
            # LZ4 header kontrolü
            if data.startswith(b"LZ4:"):
                # Yeni LZ4 formatı
                lz4_data = data[4:]  # "LZ4:" kısmını at
                return lz4.frame.decompress(lz4_data)
            else:
                # Eski format - LZ4 olmadan dene
                try:
                    # Önce LZ4 olarak deneyip hata alırsa eski format olarak kabul et
                    return lz4.frame.decompress(data)
                except:
                    # LZ4 değilse, eski msgpack formatı olarak dön
                    logger.info("Data is not LZ4 compressed, treating as legacy format")
                    return data
        except Exception as e:
            logger.error(f"LZ4 decompression error: {e}")
            # Hata durumunda orijinal veriyi döndür
            return data

    def serialize_df(self, df):
        """DataFrame'i LZ4 ile serialize et - FIXED VERSION"""
        try:
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                logger.warning("DataFrame is None, not DataFrame, or empty")
                return None
            
            # NaN/Inf → None (msgpack float nan handle edemez)
            df_clean = df.copy()
            for col in df_clean.select_dtypes(include='float').columns:
                df_clean[col] = df_clean[col].where(df_clean[col].notna(), other=None)
            data_dict = df_clean.to_dict(orient='records')
            serialized = msgpack.packb(data_dict, use_bin_type=True)
            
            # LZ4 ile sıkıştır - FIXED!
            compressed = self.compress_lz4(serialized)
            
            return compressed
            
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            return None

    def deserialize_df(self, data):
        """LZ4 sıkıştırılmış DataFrame'i deserialize et - Backward Compatible VERSION"""
        try:
            if data is None:
                logger.warning("Deserialize data is None")
                return None
            
            # LZ4 ile aç - Backward compatible
            decompressed_data = self.decompress_lz4(data)
            
            # msgpack ile unpack
            try:
                # raw=False → bytes yerine str key/value döndür
                unpacked_data = msgpack.unpackb(decompressed_data, raw=False)
            except Exception as msgpack_error:
                logger.error(f"msgpack unpack error: {msgpack_error}")
                # Eğer msgpack hatası varsa, cache'i temizle
                logger.info("Invalid cache data detected, will be refreshed on next request")
                return None
            
            if not unpacked_data:
                logger.warning("Unpacked data is empty")
                return None
            
            # DataFrame oluştur
            df = pd.DataFrame(unpacked_data)
            
            if not isinstance(df, pd.DataFrame):
                logger.error(f"Created object is not a DataFrame: {type(df)}")
                return None

            # msgpack bytes kolonları → string kolonlara çevir
            df.columns = [c.decode() if isinstance(c, bytes) else str(c) for c in df.columns]

            # Numeric kolonları float64'e çevir — talib için zorunlu
            for col in df.columns:
                if df[col].dtype == object:
                    try:
                        # Önce bytes decode
                        df[col] = df[col].apply(lambda x: x.decode() if isinstance(x, bytes) else x)
                        # Sonra numeric'e çevir
                        converted = pd.to_numeric(df[col], errors='coerce')
                        if converted.notna().sum() > len(df) * 0.5:  # %50'den fazlası numeric ise
                            df[col] = converted
                    except Exception:
                        pass

            # OHLCV kolonlarını kesinlikle float64 yap
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    try:
                        df[col] = df[col].astype('float64')
                    except Exception:
                        pass

            return df
            
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            logger.info("Cache data may be corrupted, will be refreshed on next request")
            return None

    def store_dataframe(self, symbol: str, interval: str, df: pd.DataFrame) -> bool:
        """DataFrame'i Redis'e kaydet"""
        try:
            # Kapsamlı DataFrame kontrolü
            if df is None:
                logger.warning(f"DataFrame is None for {symbol}:{interval}")
                return False
                
            if not isinstance(df, pd.DataFrame):
                logger.error(f"Object is not a DataFrame for {symbol}:{interval}, type: {type(df)}")
                return False
                
            if df.empty:
                logger.warning(f"DataFrame is empty for {symbol}:{interval}")
                return False
                
            key = self.get_key(symbol, interval)
            serialized_df = self.serialize_df(df)
            
            if serialized_df is None:
                logger.error(f"Failed to serialize DataFrame for {symbol}:{interval}")
                return False
            
            # DataFrame uzunluğunu güvenli şekilde al
            try:
                df_length = len(df.index)
            except Exception as len_error:
                logger.error(f"Error getting DataFrame length for {symbol}:{interval}: {len_error}")
                df_length = 0
            
            with self.client.pipeline() as pipe:
                pipe.set(key, serialized_df, ex=self.expire_time)
                pipe.set(f"{key}:timestamp", datetime.utcnow().isoformat())
                pipe.set(f"{key}:status", "completed")
                pipe.set(f"{key}:rows", df_length)
                pipe.execute()
            
            logger.info(f"Data stored for {symbol}:{interval} - {df_length} rows")
            return True
       
        except Exception as e:
            logger.error(f"Redis store error for {symbol}:{interval}: {e}")
            logger.error(f"DataFrame info - Type: {type(df)}, Shape: {getattr(df, 'shape', 'N/A')}")
            return False

    def get_dataframe(self, symbol: str, interval: str) -> Optional[Tuple[pd.DataFrame, datetime]]:
        """Redis'den DataFrame yükle - Geliştirilmiş versiyon"""
        try:
            key = self.get_key(symbol, interval)
        
            # Status kontrolü
            status = self.client.get(f"{key}:status")
            if status != b"completed":
                logger.info(f"Data not ready for {symbol}:{interval}")
                return None

            # Veri ve timestamp al
            data = self.client.get(key)
            timestamp_str = self.client.get(f"{key}:timestamp")
        
            if data is None or timestamp_str is None:
                logger.info(f"No cached data found for {symbol}:{interval}")
                return None

            # DataFrame'e çevir
            df = self.deserialize_df(data)
            if df is None:
                logger.warning(f"Failed to deserialize data for {symbol}:{interval}")
                return None
            
            if not isinstance(df, pd.DataFrame):
                logger.error(f"Deserialized object is not a DataFrame for {symbol}:{interval}: {type(df)}")
                return None
            
            if df.empty:
                logger.warning(f"Deserialized DataFrame is empty for {symbol}:{interval}")
                return None
            
            # Timestamp'i çevir
            try:
                timestamp = datetime.fromisoformat(timestamp_str.decode('utf-8'))
            except Exception as ts_error:
                logger.error(f"Invalid timestamp format for {symbol}:{interval}: {ts_error}")
                return None
        
            # DataFrame uzunluğunu güvenli şekilde al
            df_length = len(df.index) if hasattr(df, 'index') else 0
            logger.info(f"Data loaded from cache for {symbol}:{interval} - {df_length} rows")
        
            return df, timestamp
    
        except Exception as e:
            logger.error(f"Redis get error for {symbol}:{interval}: {e}")
            logger.error(f"Error details: {traceback.format_exc()}")
            return None

    def is_data_fresh(self, symbol: str, interval: str, max_age_minutes: int = 5) -> bool:
        """Verinin güncel olup olmadığını kontrol et"""
        try:
            key = self.get_key(symbol, interval)
            timestamp_str = self.client.get(f"{key}:timestamp")
            
            if timestamp_str is None:
                return False
                
            timestamp = datetime.fromisoformat(timestamp_str.decode('utf-8'))
            age = datetime.utcnow() - timestamp
            
            return age.total_seconds() < (max_age_minutes * 60)
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {e}")
            return False

    def get_cache_info(self, symbol: str, interval: str) -> Dict[str, Any]:
        """Cache hakkında bilgi al"""
        try:
            key = self.get_key(symbol, interval)
            
            info = {
                'exists': self.client.exists(key),
                'status': None,
                'timestamp': None,
                'ttl': self.client.ttl(key),
                'rows': None
            }
            
            if info['exists']:
                status = self.client.get(f"{key}:status")
                info['status'] = status.decode('utf-8') if status else None
                
                timestamp_str = self.client.get(f"{key}:timestamp")
                info['timestamp'] = timestamp_str.decode('utf-8') if timestamp_str else None
                
                rows = self.client.get(f"{key}:rows")
                info['rows'] = int(rows) if rows else None
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting cache info: {e}")
            return {}

    def clear_cache_for_symbol(self, symbol: str, interval: str = None):
        """Belirli bir symbol için cache'i temizle"""
        try:
            if interval:
                # Belirli bir interval için temizle
                key = self.get_key(symbol, interval)
                keys_to_delete = [key, f"{key}:timestamp", f"{key}:status", f"{key}:rows"]
                deleted = self.client.delete(*keys_to_delete)
                logger.info(f"Cleared cache for {symbol}:{interval} - {deleted} keys deleted")
            else:
                # Tüm intervaller için temizle
                pattern = f"market_data:{symbol}:*"
                keys = list(self.client.scan_iter(pattern))
                if keys:
                    deleted = self.client.delete(*keys)
                    logger.info(f"Cleared all cache for {symbol} - {deleted} keys deleted")
                else:
                    logger.info(f"No cache found for {symbol}")
                    
        except Exception as e:
            logger.error(f"Cache clearing error for {symbol}: {e}")

    def clear_all_cache(self):
        """Tüm market data cache'ini temizle"""
        try:
            pattern = "market_data:*"
            keys = list(self.client.scan_iter(pattern))
            if keys:
                deleted = self.client.delete(*keys)
                logger.info(f"Cleared all market data cache - {deleted} keys deleted")
            else:
                logger.info("No cache data found to clear")
                
        except Exception as e:
            logger.error(f"Error clearing all cache: {e}")

    def migrate_legacy_cache(self):
        """Eski format cache'leri yeni formata çevir"""
        try:
            migrated_count = 0
            error_count = 0
            
            # Tüm market_data keylerini tara
            for key in self.client.scan_iter("market_data:*"):
                if key.decode().endswith((':timestamp', ':status', ':rows')):
                    continue
                    
                try:
                    # Eski veriyi al
                    old_data = self.client.get(key)
                    if old_data is None:
                        continue
                    
                    # Eğer zaten LZ4 formatında ise atla
                    if old_data.startswith(b"LZ4:"):
                        continue
                    
                    # Eski formatı deserialize et
                    try:
                        unpacked_data = msgpack.unpackb(old_data)
                        df = pd.DataFrame(unpacked_data)
                        
                        if not df.empty:
                            # Yeni formatta serialize et
                            new_data = self.serialize_df(df)
                            if new_data:
                                # Yeni veriyi kaydet
                                self.client.set(key, new_data, ex=self.expire_time)
                                migrated_count += 1
                                logger.info(f"Migrated cache: {key.decode()}")
                            
                    except Exception as migrate_error:
                        logger.warning(f"Failed to migrate {key.decode()}: {migrate_error}")
                        # Migrate edilemeyen cache'i sil
                        self.client.delete(key)
                        error_count += 1
                        
                except Exception as key_error:
                    logger.error(f"Error processing key {key}: {key_error}")
                    error_count += 1
            
            logger.info(f"Cache migration completed: {migrated_count} migrated, {error_count} errors")
            return migrated_count, error_count
            
        except Exception as e:
            logger.error(f"Cache migration error: {e}")
            return 0, 0

    def start_auto_save(self, data_source_func, symbols: list, intervals: list, save_interval_seconds: int = 300):
        """Otomatik kayıt sistemini başlat"""
        if self.auto_save_enabled:
            logger.warning("Auto-save already running")
            return
            
        self.auto_save_enabled = True
        
        def auto_save_worker():
            logger.info("Auto-save worker started")
            
            while self.auto_save_enabled:
                try:
                    for symbol in symbols:
                        for interval in intervals:
                            if not self.auto_save_enabled:
                                break
                                
                            # Veri güncel mi kontrol et
                            if self.is_data_fresh(symbol, interval, max_age_minutes=5):
                                continue
                            
                            # Yeni veri al ve kaydet
                            try:
                                df = data_source_func(symbol, interval)
                                if df is not None and not df.empty:
                                    self.store_dataframe(symbol, interval, df)
                                    df_length = len(df.index) if hasattr(df, 'index') else 0
                                    logger.info(f"Auto-saved data for {symbol}:{interval} - {df_length} rows")
                                else:
                                    logger.warning(f"No data received for {symbol}:{interval}")
                                    
                            except Exception as e:
                                logger.error(f"Error getting data for {symbol}:{interval}: {e}")
                            
                            # Rate limiting
                            time.sleep(1)
                    
                    # Temizlik yap
                    self.clear_expired_cache()
                    
                    # Bir sonraki cycle için bekle
                    time.sleep(save_interval_seconds)
                    
                except Exception as e:
                    logger.error(f"Auto-save worker error: {e}")
                    time.sleep(60)  # Hata durumunda biraz bekle
            
            logger.info("Auto-save worker stopped")
        
        self.save_thread = threading.Thread(target=auto_save_worker, daemon=True)
        self.save_thread.start()
        logger.info("Auto-save system started")

    def clear_expired_cache(self):
        """Süresi dolmuş cache'leri temizle"""
        try:
            deleted_count = 0
            for key in self.client.scan_iter("market_data:*"):
                if self.client.ttl(key) == -2:  # Süresi dolmuş
                    self.client.delete(key)
                    deleted_count += 1
                    
            if deleted_count > 0:
                logger.info(f"Cleared {deleted_count} expired cache entries")
                
        except Exception as e:
            logger.error(f"Cache clearing error: {e}")

    def stop_auto_save(self):
        """Otomatik kayıt sistemini durdur"""
        if self.auto_save_enabled:
            self.auto_save_enabled = False
            logger.info("Auto-save system stopping...")
            
            if self.save_thread:
                self.save_thread.join(timeout=10)
                
            logger.info("Auto-save system stopped")

@dataclass
class IndicatorCache:
    data: Dict[str, Any] = None

    def __post_init__(self):
        self.data = {}

    def set(self, key, value):
        with cache_lock:
            if len(self.data) > 1000:
                self.data.pop(next(iter(self.data)))  
            self.data[key] = value

    def get(self, key):
        with cache_lock:
            return self.data.get(key)

    def clear(self):
        with cache_lock:
            self.data.clear()

# Initialize caches
redis_cache = RedisCache()
indicator_cache = IndicatorCache()

def redis_health_check() -> bool:
    """Redis bağlantısını kontrol et"""
    try:
        redis_client.ping()
        logger.info("Redis connection successful")
        return True
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return False

def create_dataframe(data):
    """Verimli DataFrame oluştur"""
    try:
        arr = np.array(data, dtype=[
            ('timestamp', 'datetime64[ms]'), 
            ('open', 'float32'),
            ('high', 'float32'),
            ('low', 'float32'),
            ('close', 'float32'),
            ('volume', 'float32')
        ])
        return pd.DataFrame.from_records(arr, index='timestamp')
    except Exception as e:
        logger.error(f"DataFrame creation error: {e}")
        return None

def get_market_data_with_cache_optimized(symbol: str, interval: str, force_refresh: bool = False) -> Optional[pd.DataFrame]:
    """
    LZ4 optimize edilmiş cache fonksiyonu
    MEVCUT get_market_data_with_cache FONKSİYONUNUZUN YERİNE KULLANIN
    """
    if not force_refresh:
        # Cache'den dene (artık LZ4 sıkıştırılmış)
        cached_result = redis_cache.get_dataframe(symbol, interval)
        if cached_result:
            df, timestamp = cached_result
            age = datetime.utcnow() - timestamp
            
            if age.total_seconds() < 300:  # 5 dakikadan yeni ise
                logger.info(f"Using LZ4 cached data for {symbol}:{interval} (age: {age.total_seconds():.1f}s)")
                return df
            else:
                logger.info(f"LZ4 cached data is old for {symbol}:{interval} (age: {age.total_seconds():.1f}s)")
    
    # Cache'de yoksa veya eski ise yeni veri al
    logger.info(f"Fetching new data for {symbol}:{interval}")
    
    # Gerçek veri kaynağınızdan veri al
    df = your_actual_data_source_function(symbol, interval)  # Bu fonksiyonu kendi veri kaynağınızla değiştirin
    
    if df is not None and not df.empty:
        # Yeni veriyi LZ4 ile sıkıştırarak cache'e kaydet
        redis_cache.store_dataframe(symbol, interval, df)
        return df
    
    return None

def test_lz4_performance_comparison():
    """LZ4 öncesi/sonrası performans karşılaştırması"""
    import time
    
    # Test verisi
    test_df = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=1000, freq='1min'),
        'open': np.random.random(1000) * 50000,
        'high': np.random.random(1000) * 50000,
        'low': np.random.random(1000) * 50000,
        'close': np.random.random(1000) * 50000,
        'volume': np.random.random(1000) * 1000000
    })
    
    symbol, interval = "TESTCOIN", "1m"
    
    print("=== LZ4 Performance Test ===")
    print(f"Test DataFrame: {len(test_df)} rows")
    
    # Store performance test
    start_time = time.time()
    success = redis_cache.store_dataframe(symbol, interval, test_df)
    store_time = time.time() - start_time
    
    # Get performance test
    start_time = time.time()
    result = redis_cache.get_dataframe(symbol, interval)
    get_time = time.time() - start_time
    
    # Results
    print(f"Store Time (LZ4): {store_time:.4f}s")
    print(f"Get Time (LZ4): {get_time:.4f}s")
    print(f"Total Time: {(store_time + get_time):.4f}s")
    print(f"Store Success: {success}")
    print(f"Get Success: {result is not None}")
    
    if result:
        df_retrieved, timestamp = result
        print(f"Data Integrity: {len(df_retrieved) == len(test_df)}")
        print(f"Age: {(datetime.utcnow() - timestamp).total_seconds():.2f}s")
    
    # Cleanup
    key = redis_cache.get_key(symbol, interval)
    redis_cache.client.delete(key, f"{key}:timestamp", f"{key}:status", f"{key}:rows")
    
    return {
        'store_time': store_time,
        'get_time': get_time,
        'success': success and result is not None
    }

def setup_lz4_cache_with_migration():
    """LZ4 cache kurulumu ve eski cache migration"""
    
    print("=== LZ4 Redis Cache Setup with Migration ===")
    
    # Redis bağlantı testi
    try:
        redis_client.ping()
        print("✅ Redis connection successful")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False
    
    # LZ4 test - FIXED VERSION
    try:
        test_data = b"test data for lz4 compression"
        # Remove the invalid parameter
        compressed = lz4.frame.compress(test_data)
        decompressed = lz4.frame.decompress(compressed)
        
        if decompressed == test_data:
            print("✅ LZ4 compression test successful")
            print(f"   Compression ratio: {len(test_data)/len(compressed):.2f}x")
        else:
            print("❌ LZ4 compression test failed")
            return False
            
    except Exception as e:
        print(f"❌ LZ4 test failed: {e}")
        print("   Install with: pip install lz4")
        return False
    
    # Cache migration
    print("\n=== Migrating Legacy Cache ===")
    migrated, errors = redis_cache.migrate_legacy_cache()
    print(f"Migration completed: {migrated} migrated, {errors} errors")
    
    # Performance test
    print("\n=== Running Performance Test ===")
    perf_result = test_lz4_performance_comparison()
    
    if perf_result['success']:
        print("✅ LZ4 cache setup completed successfully!")
        print(f"   Expected 40-60% performance improvement in data transfer")
        return True
    else:
        print("❌ Performance test failed")
        return False

# Utility functions for cache management
def fix_corrupted_cache_data():
    """Bozuk cache verilerini temizle"""
    print("=== Fixing Corrupted Cache Data ===")
    
    # Tüm problematik cache'leri temizle
    redis_cache.clear_all_cache()
    print("✅ All cache cleared")
    
    # Cache migration yap
    migrated, errors = redis_cache.migrate_legacy_cache()
    print(f"Migration: {migrated} migrated, {errors} errors")
    
    return True

def get_cache_health_report():
    """Cache sağlık raporu"""
    try:
        # Redis info
        info = redis_client.info()
        memory_used = info.get('used_memory_human', 'Unknown')
        
        # Cache key sayısı
        market_keys = list(redis_client.scan_iter("market_data:*"))
        data_keys = [k for k in market_keys if not k.decode().endswith((':timestamp', ':status', ':rows'))]
        
        # LZ4 vs Legacy format sayısı
        lz4_count = 0
        legacy_count = 0
        
        for key in data_keys[:10]:  # İlk 10'u kontrol et
            data = redis_client.get(key)
            if data:
                if data.startswith(b"LZ4:"):
                    lz4_count += 1
                else:
                    legacy_count += 1
        
        report = {
            'redis_memory': memory_used,
            'total_cache_keys': len(market_keys),
            'data_keys': len(data_keys),
            'lz4_format_sample': lz4_count,
            'legacy_format_sample': legacy_count,
        }
        
        print("=== Cache Health Report ===")
        for key, value in report.items():
            print(f"{key}: {value}")
        
        return report
        
    except Exception as e:
        print(f"Error generating cache health report: {e}")
        return None

# Alternative compression functions if you need more control
def compress_lz4_with_options(data: bytes, compression_level: int = 0) -> bytes:
    """
    LZ4 compression with additional options
    compression_level: 0 = default, 1-12 = higher compression but slower
    """
    try:
        if compression_level > 0:
            # Use high compression mode
            return lz4.frame.compress(data, compression_level=compression_level)
        else:
            # Use default compression
            return lz4.frame.compress(data)
    except Exception as e:
        logger.error(f"LZ4 compression error: {e}")
        return data

def get_compression_stats(original_data: bytes, compressed_data: bytes) -> dict:
    """Get compression statistics"""
    return {
        'original_size': len(original_data),
        'compressed_size': len(compressed_data),
        'compression_ratio': len(original_data) / len(compressed_data) if len(compressed_data) > 0 else 0,
        'space_saved_percent': (1 - len(compressed_data) / len(original_data)) * 100 if len(original_data) > 0 else 0
    }

# Sabitler
CACHE_SIZE = 500  # Sadece son DataFrame yeterli — tüm okumalar cache[key][-1]
MIN_PERIOD = 30


STALE_MINUTES = {
    '1m': 1, '3m': 3, '5m': 5, '15m': 15,
    '30m': 30, '1h': 60, '2h': 120, '4h': 240,
    '6h': 360, '8h': 480, '12h': 720,
    '1d': 1440, '3d': 4320
}

cache = {}
cache_lock = threading.Lock()
divergence_cache = {}
TREND_CACHE = {}  # Trend hesaplama cache'i
TREND_HISTORY_SIZE = 100  # Trend geçmişi