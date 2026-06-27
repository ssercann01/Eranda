"""
ERANDA V2 — collector.py  (LAUNCHER)
================================================================
Eski 4300 satırlık collector.py'nin yerine geçen ince başlatıcı.
İş mantığı yok; sadece process topolojisini kurar:

  Producer (1 process)  ──RAW Queue(lar)──►  Consumer (N process)  ──►  Redis
       Katman 1/2                                 Katman 3              Katman 4

  - Producer:  WS/REST toplar, SADECE Queue'ya yazar (CPU yok).
  - Consumer:  Queue'dan okur, TÜM hesabı yapar, Redis + Pub/Sub.
  - N consumer: sembol-bazlı shard (ShardRouter) → cache tutarlılığı korunur.

Çalıştır:
  python3 collector.py            # 1 consumer (default)
  python3 collector.py 4          # 4 consumer process (yatay ölçek)
"""

import multiprocessing as mp
import sys
import time

import bus
from producer import run_producer
from consumer import run_consumer
from shared import (
    COLLECTOR_SYMBOLS, COLLECTOR_INTERVALS, COLLECTOR_INDICATORS,
    SYMBOL_SOURCE, MEXC_ONLY_SYMBOLS, redis_client,
)


def _clear_mexc_stale():
    try:
        for _sym in list(MEXC_ONLY_SYMBOLS):
            for _rk in redis_client.scan_iter(f"market_data:{_sym}:*"):
                redis_client.delete(_rk)
        print("[LAUNCHER] MEXC Redis cache temizlendi")
    except Exception as e:
        print(f"[LAUNCHER] Redis temizleme: {e}")


def main(n_consumers: int = 1):
    ctx = mp.get_context("spawn")  # macOS + torch uyumu için açık spawn

    _clear_mexc_stale()

    print(f"[LAUNCHER] {len(COLLECTOR_SYMBOLS)} sembol x {len(COLLECTOR_INTERVALS)} interval x "
          f"{len(COLLECTOR_INDICATORS)} indikatör")
    print(f"[LAUNCHER] Binance: {len([s for s in COLLECTOR_SYMBOLS if SYMBOL_SOURCE.get(s,'binance')=='binance'])} | "
          f"MEXC: {len([s for s in COLLECTOR_SYMBOLS if SYMBOL_SOURCE.get(s,'binance')=='mexc'])}")
    print(f"[LAUNCHER] Consumer process sayısı: {n_consumers}")

    # ── Queue(lar) + put sayaçları + router ───────────────────────────────────
    queues   = [bus.make_raw_queue(ctx=ctx) for _ in range(n_consumers)]
    counters = [ctx.Value("L", 0) for _ in range(n_consumers)]  # queue derinliği
    router   = bus.ShardRouter(queues, counters)

    # ── Consumer process'leri ─────────────────────────────────────────────────
    consumers = []
    for sid, q in enumerate(queues):
        p = ctx.Process(target=run_consumer, args=(q, sid, counters[sid]), daemon=True)
        p.start()
        consumers.append(p)

    # ── Producer process ──────────────────────────────────────────────────────
    producer = ctx.Process(
        target=run_producer,
        args=(router, COLLECTOR_SYMBOLS, COLLECTOR_INTERVALS),
        daemon=False,
    )
    producer.start()

    print("[LAUNCHER] Tüm process'ler başlatıldı. Ctrl+C ile durdur.")
    try:
        while True:
            time.sleep(1)
            # Heartbeat — dashboard bunu izler; bayatlarsa "veri akışı durdu" uyarır
            try:
                redis_client.set('collector:heartbeat', time.time(), ex=30)
            except Exception:
                pass
            if not producer.is_alive():
                print("[LAUNCHER] Producer öldü — kapatılıyor")
                break
    except KeyboardInterrupt:
        print("\n[LAUNCHER] Durduruluyor...")
    finally:
        # Heartbeat'i sil → dashboard ANINDA "collector kapalı" uyarısı versin
        try:
            redis_client.delete('collector:heartbeat')
        except Exception:
            pass
        # Producer'ı durdur
        if producer.is_alive():
            producer.terminate()
        # Consumer'lara poison pill gönder
        for q in queues:
            try:
                q.put_nowait(None)
            except Exception:
                pass
        for p in consumers:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        print("[LAUNCHER] Kapandı.")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    main(n_consumers=n)