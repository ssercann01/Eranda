# ERANDA V2 — Container Runbook (lokal test)

Bu klasör artık container'a hazır. Aşağıdaki adımlar **Intel Mac'te** (x86_64 — VPS ile aynı mimari) çalıştırmak içindir.

## 0. Gereksinim
Docker Desktop kurulu olsun. Test:
```bash
docker --version
docker compose version
```

## 1. Ortam dosyasını hazırla
```bash
cd /path/to/erandatest1
cp .env.example .env
# .env'i aç: lokal testte N_CONSUMERS=1 yeterli, REDIS_PASSWORD boş kalabilir
```

## 2. Build + ayağa kaldır
```bash
docker compose up --build
```
İlk build TA-Lib'i kaynaktan derler → birkaç dakika sürer (bir kez). Sonraki açılışlar saniyeler.

## 3. Kontrol et
- Dashboard:  http://localhost:9050
- Loglar canlı akar. Ayrı terminalden tek servis logu:
  ```bash
  docker compose logs -f collector
  docker compose logs -f dashboard
  docker compose logs -f redis
  ```
- Redis içine bak:
  ```bash
  docker compose exec redis redis-cli
  > KEYS market_data:*
  > GET collector:heartbeat
  ```

## 4. Durdur
```bash
docker compose down        # konteynerleri durdurur, Redis/setup verisi KALIR
docker compose down -v     # veriyi de siler (temiz başlangıç)
```

---

## Soak test ipuçları (8GB RAM)
- `N_CONSUMERS=1` ile başla. Bellek rahatsa `2` dene.
- Bellek sıkışırsa: dashboard'dan ya da koddan **sembol sayısını geçici azalt** (40 yerine 10-15).
  Kopma/reconnect/restart davranışı az sembolle de aynı şekilde test edilir.
- 24/7 açık bırakıp şunları izle:
  - WebSocket günlerce kopmadan duruyor mu? Kopunca reconnect var mı?
  - `docker stats` ile bellek zamanla şişiyor mu (Redis TTL temizliği çalışıyor mu)?
  - `docker compose restart collector` → N worker temiz kalkıyor mu?

## Bilinçli ertelenenler (VPS'e)
- **N worker'ın gerçek CPU/RAM profili** → asıl VPS'te ölç (8GB Mac yanıltır).
- **nginx + HTTPS** → domain VPS'e bağlanınca kurulacak (bir sonraki dosya).
- **Tam 40 sembol + yüksek N kapasite testi** → VPS.

## Yapılan kod değişiklikleri (shared.py)
1. `REDIS_HOST/PORT/DB/PASSWORD` artık env'den (varsayılan localhost → Mac'te Docker'sız da çalışır).
2. `SETUP_FILE` env'den (container'da /data/setup2.json).
3. `torch` kaldırıldı (yalnız multiprocessing için vardı; stdlib birebir aynı). Sorun çıkarsa
   18. ve 38. satırları eski haline almak yeterli.
