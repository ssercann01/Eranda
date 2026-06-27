# ERANDA — VPS Deploy + HTTPS Runbook

Bu adımlar **VPS kiralandıktan sonra** uygulanır. Lokal soak testte bunlara gerek yok.
Local stack (collector + redis + dashboard) ile prod stack arasındaki tek fark: prod'da
öne **nginx + certbot** ekleniyor ve dashboard dışarı kapatılıp sadece nginx üzerinden HTTPS ile açılıyor.

## Ön koşullar
1. VPS'te Docker + Docker Compose kurulu.
2. Bir alan adın var ve **DNS A kaydı** VPS'in public IP'sine bakıyor (örn. `eranda.example.com → 1.2.3.4`).
   DNS'in yayılmasını bekle (`dig +short eranda.example.com` IP'yi vermeli).
3. Proje dosyaları VPS'te (git clone ya da scp ile).

## 1. Güvenlik duvarı (sadece 22/80/443 açık)
```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```
> Önemli: collector ve Redis zaten dışarı port açmıyor. Dashboard (9050) prod overlay'inde
> sadece `127.0.0.1`'e bağlı — yani public IP'den erişilemez, yalnız nginx içeriden ulaşır.

## 2. .env'i prod için doldur
```bash
cp .env.example .env   # yoksa
nano .env
```
Doldur:
```
N_CONSUMERS=2                 # VPS CPU'suna göre artır
REDIS_PASSWORD=<güçlü-bir-şifre>   # ÜRETİMDE MUTLAKA DOLDUR
COLLECTOR_SYMBOLS_OVERRIDE=        # tam 40 sembol için BOŞ bırak
DOMAIN=eranda.example.com
CERTBOT_EMAIL=sen@example.com
STAGING=1                     # İLK denemede 1 (test sertifikası), çalışınca 0 yapıp tekrar
```

## 3. İmajı build et
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml build
```

## 4. İlk SSL sertifikasını al (BİR KEZ)
```bash
chmod +x init-letsencrypt.sh
./init-letsencrypt.sh
```
Bu script: geçici sertifika koyar → nginx'i kaldırır → Let's Encrypt'ten gerçek sertifika ister → nginx'i reload eder.
- **STAGING=1** ile çalıştır, "test sertifikası" mesajı + başarıyla biterse,
- `.env`'de `STAGING=0` yap ve scripti **tekrar** çalıştır → gerçek (tarayıcının güvendiği) sertifika gelir.

## 5. Tüm stack'i kalıcı başlat
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```
Tarayıcıdan: **https://eranda.example.com**

## Günlük komutlar (prod)
Kolaylık için alias tanımla:
```bash
alias dcp='docker compose -f docker-compose.yml -f docker-compose.prod.yml'
```
```bash
dcp ps                 # durum
dcp logs -f collector  # log
dcp logs -f nginx
dcp restart collector
dcp down               # durdur (veri kalır)
```

## Sertifika yenileme
Otomatik: `certbot` servisi 12 saatte bir `renew` dener, `nginx` 6 saatte bir reload eder.
Elle test: `dcp run --rm certbot renew --dry-run`

## Sık sorun
- **nginx başlamıyor / cert yok hatası** → `init-letsencrypt.sh`'i çalıştırmayı atlamışsındır. Önce o.
- **certbot "challenge failed"** → DNS A kaydı VPS IP'sine bakmıyor ya da 80 portu kapalı (ufw / sağlayıcı firewall).
- **Rate limit** → çok deneme yaptın; `STAGING=1` ile çalışıp doğrula, sonra 0'a al.
- **Dashboard açılmıyor ama nginx ayakta** → `dcp logs dashboard` ve `dcp logs collector` bak; Redis bağlantısı/veri var mı.
