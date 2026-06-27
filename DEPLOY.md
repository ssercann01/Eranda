# ERANDA — Tam Deploy Kılavuzu (baştan sona)

Bu, VPS kiraladıktan sonra izleyeceğin tam yol. Sıra önemli.

---

## AŞAMA 0 — Kodu GitHub'a gönder (bir kez, Mac'te)

VPS'e kodu `git clone` ile indireceğiz, o yüzden önce kod GitHub'da **private** repo'da olmalı.

1. github.com'da yeni bir **private** repo aç (örn. `eranda`). Boş oluştur (README ekleme).
2. Mac'te, proje klasöründe:
```bash
cd /Users/eranda/Desktop/kod/erandatest1
git init
git add .
git commit -m "ERANDA container deploy hazır"
git branch -M main
git remote add origin https://github.com/<KULLANICI-ADIN>/eranda.git
git push -u origin main
```
> `.env`, loglar, `__pycache__` zaten `.gitignore`'da — repo'ya GİRMEZ. Sadece kod ve config şablonları gider. Bu doğru: sırlar VPS'te elle girilecek.

İleride kod değiştirince:  `git add . && git commit -m "..." && git push`
VPS'te güncellemek için:    `git pull && dcp up -d --build`

---

## AŞAMA 1 — VPS kirala

- Ubuntu 22.04 ya da 24.04, en az 2 vCPU / 4 GB RAM (40 sembol + N=2-3 için).
- **DNS:** alan adının A kaydını VPS'in public IP'sine yönlendir. Yayılmasını bekle:
  `dig +short eranda.example.com` → VPS IP'sini vermeli.

---

## AŞAMA 2 — VPS'e bağlan ve kur

SSH ile gir, sonra:
```bash
# git yoksa:
sudo apt update && sudo apt install -y git

# repo'yu indir (private repo → GitHub kullanıcı adı + token sorar)
git clone https://github.com/<KULLANICI-ADIN>/eranda.git
cd eranda

# kurulum (Docker + firewall + .env + build)
chmod +x setup.sh && ./setup.sh
```
> Docker yeni kurulduysa script "logout/login yap" diyebilir. Çıkıp tekrar girip
> `cd eranda && ./setup.sh` ile devam et (kaldığı yerden, build'i yapar).

---

## AŞAMA 3 — .env'i doldur
```bash
nano .env
```
Doldur (kaydet: Ctrl+O, Enter, Ctrl+X):
```
N_CONSUMERS=2
REDIS_PASSWORD=<güçlü-bir-şifre>
COLLECTOR_SYMBOLS_OVERRIDE=          # tam 40 sembol için BOŞ
DOMAIN=eranda.example.com
CERTBOT_EMAIL=sen@example.com
STAGING=1                            # ilk denemede 1
```

---

## AŞAMA 4 — SSL sertifikası (bir kez)
```bash
./init-letsencrypt.sh
```
- `STAGING=1` ile "test sertifikası" mesajıyla başarılı biterse → `.env`'de `STAGING=0` yap,
  scripti **tekrar** çalıştır → gerçek (tarayıcının güvendiği) sertifika.

---

## AŞAMA 5 — Başlat
```bash
alias dcp='docker compose -f docker-compose.yml -f docker-compose.prod.yml'
dcp up -d
dcp ps
```
Tarayıcı: **https://eranda.example.com**

---

## Günlük kullanım
```bash
dcp ps                 # durum
dcp logs -f collector  # canlı log
dcp logs eranda-collector 2>&1 | grep PRODUCER   # DROP var mı kontrol
dcp restart collector
dcp down               # durdur (veri kalır)
git pull && dcp up -d --build   # kod güncelle + yeniden başlat
```

## İlk gün izlenecekler (lokal soak'ta öğrendiklerimiz)
- `[PRODUCER]` satırında **DROP** çıkıyor mu? (kuyruk dolup veri düşüyor mu — N_CONSUMERS artır)
- `docker stats` ile collector RAM saatler içinde sabit mi (sızıntı yok — lokalde temizdi)
- MEXC reconnect sonrası kuyruk hızlı eriyor mu (depth düzeltmesi sonrası temiz olmalı)
- 40 sembolde N=2 yetmezse N=3 dene (lokalde 5 sembol N=2 ile dengeydi; 40'ta daha fazla gerekebilir)
