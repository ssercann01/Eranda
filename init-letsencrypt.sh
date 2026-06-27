#!/bin/bash
# ERANDA — ilk SSL sertifikası kurulum scripti (VPS'te BİR KEZ çalıştırılır)
# Gereksinim: .env içinde DOMAIN tanımlı, ve DOMAIN'in DNS A kaydı bu VPS'in IP'sine bakıyor olmalı.
#
# Kullanım:   chmod +x init-letsencrypt.sh && ./init-letsencrypt.sh
#
# .env değişkenleri:
#   DOMAIN          (zorunlu)  örn. eranda.example.com
#   CERTBOT_EMAIL   (önerilir) yenileme uyarıları için
#   STAGING=1       (opsiyon)  test sertifikası — rate limit'e takılmadan denemek için

set -e

if [ -f .env ]; then set -a; . ./.env; set +a; fi

if [ -z "$DOMAIN" ]; then
  echo "❌ HATA: .env içinde DOMAIN tanımlı değil."; exit 1
fi

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml"
STAGING="${STAGING:-0}"
EMAIL="${CERTBOT_EMAIL:-}"

echo "### 1/5  Geçici (dummy) sertifika oluşturuluyor: $DOMAIN"
$COMPOSE run --rm --entrypoint "\
  sh -c 'mkdir -p /etc/letsencrypt/live/$DOMAIN && \
  openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
    -keyout /etc/letsencrypt/live/$DOMAIN/privkey.pem \
    -out    /etc/letsencrypt/live/$DOMAIN/fullchain.pem \
    -subj   /CN=localhost'" certbot

echo "### 2/5  nginx başlatılıyor (dummy sertifikayla ayağa kalkacak)"
$COMPOSE up -d nginx

echo "### 3/5  Dummy sertifika siliniyor"
$COMPOSE run --rm --entrypoint "\
  rm -rf /etc/letsencrypt/live/$DOMAIN \
         /etc/letsencrypt/archive/$DOMAIN \
         /etc/letsencrypt/renewal/$DOMAIN.conf" certbot

echo "### 4/5  Let's Encrypt'ten GERÇEK sertifika isteniyor"
STAGING_ARG=""; [ "$STAGING" != "0" ] && STAGING_ARG="--staging" && echo "    (STAGING modu — test sertifikası)"
if [ -n "$EMAIL" ]; then EMAIL_ARG="--email $EMAIL"; else EMAIL_ARG="--register-unsafely-without-email"; fi

$COMPOSE run --rm --entrypoint "\
  certbot certonly --webroot -w /var/www/certbot \
    $STAGING_ARG $EMAIL_ARG \
    -d $DOMAIN \
    --rsa-key-size 4096 --agree-tos --no-eff-email --force-renewal" certbot

echo "### 5/5  nginx yeniden yükleniyor (gerçek sertifikayı al)"
$COMPOSE exec nginx nginx -s reload

echo ""
echo "✅ Bitti.  https://$DOMAIN"
echo "   Tüm stack'i kalıcı başlatmak için:"
echo "   $COMPOSE up -d"
