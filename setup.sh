#!/bin/bash
# ERANDA — VPS kurulum scripti (temiz Ubuntu 22.04/24.04 üzerinde çalıştırılır)
#
# Kullanım (repo'yu klonladıktan SONRA, repo klasörünün İÇİNDEN):
#     chmod +x setup.sh && ./setup.sh
#
# Ne yapar: Docker'ı kurar (yoksa), firewall'u ayarlar, .env'i hazırlar, imajı build eder.
# Ne YAPMAZ: .env'ini senin yerine doldurmaz, sertifika almaz, servisi başlatmaz —
#            bunlar bilinçli olarak senin onayınla, sonraki adımda yapılır.
# Tekrar tekrar çalıştırılabilir (idempotent): kurulu olanı atlar.

set -e
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok(){ echo -e "${GREEN}✓${NC} $1"; }
warn(){ echo -e "${YELLOW}!${NC} $1"; }
step(){ echo -e "\n${GREEN}### $1${NC}"; }

# Repo klasöründe miyiz?
if [ ! -f docker-compose.yml ] || [ ! -f Dockerfile ]; then
  echo -e "${RED}HATA:${NC} Bu scripti ERANDA repo klasörünün İÇİNDEN çalıştır."
  echo "Önce:  git clone <repo-url> && cd <klasör> && ./setup.sh"
  exit 1
fi

# ── 1. Docker ───────────────────────────────────────────────────────────────
step "1/5  Docker kontrol"
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  ok "Docker + Compose zaten kurulu ($(docker --version | cut -d',' -f1))"
else
  warn "Docker kurulu değil — resmi kurulum scripti indiriliyor..."
  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
  sudo sh /tmp/get-docker.sh
  sudo usermod -aG docker "$USER" || true
  rm -f /tmp/get-docker.sh
  ok "Docker kuruldu"
  warn "NOT: 'docker' komutunu sudo'suz kullanmak için bir kez çıkış/giriş yap (logout/login)."
fi

# ── 2. Firewall (ufw) ────────────────────────────────────────────────────────
step "2/5  Güvenlik duvarı (22/80/443)"
if command -v ufw >/dev/null 2>&1; then
  sudo ufw allow OpenSSH       >/dev/null 2>&1 || sudo ufw allow 22/tcp >/dev/null 2>&1
  sudo ufw allow 80/tcp        >/dev/null 2>&1
  sudo ufw allow 443/tcp       >/dev/null 2>&1
  sudo ufw --force enable      >/dev/null 2>&1
  ok "Firewall ayarlandı (sadece 22/80/443 açık). collector/redis dışarı kapalı."
else
  warn "ufw yok — sağlayıcının panelinden 22/80/443 dışını kapatmayı unutma."
fi

# ── 3. .env ──────────────────────────────────────────────────────────────────
step "3/5  Ortam dosyası (.env)"
if [ -f .env ]; then
  ok ".env zaten var (korunuyor)."
else
  cp .env.example .env
  warn ".env oluşturuldu — DOLDURMAN GEREKİYOR (aşağıda)."
  NEEDS_ENV=1
fi

# ── 4. Image build ───────────────────────────────────────────────────────────
step "4/5  Docker imajı build ediliyor (ilk sefer TA-Lib derlenir, birkaç dakika)"
if command -v docker >/dev/null 2>&1; then
  docker compose -f docker-compose.yml -f docker-compose.prod.yml build
  ok "Image hazır."
else
  warn "Docker yeni kuruldu; logout/login sonrası tekrar './setup.sh' çalıştır (build için)."
  exit 0
fi

# ── 5. Sıradaki adımlar ──────────────────────────────────────────────────────
step "5/5  Hazır. Sıradaki adımlar:"
echo ""
if [ "${NEEDS_ENV:-0}" = "1" ]; then
  echo -e "${YELLOW}A) Önce .env'i doldur:${NC}  nano .env"
  echo "   - REDIS_PASSWORD : güçlü bir şifre (ZORUNLU)"
  echo "   - N_CONSUMERS    : VPS CPU'na göre (4 vCPU → 2-3)"
  echo "   - COLLECTOR_SYMBOLS_OVERRIDE : tam 40 sembol için BOŞ bırak"
  echo "   - DOMAIN         : alan adın (DNS A kaydı bu VPS IP'sine baksın)"
  echo "   - CERTBOT_EMAIL  : e-postan"
  echo "   - STAGING=1      : ilk denemede 1, çalışınca 0"
  echo ""
fi
echo -e "${YELLOW}B) İlk SSL sertifikası (BİR KEZ):${NC}  ./init-letsencrypt.sh"
echo -e "${YELLOW}C) Tüm stack'i başlat:${NC}  docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d"
echo ""
echo "   Kolaylık için:  alias dcp='docker compose -f docker-compose.yml -f docker-compose.prod.yml'"
echo "   Sonra:  dcp ps | dcp logs -f collector | dcp down"
echo ""
ok "Detaylar için: NGINX-DEPLOY.md"
