# ERANDA V2 — ortak imaj (collector + dashboard aynı imajı kullanır, komut farklı)
# TA-Lib C kütüphanesi kaynaktan derlenir → amd64 (Intel Mac / VPS) ve arm64 (M2) ikisinde de çalışır.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Europe/Istanbul

# ── TA-Lib C kütüphanesi ───────────────────────────────────────────────────
ARG TALIB_VERSION=0.6.4
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential wget ca-certificates tzdata \
 && wget -q "https://github.com/TA-Lib/ta-lib/releases/download/v${TALIB_VERSION}/ta-lib-${TALIB_VERSION}-src.tar.gz" \
 && tar -xzf "ta-lib-${TALIB_VERSION}-src.tar.gz" \
 && cd "ta-lib-${TALIB_VERSION}" \
 && ./configure --prefix=/usr \
 && make -j"$(nproc)" \
 && make install \
 && cd .. && rm -rf "ta-lib-${TALIB_VERSION}" "ta-lib-${TALIB_VERSION}-src.tar.gz" \
 && ldconfig

WORKDIR /app

# ── Python bağımlılıkları (kod değişince layer cache korunsun diye önce bu) ──
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# ── Build araçlarını at (imaj küçülsün) — TA-Lib .so zaten /usr/lib'de ──────
RUN apt-get purge -y build-essential wget \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# ── Uygulama kodu ───────────────────────────────────────────────────────────
COPY . .

# setup dosyası ve olası runtime çıktıları için yazılabilir dizin
RUN mkdir -p /data
ENV SETUP_FILE=/data/setup2.json

# Varsayılan: collector (compose her servis için override eder)
CMD ["python", "collector.py", "1"]
