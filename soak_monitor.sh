#!/bin/bash
OUT="soak.log"
echo "=== SOAK START $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$OUT"
while true; do
  TS=$(date '+%H:%M:%S')
  STATS=$(docker stats --no-stream --format '{{.Name}} CPU={{.CPUPerc}} MEM={{.MemUsage}}' eranda-collector eranda-redis 2>/dev/null | tr '\n' ' | ')
  QLINE=$(docker logs --tail 40 eranda-collector 2>&1 | grep CONSUMER | tail -2 | sed 's/.*\(CONSUMER-[0-9]\).*queue=\([0-9]*\).*cache_keys=\([0-9]*\).*/\1 q=\2 keys=\3/' | tr '\n' ' ')
  RECON=$(docker logs --tail 200 eranda-collector 2>&1 | grep -ciE "reconnect|reconnecting|disconnect|connection closed|ws closed|yeniden")
  echo "[$TS] $STATS || $QLINE || reconnect_olayı(son200satır)=$RECON" >> "$OUT"
  sleep 120
done
