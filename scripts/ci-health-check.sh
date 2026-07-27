#!/usr/bin/env bash
# Health check HTTP para uso no CI/CD (GitHub Actions ou servidor).
# Uso: scripts/ci-health-check.sh URL MAX_ATTEMPTS SLEEP_SECONDS
set -euo pipefail

URL="${1:?URL obrigatória (ex: http://host:8010/health?format=json)}"
MAX_ATTEMPTS="${2:-30}"
SLEEP_SEC="${3:-10}"

echo "Health check: $URL (até ${MAX_ATTEMPTS} tentativas, intervalo ${SLEEP_SEC}s)"

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  if response="$(curl -fsS --max-time 15 "$URL" 2>/dev/null)"; then
    if echo "$response" | python -c "
import json, sys
data = json.load(sys.stdin)
status = data.get('status', '')
if status in ('healthy', 'degraded'):
    sys.exit(0)
sys.exit(1)
" 2>/dev/null; then
      echo "OK na tentativa $attempt — status aceito."
      exit 0
    fi
    echo "Tentativa $attempt: resposta recebida mas status não aceito."
  else
    echo "Tentativa $attempt: sem resposta válida."
  fi
  sleep "$SLEEP_SEC"
done

echo "Health check falhou após $MAX_ATTEMPTS tentativas."
exit 1
