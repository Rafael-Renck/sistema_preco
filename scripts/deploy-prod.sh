#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -p sistema_preco_prod -f docker-compose.prod.yml)
elif docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -p sistema_preco_prod -f docker-compose.prod.yml)
else
  echo "Instale docker-compose ou o plugin docker compose." >&2
  exit 1
fi

echo "Parando stack dev (db/web na 8000) se estiver rodando..."
docker-compose down --remove-orphans 2>/dev/null || true

"${COMPOSE[@]}" up -d --build web_prod import_worker_prod
"${COMPOSE[@]}" ps
echo
echo "Logs do worker: ${COMPOSE[*]} logs -f import_worker_prod"
