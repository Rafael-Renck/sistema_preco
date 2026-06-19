#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

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

"${COMPOSE[@]}" up -d --build --force-recreate web_prod import_worker_prod

echo "Aplicando migrations..."
bash "$ROOT/scripts/migrate-prod.sh"

"${COMPOSE[@]}" ps
echo
echo "Logs do worker: ${COMPOSE[*]} logs -f import_worker_prod"
