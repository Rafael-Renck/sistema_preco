#!/usr/bin/env bash
# Aplica migrations Alembic no container de produção.
# Usa alembic.ini e migrations/ do host (git pull), sem depender de rebuild da imagem.
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

if [[ ! -f "$ROOT/alembic.ini" ]]; then
  echo "alembic.ini não encontrado em $ROOT — rode: git pull" >&2
  exit 1
fi

if [[ ! -d "$ROOT/migrations/versions" ]]; then
  echo "Pasta migrations/ não encontrada — rode: git pull" >&2
  exit 1
fi

echo "Aplicando migrations (arquivos montados de $ROOT)..."
"${COMPOSE[@]}" run --rm --no-deps \
  -v "$ROOT/alembic.ini:/app/alembic.ini:ro" \
  -v "$ROOT/migrations:/app/migrations:ro" \
  -v "$ROOT/app.py:/app/app.py:ro" \
  -v "$ROOT/rol_import.py:/app/rol_import.py:ro" \
  web_prod alembic -c /app/alembic.ini upgrade head

echo "Migrations aplicadas."
