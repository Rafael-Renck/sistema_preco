#!/usr/bin/env bash
# Aplica migrations Alembic no container de produção.
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

if ! "${COMPOSE[@]}" exec -T -w /app web_prod test -f /app/alembic.ini; then
  echo "alembic.ini não está no container — rebuild necessário:" >&2
  echo "  ${COMPOSE[*]} build --no-cache web_prod" >&2
  echo "  ${COMPOSE[*]} up -d web_prod" >&2
  exit 1
fi

"${COMPOSE[@]}" exec -T -w /app web_prod alembic -c /app/alembic.ini upgrade head
echo "Migrations aplicadas."
