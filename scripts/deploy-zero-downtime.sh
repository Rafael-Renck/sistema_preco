#!/usr/bin/env bash
# Deploy com minimização de downtime: migrations antes da troca de containers.
# Uso: bash scripts/deploy-zero-downtime.sh [staging|production]
set -euo pipefail

ENV_TARGET="${1:-production}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

STATE_DIR="$ROOT/.deploy-state"
mkdir -p "$STATE_DIR"
ROLLBACK_FILE="$STATE_DIR/last-good-sha"

if [[ "$ENV_TARGET" == "staging" ]]; then
  COMPOSE_FILE="docker-compose.staging.yml"
  PROJECT="sistema_preco_staging"
  WEB_SERVICE="web_staging"
  WORKER_SERVICE="import_worker_staging"
elif [[ "$ENV_TARGET" == "production" ]]; then
  COMPOSE_FILE="docker-compose.prod.yml"
  PROJECT="sistema_preco_prod"
  WEB_SERVICE="web_prod"
  WORKER_SERVICE="import_worker_prod"
else
  echo "Ambiente inválido: $ENV_TARGET (use staging ou production)" >&2
  exit 1
fi

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -p "$PROJECT" -f "$COMPOSE_FILE")
elif docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")
else
  echo "Instale docker-compose ou o plugin docker compose." >&2
  exit 1
fi

CURRENT_SHA="$(git rev-parse HEAD)"
PREVIOUS_SHA=""
if [[ -f "$ROLLBACK_FILE" ]]; then
  PREVIOUS_SHA="$(cat "$ROLLBACK_FILE")"
fi

echo "=== Deploy $ENV_TARGET ==="
echo "Commit atual: $CURRENT_SHA"
echo "Último commit estável: ${PREVIOUS_SHA:-nenhum}"

# 1. Migrations ANTES de atualizar a app (expand-contract / retrocompatível)
if [[ "$ENV_TARGET" == "production" ]]; then
  bash "$ROOT/scripts/migrate-prod.sh"
else
  echo "Aplicando migrations em staging..."
  "${COMPOSE[@]}" run --rm --no-deps \
    -v "$ROOT/alembic.ini:/app/alembic.ini:ro" \
    -v "$ROOT/migrations:/app/migrations:ro" \
    -v "$ROOT/app.py:/app/app.py:ro" \
    -v "$ROOT/rol_import.py:/app/rol_import.py:ro" \
    "$WEB_SERVICE" alembic -c /app/alembic.ini upgrade head
fi

# 2. Rolling update: web primeiro (aceita tráfego), worker depois
echo "Atualizando $WEB_SERVICE..."
"${COMPOSE[@]}" up -d --build --no-deps "$WEB_SERVICE"

echo "Aguardando estabilização do web..."
sleep 5

echo "Atualizando $WORKER_SERVICE..."
"${COMPOSE[@]}" up -d --build --no-deps "$WORKER_SERVICE"

# 3. Registrar SHA estável para rollback
echo "$CURRENT_SHA" > "$ROLLBACK_FILE"
"${COMPOSE[@]}" ps

echo "Deploy $ENV_TARGET concluído."
