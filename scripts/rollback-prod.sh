#!/usr/bin/env bash
# Rollback ao último commit estável registrado em .deploy-state/last-good-sha
# Uso: bash scripts/rollback-prod.sh [staging|production]
set -euo pipefail

ENV_TARGET="${1:-production}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

STATE_DIR="$ROOT/.deploy-state"
ROLLBACK_FILE="$STATE_DIR/last-good-sha"

if [[ ! -f "$ROLLBACK_FILE" ]]; then
  echo "Nenhum SHA de rollback registrado em $ROLLBACK_FILE" >&2
  exit 1
fi

TARGET_SHA="$(cat "$ROLLBACK_FILE")"
CURRENT_SHA="$(git rev-parse HEAD)"

if [[ "$TARGET_SHA" == "$CURRENT_SHA" ]]; then
  echo "Já estamos no SHA registrado ($TARGET_SHA). Tentando checkout anterior..."
  TARGET_SHA="$(git rev-parse HEAD~1 2>/dev/null || true)"
fi

if [[ -z "$TARGET_SHA" ]]; then
  echo "Não foi possível determinar SHA para rollback." >&2
  exit 1
fi

echo "=== Rollback $ENV_TARGET → $TARGET_SHA ==="
git fetch origin main
git checkout "$TARGET_SHA"

export DEPLOY_SHA="$TARGET_SHA"
bash "$ROOT/scripts/deploy-zero-downtime.sh" "$ENV_TARGET"

echo "Rollback concluído."
