#!/usr/bin/env bash
# Setup do projeto sistema_preco no WSL.
# Uso dentro do WSL:
#   cd ~ && curl -sO <url-deste-script> && chmod +x wsl-setup.sh && ./wsl-setup.sh
# Ou, se já estiver na pasta do projeto (copiada ou clonada):
#   bash scripts/wsl-setup.sh

set -e
REPO_URL="${REPO_URL:-https://github.com/RafaelRenck/sistema_preco.git}"
PROJECT_DIR="${PROJECT_DIR:-$HOME/projetos/sistema_preco}"

echo "=== Setup sistema_preco no WSL ==="

if [ ! -f "app.py" ] && [ ! -f "docker-compose.yml" ]; then
  echo "Pasta do projeto não encontrada. Clonando repositório em $PROJECT_DIR ..."
  mkdir -p "$(dirname "$PROJECT_DIR")"
  if [ -d "$PROJECT_DIR" ]; then
    echo "A pasta $PROJECT_DIR já existe. Atualizando com git pull..."
    (cd "$PROJECT_DIR" && git pull)
  else
    git clone "$REPO_URL" "$PROJECT_DIR"
  fi
  cd "$PROJECT_DIR"
else
  echo "Usando pasta atual como projeto: $(pwd)"
fi

if [ ! -f .env ]; then
  echo "Criando .env a partir de .env.example..."
  cp .env.example .env
  echo "Arquivo .env criado. Ajuste se necessário (nano .env)."
else
  echo ".env já existe."
fi

echo ""
echo "Subindo serviços com Docker (profile dev)..."
docker compose --profile dev up --build -d

echo ""
echo "=== Pronto ==="
echo "  App:    http://localhost:8001"
echo "  Adminer: http://localhost:8081"
echo "  Para parar: docker compose --profile dev down"
echo ""
