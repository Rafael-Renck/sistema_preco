# CI/CD — Sistema de Preços

Este documento descreve a esteira GitHub Actions, gestão de segredos e o fluxo de trabalho do time.

## Fluxo do time (como você opera hoje)

1. Contribuidor abre PR contra `main`
2. GitHub Actions executa o workflow **CI** automaticamente
3. Tech leader revisa o PR; quando tudo está verde, confirma o **merge** no GitHub
4. Após merge, o workflow **CD** valida o build (deploy automático é opcional)

## Workflows

| Arquivo | Trigger | Função |
|---------|---------|--------|
| `.github/workflows/ci.yml` | PR e push em `main` | Lint, segurança, testes, migrations, build |
| `.github/workflows/cd.yml` | Push em `main` (após merge) | Build release, deploy staging/prod |

### Jobs do CI

- **Lint & Formatting** — Ruff, Black, compileall
- **Security Check** — pip-audit, Bandit
- **Unit & Integration Tests** — pytest (SQLite)
- **Migrations Validation** — `alembic upgrade head` em MySQL 8
- **Build** — `docker build` + smoke import
- **CI Gate** — falha se qualquer job obrigatório falhou

## Branch Protection Rules (obrigatório)

Configure no GitHub: **Settings → Branches → Add branch protection rule** para `main`:

- [x] Require a pull request before merging
- [x] Require status checks to pass before merging
  - Selecione: `Lint & Formatting`, `Security Check`, `Unit & Integration Tests`, `Migrations Validation`, `Build`, `CI Gate`
- [x] Require branches to be up to date before merging (recomendado)
- [x] Do not allow bypassing the above settings (recomendado para admins)

Sem isso, o merge pode ocorrer mesmo com CI vermelho.

## Gestão de segredos e ambientes

### Development (local)

- Arquivo: `.env` (copiar de `.env.example`)
- Banco: `docker compose --profile dev up` (MySQL local na porta 3308)
- **Nunca** commitar `.env`

### Staging (quando existir)

- Template: `.env.staging.example` → `.env.staging` no servidor
- Compose: `docker-compose.staging.yml` (porta padrão **8011**)
- Ativar CD staging:
  1. Criar environment `staging` em **Settings → Environments**
  2. Definir variável de repositório `STAGING_ENABLED=true`
  3. Configurar secrets e variables abaixo

### Production

- Secrets no servidor (`.env` no host) — **não** no repositório
- Deploy manual atual: `bash scripts/deploy-prod.sh`
- Deploy automático (opcional): ver seção CD abaixo

### Secrets do GitHub (Settings → Secrets and variables → Actions)

#### Staging (quando ativar)

| Secret | Descrição |
|--------|-----------|
| `STAGING_SSH_HOST` | IP/hostname do servidor staging |
| `STAGING_SSH_USER` | Usuário SSH |
| `STAGING_SSH_KEY` | Chave privada SSH |
| `STAGING_SSH_PORT` | Porta SSH (opcional, padrão 22) |
| `STAGING_DEPLOY_PATH` | Caminho do repo no servidor (ex: `/opt/sistema_preco`) |

#### Production (quando ativar deploy automático)

| Secret | Descrição |
|--------|-----------|
| `PRODUCTION_SSH_HOST` | IP do servidor (ex: host com Docker) |
| `PRODUCTION_SSH_USER` | Usuário SSH |
| `PRODUCTION_SSH_KEY` | Chave privada SSH |
| `PRODUCTION_SSH_PORT` | Porta SSH (opcional) |
| `PRODUCTION_DEPLOY_PATH` | Caminho do repo no servidor |

### Variables do repositório (Settings → Variables)

| Variable | Padrão | Descrição |
|----------|--------|-----------|
| `STAGING_ENABLED` | `false` | `true` quando staging existir |
| `CD_DEPLOY_ENABLED` | `false` | `true` para deploy SSH automático em produção |
| `STAGING_URL` | — | URL pública do staging (ex: `http://staging:8011`) |
| `STAGING_HEALTH_URL` | — | `http://host:8011/health?format=json` |
| `PRODUCTION_URL` | — | URL de produção |
| `PRODUCTION_HEALTH_URL` | — | URL **no servidor** (ex: `http://127.0.0.1:8010/health?format=json`) |

O health check roda via SSH no servidor — use URL acessível de dentro do host, não IP interno da rede corporativa.

### Aprovação manual (Production)

Em **Settings → Environments → production**:

- [x] Required reviewers (tech leader)
- [x] Deployment branches: `main` only

O job `Deploy Production` no CD pausa até aprovação antes do SSH deploy.

## Zero downtime e rollback

Scripts:

- `scripts/deploy-zero-downtime.sh` — migrations → web → worker
- `scripts/rollback-prod.sh` — volta ao SHA em `.deploy-state/last-good-sha`
- `scripts/ci-health-check.sh` — valida `/health?format=json`

Se o health check falha após deploy em produção, o CD executa rollback automático e falha o workflow (e-mail de falha).

**Nota:** com um único container web, há uma janela curta durante `docker compose up`. Para zero downtime absoluto, evoluir para blue-green com load balancer (futuro).

## Notificações por e-mail

O GitHub envia e-mails automaticamente quando:

- Um workflow falha (CI ou CD)
- Um deploy aguarda aprovação (environment `production`)

Configuração pessoal: **GitHub → Settings → Notifications → Actions** — marcar "Send notifications for failed workflows only" ou "All workflows".

Não é necessário Slack/Discord para o fluxo atual.

## Padronização de código (local)

```bash
pip install -r requirements-dev.txt
ruff check tests scripts migrations
black tests scripts migrations
pytest -m smoke
```

Configuração: `.editorconfig`, `pyproject.toml`

## Ativar staging (quando o ambiente existir)

Envie ao time DevOps:

1. Host, porta SSH, path do repositório
2. `DATABASE_URL` do banco staging (isolado de produção)
3. Porta web (sugestão: 8011)
4. URL do health check

O time configura secrets/variables e define `STAGING_ENABLED=true`.

## Ativar deploy automático em produção

1. Configurar secrets `PRODUCTION_*`
2. Criar environment `production` com reviewers
3. Definir `CD_DEPLOY_ENABLED=true`
4. Definir `PRODUCTION_HEALTH_URL`
5. Garantir que o servidor tem git, docker e acesso SSH do GitHub Actions (runner ou IP permitido)

Até isso, o CD apenas **valida o build** após merge; deploy manual continua com `deploy-prod.sh`.
