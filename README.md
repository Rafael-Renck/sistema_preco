# Sistema de Banco de Preços (Operadora de Saúde)

Projeto Flask + MySQL, dockerizado, com páginas para login, dashboard e gerenciamento inicial de usuários, operadoras e tabelas.

O repositório inclui uma esteira de **CI/CD via GitHub Actions**: validação automática em Pull Requests e entrega contínua após merge na `main`.

---

## CI/CD — visão geral

### Fluxo do time no GitHub

Este é o processo operacional do projeto:

```
Contribuidor                    Tech leader                         GitHub Actions
     │                               │                                    │
     ├─ Cria branch ────────────────►│                                    │
     ├─ Commits + abre PR ──────────►│                                    │
     │                               ├─ Revisa o código                   │
     │                               │                                    ├─ CI dispara no PR
     │                               │                                    ├─ Jobs rodam em paralelo
     │                               ├─ Aguarda tudo verde ✅             │
     │                               ├─ Clica "Merge" na GitHub ────────►├─ CD dispara na main
     │                               │                                    ├─ Build validado
     │                               │                                    └─ Deploy (se configurado)
     └─ Recebe e-mail se CI falhou ──┴─ Recebe e-mail no merge/CD ───────┘
```

1. O **contribuidor** cria uma branch, faz commits e abre um **Pull Request** contra `main`.
2. O GitHub Actions executa o workflow **CI** automaticamente no PR.
3. O **tech leader** revisa o código e aguarda todos os jobs ficar verdes.
4. Com o CI aprovado, o tech leader confirma o **merge** diretamente no GitHub.
5. O merge na `main` dispara o workflow **CD** (build + deploy opcional).
6. **E-mails** do GitHub notificam falhas de workflow ou sucesso do pipeline (configurar em GitHub → Settings → Notifications → Actions).

Documentação detalhada: [`docs/CI_CD.md`](docs/CI_CD.md) · Migrations: [`docs/MIGRATIONS.md`](docs/MIGRATIONS.md)

### Workflows

| Arquivo | Quando roda | Objetivo |
|---------|-------------|----------|
| `.github/workflows/ci.yml` | PR e push em `main` | Integração contínua — qualidade e testes |
| `.github/workflows/cd.yml` | Push em `main` (após merge) | Entrega contínua — build e deploy |

### CI — Integração Contínua (`ci.yml`)

Disparado em **cada Pull Request** para `main`. Os jobs rodam **isolados** (em paralelo); todos precisam passar para o merge ser seguro.

| Job | O que valida |
|-----|----------------|
| **Lint & Formatting** | Ruff (lint), Black (formato), sintaxe Python (`compileall`) |
| **Security Check** | `pip-audit` (vulnerabilidades em dependências), Bandit (SAST) |
| **Unit & Integration Tests** | `pytest` com SQLite isolado + smoke tests (`/health`, `/login`) |
| **Migrations Validation** | `alembic upgrade head` em MySQL 8 temporário (CI) |
| **Build** | `docker build` + smoke import do módulo `app` |
| **CI Gate** | Falha o pipeline se qualquer job obrigatório falhou |

**Migrations no CI:** o job valida que a cadeia Alembic é consistente e que todas as migrations aplicam sem erro. Isso **não altera** bancos de produção — apenas um MySQL ephemeral no runner do GitHub.

### CD — Entrega Contínua (`cd.yml`)

Disparado **somente após merge na `main`**.

| Job | Condição | O que faz |
|-----|----------|-----------|
| **Build & Validate** | Sempre | Constrói imagem Docker e valida import da app |
| **Deploy Staging** | `STAGING_ENABLED=true` | SSH → `deploy-zero-downtime.sh staging` → health check |
| **Deploy Production** | `CD_DEPLOY_ENABLED=true` | Aguarda aprovação manual → SSH deploy → health check → rollback se falhar |
| **CD Summary** | Sempre | Resumo no Job Summary do Actions |

**Estado atual (padrão):**

- Staging **desabilitado** — ambiente ainda não existe; template pronto em `docker-compose.staging.yml` e `.env.staging.example`.
- Deploy automático em produção **desabilitado** — deploy manual com `bash scripts/deploy-prod.sh` no servidor.
- Após merge, o CD **valida o build** e registra o resumo; não altera produção até você ativar os secrets.

**Ordem no deploy (quando ativado):** migrations → container `web` → container `worker` → health check em `/health?format=json`.

### Proteção da branch `main`

Configure em **Settings → Branches → Branch protection rule**:

- Require a pull request before merging
- Require status checks: `Lint & Formatting`, `Security Check`, `Unit & Integration Tests`, `Migrations Validation`, `Build`, `CI Gate`
- Require branches to be up to date before merging (recomendado)

Sem isso, o merge pode ocorrer mesmo com CI vermelho.

### Ambientes e segredos

| Ambiente | Configuração | Onde |
|----------|--------------|------|
| **Development** | `.env` (copiar de `.env.example`) | Local / Docker dev |
| **Staging** | `.env.staging.example` → `.env.staging` | Servidor de homologação (futuro) |
| **Production** | `.env` no servidor | Nunca no GitHub |

Secrets e variáveis do GitHub Actions (`STAGING_*`, `PRODUCTION_*`, `STAGING_ENABLED`, `CD_DEPLOY_ENABLED`) — ver [`docs/CI_CD.md`](docs/CI_CD.md).

### Padronização de código (local)

Antes de abrir o PR, rode localmente:

```bash
pip install -r requirements-dev.txt
ruff check tests scripts migrations
black --check tests scripts migrations
pytest -m smoke
```

Configuração: `.editorconfig`, `pyproject.toml`

### Scripts de deploy e rollback

| Script | Uso |
|--------|-----|
| `scripts/deploy-prod.sh` | Deploy manual em produção (fluxo atual) |
| `scripts/deploy-zero-downtime.sh` | Deploy com migrations antes da troca de containers (CI/CD) |
| `scripts/rollback-prod.sh` | Rollback ao último commit estável |
| `scripts/ci-health-check.sh` | Health check HTTP usado no CD |
| `scripts/migrate-prod.sh` | Aplica migrations Alembic em produção |

### Migrations (retrocompatibilidade)

Todas as migrations devem ser **backward compatible** (expand-contract): código novo funciona com banco antigo; código antigo funciona com banco novo até o deploy completar.

- **CI:** valida `alembic upgrade head`
- **CD/produção:** migrations rodam **antes** da atualização dos containers

Detalhes: [`docs/MIGRATIONS.md`](docs/MIGRATIONS.md)

---

## Rodar com Docker

1. Copie o exemplo de variáveis de ambiente:
   - `cp .env.example .env` (ou crie manualmente)
2. Suba os serviços de desenvolvimento:
   - `docker compose --profile dev up --build`
3. Acesse:
   - App: http://localhost:8010
   - Adminer (opcional, dev): http://localhost:8081 (Servidor: `db`, Usuário: `root`, Senha: `rootpassword`)

O backend usa `DATABASE_URL` (definida no compose) e cria as tabelas automaticamente na inicialização.
Se não houver usuários, ele cria um admin inicial definido por variáveis de ambiente (`ADMIN_EMAIL`, `ADMIN_PASSWORD`).

### Produção (servidor)

```bash
docker-compose -p sistema_preco_prod -f docker-compose.prod.yml up -d --build
bash scripts/deploy-prod.sh
```

App em produção: porta **8010** (configurável no compose).

---

## Rodar localmente (sem Docker)

1. Crie e ative um virtualenv (opcional).
2. Instale dependências: `pip install -r requirements.txt`.
3. Garanta um MySQL rodando localmente com um banco `operadora_saude`.
4. Ajuste a `DATABASE_URL` no ambiente ou edite o fallback em `app.py`.
5. Rode: `flask --app app run` e acesse http://localhost:5000.

### Login padrão

- E-mail: valor de `ADMIN_EMAIL` (padrão `admin@local`)
- Senha: valor de `ADMIN_PASSWORD` (padrão `admin123`)

---

## Segurança

- Hash de senha com PBKDF2 (`werkzeug.security`), histórico recente para evitar reutilização e migração automática dos cadastros existentes.
- Política de complexidade configurável (tamanho mínimo, combinações de caracteres) aplicada em criação, edição e troca de senha.
- Bloqueio temporário após múltiplas falhas consecutivas, com auditoria completa de sucessos, falhas, bloqueios e mudanças sensíveis (senha, permissões).
- Sessões rotacionadas em cada login/logout, expiração configurada (`PERMANENT_SESSION_LIFETIME`) e invalidação imediata quando a senha é trocada ou o usuário sai.
- Trilha de auditoria disponível para administradores no menu **Auditoria**, com filtros por evento, usuário, IP e datas.

---

## Estrutura

| Caminho | Descrição |
|---------|-----------|
| `app.py` | App Flask + modelos SQLAlchemy + rotas |
| `templates/` | Páginas HTML (Jinja2) |
| `static/` | Assets estáticos (CSS, JS) |
| `migrations/` | Migrations Alembic |
| `tests/` | Testes pytest |
| `docker-compose.yml` | Dev (`--profile dev`) e prod (`--profile prod`) |
| `docker-compose.prod.yml` | Produção isolada (`sistema_preco_prod`) |
| `docker-compose.staging.yml` | Template staging (porta 8011) |
| `Dockerfile` | Imagem do backend |
| `.github/workflows/` | Workflows CI e CD |
| `docs/CI_CD.md` | Documentação operacional da esteira |
| `docs/MIGRATIONS.md` | Diretrizes de migrations |
| `pyproject.toml` | Ruff, Black, pytest, Bandit |
| `requirements.txt` | Dependências de produção |
| `requirements-dev.txt` | Ferramentas de qualidade (CI/local) |

---

## Testes

Suite automatizada com **pytest**. No CI roda em cada PR; localmente:

```bash
pip install -r requirements.txt
pytest                          # suite completa
pytest -m smoke                 # health check + smoke da API
pytest tests/test_insumos.py    # módulo específico
```

Os testes usam SQLite em disco temporário — nenhuma base MySQL de produção é alterada.

**Smoke tests** (`tests/test_health_smoke.py`):

- `GET /health?format=json` — estrutura e status da API
- `GET /login` — página de login responde
- Import do módulo `app` — validação de build

**CI:** a suite completa roda no GitHub Actions; dois testes CBHPM ficam temporariamente ignorados no CI (dependências `cbhpm_import` / `cbhpm_pricing` ausentes no repo).

---

## Simpro & Brasíndice

O sistema agora possui um módulo completo de consulta aos insumos do Brasíndice e do SIMPRO:

- Menu lateral **Simpro & Brasíndice** exibindo resumos por origem, filtros avançados (termo, versão, TUSS/TISS, fabricante) e paginação dinâmica.
- Exportação direta da busca para XLSX (`/insumos/export/xlsx`).
- Importação web (apenas administradores) com suporte a TXT delimitado ou largura fixa – os arquivos JSON de mapeamento podem ser enviados junto ao upload.
- Feedback visual de erros/sucesso durante a importação.

### CLI de importação

As importações também podem ser executadas via CLI Flask (útil para cargas grandes ou automações).

#### Brasíndice (`bras:import`)

A rotina cria um pipeline completo: staging (`bras_raw`), staging de largura fixa (`bras_fixed_stage`), view normalizada (`bras_item_v`), materialização tipada (`bras_item_n`) e atualização do índice global (`insumos_index`).

```
flask bras:import --file caminho/bras.txt --versao 2025-09 \
    --format delimited --delimiter ';' --quotechar '"' --lines-terminated '\n'

flask bras:import --file caminho/bras_fixed.txt --versao 2025-09 \
    --format fixed --map bras_fixed.json
```

Opções principais:

- `--format`: `delimited` (padrão) ou `fixed`.
- `--delimiter`, `--quotechar`, `--no-header`, `--lines-terminated` para ajustar TXT delimitado.
- `--map`: JSON com configurações extras. Para largura fixa defina `columns` com `{ "name": "col01", "start": 1, "length": 10 }` etc. Também é possível informar `encoding`, `lines_terminated`, `skip_header` ou `disable_load_data`.
- `--truncate`: limpa `bras_raw`, `bras_item_n`, `bras_fixed_stage` e remove itens BRAS do índice antes de carregar.
- `--encoding`: força a codificação (UTF-8/Latin-1/Windows-1252). Caso omita, o loader tenta automaticamente múltiplas opções.

Fluxo resumido:

1. O arquivo é carregado em `bras_raw` (via `LOAD DATA LOCAL INFILE`; fallback Python/csv quando Local Infile estiver desligado).
2. Opcionalmente, um arquivo de largura fixa passa primeiro por `bras_fixed_stage` antes de ser decomposto em `bras_raw`.
3. A view `bras_item_v` normaliza e converte os números (PMC/PFB, alíquota, etc.).
4. Os dados são materializados em `bras_item_n` e o índice unificado (`insumos_index`) recebe upsert automático para os itens BRAS.

#### SIMPRO (`simpro:import`)

Permanece com o fluxo anterior, escrevendo direto na tabela tipada `simpro_item` e atualizando o índice (triggers existentes). Exemplo:

```
flask simpro:import --file caminho/simpro.txt --versao 2025-09 --data 2025-09-01 \
    --format fixed --map config.json --uf RJ --aliquota 12
```

As mesmas opções de delimitador, mapa e encoding são válidas. No SIMPRO os campos `--uf` e `--aliquota` ainda alimentam metadados do índice.

> **Importação manual (largura fixa)**: para executar diretamente no MySQL sem passar pela CLI,
> utilize o roteiro em `sql/simpro_fixed_pipeline.sql`, que inclui criação de tabelas de staging,
> comandos `LOAD DATA`, normalização com `INSERT ... SELECT`, rotinas de reimportação e consultas
> básicas de validação.

> **Novo pipeline SIMPRO**: o app agora grava os arquivos de largura fixa em `simpro_fixed_stage`
> e materializa os campos normalizados em `simpro_item_norm` (códigos, quatro preços, validade
> ANVISA, situação etc.). Rode `flask db upgrade` para criar as tabelas e reimporte usando um
> mapa JSON com os offsets (`codigo`, `descricao`, `preco1` … `preco4`, `validade_anvisa`, `ean`).

As mesmas regras valem para o formulário web (campos espelham as flags da CLI). O import de Brasíndice agora aceita também um arquivo de mapeamento JSON para largura fixa diretamente na interface.

---

## Simulador CBHPM: redutor individual e teto

- O redutor por via de entrada passou a ser individual por procedimento. A tabela e o PDF informam o percentual usado em cada linha.
- O cálculo exibe alertas quando o total ultrapassa o valor teto cadastrado em `cbhpm_teto`:
  - Na tela: badge/alerta em vermelho e detalhamento do excedente.
  - No PDF/XLSX: colunas adicionais (Teto / Excedente) e seção explicativa “Explicação do cálculo, redutor aplicado e regra de teto”.
- A explicação do filme radiológico ganhou um passo-a-passo explicitando fator, valor unitário e incidências.

---

## Próximos passos sugeridos

- Configurar Branch Protection em `main` com os status checks do CI.
- Ativar ambiente de staging quando disponível (`STAGING_ENABLED=true`).
- Autenticação multifator (ex.: TOTP ou WebAuthn) e alertas em tempo real de acesso suspeito.
- Restaurar testes CBHPM ignorados no CI (módulos `cbhpm_import` / `cbhpm_pricing`).
