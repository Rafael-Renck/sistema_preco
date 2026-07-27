# Diretrizes de Migrations (Alembic)

Este documento define o fluxo obrigatório para evitar inconsistência de dados e downtime durante deploys.

## Princípio: Expand-Contract (retrocompatibilidade)

Toda migration deve ser **backward compatible**:

| Fase | Código | Banco | Regra |
|------|--------|-------|-------|
| Expand | Novo | Antigo | Código novo funciona com schema antigo |
| Migrate | Novo | Novo | Migration aplicada; código antigo ainda funciona |
| Contract | Novo | Novo | Remover colunas/índices obsoletos em migration **posterior** |

### Exemplos permitidos (Expand)

- Adicionar coluna **nullable** ou com **default**
- Adicionar tabela nova
- Adicionar índice (online, se possível)
- Renomear coluna em duas migrations: (1) adicionar nova, (2) remover antiga depois do deploy

### Exemplos proibidos em uma única release

- Remover coluna usada pelo código em produção
- Alterar tipo de coluna sem conversão segura
- `NOT NULL` sem default em coluna existente com dados
- Renomear tabela/coluna sem período de transição

## Quando as migrations rodam

| Momento | O que acontece |
|---------|----------------|
| **CI (PR)** | `alembic upgrade head` em MySQL 8 temporário — valida sintaxe e cadeia de revisões |
| **CD Staging** | `alembic upgrade head` **antes** de recriar containers (`deploy-zero-downtime.sh`) |
| **CD Production** | `migrate-prod.sh` **antes** do rolling update do `web_prod` |

Ordem no deploy: **migration → web → worker**. O worker importa código novo após o web já estar saudável.

## Checklist do desenvolvedor (PR)

1. Migration testada localmente: `alembic upgrade head`
2. Código funciona **sem** a migration (rollback mental) ou migration é apenas expand
3. Revisão única: `alembic heads` retorna uma única head
4. Nome do arquivo: `YYYYMMDD_NN_descricao_curta.py`

## Rollback de migration

- **Preferido:** migration forward-only + nova migration corretiva
- **Downgrade:** só se `downgrade()` foi implementado e testado — nunca em produção sem plano

Se o deploy falha no health check, `scripts/rollback-prod.sh` reverte o **código** ao último SHA estável. Migrations já aplicadas **não** são revertidas automaticamente — por isso expand-contract é obrigatório.

## Comandos úteis

```bash
# Local com MySQL dev
export DATABASE_URL=mysql+pymysql://root:rootpassword@localhost:3308/operadora_saude
export SKIP_ENSURE_DB=1
alembic -c alembic.ini upgrade head
alembic -c alembic.ini current

# Produção (no servidor)
bash scripts/migrate-prod.sh
```
