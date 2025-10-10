# Sistema de Banco de Preços (Operadora de Saúde)

Projeto Flask + MySQL, dockerizado, com páginas para login, dashboard e gerenciamento inicial de usuários, operadoras e tabelas.

## Rodar com Docker

1. Copie o exemplo de variáveis de ambiente:
   - `cp .env.example .env` (ou crie manualmente)
2. Suba os serviços:
   - `docker compose up --build`
3. Acesse:
   - App: http://localhost:8000
   - Adminer (opcional): http://localhost:8080 (Servidor: `db`, Usuário: `root`, Senha: `rootpassword`)

O backend usa `DATABASE_URL` (definida no compose) e cria as tabelas automaticamente na inicialização.
Se não houver usuários, ele cria um admin inicial definido por variáveis de ambiente (`ADMIN_EMAIL`, `ADMIN_PASSWORD`).

## Rodar localmente (sem Docker)

1. Crie e ative um virtualenv (opcional).
2. Instale dependências: `pip install -r requirements.txt`.
3. Garanta um MySQL rodando localmente com um banco `operadora_saude`.
4. Ajuste a `DATABASE_URL` no ambiente ou edite o fallback em `app.py`.
5. Rode: `flask --app app run` e acesse http://localhost:5000.

### Login padrão

- E-mail: valor de `ADMIN_EMAIL` (padrão `admin@local`)
- Senha: valor de `ADMIN_PASSWORD` (padrão `admin123`)

## Segurança

- Hash de senha com PBKDF2 (`werkzeug.security`), histórico recente para evitar reutilização e migração automática dos cadastros existentes.
- Política de complexidade configurável (tamanho mínimo, combinações de caracteres) aplicada em criação, edição e troca de senha.
- Bloqueio temporário após múltiplas falhas consecutivas, com auditoria completa de sucessos, falhas, bloqueios e mudanças sensíveis (senha, permissões).
- Sessões rotacionadas em cada login/logout, expiração configurada (`PERMANENT_SESSION_LIFETIME`) e invalidação imediata quando a senha é trocada ou o usuário sai.
- Trilha de auditoria disponível para administradores no menu **Auditoria**, com filtros por evento, usuário, IP e datas.

## Estrutura

- `app.py`: app Flask + modelos SQLAlchemy + rotas.
- `templates/`: páginas HTML (Jinja2) para renderização server-side.
- `static/`: assets estáticos (CSS).
- `docker-compose.yml` e `Dockerfile`: orquestração e imagem do backend.
- `requirements.txt`: dependências Python.

## Próximos passos sugeridos

- Autenticação multifator (ex.: TOTP ou WebAuthn) e alertas em tempo real de acesso suspeito.
- CRUD completo (forms e APIs) para Usuários, Operadoras, Tabelas e Procedimentos.
- Upload/parse de tabelas de preços (CSV/Excel) e rotina de comparação.
- Testes automatizados e migrações com Alembic.

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
> mapa JSON com os offsets (`codigo`, `descricao`, `preco1` … `preco4`, `validade_anvisa`, `ean`).

As mesmas regras valem para o formulário web (campos espelham as flags da CLI). O import de Brasíndice agora aceita também um arquivo de mapeamento JSON para largura fixa diretamente na interface.

## Simulador CBHPM: redutor individual e teto

- O redutor por via de entrada passou a ser individual por procedimento. A tabela e o PDF informam o percentual usado em cada linha.
- O cálculo exibe alertas quando o total ultrapassa o valor teto cadastrado em `cbhpm_teto`:
  - Na tela: badge/alerta em vermelho e detalhamento do excedente.
  - No PDF/XLSX: colunas adicionais (Teto / Excedente) e seção explicativa “Explicação do cálculo, redutor aplicado e regra de teto”.
- A explicação do filme radiológico ganhou um passo-a-passo explicitando fator, valor unitário e incidências.

## Testes

Foi iniciado um conjunto de testes automatizados (pytest) cobrindo:

- Busca e detalhamento de insumos (`/insumos/search` e `/insumos/<origem>/<id>`).
- Cálculo CBHPM com alerta de teto (função `_compute_simulacao_cbhpm`).

Execute-os com:

```
pytest
```

Os testes utilizam SQLite em disco temporário para isolamento – nenhuma base MySQL é alterada.
