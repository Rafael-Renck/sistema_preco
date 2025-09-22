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

## Estrutura

- `app.py`: app Flask + modelos SQLAlchemy + rotas.
- `templates/`: páginas HTML (Jinja2) para renderização server-side.
- `static/`: assets estáticos (CSS).
- `docker-compose.yml` e `Dockerfile`: orquestração e imagem do backend.
- `requirements.txt`: dependências Python.

## Próximos passos sugeridos

- Autenticação com hashing de senha (Werkzeug) e sessões.
- CRUD completo (forms e APIs) para Usuários, Operadoras, Tabelas e Procedimentos.
- Upload/parse de tabelas de preços (CSV/Excel) e rotina de comparação.
- Testes automatizados e migrações com Alembic.
