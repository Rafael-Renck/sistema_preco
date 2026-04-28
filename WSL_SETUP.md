# Colocar o projeto no WSL

Este guia explica como usar o **Sistema de Banco de Preços** dentro do WSL (Windows Subsystem for Linux).

## Pré-requisito

- **WSL instalado**: no PowerShell (como Admin): `wsl --install`
- Após instalar, reinicie se necessário e abra o Ubuntu (ou sua distro) pelo menu Iniciar.

---

## Opção 1: Clonar o repositório dentro do WSL (recomendado)

Se o projeto está no GitHub, clone direto no sistema de arquivos do Linux. Assim o desempenho e as ferramentas (Docker, Python, etc.) funcionam melhor.

1. Abra o terminal do WSL (Ubuntu):
   - Digite `wsl` no PowerShell ou
   - Abra "Ubuntu" no menu Iniciar.

2. Crie uma pasta e clone o repositório (troque a URL se for outro repositório):

```bash
mkdir -p ~/projetos
cd ~/projetos
git clone https://github.com/SEU_USUARIO/sistema_preco.git
cd sistema_preco
```

3. Configure o ambiente e suba com Docker:

```bash
cp .env.example .env
# Edite .env se quiser: nano .env
docker compose --profile dev up --build
```

4. Acesse:
   - App: http://localhost:8001  
   - Adminer: http://localhost:8081  

---

## Opção 2: Copiar a pasta do Windows para o WSL

Se você não quer clonar de novo e prefere usar a pasta que já está em `Documents\GitHub\sistema_preco`:

1. No terminal do WSL:

```bash
mkdir -p ~/projetos
cp -r /mnt/c/Users/Rafael\ Renck/Documents/GitHub/sistema_preco ~/projetos/
cd ~/projetos/sistema_preco
```

2. Depois:

```bash
cp .env.example .env
docker compose --profile dev up --build
```

**Observação:** Trabalhar em arquivos que ficam em `/mnt/c/...` (disco do Windows) costuma ser mais lento no WSL. Para desenvolvimento contínuo, a Opção 1 costuma ser melhor.

---

## Opção 3: Só abrir no Cursor a partir do WSL

Você pode continuar com os arquivos no Windows e apenas **abrir a pasta via WSL** no Cursor:

1. No Cursor: **File → Open Folder** (ou **Arquivo → Abrir Pasta**).
2. No seletor de pasta, clique em **WSL** (ou no ícone de conexão remota) e escolha sua distro (ex.: Ubuntu).
3. Navegue até a pasta do projeto no WSL (ex.: `\\wsl$\Ubuntu\home\seu_usuario\projetos\sistema_preco` ou pelo caminho Linux dentro do WSL).

Assim o Cursor usa o ambiente do WSL (terminal, interpretador Python, etc.) mesmo que a pasta esteja no Linux.

---

## Comandos úteis no WSL

| Ação              | Comando |
|-------------------|--------|
| Entrar no WSL     | `wsl` (no PowerShell ou CMD) |
| Ver distros       | `wsl -l -v` |
| Subir app + DB    | `docker compose --profile dev up --build` |
| Parar             | `Ctrl+C` e depois `docker compose --profile dev down` |
| Rodar testes      | `pytest` (com venv ativado ou dentro do container) |

---

## Resumo rápido (recomendado)

```bash
wsl
cd ~
mkdir -p projetos && cd projetos
git clone https://github.com/SEU_USUARIO/sistema_preco.git
cd sistema_preco
cp .env.example .env
docker compose --profile dev up --build
```

Depois acesse **http://localhost:8001** (app) e **http://localhost:8081** (Adminer) no navegador.

---

## Porta 8080 não disponível no WSL

Se aparecer erro do tipo `ports are not available: exposing port TCP 0.0.0.0:8080 ... status: 500`:

1. **Ver o que está usando a 8080**  
   No **PowerShell** (Windows):
   ```powershell
   netstat -ano | findstr :8080
   ```
   No **WSL**:
   ```bash
   ss -tlnp | grep 8080
   ```
   Se aparecer um processo, feche-o ou mate o PID (no Windows: Task Manager; no WSL: `kill <PID>`).

2. **Reiniciar o Docker**  
   Feche o Docker Desktop e abra de novo, ou no PowerShell (como Admin):
   ```powershell
   Restart-Service com.docker.service
   ```
   Depois tente de novo no WSL: `docker compose --profile dev up --build`.

3. **Reiniciar o WSL**  
   No PowerShell (como Admin):
   ```powershell
   wsl --shutdown
   ```
   Abra de novo o Ubuntu/WSL e rode o `docker compose` outra vez.

4. **Garantir que a 8080 está livre no Windows**  
   Às vezes o Windows reserva portas. Abra PowerShell como Admin e confira:
   ```powershell
   netsh interface ipv4 show excludedportrange protocol=tcp
   ```
   Se 8080 estiver em algum intervalo reservado, reiniciar o PC ou desativar/reativar o WSL pode liberar.
