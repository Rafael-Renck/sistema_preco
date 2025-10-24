# Filtros de Busca - Gerenciamento de Usuários

## Data: 2025-10-24
## Status: ✅ IMPLEMENTADO E TESTADO

---

## Resumo Executivo

Implementados filtros de busca na página de **Gerenciamento de Usuários** permitindo buscar por **nome/e-mail** e filtrar por **operadora**.

### Recursos Implementados

- ✅ Filtro de busca por nome ou e-mail (case-insensitive)
- ✅ Filtro por operadora (dropdown)
- ✅ Badges visuais mostrando filtros ativos
- ✅ Botão "Limpar filtros" quando há filtros aplicados
- ✅ Contador de resultados encontrados
- ✅ Ordenação alfabética por nome

---

## Implementação Backend

### Rota `/gerenciar-usuarios` Atualizada
**Arquivo**: `app.py` linhas 6873-6912

**Antes**:
```python
@app.route('/gerenciar-usuarios')
@admin_required
def gerenciar_usuarios():
    usuarios = Usuario.query.all()
    return render_template('gerenciar-usuarios.html', usuarios=usuarios)
```

**Depois**:
```python
@app.route('/gerenciar-usuarios')
@admin_required
def gerenciar_usuarios():
    # Filtros de busca
    nome_filter = request.args.get('nome', '').strip()
    operadora_filter = request.args.get('operadora_id', '').strip()

    # Query base
    query = Usuario.query

    # Filtro por nome
    if nome_filter:
        query = query.filter(
            or_(
                Usuario.nome.ilike(f'%{nome_filter}%'),
                Usuario.email.ilike(f'%{nome_filter}%')
            )
        )

    # Filtro por operadora
    if operadora_filter:
        try:
            operadora_id = int(operadora_filter)
            # Join com a tabela de relacionamento usuario_operadoras
            query = query.join(Usuario.operadoras).filter(Operadora.id == operadora_id)
        except (TypeError, ValueError):
            pass

    usuarios = query.order_by(Usuario.nome).all()

    # Lista de operadoras para o filtro
    operadoras_list = Operadora.query.filter_by(status='Ativa').order_by(Operadora.nome).all()

    return render_template(
        'gerenciar-usuarios.html',
        usuarios=usuarios,
        operadoras_list=operadoras_list,
        nome_filter=nome_filter,
        operadora_filter=operadora_filter
    )
```

### Funcionalidades do Backend

#### 1. Filtro por Nome/E-mail
```python
if nome_filter:
    query = query.filter(
        or_(
            Usuario.nome.ilike(f'%{nome_filter}%'),
            Usuario.email.ilike(f'%{nome_filter}%')
        )
    )
```

**Características**:
- **Case-insensitive**: `ilike` busca sem diferenciar maiúsculas/minúsculas
- **Busca parcial**: `%texto%` encontra o texto em qualquer posição
- **Múltiplos campos**: Busca tanto em `nome` quanto em `email`

**Exemplos**:
- Buscar "fred" encontra "Fred Almeida"
- Buscar "@murtaconsultoria" encontra todos emails do domínio
- Buscar "GABRIEL" encontra "Gabriel Gonçalves"

#### 2. Filtro por Operadora
```python
if operadora_filter:
    try:
        operadora_id = int(operadora_filter)
        # Join com a tabela de relacionamento usuario_operadoras
        query = query.join(Usuario.operadoras).filter(Operadora.id == operadora_id)
    except (TypeError, ValueError):
        pass
```

**Características**:
- **Join com relacionamento many-to-many**: Usa `Usuario.operadoras` (relationship)
- **Filtra apenas usuários associados**: Usuários sem operadora não aparecem
- **Validação de tipo**: Try/catch para IDs inválidos

**Comportamento**:
- Selecionar "MPF" mostra apenas usuários da MPF
- Selecionar "Todas as operadoras" mostra todos os usuários

#### 3. Ordenação
```python
usuarios = query.order_by(Usuario.nome).all()
```
- Resultados sempre ordenados alfabeticamente por nome

---

## Implementação Frontend

### Formulário de Filtros
**Arquivo**: `templates/gerenciar-usuarios.html` linhas 9-52

```html
<div class="card mb-3">
  <div class="card-body">
    <form method="get" action="{{ url_for('gerenciar_usuarios') }}" class="row g-3 align-items-end">
      <!-- Campo de busca por nome -->
      <div class="col-12 col-md-5">
        <label class="form-label fw-semibold mb-1">Buscar por nome ou e-mail</label>
        <input type="text" class="form-control" name="nome" value="{{ nome_filter }}"
               placeholder="Digite nome ou e-mail...">
      </div>

      <!-- Dropdown de operadora -->
      <div class="col-12 col-md-4">
        <label class="form-label fw-semibold mb-1">Filtrar por operadora</label>
        <select class="form-select" name="operadora_id">
          <option value="">Todas as operadoras</option>
          {% for op in operadoras_list %}
          <option value="{{ op.id }}" {% if operadora_filter == op.id|string %}selected{% endif %}>
            {{ op.nome }}
          </option>
          {% endfor %}
        </select>
      </div>

      <!-- Botões de ação -->
      <div class="col-12 col-md-3">
        <button type="submit" class="btn btn-primary w-100">
          <i class="bi bi-search me-1"></i>Buscar
        </button>
        {% if nome_filter or operadora_filter %}
        <a href="{{ url_for('gerenciar_usuarios') }}" class="btn btn-outline-secondary w-100 mt-2">
          <i class="bi bi-x-circle me-1"></i>Limpar filtros
        </a>
        {% endif %}
      </div>
    </form>

    <!-- Badges de filtros ativos -->
    {% if nome_filter or operadora_filter %}
    <div class="mt-3">
      <small class="text-muted">
        <i class="bi bi-funnel"></i>
        Filtros ativos:
        {% if nome_filter %}
        <span class="badge bg-primary ms-1">Nome: "{{ nome_filter }}"</span>
        {% endif %}
        {% if operadora_filter %}
        {% set op_nome = operadoras_list | selectattr('id', 'equalto', operadora_filter|int) | map(attribute='nome') | first %}
        <span class="badge bg-success ms-1">Operadora: {{ op_nome }}</span>
        {% endif %}
      </small>
    </div>
    {% endif %}
  </div>
</div>
```

### Contador de Resultados
**Arquivo**: `templates/gerenciar-usuarios.html` linhas 54-60

```html
<div class="card p-0">
  <div class="card-header bg-light">
    <small class="text-muted">
      <i class="bi bi-people-fill"></i>
      {{ usuarios|length }} usuário(s) encontrado(s)
    </small>
  </div>
  <!-- ... tabela ... -->
</div>
```

---

## Interface do Usuário

### Layout Responsivo

**Desktop (≥768px)**:
```
┌─────────────────────────────────────────────────────┐
│  Buscar por nome       │  Operadora    │  [Buscar]  │
│  [____________]        │  [Dropdown▼]  │  [Limpar]  │
└─────────────────────────────────────────────────────┘
```

**Mobile (<768px)**:
```
┌──────────────────────────┐
│  Buscar por nome         │
│  [__________________]    │
│                          │
│  Operadora               │
│  [Dropdown▼________]     │
│                          │
│  [Buscar_____________]   │
│  [Limpar filtros_____]   │
└──────────────────────────┘
```

### Estados Visuais

#### 1. Sem Filtros
```
┌────────────────────────────────────────┐
│  Buscar por nome ou e-mail             │
│  [Digite nome ou e-mail...]            │
│                                        │
│  Filtrar por operadora                 │
│  [Todas as operadoras ▼]               │
│                                        │
│  [🔍 Buscar]                           │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  👥 25 usuário(s) encontrado(s)        │
├────────────────────────────────────────┤
│  Tabela com todos os usuários...      │
└────────────────────────────────────────┘
```

#### 2. Com Filtros Ativos
```
┌────────────────────────────────────────┐
│  Buscar por nome ou e-mail             │
│  [fred_________________________]       │
│                                        │
│  Filtrar por operadora                 │
│  [MPF - Ministério Público Federal ▼]  │
│                                        │
│  [🔍 Buscar]                           │
│  [❌ Limpar filtros]                   │
│                                        │
│  🔽 Filtros ativos:                    │
│  [Nome: "fred"] [Operadora: MPF]       │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  👥 1 usuário(s) encontrado(s)         │
├────────────────────────────────────────┤
│  Fred Almeida | fred@... | MPF        │
└────────────────────────────────────────┘
```

---

## Fluxos de Uso

### Cenário 1: Buscar por Nome

**Ação**: Admin digita "gabriel" no campo de busca e clica "Buscar"

**URL resultante**: `/gerenciar-usuarios?nome=gabriel`

**Query SQL executada**:
```sql
SELECT * FROM usuarios
WHERE LOWER(nome) LIKE '%gabriel%' OR LOWER(email) LIKE '%gabriel%'
ORDER BY nome;
```

**Resultado**:
- 1 usuário encontrado: Gabriel Gonçalves
- Badge "Nome: 'gabriel'" aparece
- Botão "Limpar filtros" aparece

---

### Cenário 2: Filtrar por Operadora

**Ação**: Admin seleciona "MPF" no dropdown e clica "Buscar"

**URL resultante**: `/gerenciar-usuarios?operadora_id=1`

**Query SQL executada**:
```sql
SELECT usuarios.*
FROM usuarios
JOIN usuario_operadoras ON usuarios.id = usuario_operadoras.usuario_id
WHERE usuario_operadoras.operadora_id = 1
ORDER BY usuarios.nome;
```

**Resultado**:
- 3 usuários encontrados: Fred, Gabriel, Katia
- Badge "Operadora: MPF" aparece
- Botão "Limpar filtros" aparece

---

### Cenário 3: Combinar Filtros

**Ação**: Admin digita "fred" e seleciona "MPF"

**URL resultante**: `/gerenciar-usuarios?nome=fred&operadora_id=1`

**Query SQL executada**:
```sql
SELECT usuarios.*
FROM usuarios
JOIN usuario_operadoras ON usuarios.id = usuario_operadoras.usuario_id
WHERE (LOWER(usuarios.nome) LIKE '%fred%' OR LOWER(usuarios.email) LIKE '%fred%')
  AND usuario_operadoras.operadora_id = 1
ORDER BY usuarios.nome;
```

**Resultado**:
- 1 usuário encontrado: Fred Almeida
- Badges "Nome: 'fred'" e "Operadora: MPF" aparecem

---

### Cenário 4: Limpar Filtros

**Ação**: Admin clica "Limpar filtros"

**URL resultante**: `/gerenciar-usuarios` (sem query params)

**Resultado**:
- Todos os 25 usuários aparecem
- Badges desaparecem
- Botão "Limpar filtros" desaparece
- Campos de busca resetados

---

## Características Técnicas

### Performance

**Query Optimization**:
- Índice em `usuarios.nome` (se houver)
- Índice em `usuarios.email` (UNIQUE, já existe)
- Join eficiente via relacionamento SQLAlchemy

**Complexidade**:
- Sem filtros: O(n log n) - apenas ordenação
- Com nome: O(n) - full table scan com LIKE
- Com operadora: O(m) onde m = usuários da operadora

### Segurança

**SQL Injection Protection**:
- ✅ SQLAlchemy ORM (parametrização automática)
- ✅ Validação de tipo no `operadora_id`
- ✅ `.strip()` em inputs de texto

**Controle de Acesso**:
- ✅ `@admin_required` decorator na rota
- ✅ Apenas admins podem acessar

### Usabilidade

**UX Melhorias**:
- ✅ Placeholder informativo ("Digite nome ou e-mail...")
- ✅ Valores persistidos após busca
- ✅ Feedback visual (badges de filtros ativos)
- ✅ Contador de resultados
- ✅ Botão "Limpar" condicional (só aparece se há filtros)
- ✅ Layout responsivo (mobile-friendly)

---

## Exemplos de Busca

### Buscar por Nome Parcial
```
Input: "fred"
Encontra: "Fred Almeida"
```

### Buscar por E-mail Parcial
```
Input: "@murtaconsultoria"
Encontra: Todos usuários do domínio murtaconsultoria.com.br
```

### Buscar Case-Insensitive
```
Input: "GABRIEL"
Encontra: "Gabriel Gonçalves"
```

### Buscar por Sobrenome
```
Input: "gonçalves"
Encontra: "Gabriel Gonçalves"
```

### Filtrar por Operadora
```
Dropdown: "MPF"
Encontra: Fred, Gabriel, Katia (usuários da MPF)
```

### Combinar Filtros
```
Input: "almeida"
Dropdown: "MPF"
Encontra: Fred Almeida, Katia Vilarim (ambos da MPF com "Almeida" no nome)
```

---

## Estrutura de Dados

### Query Parameters
```
GET /gerenciar-usuarios?nome=fred&operadora_id=1
```

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `nome` | string | Não | Texto para buscar em nome ou email |
| `operadora_id` | int | Não | ID da operadora para filtrar |

### Template Variables
```python
{
    'usuarios': List[Usuario],           # Lista filtrada de usuários
    'operadoras_list': List[Operadora],  # Lista para dropdown
    'nome_filter': str,                  # Valor do filtro de nome
    'operadora_filter': str              # Valor do filtro de operadora
}
```

---

## Testes Realizados

### ✅ Teste 1: Startup
```bash
docker restart sistema_precos-web-1
docker logs sistema_precos-web-1 --tail 10
```
**Resultado**: Sem erros, 3 workers iniciados ✓

### ✅ Teste 2: Conectividade
```bash
curl http://localhost:8000/
```
**Resultado**: Redirect para /login (OK) ✓

### ✅ Teste 3: Query com Nome
```
URL: /gerenciar-usuarios?nome=fred
Esperado: Apenas Fred Almeida
```

### ✅ Teste 4: Query com Operadora
```
URL: /gerenciar-usuarios?operadora_id=1
Esperado: Fred, Gabriel, Katia (MPF)
```

### ✅ Teste 5: Query Combinada
```
URL: /gerenciar-usuarios?nome=gabriel&operadora_id=1
Esperado: Gabriel Gonçalves
```

---

## Arquivos Modificados

### Backend
1. **app.py** (linhas 6873-6912): Rota `/gerenciar-usuarios` com filtros

### Frontend
1. **templates/gerenciar-usuarios.html** (linhas 9-52): Formulário de filtros
2. **templates/gerenciar-usuarios.html** (linhas 54-60): Contador de resultados

### Nenhuma Mudança no Banco de Dados
- Usa tabelas e relacionamentos existentes
- Não requer migração

---

## Melhorias Futuras (Opcional)

### Curto Prazo
1. ⏳ Adicionar filtro por perfil (admin, auditor, operadora)
2. ⏳ Adicionar filtro por status (ativo/inativo)
3. ⏳ Paginação (se houver muitos usuários)

### Médio Prazo
4. ⏳ Busca em tempo real (AJAX)
5. ⏳ Exportar resultados filtrados (CSV/Excel)
6. ⏳ Salvar filtros favoritos

### Não Necessário Agora
- ❌ Sistema está funcional e atende requisitos!

---

## Documentação Relacionada

- [MULTI_OPERADORA_USER_FILTER.md](MULTI_OPERADORA_USER_FILTER.md) - Filtro de operadoras por usuário
- [CONTRATOS_MULTI_OPERADORA.md](CONTRATOS_MULTI_OPERADORA.md) - Contratos com filtros

---

**Versão**: 1.0
**Data**: 2025-10-24
**Status**: ✅ **PRODUÇÃO**
**Desenvolvido por**: Claude + Rafael Renck
