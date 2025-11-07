# 🔌 API Smart Filters - Documentação Técnica

**Data:** 2025-11-04
**Versão:** 1.0
**Status:** ✅ Implementado em app.py

---

## 📍 Endpoints Criados

Três novos endpoints foram adicionados ao **app.py** (linhas 6648-6776) para suportar os smart filters do Consulta & Comparar.

### 1️⃣ `GET /api/tabela-info/<int:table_id>`

**Descrição:** Retorna informações sobre a tabela (tipo CBHPM ou DTP)

**URL:** `http://localhost:5000/api/tabela-info/1`

**Headers Obrigatórios:**
```
Authorization: Bearer <token>
```
(Incluso automaticamente com `@login_required`)

**Parâmetros:**
| Parâmetro | Tipo | Localização | Obrigatório | Descrição |
|-----------|------|------------|-----------|-----------|
| table_id | int | URL Path | ✅ Sim | ID da tabela |

**Resposta (200 OK):**
```json
{
  "id": 1,
  "nome": "CBHPM 2024",
  "tipo": "cbhpm"
}
```

**Resposta (404 Not Found):**
```json
{
  "error": "Tabela não encontrada"
}
```

**Resposta (403 Forbidden):**
```json
{
  "error": "Acesso negado"
}
```

**Código da Aplicação:** [app.py:6652-6681](app.py#L6652-L6681)

---

### 2️⃣ `GET /api/prestadores/<int:table_id>`

**Descrição:** Retorna lista de prestadores únicos da tabela selecionada

**URL:** `http://localhost:5000/api/prestadores/1?uf=SP`

**Headers Obrigatórios:**
```
Authorization: Bearer <token>
```

**Parâmetros:**
| Parâmetro | Tipo | Localização | Obrigatório | Descrição |
|-----------|------|------------|-----------|-----------|
| table_id | int | URL Path | ✅ Sim | ID da tabela |
| uf | string | Query String | ❌ Não | UF para filtro (ex: "SP", "RJ") |

**Exemplos de Request:**
```bash
# Todos os prestadores
GET /api/prestadores/1

# Apenas prestadores de São Paulo
GET /api/prestadores/1?uf=SP
```

**Resposta (200 OK):**
```json
{
  "tabela_id": 1,
  "prestadores": [
    "Hospital A",
    "Hospital B",
    "Clínica C",
    "Consultório D"
  ],
  "total": 4
}
```

**Resposta (404 Not Found):**
```json
{
  "error": "Tabela não encontrada"
}
```

**Nota:** Se não houver prestadores, retorna array vazio: `"prestadores": []`

**Código da Aplicação:** [app.py:6684-6731](app.py#L6684-L6731)

---

### 3️⃣ `GET /api/versoes/<int:table_id>`

**Descrição:** Retorna lista de versões para tabelas CBHPM

**URL:** `http://localhost:5000/api/versoes/1`

**Headers Obrigatórios:**
```
Authorization: Bearer <token>
```

**Parâmetros:**
| Parâmetro | Tipo | Localização | Obrigatório | Descrição |
|-----------|------|------------|-----------|-----------|
| table_id | int | URL Path | ✅ Sim | ID da tabela (apenas para validação) |

**Resposta (200 OK):**
```json
{
  "tabela_id": 1,
  "versoes": [
    "CBHPM 2020",
    "CBHPM 2021",
    "CBHPM 2022",
    "CBHPM 2023",
    "CBHPM 2024"
  ],
  "total": 5
}
```

**Resposta (404 Not Found):**
```json
{
  "error": "Tabela não encontrada"
}
```

**Nota:** Se não houver versões CBHPM, retorna array vazio: `"versoes": []`

**Código da Aplicação:** [app.py:6734-6775](app.py#L6734-L6775)

---

## 🔐 Segurança

Todos os endpoints incluem:

### 1. **@login_required**
Verifica que o usuário está autenticado. Se não estiver, redireciona para login.

### 2. **Multi-operadora**
Cada endpoint valida que o usuário tem acesso à tabela:
```python
operadora_id = session.get('operadora_id')
if operadora_id and tabela.id_operadora != operadora_id:
    return jsonify({'error': 'Acesso negado'}), 403
```

### 3. **Validação de Input**
- IDs são validados pelo tipo de parâmetro (int)
- Campos de texto são escapados automaticamente pelo ORM

---

## 🔄 Fluxo de Integração Frontend

```
1. Usuário seleciona Tabela
        ↓
2. onChange trigger → filterManager.onTabelaChange()
        ↓
3. POST /api/tabela-info/{id}
        ↓
4. Frontend recebe tipo (CBHPM ou DTP)
        ↓
5a. Se DTP → POST /api/prestadores/{id}
5b. Se CBHPM → POST /api/versoes/{id}
        ↓
6. Renderiza checkboxes dinâmicas
        ↓
7. Atualiza placeholder do input
```

---

## 🧪 Testes Manual (com curl)

### Pré-requisito
Você precisa estar autenticado (ter um cookie de sessão).

### Teste 1: Verificar tipo de tabela

```bash
# Assumindo que você está logado no navegador
# Obtenha o cookie da sessão

# Via curl (de uma aba do navegador autenticada):
curl -b "session=<seu_cookie>" \
  http://localhost:5000/api/tabela-info/1 \
  | json_pp
```

**Resposta Esperada:**
```json
{
  "id": 1,
  "nome": "Nome da Tabela",
  "tipo": "cbhpm"
}
```

### Teste 2: Carregar prestadores

```bash
curl -b "session=<seu_cookie>" \
  "http://localhost:5000/api/prestadores/1?uf=SP" \
  | json_pp
```

**Resposta Esperada:**
```json
{
  "tabela_id": 1,
  "prestadores": ["Hospital A", "Clínica B"],
  "total": 2
}
```

### Teste 3: Carregar versões CBHPM

```bash
curl -b "session=<seu_cookie>" \
  http://localhost:5000/api/versoes/1 \
  | json_pp
```

**Resposta Esperada:**
```json
{
  "tabela_id": 1,
  "versoes": ["CBHPM 2022", "CBHPM 2023"],
  "total": 2
}
```

---

## 📊 Consultas SQL Utilizadas

### /api/tabela-info/<int:table_id>
```sql
SELECT id, nome, tipo_tabela FROM tabelas WHERE id = ?
```

### /api/prestadores/<int:table_id>
```sql
SELECT DISTINCT prestador FROM procedimentos
WHERE id_tabela = ?
  AND prestador IS NOT NULL
  AND prestador != ''
  AND operadora_id = ?
ORDER BY prestador ASC
```

### /api/versoes/<int:table_id>
```sql
SELECT DISTINCT nome FROM tabelas
WHERE tipo_tabela = 'cbhpm'
  AND id_operadora = ?
ORDER BY nome ASC
```

---

## ⚡ Performance

### Índices Recomendados
Para otimizar as queries, considere adicionar estes índices ao banco:

```sql
-- Para /api/prestadores
CREATE INDEX idx_procedimentos_tabela_operadora
  ON procedimentos(id_tabela, operadora_id);

CREATE INDEX idx_procedimentos_prestador
  ON procedimentos(prestador);

-- Para /api/versoes
CREATE INDEX idx_tabelas_tipo_operadora
  ON tabelas(tipo_tabela, id_operadora);
```

### Tempo de Resposta Esperado
- `/api/tabela-info/<id>`: ~1-2ms (lookup direto)
- `/api/prestadores/<id>`: ~10-50ms (depende de volume)
- `/api/versoes/<id>`: ~5-20ms (depende de quantidade de versões)

---

## 🐛 Troubleshooting

### Problema 1: "Acesso negado" (403)
**Causa:** Seu operadora_id não corresponde ao da tabela
**Solução:** Verifique que:
1. Você está logado como usuário correto
2. A tabela pertence à sua operadora
3. O campo `id_operadora` na tabela está preenchido

### Problema 2: Array vazio de prestadores
**Verificar:**
1. A tabela realmente existe? (`/api/tabela-info/{id}`)
2. Há procedimentos com `prestador` preenchido?
   ```sql
   SELECT COUNT(*) FROM procedimentos
   WHERE id_tabela = 1 AND prestador IS NOT NULL;
   ```
3. Os procedimentos pertencem à sua operadora?

### Problema 3: Erro 404
**Causa:** Tabela não existe ou ID inválido
**Solução:** Verificar ID da tabela no banco:
```sql
SELECT id, nome FROM tabelas LIMIT 5;
```

---

## 📝 Histórico

| Data | Versão | Alteração |
|------|--------|-----------|
| 2025-11-04 | 1.0 | Endpoints criados e testados |

---

## ✅ Checklist de Implementação

- [x] Endpoint `/api/tabela-info/<id>` criado
- [x] Endpoint `/api/prestadores/<id>` criado
- [x] Endpoint `/api/versoes/<id>` criado
- [x] Segurança com @login_required
- [x] Multi-operadora validado
- [x] Tratamento de erros
- [x] Documentação completa
- [ ] Testes de carga executados
- [ ] Índices de banco criados (opcional)
- [ ] Deploy em produção

---

## 🔗 Referências

- [SMART_FILTERS_GUIDE.md](SMART_FILTERS_GUIDE.md) - Guia de implementação frontend
- [CONSULTA_COMPARAR_NOVO.md](CONSULTA_COMPARAR_NOVO.md) - Documentação da interface
- [app.py:6648-6776](app.py#L6648-L6776) - Código-fonte dos endpoints

---

**Criado em:** 2025-11-04
**Status:** ✅ Pronto para Integração
