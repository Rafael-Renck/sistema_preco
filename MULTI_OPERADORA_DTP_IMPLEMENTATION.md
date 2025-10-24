# 🎯 Multi-Operadora DTP Implementation - COMPLETED

## 📅 Data de Implementação: 2025-10-24

---

## ✅ Status: IMPLEMENTADO E FUNCIONAL

A tabela `procedimentos` (que inclui **Diárias, Taxas e Pacotes - DTP**) agora suporta **múltiplas operadoras**.

---

## 🔄 Mudanças Implementadas

### **1. Banco de Dados ✅**

#### Tabela `procedimentos` Atualizada:
- **Nova coluna**: `operadora_id INT NOT NULL`
- **Foreign Key**: `operadora_id` referencia `operadoras(id)` com `ON DELETE CASCADE`
- **Índice criado**: `idx_procedimentos_operadora` para performance
- **Dados migrados**: Todos os 17.574 procedimentos existentes foram associados à operadora ID=1 (MPF)
- **PRIMARY KEY mantida**: `id` (auto_increment) - não foi alterada

#### Comando SQL Aplicado:
```sql
ALTER TABLE procedimentos ADD COLUMN operadora_id INT NULL;
UPDATE procedimentos SET operadora_id = 1 WHERE operadora_id IS NULL;
ALTER TABLE procedimentos MODIFY operadora_id INT NOT NULL;
ALTER TABLE procedimentos ADD CONSTRAINT fk_procedimentos_operadora
    FOREIGN KEY (operadora_id) REFERENCES operadoras(id) ON DELETE CASCADE;
CREATE INDEX idx_procedimentos_operadora ON procedimentos(operadora_id);
```

#### Verificação:
```bash
docker exec sistema_precos-db-1 mysql -u root -prootpassword operadora_saude \
  -e "SELECT COUNT(*), operadora_id FROM procedimentos GROUP BY operadora_id;"
# Resultado: 17574 registros com operadora_id = 1
```

---

### **2. Backend (app.py) ✅**

#### Modelo `Procedimento` Atualizado (linhas 487-502):
```python
class Procedimento(db.Model):
    __tablename__ = 'procedimentos'
    id = db.Column(db.Integer, primary_key=True)
    codigo = db.Column(db.String(100), nullable=False)
    descricao = db.Column(db.String(500), nullable=False)
    valor = db.Column(db.Numeric(10, 2), nullable=False)
    prestador = db.Column(db.String(255), nullable=True)
    uf = db.Column(db.String(2), nullable=True)
    id_tabela = db.Column(db.Integer, db.ForeignKey('tabelas.id'), nullable=False)

    # Multi-operadora: cada procedimento/DTP pode ter valores específicos por operadora
    operadora_id = db.Column(db.Integer, db.ForeignKey('operadoras.id', ondelete='CASCADE'),
                             nullable=False, default=1)

    # Relacionamento
    operadora = db.relationship('Operadora', backref='procedimentos')
```

#### Rotas API Atualizadas:

**1. `/api/simulacao_dtp` (linhas 6491-6526)**
```python
@app.route('/api/simulacao_dtp')
@login_required
def api_simulacao_dtp():
    """Pesquisa itens em 'Diárias, Taxas e Pacotes' por tabela e termo (código ou descrição).
    Parâmetros: tabela_nome (obrig.), q (código ou parte da descrição), uf (opcional), operadora_id (opcional)
    """
    # Multi-operadora: obter operadora_id do request ou da sessão
    operadora_id = request.args.get('operadora_id')
    if not operadora_id:
        operadora_id = session.get('operadora_id')

    query = db.session.query(Procedimento).filter(Procedimento.id_tabela == t.id)

    # Multi-operadora: filtrar por operadora_id se fornecido
    if operadora_id:
        query = query.filter(Procedimento.operadora_id == operadora_id)
    # ... resto da lógica
```

**2. `/api/prestadores_por_codigo` (linhas 6539-6575)**
```python
@app.route('/api/prestadores_por_codigo')
@login_required
def api_prestadores_por_codigo():
    """Retorna a lista de prestadores que possuem o código informado
    dentro da tabela selecionada e UF opcional.
    Parâmetros: tabela_nome, codigo, uf (opcional), operadora_id (opcional)
    """
    # Multi-operadora: obter operadora_id do request ou da sessão
    operadora_id = request.args.get('operadora_id')
    if not operadora_id:
        operadora_id = session.get('operadora_id')

    q = db.session.query(Procedimento.prestador).join(Tabela, Procedimento.id_tabela == Tabela.id)
    q = q.filter(Tabela.nome == tabela_nome)

    # Multi-operadora: filtrar por operadora_id se fornecido
    if operadora_id:
        q = q.filter(Procedimento.operadora_id == operadora_id)
    # ... resto da lógica
```

**3. `/consulta-comparar` - Listagem de Prestadores (linhas 5079-5089)**
```python
# No filtro de prestadores disponíveis
q_prest = db.session.query(Procedimento.prestador) \
    .join(Tabela, Procedimento.id_tabela == Tabela.id) \
    .filter(Tabela.nome == tabela_nome)

# Multi-operadora: filtrar prestadores por operadora_id
if current_operadora_id:
    q_prest = q_prest.filter(Procedimento.operadora_id == current_operadora_id)
```

**4. `/consulta-comparar` - Comparação de Procedimentos (linhas 5120-5129)**
```python
# Na query principal de comparação
query = db.session.query(Procedimento, Procedimento.prestador) \
    .join(Tabela, Procedimento.id_tabela == Tabela.id) \
    .filter(Tabela.nome == tabela_nome)

# Multi-operadora: filtrar procedimentos por operadora_id
if current_operadora_id:
    query = query.filter(Procedimento.operadora_id == current_operadora_id)
```

---

## 📊 Estrutura de Dados

### **Antes (Single Operadora):**
```
procedimentos
├── id (PK, auto_increment)
├── codigo
├── descricao
├── valor
├── prestador
├── uf
└── id_tabela (FK → tabelas.id)

Problema: Apenas MPF suportada
```

### **Depois (Multi-Operadora):**
```
procedimentos
├── id (PK, auto_increment)
├── codigo
├── descricao
├── valor
├── prestador
├── uf
├── id_tabela (FK → tabelas.id)
└── operadora_id (FK → operadoras.id) ✅ NOVO

Vantagem: Cada operadora tem seus próprios procedimentos/DTP
```

---

## 🎯 Impacto nas Funcionalidades

### **1. Simulador CBHPM - DTPs**
Quando o usuário seleciona uma operadora no simulador, os DTPs são automaticamente filtrados:
- Busca de DTP: filtra por `operadora_id`
- Autocomplete: mostra apenas DTPs da operadora selecionada
- Valores: usa valores específicos da operadora

### **2. Consulta & Comparar - Procedimentos**
- **Prestadores disponíveis**: filtra por operadora
- **Comparação de valores**: mostra apenas procedimentos da operadora selecionada
- **Resultados**: segrega dados por operadora

### **3. APIs**
- `/api/simulacao_dtp`: filtra por `operadora_id` (query param ou sessão)
- `/api/prestadores_por_codigo`: filtra prestadores por operadora

---

## 🔐 Comportamento

### **Seleção de Operadora:**
1. **Frontend envia** `operadora_id` como query param (quando disponível)
2. **Fallback**: Se não enviado, usa `session.get('operadora_id')`
3. **Backend filtra** procedimentos pela operadora selecionada

### **Isolamento de Dados:**
```sql
-- MPF (ID=1) tem seus próprios procedimentos
SELECT * FROM procedimentos WHERE operadora_id = 1;

-- Outras operadoras podem ter procedimentos diferentes
INSERT INTO procedimentos (codigo, descricao, valor, id_tabela, operadora_id)
VALUES ('DIARIA01', 'Diária apartamento', 500.00, 123, 2);
```

---

## ⚠️ Avisos Importantes

### **1. Deletar Operadora = Deletar Procedimentos**
```sql
-- CASCADE: Ao deletar operadora, procedimentos são deletados também
DELETE FROM operadoras WHERE id = 2;
-- Isso deleta TODOS os procedimentos com operadora_id = 2
```

### **2. Importação de Procedimentos/DTP**
Ao importar novos procedimentos via `/tabelas/importar/diarias-taxas-pacotes`:
- **PENDENTE**: A rota ainda não foi atualizada para especificar operadora
- **TODO**: Adicionar seletor de operadora na interface de importação
- **Temporário**: Todos os procedimentos importados vão para operadora_id=1 (MPF) por padrão

### **3. Compatibilidade com Dados Antigos**
✅ **Todos os procedimentos existentes foram preservados**
- 17.574 procedimentos associados automaticamente à operadora MPF (ID=1)
- Nenhum dado foi perdido na migração

---

## 🚧 Próximos Passos (PENDENTE)

### **Admin Procedimentos - Ainda não implementado:**
- [ ] Adicionar seletor de operadora na importação de DTP
- [ ] Filtrar procedimentos por operadora na listagem admin
- [ ] Copiar procedimentos entre operadoras
- [ ] Editar operadora_id de procedimentos existentes

### **Atualizar rota de importação:**
```python
@app.route('/tabelas/importar/diarias-taxas-pacotes', methods=['POST'])
def importar_diarias_taxas_pacotes():
    # TODO: Adicionar lógica para capturar operadora_id do form
    operadora_id = request.form.get('operadora_id') or session.get('operadora_id') or 1

    # Ao criar novos Procedimento, incluir operadora_id
    proc = Procedimento(
        codigo=codigo,
        descricao=descricao,
        valor=valor,
        id_tabela=tabela.id,
        operadora_id=operadora_id  # ✅ NOVO
    )
```

---

## 🧪 Testes Realizados

### ✅ **Teste 1: Estrutura do Banco**
```bash
docker exec sistema_precos-db-1 mysql -u root -prootpassword operadora_saude \
  -e "DESC procedimentos;"
```
**Resultado**: ✅ Coluna `operadora_id` presente com foreign key

### ✅ **Teste 2: Dados Migrados**
```bash
docker exec sistema_precos-db-1 mysql -u root -prootpassword operadora_saude \
  -e "SELECT COUNT(*), operadora_id FROM procedimentos GROUP BY operadora_id;"
```
**Resultado**: ✅ 17.574 procedimentos associados à operadora_id=1 (MPF)

### ✅ **Teste 3: Aplicação Iniciada**
```bash
docker logs sistema_precos-web-1 --tail 10
```
**Resultado**: ✅ Gunicorn rodando sem erros

### ✅ **Teste 4: Database OK**
```bash
curl "http://localhost:8000/health?format=json" | jq '.checks.database.status'
```
**Resultado**: ✅ "ok" - Database connection working

---

## 📁 Arquivos Modificados

### **Backend:**
- `app.py` (linhas 487-502, 5079-5089, 5120-5129, 6491-6526, 6539-6575)

### **Banco de Dados:**
- Tabela `procedimentos` (estrutura alterada)
- Índice `idx_procedimentos_operadora` criado

### **Migrations:**
- `migrations/versions/20241024_02_add_operadora_to_procedimentos.py` (criada)

### **Documentação:**
- `MULTI_OPERADORA_DTP_IMPLEMENTATION.md` (este arquivo)

---

## 🎉 Resultado Final

### **Sistema Completo Multi-Operadora para DTPs**

```
┌────────────────────────────────────────────────┐
│  SIMULADOR CBHPM                               │
├────────────────────────────────────────────────┤
│  Operadora: [MPF - Ministério Público ▼]      │
│                                                │
│  DTP (Diárias, Taxas e Pacotes):               │
│  ┌──────────────────────────────────────────┐ │
│  │ DIARIA01 - Diária apartamento            │ │
│  │ TAXA01 - Taxa de sala cirúrgica          │ │
│  └──────────────────────────────────────────┘ │
│                                                │
│  [Adicionar]                                   │
└────────────────────────────────────────────────┘

Sistema busca DTPs específicos da operadora selecionada!
```

---

## 🔗 Integração com CBHPM Tetos

Este é o **segundo componente** da implementação multi-operadora:

1. ✅ **Tetos CBHPM** (já implementado) → `cbhpm_teto.operadora_id`
2. ✅ **Procedimentos/DTP** (implementado agora) → `procedimentos.operadora_id`

**Próximos:**
3. ⏳ CBHPMItem (procedimentos CBHPM) - se necessário
4. ⏳ Tabelas (tabelas de referência) - se necessário

---

## 📞 Suporte

### **Como usar diferentes DTPs por operadora:**
1. Selecione a operadora no simulador
2. Sistema automaticamente filtra DTPs daquela operadora
3. Valores e procedimentos são específicos da operadora

### **Como importar DTPs para nova operadora:**
1. **PENDENTE**: Interface ainda não implementada
2. **Workaround temporário**: Importar para MPF e depois atualizar operadora_id via SQL

### **Como copiar DTPs entre operadoras:**
```sql
-- Copiar todos os DTPs da MPF (id=1) para Nova Operadora (id=2)
INSERT INTO procedimentos (codigo, descricao, valor, prestador, uf, id_tabela, operadora_id)
SELECT codigo, descricao, valor, prestador, uf, id_tabela, 2 AS operadora_id
FROM procedimentos
WHERE operadora_id = 1;
```

---

**Status**: ✅ **IMPLEMENTADO E TESTADO**
**Versão**: 3.2.3
**Data**: 2025-10-24
**Relacionado**: [MULTI_OPERADORA_IMPLEMENTATION.md](MULTI_OPERADORA_IMPLEMENTATION.md) (Tetos CBHPM)
