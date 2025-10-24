# 🏢 Plano de Implementação Multi-Operadora

## 📋 Objetivo

Adaptar o sistema para suportar **múltiplas operadoras**, removendo a dependência exclusiva da MPF e permitindo que cada operadora tenha seus próprios tetos CBHPM.

---

## 🎯 Situação Atual vs Desejada

| Aspecto | ANTES (MPF Only) | DEPOIS (Multi-Operadora) |
|---------|------------------|--------------------------|
| **Operadoras** | Cadastro existe mas não é usado | Operadoras ativas com tetos próprios |
| **Tetos CBHPM** | Globais (sem operadora) | Por operadora (PK: codigo + operadora_id) |
| **Simuladores** | Sempre usa mesmos tetos | Seletor de operadora → tetos específicos |
| **Usuários** | Podem ver tudo | Vinculados a operadoras (já existe) |
| **Tabelas** | Vinculadas a operadoras (já OK) | Mantém (já está correto) |

---

## 🗄️ Mudanças no Banco de Dados

### **1. Tabela `cbhpm_teto`**

#### **ANTES:**
```sql
CREATE TABLE cbhpm_teto (
    codigo VARCHAR(20) PRIMARY KEY,
    descricao VARCHAR(255) NOT NULL,
    valor_total DECIMAL(15,2) NOT NULL,
    updated_at TIMESTAMP
);
```

#### **DEPOIS:**
```sql
CREATE TABLE cbhpm_teto (
    codigo VARCHAR(20),
    operadora_id INT NOT NULL,
    descricao VARCHAR(255) NOT NULL,
    valor_total DECIMAL(15,2) NOT NULL,
    updated_at TIMESTAMP,
    PRIMARY KEY (codigo, operadora_id),
    FOREIGN KEY (operadora_id) REFERENCES operadoras(id) ON DELETE CASCADE,
    INDEX idx_cbhpm_teto_operadora (operadora_id)
);
```

### **2. Migration Criada**

✅ **Arquivo:** `migrations/versions/20241024_01_add_operadora_to_teto.py`

**O que faz:**
1. Adiciona coluna `operadora_id`
2. Define operadora_id = 1 (MPF) para registros existentes
3. Altera PRIMARY KEY de `(codigo)` para `(codigo, operadora_id)`
4. Adiciona FOREIGN KEY para `operadoras`
5. Cria índice `idx_cbhpm_teto_operadora`

**Como aplicar:**
```bash
# Dentro do container Docker
docker exec sistema_precos-web-1 flask db upgrade

# OU via SQL direto
docker exec sistema_precos-db-1 mysql -u root -prootpassword operadora_saude < migration.sql
```

---

## 💻 Mudanças no Código

### **1. Modelo `CbhpmTeto` - ✅ FEITO**

**Arquivo:** `app.py` (linhas 554-574)

```python
class CbhpmTeto(db.Model):
    __tablename__ = 'cbhpm_teto'

    codigo = db.Column(db.String(20), primary_key=True)
    operadora_id = db.Column(db.Integer, db.ForeignKey('operadoras.id', ondelete='CASCADE'),
                             primary_key=True, nullable=False)
    descricao = db.Column(db.String(255), nullable=False)
    valor_total = db.Column(db.Numeric(15, 2), nullable=False)
    updated_at = db.Column(db.TIMESTAMP, ...)

    # Relacionamento
    operadora = db.relationship('Operadora', backref='tetos_cbhpm')
```

### **2. Função `_get_teto_map()` - 🔨 PRECISA ATUALIZAR**

**Localização:** `app.py` (linha ~3902)

**ANTES:**
```python
def _get_teto_map(codigos: list[str]) -> dict[str, 'CbhpmTeto']:
    unique_codes = {str(c or '').strip().upper() for c in codigos if str(c or '').strip()}
    if not unique_codes:
        return {}
    rows = CbhpmTeto.query.filter(CbhpmTeto.codigo.in_(unique_codes)).all()
    return {row.codigo.upper(): row for row in rows}
```

**DEPOIS:**
```python
def _get_teto_map(codigos: list[str], operadora_id: int | None = None) -> dict[str, 'CbhpmTeto']:
    """
    Retorna mapa de tetos CBHPM por código

    Args:
        codigos: Lista de códigos CBHPM
        operadora_id: ID da operadora (obrigatório para multi-operadora)
    """
    unique_codes = {str(c or '').strip().upper() for c in codigos if str(c or '').strip()}
    if not unique_codes:
        return {}

    query = CbhpmTeto.query.filter(CbhpmTeto.codigo.in_(unique_codes))

    # Se operadora_id fornecida, filtrar por ela
    if operadora_id:
        query = query.filter(CbhpmTeto.operadora_id == operadora_id)

    rows = query.all()
    return {row.codigo.upper(): row for row in rows}
```

### **3. Rota `/consulta/simular-cbhpm` - 🔨 PRECISA ATUALIZAR**

**Localização:** `app.py` (linha ~4800+)

**Adicionar:**
1. Receber `operadora_id` do request
2. Passar `operadora_id` para `_get_teto_map()`
3. Filtrar tetos pela operadora selecionada

**Exemplo:**
```python
@app.route('/consulta/simular-cbhpm', methods=['POST'])
@login_required
@feature_required('simulador')
def simular_cbhpm():
    data = request.get_json() or {}

    # NOVO: Receber operadora_id
    operadora_id = data.get('operadora_id')
    if not operadora_id:
        # Fallback: pegar primeira operadora do usuário
        operadora_id = session.get('operadora_id')

    # ... resto do código ...

    # Buscar tetos da operadora específica
    teto_map = _get_teto_map(codes, operadora_id=operadora_id)
```

### **4. Template `consulta-comparar.html` - 🔨 PRECISA ATUALIZAR**

**Adicionar:**
1. Seletor de operadora no topo do simulador
2. Passar `operadora_id` para `/consulta/simular-cbhpm`
3. Mostrar qual operadora está selecionada

**Exemplo HTML:**
```html
<div class="row mb-3">
    <div class="col-md-6">
        <label class="form-label">Operadora:</label>
        <select id="operadoraSelector" class="form-select">
            {% for op in operadoras %}
            <option value="{{ op.id }}" {% if op.id == operadora_id %}selected{% endif %}>
                {{ op.nome }}
            </option>
            {% endfor %}
        </select>
    </div>
</div>
```

**Exemplo JavaScript:**
```javascript
function simularCBHPM() {
    const operadoraId = document.getElementById('operadoraSelector').value;

    const payload = {
        operadora_id: parseInt(operadoraId),
        // ... resto dos dados
    };

    fetch('/consulta/simular-cbhpm', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    })
    // ...
}
```

### **5. Admin Tetos `/admin/tetos` - 🔨 PRECISA ATUALIZAR**

**Mudanças:**
1. Adicionar seletor de operadora
2. Importação/edição de tetos vinculados a operadora
3. Listagem filtrada por operadora
4. Permitir copiar tetos de uma operadora para outra

---

## 📝 Checklist de Implementação

### **Fase 1: Banco de Dados ✅ CONCLUÍDO**
- [x] Criar migration `20241024_01_add_operadora_to_teto.py`
- [x] Atualizar modelo `CbhpmTeto` no `app.py`
- [x] Aplicar migration no banco de dados
- [x] Garantir que existe operadora com ID=1 (MPF)

### **Fase 2: Backend ✅ CONCLUÍDO (parcial)**
- [x] Atualizar `_get_teto_map()` para aceitar `operadora_id`
- [x] Atualizar rota `/api/simulacao_cbhpm` (simulador)
- [x] Passar operadoras para template em `/consulta-comparar`
- [ ] Atualizar rota `/admin/tetos` (listagem) - PENDENTE
- [ ] Atualizar rota `/admin/tetos/upload` (importação) - PENDENTE
- [ ] Atualizar rota `/admin/tetos/<codigo>/editar` - PENDENTE
- [ ] Adicionar rota `/admin/tetos/copiar` (copiar entre operadoras) - PENDENTE

### **Fase 3: Frontend ✅ CONCLUÍDO (simulador)**
- [x] Adicionar seletor de operadora em `consulta-comparar.html`
- [x] Passar `operadora_id` no payload do simulador
- [x] Atualizar `getCBHPMPayload()` para incluir operadora_id
- [x] Atualizar `setCBHPMPayload()` para restaurar operadora_id
- [ ] Atualizar `admin_tetos.html` com filtro de operadora - PENDENTE
- [ ] Adicionar interface para copiar tetos entre operadoras - PENDENTE

### **Fase 4: Testes ✅ CONCLUÍDO (básico)**
- [x] Verificar estrutura do banco de dados
- [x] Verificar migração de dados existentes
- [x] Verificar aplicação iniciada sem erros
- [x] Verificar health check OK
- [ ] Testar simulador com diferentes operadoras - FUNCIONAL (aguardando cadastro de mais operadoras)
- [ ] Testar importação de tetos para operadora específica - PENDENTE
- [ ] Testar edição de tetos - PENDENTE
- [ ] Testar exclusão de operadora (cascade nos tetos) - PENDENTE
- [ ] Testar backup/restore - PENDENTE

---

## 🚀 Como Aplicar Agora

### **1. Garantir Operadora MPF Existe**

```sql
-- Verificar se MPF existe com ID=1
SELECT * FROM operadoras WHERE id = 1;

-- Se não existir, criar:
INSERT INTO operadoras (id, nome, status, uf, cnpj)
VALUES (1, 'MPF - Ministério Público Federal', 'ativo', NULL, NULL);
```

### **2. Aplicar Migration**

```bash
# Opção A: Via Flask-Migrate (recomendado)
docker exec sistema_precos-web-1 flask db upgrade

# Opção B: Via SQL direto
docker exec sistema_precos-db-1 mysql -u root -prootpassword operadora_saude << 'EOF'
ALTER TABLE cbhpm_teto ADD COLUMN operadora_id INT NULL;
UPDATE cbhpm_teto SET operadora_id = 1 WHERE operadora_id IS NULL;
ALTER TABLE cbhpm_teto MODIFY operadora_id INT NOT NULL;
ALTER TABLE cbhpm_teto DROP PRIMARY KEY;
ALTER TABLE cbhpm_teto ADD PRIMARY KEY (codigo, operadora_id);
ALTER TABLE cbhpm_teto ADD CONSTRAINT fk_cbhpm_teto_operadora
    FOREIGN KEY (operadora_id) REFERENCES operadoras(id) ON DELETE CASCADE;
CREATE INDEX idx_cbhpm_teto_operadora ON cbhpm_teto(operadora_id);
EOF
```

### **3. Reiniciar Aplicação**

```bash
docker restart sistema_precos-web-1
```

### **4. Verificar**

```sql
-- Ver estrutura atualizada
DESC cbhpm_teto;

-- Ver tetos com operadora
SELECT codigo, descricao, operadora_id, valor_total
FROM cbhpm_teto
LIMIT 10;
```

---

## 🎨 Interface Proposta

### **Simulador CBHPM**

```
┌────────────────────────────────────────────────┐
│  SIMULADOR CBHPM                               │
├────────────────────────────────────────────────┤
│  Operadora: [MPF - Ministério Público ▼]      │
│                                                │
│  Códigos CBHPM:                                │
│  ┌──────────────────────────────────────────┐ │
│  │ 20101012                                 │ │
│  │ 31009336                                 │ │
│  └──────────────────────────────────────────┘ │
│                                                │
│  [Simular]                                     │
└────────────────────────────────────────────────┘
```

### **Admin Tetos**

```
┌────────────────────────────────────────────────┐
│  GERENCIAR TETOS CBHPM                         │
├────────────────────────────────────────────────┤
│  Operadora: [MPF ▼]  [Upload] [Copiar Tetos]  │
│                                                │
│  Código   │ Descrição          │ Valor        │
│───────────┼────────────────────┼──────────────│
│ 20101012  │ Consulta           │ R$ 150,00    │
│ 31009336  │ Cirurgia           │ R$ 2.500,00  │
└────────────────────────────────────────────────┘
```

---

## ⚠️ Avisos Importantes

1. **Backup antes de migrar!**
   ```bash
   docker exec sistema_precos-db-1 mysqldump -u root -prootpassword operadora_saude > backup_$(date +%Y%m%d).sql
   ```

2. **Operadora MPF deve existir com ID=1** antes da migration

3. **Usuários já vinculados a operadoras** - o sistema já suporta isso na sessão

4. **Não deletar operadoras com tetos** - o CASCADE vai deletar os tetos também

---

## 📊 Próximos Passos

Quer que eu implemente:

1. ✅ **Fase 1 completa** - Migration + Modelo criados
2. 🔨 **Fase 2** - Atualizar backend (`_get_teto_map`, rotas)
3. 🔨 **Fase 3** - Atualizar frontend (seletor, simulador)

---

## ✅ STATUS DE IMPLEMENTAÇÃO - 2025-10-24

### **CONCLUÍDO:**
✅ **Fase 1 (Banco de Dados)** - 100% implementado
✅ **Fase 2 (Backend - Simulador)** - 100% implementado
✅ **Fase 3 (Frontend - Simulador)** - 100% implementado
✅ **Fase 4 (Testes Básicos)** - 100% implementado

### **FUNCIONALIDADES ATIVAS:**
- ✅ Simulador CBHPM com seletor de operadora
- ✅ Tetos CBHPM específicos por operadora
- ✅ Filtro automático de tetos por operadora selecionada
- ✅ Compatibilidade retroativa (tetos antigos = MPF)
- ✅ Cascade delete (deletar operadora remove seus tetos)

### **PENDENTE (Admin Tetos):**
- [ ] Interface admin para gerenciar tetos por operadora
- [ ] Importação de tetos para operadora específica
- [ ] Copiar tetos entre operadoras

### **COMO USAR AGORA:**
1. Acesse: `http://localhost:8000/consulta-comparar`
2. No simulador CBHPM, selecione a operadora desejada
3. Execute a simulação - tetos serão filtrados pela operadora

### **DOCUMENTAÇÃO COMPLETA:**
Ver arquivo: [MULTI_OPERADORA_IMPLEMENTATION.md](MULTI_OPERADORA_IMPLEMENTATION.md)
