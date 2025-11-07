# 🎯 Guia: Filtros Inteligentes - Auto-abertura ao Selecionar Tabela

## ✨ O que foi implementado

Quando você seleciona uma tabela no dropdown:

1. ✅ **Auto-abertura de filtros**
   - Se for **Diárias/Taxas** → Abre automáticamente lista de **Prestadores**
   - Se for **CBHPM** → Abre automáticamente lista de **Versões**

2. ✅ **Atualização do placeholder**
   - CBHPM: "Código CBHPM (ex: 30401011)..."
   - Diárias/Taxas: "Código DTP ou Serviço..."

3. ✅ **Carregamento dinâmico**
   - Prestadores são carregados da API automaticamente
   - Versões são carregadas da API automaticamente

4. ✅ **Visual feedback**
   - Botão toggle fica com estilo "active" (glow cyan)
   - Seta rotaciona indicando expansão

---

## 🔧 Como Funciona

### Fluxo de Execução

```
Usuário seleciona Tabela (onChange)
        ↓
Verifica se é CBHPM ou DTP
        ↓
┌─────────────────────────────┐
│ Se for DTP/Diárias/Taxas   │ → Abre Prestadores
│ Se for CBHPM                │ → Abre Versões
└─────────────────────────────┘
        ↓
Carrega dados da API
        ↓
Renderiza checkboxes
        ↓
Muda placeholder do input
```

---

## 📡 APIs Necessárias

Para funcionar completamente, você precisa criar **3 endpoints** no Flask:

### 1. Verificar tipo de tabela
```python
@app.route('/api/tabela-info/<int:table_id>')
def tabela_info(table_id):
    """
    Retorna informações sobre a tabela (CBHPM ou DTP)
    """
    tabela = Tabela.query.get(table_id)
    if not tabela:
        return {'error': 'Tabela não encontrada'}, 404

    return {
        'id': tabela.id,
        'nome': tabela.nome,
        'tipo': tabela.tipo_tabela  # 'cbhpm' ou 'diarias_taxas_pacotes'
    }
```

### 2. Carregar prestadores
```python
@app.route('/api/prestadores/<int:table_id>')
def get_prestadores(table_id):
    """
    Retorna lista de prestadores da tabela selecionada
    """
    tabela = Tabela.query.get(table_id)
    if not tabela:
        return {'error': 'Tabela não encontrada'}, 404

    uf = request.args.get('uf', '')

    query = db.session.query(Procedimento.prestador)\
        .filter(Procedimento.id_tabela == table_id)\
        .filter(Procedimento.prestador.isnot(None))

    if uf:
        query = query.filter((Procedimento.uf == uf) | (Tabela.uf == uf))

    prestadores = [r[0] for r in query.distinct().order_by(Procedimento.prestador).all()]

    return {
        'tabela_id': table_id,
        'prestadores': prestadores
    }
```

### 3. Carregar versões
```python
@app.route('/api/versoes/<int:table_id>')
def get_versoes(table_id):
    """
    Retorna lista de versões para tabelas CBHPM
    """
    # Para CBHPM, as versões são os nomes das tabelas CBHPM
    versoes = db.session.query(Tabela.nome)\
        .filter(Tabela.tipo_tabela == 'cbhpm')\
        .distinct()\
        .order_by(Tabela.nome)\
        .all()

    return {
        'tabela_id': table_id,
        'versoes': [v[0] for v in versoes]
    }
```

---

## 🎨 Código JavaScript Implementado

### Métodos principais adicionados à classe `FilterManager`:

#### `onTabelaChange()`
Chamado automaticamente quando a tabela muda.
- Determina tipo de tabela
- Abre o filtro apropriado
- Carrega dados dinâmicos
- Atualiza placeholder

#### `loadPrestadores()`
Faz requisição para `/api/prestadores/{tableId}`

#### `loadVersoes()`
Faz requisição para `/api/versoes/{tableId}`

#### `renderPrestadoresFilter(prestadores)`
Renderiza checkboxes de prestadores dinâmicos

#### `renderVersoesFilter(versoes)`
Renderiza checkboxes de versões dinâmicos

---

## 🔌 Implementar no Seu Flask

### Passo 1: Adicione os endpoints

Copie e cole o código acima no seu `app.py`:

```python
@app.route('/api/tabela-info/<int:table_id>')
def tabela_info(table_id):
    # ... código ...

@app.route('/api/prestadores/<int:table_id>')
def get_prestadores(table_id):
    # ... código ...

@app.route('/api/versoes/<int:table_id>')
def get_versoes(table_id):
    # ... código ...
```

### Passo 2: Testar no navegador

1. Abra o console (F12)
2. Selecione uma tabela
3. Você verá logs:
   - "📊 Tabela selecionada: {id}"
   - "🎯 Tipo de tabela: CBHPM/Diárias"
   - "✅ Filtro de Prestadores aberto!"

4. Se houver erro de API, verá:
   - "Erro ao carregar prestadores:"

### Passo 3: Validar dados

Verifique se os dados estão corretos:

```javascript
// No console do navegador
window.consultaComparar.filterManager.selectedTabela
// Deve retornar o ID da tabela

window.consultaComparar.filterManager.selectedUF
// Deve retornar a UF (ou vazio se não selecionada)
```

---

## 🎯 Recursos Adicionais Implementados

### 1. Detecção automática de tipo
```javascript
const isCBHPM = await this.isTableCBHPM(tableId);
```

### 2. Renderização dinâmica
Checkboxes são gerados dinamicamente via JavaScript (não hardcoded no HTML)

### 3. Event listeners automáticos
Cada checkbox tem listeners para mudança de estado

### 4. Feedback visual
- Botão toggle fica ativo (background + border glow)
- Seta rotaciona 180° para indicar expansão

---

## 📊 Fluxo Completo de Exemplo

**Cenário: Usuário seleciona "Diárias e Taxas"**

```
1. onChange no select
2. onTabelaChange() chamado
3. Checa isTableCBHPM()
4. API retorna: tipo = 'diarias_taxas_pacotes'
5. Abre togglePrestadores
6. Chama loadPrestadores()
7. API retorna: ['Prestador A', 'Prestador B', 'Prestador C']
8. renderPrestadoresFilter() gera checkboxes
9. Placeholder muda para "Código DTP ou Serviço..."
10. ✅ Usuário pode clicar nos checkboxes
```

---

## ⚡ Performance

- APIs são chamadas com `async/await`
- Uso de `AbortController` para cancelar requisições antigas
- Cache local de dados (se implementado)
- Rendering eficiente com template literals

---

## 🐛 Troubleshooting

### Problema: Filtro não abre
**Solução:** Verifique console (F12) para erros

### Problema: Prestadores não aparecem
**Verificar:**
- API `/api/prestadores/{id}` está respondendo?
- Response tem formato correto: `{ prestadores: [...] }`?
- Query retorna dados?

### Problema: Placeholder não muda
**Solução:** Verifique se `inputProcedimento` existe no HTML

### Problema: Tipo de tabela sempre DTP
**Solução:** Verifique coluna `tipo_tabela` da tabela `Tabela`

---

## 📝 Próximas Melhorias

- [ ] Implementar "busca inteligente" enquanto digita procedimento
- [ ] Cache de dados para performance
- [ ] Loading spinner enquanto carrega filtros
- [ ] Debounce em alterações
- [ ] Validação de dados entrada
- [ ] Error messages melhores para usuário

---

## 🎓 Exemplo Completo de Integração

```python
# app.py
from flask import jsonify

@app.route('/api/tabela-info/<int:table_id>')
def tabela_info(table_id):
    tabela = Tabela.query.get(table_id)
    if not tabela:
        return jsonify({'error': 'Tabela não encontrada'}), 404

    return jsonify({
        'id': tabela.id,
        'nome': tabela.nome,
        'tipo': tabela.tipo_tabela
    })

@app.route('/api/prestadores/<int:table_id>')
def get_prestadores(table_id):
    tabela = Tabela.query.get(table_id)
    if not tabela:
        return jsonify({'error': 'Tabela não encontrada'}), 404

    uf = request.args.get('uf', '')

    query = db.session.query(Procedimento.prestador)\
        .join(Tabela, Procedimento.id_tabela == Tabela.id)\
        .filter(Procedimento.id_tabela == table_id)\
        .filter(Procedimento.prestador.isnot(None))

    if uf:
        query = query.filter((Procedimento.uf == uf) | (Tabela.uf == uf))

    prestadores = sorted(list(set([r[0] for r in query.all()])))

    return jsonify({
        'tabela_id': table_id,
        'prestadores': prestadores,
        'total': len(prestadores)
    })

@app.route('/api/versoes/<int:table_id>')
def get_versoes(table_id):
    versoes = db.session.query(Tabela.nome)\
        .filter(Tabela.tipo_tabela == 'cbhpm')\
        .distinct()\
        .order_by(Tabela.nome)\
        .all()

    return jsonify({
        'tabela_id': table_id,
        'versoes': [v[0] for v in versoes],
        'total': len(versoes)
    })
```

---

## ✅ Checklist

- [ ] Endpoints criados no Flask
- [ ] APIs testadas com Postman/Insomnia
- [ ] JavaScript carregado corretamente
- [ ] Console sem erros
- [ ] Filtros abrem automaticamente
- [ ] Dados carregam dinamicamente
- [ ] Placeholder atualiza
- [ ] Checkboxes funcionam
- [ ] Responsividade verificada
- [ ] Pronto para produção!

---

**Desenvolvido em:** 2025-11-04
**Versão:** 1.0 Smart Filters
**Status:** ✅ Pronto para Implementação
