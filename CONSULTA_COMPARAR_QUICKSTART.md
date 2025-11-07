# ⚡ Quick Start - Consulta & Comparar Nova Interface

## 🚀 Começar em 3 Minutos

### Passo 1: Copiar Arquivos
```bash
# CSS
cp static/css/consulta-comparar.css /seu/projeto/

# HTML
cp templates/consulta-comparar-novo.html /seu/projeto/templates/

# JavaScript
cp static/js/modules/consulta-comparar.js /seu/projeto/static/js/modules/
```

### Passo 2: Atualizar Rota (Flask)
```python
@app.route('/consulta-comparar')
def consulta_comparar():
    # Substitua a template antiga
    return render_template('consulta-comparar-novo.html')
```

### Passo 3: Acessar
```
http://localhost:5000/consulta-comparar
```

---

## 🎨 Personalizar Cores

Edite `static/css/consulta-comparar.css`:

```css
:root {
  --cc-primary: #00d4ff;      /* Mude para #FF6B6B para vermelho */
  --cc-secondary: #7c3aed;    /* Ou qualquer outra cor */
  --cc-accent: #ec4899;       /* E aqui */
  /* ... */
}
```

---

## 📊 Estrutura de Dados Esperada

O seu backend deve retornar:

```javascript
{
  "data": [
    {
      "codigo": "30401011",
      "descricao": "Consulta de Profissional Especialista",
      "rol": true,
      "minimo": 50.00,
      "media": 150.00,
      "maximo": 300.00
    },
    // ... mais itens
  ]
}
```

---

## 🔌 Integrar com Backend

No seu `consulta-comparar.js`, substituir a chamada de API:

```javascript
// Linha ~130 - FilterManager.compare()
async compare() {
  const procedimentos = Array.from(this.selectedProcedimentos);
  const uf = this.selectedUF;
  const tabela = this.selectedTabela;

  // Chamar sua API
  const response = await Utils.fetchAPI('/api/comparar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ procedimentos, uf, tabela })
  });

  // Renderizar resultados
  comparador.render(response.data);
  UI.showTab('comparador');
}
```

---

## 🧩 Usar Componentes CSS em Outro Lugar

```html
<!-- Cards -->
<div class="cc-card">
  <div class="cc-card-header">
    <div class="cc-card-header-icon">📊</div>
    <h3 class="cc-card-header-title">Título</h3>
  </div>
  <!-- Conteúdo -->
</div>

<!-- Buttons -->
<button class="cc-button">Ação Primária</button>
<button class="cc-button cc-button--secondary">Ação Secundária</button>

<!-- Badges -->
<span class="cc-badge">Status</span>
<span class="cc-badge cc-badge--success">Sucesso</span>

<!-- Chips -->
<div class="cc-chips">
  <div class="cc-chip">Item <span class="cc-chip-remove">×</span></div>
</div>

<!-- Stats -->
<div class="cc-stat">
  <div class="cc-stat-label">Métrica</div>
  <div class="cc-stat-value">123</div>
</div>
```

---

## 💡 Dicas Importantes

### ⚠️ Cuidado com:
- IDs únicos (não duplicar `id="..."`  na página)
- Selectors do JS (verificar se existem os elementos)
- Responsive (testestestestestestestesteste em mobile/tablet)

### ✅ Certifique-se:
- jQuery NÃO é necessário (JS vanilla)
- Bootstrap CSS está carregado
- Arquivo CSS está no `<head>`
- Arquivo JS está no final do `<body>`

### 🎯 Para Produção:
- Minificar CSS com cssnano
- Minificar JS com terser
- Compactar com gzip
- Testar em IE/Edge antigos
- Verificar WCAG acessibilidade

---

## 🔍 Debugging

### No Console (F12):
```javascript
// Acessar instâncias
window.consultaComparar.filterManager
window.consultaComparar.comparador
window.consultaComparar.radar
window.consultaComparar.simulador

// Testar formatação
window.consultaComparar.Utils.formatBRL(150.00)
// Output: "R$ 150,00"
```

### Verificar:
- CSS carregou? (DevTools > Network)
- JS carregou? (DevTools > Console)
- Elementos existem? (DevTools > Inspector)

---

## 📞 Suporte

Para problemas, verifique:

1. **CSS não funciona**
   - Caminho do arquivo correto?
   - Conflito com Bootstrap?
   - Cache do navegador? (Ctrl+Shift+Delete)

2. **JS não funciona**
   - Arquivo carregou? (Console > Network)
   - Erros no console? (F12 > Console)
   - Elementos existem no HTML? (F12 > Inspector)

3. **Layout quebrado**
   - Viewport meta tag? (`<meta name="viewport"...`)
   - Bootstrap CSS carregado?
   - CSS carregou depois do HTML?

---

## 🎓 Estrutura Interna

### Arquitetura JavaScript

```javascript
Utils                    // Funções auxiliares
├─ formatBRL()          // Formata moeda
├─ formatPercent()      // Formata %
├─ escapeHTML()         // Escapa HTML
├─ createElement()      // Cria elemento
└─ fetchAPI()          // Faz requisição

FilterManager           // Gerencia filtros
├─ addProcedimento()    // Adiciona item
├─ removeProcedimento() // Remove item
├─ renderChips()        // Renderiza chips
├─ toggleFilter()       // Toggle de filtro
└─ compare()            // Faz comparação

ComparadorTable         // Renderiza tabela
├─ render()             // Renderiza dados
├─ updateStats()        // Atualiza stats
├─ exportCSV()          // Export CSV
└─ copyToClipboard()   // Copy para clipboard

RadarAnalytics          // Análise de dados
├─ analyze()            // Analisa dados
└─ render()             // Renderiza grid

SimuladorCBHPM          // Simulação
├─ setupEventListeners()
└─ simulate()           // Realiza simulação

UIController            // Controla UI
├─ setupTabNavigation()
├─ showTab()            // Mostra aba
└─ setupButtonActions() // Actions dos botões
```

---

## 🌟 Features Avançadas

### Adicionar Nova Aba

1. **HTML**: Adicione na seção de tabs
```html
<button class="cc-tab" data-tab="nova">Minha Aba</button>
<div class="cc-tab-panel" id="tab-nova">...</div>
```

2. **CSS**: Styling (herda automaticamente)

3. **JS**: Funcionalidade
```javascript
const btnNova = document.querySelector('[data-tab="nova"]');
btnNova.addEventListener('click', () => {
  // Seu código aqui
});
```

### Adicionar Nova Coluna na Tabela

1. **HTML**: Adicione `<th>` no header e `<td>` no body

2. **JS**: Na função `render()`, adicione dados:
```javascript
<td>${item.suaColunaAqui}</td>
```

### Customizar Cores

1. Edite as variáveis CSS
2. Rode minificador
3. Teste em navegadores

---

## 📈 Performance Checklist

- [ ] CSS minificado?
- [ ] JS minificado?
- [ ] Imagens otimizadas?
- [ ] Lazy loading implementado?
- [ ] Cache headers corretos?
- [ ] Gzip ativado?
- [ ] Teste de velocidade (PageSpeed)?

---

## ✅ Checklist Pré-Produção

- [ ] Dados reais integrados
- [ ] Testado em Chrome/Firefox/Safari/Edge
- [ ] Testado em tablet/mobile
- [ ] Acessibilidade verificada
- [ ] Performance otimizada
- [ ] Documentação atualizada
- [ ] Backup da versão antiga
- [ ] Deploy em staging primeiro
- [ ] Monitoramento ativado
- [ ] Suporte informado

---

**Happy coding! 🚀**

Criado em: 2025-11-04
Versão: 2.0
Status: Pronto para Produção
