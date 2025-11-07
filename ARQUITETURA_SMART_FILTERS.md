# 🏗️ Arquitetura - Smart Filters

**Visualização da arquitetura técnica dos Smart Filters**

---

## 📐 Diagrama Geral da Aplicação

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          NAVEGADOR (Client-Side)                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ consulta-comparar-novo.html                                      │   │
│  │ ────────────────────────────────────────                         │   │
│  │ <select id="selectTabela">                                       │   │
│  │ <button id="togglePrestadores">🏥 Prestadores</button>          │   │
│  │ <button id="toggleVersoes">📅 Versões</button>                  │   │
│  │ <div id="filterPrestadores">← Checkboxes gerados aqui           │   │
│  │ <div id="filterVersoes">    ← Checkboxes gerados aqui           │   │
│  └────────────────┬───────────────────────────────────────────────┘   │
│                   │                                                     │
│  ┌────────────────▼───────────────────────────────────────────────┐   │
│  │ consulta-comparar.css                                          │   │
│  │ ────────────────────────                                       │   │
│  │ .cc-filter-toggle.active { /* Glow cyan */ }                 │   │
│  │ .cc-filter-toggle.active span { transform: rotate(180deg); }  │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                   │                                                     │
│  ┌────────────────▼───────────────────────────────────────────────┐   │
│  │ consulta-comparar.js                                           │   │
│  │ ────────────────────────                                       │   │
│  │                                                                │   │
│  │ selectTabela.onChange                                         │   │
│  │    └─ onTabelaChange()                                        │   │
│  │       ├─ isTableCBHPM()    ◄── API Call: /api/tabela-info     │   │
│  │       │  └─ if CBHPM → loadVersoes()                          │   │
│  │       │     └─ renderVersoesFilter()                          │   │
│  │       └─ if DTP → loadPrestadores()  ◄── API Call: /api/pres  │   │
│  │          └─ renderPrestadoresFilter()                         │   │
│  │                                                                │   │
│  └────────────────┬───────────────────────────────────────────────┘   │
│                   │                                                     │
│           HTTP Fetch Requests                                           │
│           ╭─────────────────╮                                           │
│           │ GET /api/       │                                           │
│           │ tabela-info/<id>│ ────────────────┐                         │
│           │ prestadores/<id>│                  │                         │
│           │ versoes/<id>    │                  │                         │
│           ╰─────────────────╯                  │                         │
│                                                ▼                         │
└─────────────────────────────────────────────────────────────────────────┘
                                                  │
                ┌─────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       SERVIDOR Flask (Backend)                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ app.py                                                           │   │
│  │ ──────                                                           │   │
│  │                                                                  │   │
│  │ @app.route('/api/tabela-info/<int:table_id>')                  │   │
│  │ @login_required                                                 │   │
│  │ def api_tabela_info(table_id):                                 │   │
│  │     tabela = Tabela.query.get(table_id)                        │   │
│  │     return jsonify({                                           │   │
│  │         'id': tabela.id,                                       │   │
│  │         'tipo': tabela.tipo_tabela  # 'cbhpm' ou 'dtp'         │   │
│  │     })                                                          │   │
│  │                                                                  │   │
│  │ @app.route('/api/prestadores/<int:table_id>')                  │   │
│  │ @login_required                                                 │   │
│  │ def api_get_prestadores(table_id):                             │   │
│  │     query = db.session.query(Procedimento.prestador)\          │   │
│  │         .filter(Procedimento.id_tabela == table_id)\           │   │
│  │         .filter(Procedimento.prestador.isnot(None))            │   │
│  │     prestadores = [...results...]                              │   │
│  │     return jsonify({'prestadores': prestadores})               │   │
│  │                                                                  │   │
│  │ @app.route('/api/versoes/<int:table_id>')                      │   │
│  │ @login_required                                                 │   │
│  │ def api_get_versoes(table_id):                                 │   │
│  │     query = db.session.query(Tabela.nome)\                     │   │
│  │         .filter(Tabela.tipo_tabela == 'cbhpm')                 │   │
│  │     versoes = [...results...]                                  │   │
│  │     return jsonify({'versoes': versoes})                       │   │
│  │                                                                  │   │
│  └──────────────────────────┬───────────────────────────────────────┘   │
│                             │                                           │
│                   Database Queries                                       │
│                             │                                           │
│                   ┌─────────┼─────────┐                                 │
│                   ▼         ▼         ▼                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                  │
│  │   Tabelas    │ │Procedimentos  │ │ CBHPM Itens  │                  │
│  │ ──────────── │ │ ──────────── │ │ ──────────── │                  │
│  │ id           │ │ id           │ │ id           │                  │
│  │ nome         │ │ id_tabela    │ │ id_tabela    │                  │
│  │ tipo_tabela  │ │ prestador    │ │ codigo       │                  │
│  │ id_operadora │ │ operadora_id │ │ procedimento │                  │
│  └──────────────┘ └──────────────┘ └──────────────┘                  │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
                         DATABASE (PostgreSQL)
```

---

## 🔄 Fluxo de Dados - Detalhado

### Cenário: Usuário seleciona tabela "Diárias e Taxas"

```
┌─────────────────────────────────────────────────────────────────────────┐
│ PASSO 1: Usuario interage                                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  Browser                                                                 │
│  ┌─────────────────────────────────────┐                                │
│  │ <select id="selectTabela">          │                                │
│  │   <option value="">Selecionar...  │                                │
│  │   <option value="1">CBHPM 2024   ✓ ← Click aqui                     │
│  │   <option value="2">Diárias/Taxas ✓ ← Seleciona isso               │
│  │ </select>                           │                                │
│  └─────────────────────────────────────┘                                │
│        │                                                                 │
│        └─ dispara evento onChange                                       │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ PASSO 2: JavaScript detecta mudança                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  consulta-comparar.js                                                   │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ selectTabela.addEventListener('change', (e) => {               │   │
│  │   this.selectedTabela = e.target.value;  // "2"                │   │
│  │   this.onTabelaChange();  // ◄─ Chamado aqui                    │   │
│  │ })                                                              │   │
│  └──────────────────────────┬───────────────────────────────────────┘   │
│                             │                                           │
└─────────────────────────────┼───────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────┐
│ PASSO 3: onTabelaChange() executado                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  async onTabelaChange() {                                               │
│      console.log('📊 Tabela selecionada:', this.selectedTabela);       │
│      if (!this.selectedTabela) return;                                  │
│                                                                           │
│      const isCBHPM = await this.isTableCBHPM(this.selectedTabela);     │
│                                  ▲                                       │
│                                  │                                       │
│                    ◄────────────────────────► API CALL                   │
│  }                                                                       │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PASSO 4: isTableCBHPM() faz requisição                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  async isTableCBHPM(tableId) {                                          │
│      try {                                                              │
│          const response = await Utils.fetchAPI(                        │
│              `/api/tabela-info/${tableId}`  // GET /api/tabela-info/2  │
│          );                                                             │
│          const isCBHPM = response.tipo === 'cbhpm';                    │
│          console.log('🎯 Tipo de tabela:', response.tipo);             │
│          return isCBHPM;                                               │
│      } catch (error) {                                                 │
│          console.error('Erro ao carregar tipo:', error);               │
│          return false;                                                 │
│      }                                                                  │
│  }                                                                       │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ HTTP GET /api/tabela-info/2                                     │   │
│  │                                                                  │   │
│  │ SERVIDOR (app.py:6652)                                          │   │
│  │ def api_tabela_info(table_id):                                  │   │
│  │     tabela = Tabela.query.get(2)                               │   │
│  │     return {                                                   │   │
│  │         'id': 2,                                               │   │
│  │         'nome': 'Diárias e Taxas',                            │   │
│  │         'tipo': 'diarias_taxas_pacotes'  ◄─ RESPOSTA            │   │
│  │     }                                                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              │ Response chega ao browser
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PASSO 5: Lógica diferencia tipo                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  // Continuação em onTabelaChange():                                    │
│  const isCBHPM = await this.isTableCBHPM(2);  // retorna false          │
│                                                                           │
│  if (!isCBHPM) {  // ◄─ VERDADEIRO (é DTP)                              │
│      // Abre Prestadores                                               │
│      const toggleBtn = document.getElementById('togglePrestadores');   │
│      const filterContainer = document.getElementById('filterPrestadores');
│                                                                           │
│      toggleBtn.classList.add('active');  // ◄─ CSS glow               │
│      filterContainer.style.display = 'block';  // ◄─ Mostra container  │
│                                                                           │
│      await this.loadPrestadores();  // ◄─ Carrega dados                │
│  } else {                                                               │
│      // Abre Versões (seria para CBHPM)                               │
│      // ...não executa neste caso...                                   │
│  }                                                                       │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PASSO 6: loadPrestadores() faz segunda requisição                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  async loadPrestadores() {                                              │
│      try {                                                              │
│          const response = await Utils.fetchAPI(                        │
│              `/api/prestadores/${this.selectedTabela}?uf=${...}`       │
│                              ▲                                          │
│                              │                                          │
│              GET /api/prestadores/2?uf=                                 │
│          );                                                             │
│          console.log('✅ Prestadores carregados:', response.total);    │
│          this.renderPrestadoresFilter(response.prestadores);           │
│      } catch (error) { ... }                                           │
│  }                                                                       │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ HTTP GET /api/prestadores/2                                     │   │
│  │                                                                  │   │
│  │ SERVIDOR (app.py:6684)                                          │   │
│  │ def api_get_prestadores(table_id):                              │   │
│  │     query = db.session.query(Procedimento.prestador)\           │   │
│  │         .filter(Procedimento.id_tabela == 2)                   │   │
│  │     prestadores = ['Hospital A', 'Clínica B', 'Consultório C'] │   │
│  │     return {                                                   │   │
│  │         'prestadores': prestadores  ◄─ RESPOSTA                │   │
│  │     }                                                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              │ Response chega ao browser
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PASSO 7: renderPrestadoresFilter() renderiza dinâmicamente             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  renderPrestadoresFilter(prestadores) {                                 │
│      const container = document.getElementById('filterPrestadores');    │
│      container.innerHTML = '';                                          │
│                                                                           │
│      prestadores.forEach(prestador => {                                │
│          const label = document.createElement('label');                │
│          label.innerHTML = `                                           │
│              <input type="checkbox" value="${prestador}">              │
│              ${prestador}                                              │
│          `;                                                             │
│          container.appendChild(label);  // ◄─ Adiciona ao DOM          │
│      });                                                                │
│  }                                                                       │
│                                                                           │
│  RESULTADO NO BROWSER:                                                 │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │ 🏥 Prestadores  ✓ (toggle com glow cyan)                       ▲  │ │
│  ├───────────────────────────────────────────────────────────────────┤ │
│  │ ☐ Hospital A                                                     │ │
│  │ ☐ Clínica B                                                      │ │
│  │ ☐ Consultório C                                                  │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PASSO 8: Atualiza placeholder do input                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  // Continuação em onTabelaChange():                                    │
│  const inputProc = document.getElementById('inputProcedimento');        │
│  if (inputProc) {                                                       │
│      if (isCBHPM) {  // false neste caso                                │
│          inputProc.placeholder = 'Código CBHPM (ex: 30401011)...';     │
│      } else {  // ◄─ Executa aqui                                       │
│          inputProc.placeholder = 'Código DTP ou Serviço...';           │
│      }                                                                   │
│  }                                                                       │
│                                                                           │
│  RESULTADO NO BROWSER:                                                 │
│  ┌──────────────────────────────────────────────────────┐              │
│  │ ┌──────────────────────────────────────────────────┐ │              │
│  │ │ 💡 Código DTP ou Serviço...                      │ │ ◄─ Placeholder
│  │ │                                                  │ │ atualizado!│
│  │ └──────────────────────────────────────────────────┘ │              │
│  └──────────────────────────────────────────────────────┘              │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘

✅ FIM - Usuário vê Prestadores abertos e pode selecionar!
```

---

## 🎨 Estrutura de Classes JavaScript

```javascript
// ═══════════════════════════════════════════════════════════════════

class FilterManager {
  // PROPRIEDADES
  selectedProcedimentos = new Set()
  selectedVersoes = new Set()
  selectedPrestadores = new Set()
  selectedTabela = null
  selectedUF = null

  // SETUP
  constructor() { /* ... */ }
  setupEventListeners() { /* ... */ }

  // EVENT HANDLERS
  onTabelaChange()           // ◄─ Chamado quando tabela muda
  onUFChange()
  onInputProcedimentoChange()

  // TYPE DETECTION
  async isTableCBHPM(tableId)      // ◄─ Detecta tipo

  // DATA LOADING
  async loadPrestadores()          // ◄─ Carrega da API
  async loadVersoes()              // ◄─ Carrega da API

  // RENDERING
  renderPrestadoresFilter(data)    // ◄─ Renderiza dinâmico
  renderVersoesFilter(data)        // ◄─ Renderiza dinâmico
  renderChips()

  // UTILITIES
  addProcedimento(codigo)
  removeProcedimento(codigo)
  clear()
  compare()
}

// ═══════════════════════════════════════════════════════════════════
```

---

## 🗄️ Estrutura de Banco de Dados

```
DATABASE
│
├─ tabelas
│  ├─ id (PK)
│  ├─ nome
│  ├─ tipo_tabela      ◄─ 'cbhpm' ou 'diarias_taxas_pacotes'
│  ├─ id_operadora (FK)
│  └─ ... outros campos
│
├─ procedimentos
│  ├─ id (PK)
│  ├─ id_tabela (FK)   ◄─ Referencia tabelas.id
│  ├─ prestador        ◄─ Usado por /api/prestadores
│  ├─ operadora_id (FK)
│  └─ ... outros campos
│
└─ cbhpm_itens
   ├─ id (PK)
   ├─ id_tabela (FK)   ◄─ Referencia tabelas.id
   ├─ procedimento
   └─ ... outros campos
```

---

## 🔐 Fluxo de Segurança

```
REQUEST
  │
  ├─ HTTP Header incluir Cookie de Sessão
  │   (Cookies automaticamente inclusos pelo navegador)
  │
  ▼
SERVIDOR Flask
  │
  ├─ @login_required
  │  └─ Valida: session['user_id'] existe?
  │     └─ Se não → Redireciona para login
  │
  ├─ Extrai operadora_id da sessão
  │  └─ operadora_id = session.get('operadora_id')
  │
  ├─ Query tabela por ID
  │  └─ tabela = Tabela.query.get(table_id)
  │
  ├─ Validação de acesso
  │  └─ if operadora_id and tabela.id_operadora != operadora_id:
  │     └─ return 403 Forbidden
  │
  └─ Se tudo OK:
     └─ return JSON com dados

RESPONSE
  │
  └─ JSON com dados seguros
```

---

## 📊 Resumo da Arquitetura

| Camada | Componente | Responsabilidade |
|--------|-----------|-----------------|
| **Presentation** | HTML | Estrutura (dropdowns, containers) |
| **Styling** | CSS | Visual feedback (glow, rotação) |
| **Logic** | JavaScript | Detecção, renderização, eventos |
| **API** | Flask endpoints | Autenticação, autorização, dados |
| **Data** | Database | Tabelas, procedimentos, operadora |

---

**Criado em:** 2025-11-04
**Versão:** 1.0
