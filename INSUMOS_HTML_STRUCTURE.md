# Estrutura HTML Completa - Página Simpro & Brasíndice

## 📐 Arquitetura de Containers

```
┌─────────────────────────────────────────────────────────────┐
│  <body> (Layout principal do sistema)                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│  <div class="content-area animate-fade-in">                  │
│  (Área de conteúdo do base.html)                            │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│  <div class="insumos-container">                             │
│  (Wrapper com padding 3rem horizontal, 2rem vertical)       │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  <div class="insumos-wrapper">                         │  │
│  │  (Container interno - largura 100%)                   │  │
│  │                                                        │  │
│  │  ┌──────────────────────────────────────────────────┐ │  │
│  │  │ <!-- Cards de Resumo -->                        │ │  │
│  │  │ <div class="row g-3 mb-4">                      │ │  │
│  │  │   <div class="col-12 col-md-6">                │ │  │
│  │  │     <div class="card card-primary">            │ │  │
│  │  │       Brasíndice Card                           │ │  │
│  │  │     </div>                                      │ │  │
│  │  │   </div>                                        │ │  │
│  │  │   <div class="col-12 col-md-6">                │ │  │
│  │  │     <div class="card">                          │ │  │
│  │  │       SIMPRO Card (com border-4 verde)         │ │  │
│  │  │     </div>                                      │ │  │
│  │  │   </div>                                        │ │  │
│  │  │ </div>                                          │ │  │
│  │  └──────────────────────────────────────────────────┘ │  │
│  │                                                        │  │
│  │  ┌──────────────────────────────────────────────────┐ │  │
│  │  │ <!-- Botão Ajustar Alíquotas (Admin) -->        │ │  │
│  │  │ <div class="mb-4 text-end">                     │ │  │
│  │  │   {% if is_admin %}                             │ │  │
│  │  │   <a class="btn btn-outline-primary">           │ │  │
│  │  │     Ajustar alíquotas                           │ │  │
│  │  │   </a>                                          │ │  │
│  │  │   {% endif %}                                   │ │  │
│  │  │ </div>                                          │ │  │
│  │  └──────────────────────────────────────────────────┘ │  │
│  │                                                        │  │
│  │  ┌──────────────────────────────────────────────────┐ │  │
│  │  │ <!-- Card de Filtros -->                        │ │  │
│  │  │ <div class="card shadow-sm mb-4">              │ │  │
│  │  │   <div class="card-header bg-light border-0">  │ │  │
│  │  │     <i class="bi bi-funnel"></i>               │ │  │
│  │  │     Filtros de busca                           │ │  │
│  │  │   </div>                                        │ │  │
│  │  │   <div class="card-body">                       │ │  │
│  │  │     <form id="insumoFilters" class="row g-3">  │ │  │
│  │  │       <!-- Inputs, Selects, Buttons -->         │ │  │
│  │  │     </form>                                     │ │  │
│  │  │   </div>                                        │ │  │
│  │  │ </div>                                          │ │  │
│  │  └──────────────────────────────────────────────────┘ │  │
│  │                                                        │  │
│  │  ┌──────────────────────────────────────────────────┐ │  │
│  │  │ <!-- Card de Resultados -->                     │ │  │
│  │  │ <div class="card shadow-sm">                    │ │  │
│  │  │   <div class="card-header">                     │ │  │
│  │  │     <small id="resultadoResumo">               │ │  │
│  │  │       Preencha e clique buscar                  │ │  │
│  │  │     </small>                                    │ │  │
│  │  │     <div class="btn-group">                     │ │  │
│  │  │       <button id="paginacaoAnterior">           │ │  │
│  │  │       <button id="paginacaoProximo">            │ │  │
│  │  │     </div>                                      │ │  │
│  │  │   </div>                                        │ │  │
│  │  │   <div class="card-body p-0">                   │ │  │
│  │  │     <div class="table-responsive">              │ │  │
│  │  │       <!-- SPINNER (inicialmente d-none) -->    │ │  │
│  │  │       <div id="resultadoLoading"                │ │  │
│  │  │            class="d-none">                      │ │  │
│  │  │         <div class="spinner-border"></div>      │ │  │
│  │  │       </div>                                    │ │  │
│  │  │                                                 │ │  │
│  │  │       <!-- TABELA -->                           │ │  │
│  │  │       <table class="table table-hover"          │ │  │
│  │  │              id="resultadoTabela">              │ │  │
│  │  │         <thead class="table-light">             │ │  │
│  │  │           <tr>                                  │ │  │
│  │  │             <th>Origem</th>                     │ │  │
│  │  │             <th>Códigos</th>                    │ │  │
│  │  │             <th>Descrição</th>                  │ │  │
│  │  │             <th class="text-end">PMC</th>       │ │  │
│  │  │             <th class="text-end">PFB</th>       │ │  │
│  │  │             <th class="text-center">UF</th>     │ │  │
│  │  │             <th class="text-end">Alíquota</th>  │ │  │
│  │  │             <th>Versão</th>                     │ │  │
│  │  │             <th>Atualiz.</th>                   │ │  │
│  │  │             <th class="text-center">Ação</th>   │ │  │
│  │  │           </tr>                                 │ │  │
│  │  │         </thead>                                │ │  │
│  │  │         <tbody id="resultadoBody">              │ │  │
│  │  │           <!-- Preenchido por JavaScript -->    │ │  │
│  │  │           <tr>                                  │ │  │
│  │  │             <td colspan="10">                   │ │  │
│  │  │               Nenhuma busca realizada           │ │  │
│  │  │             </td>                               │ │  │
│  │  │           </tr>                                 │ │  │
│  │  │         </tbody>                                │ │  │
│  │  │       </table>                                  │ │  │
│  │  │     </div>                                      │ │  │
│  │  │   </div>                                        │ │  │
│  │  │ </div>                                          │ │  │
│  │  └──────────────────────────────────────────────────┘ │  │
│  │                                                        │  │
│  │  ┌──────────────────────────────────────────────────┐ │  │
│  │  │ <!-- Alerta Admin -->                           │ │  │
│  │  │ {% if is_admin %}                               │ │  │
│  │  │ <div class="alert alert-info mt-4">            │ │  │
│  │  │   Dica: Importações em Gerenciar Tabelas       │ │  │
│  │  │ </div>                                          │ │  │
│  │  │ {% endif %}                                     │ │  │
│  │  └──────────────────────────────────────────────────┘ │  │
│  │                                                        │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘

<!-- MODAIS (fora do container) -->

┌──────────────────────────────────────────────────────────────┐
│  <div class="modal fade" id="detalheModal">                  │
│    Modal para visualizar detalhes de um item                 │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  <div class="modal fade" id="deepDiveModal">                 │
│    Modal para análise detalhada (extensível)                 │
└──────────────────────────────────────────────────────────────┘
```

---

## 🎨 CSS - Estrutura de Classes

### Container Wrapper
```css
.insumos-container {
  display: block;
  width: 100%;
  min-height: calc(100vh - 200px);
  background: transparent;
  padding: 0 3rem 2rem 3rem;  /* 3rem horizontal, 2rem vertical */
}

.insumos-wrapper {
  width: 100%;
  display: block;
}
```

### Cards de Resumo
```css
.card {
  border-color: #e5e7eb;
  border-radius: 10px;
  transition: box-shadow 0.2s ease;
}

.card:hover {
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
}

.card-primary {
  background: linear-gradient(135deg, rgba(14, 165, 233, 0.05) 0%, rgba(14, 165, 233, 0.02) 100%);
  border-left: 4px solid #0ea5e9;
}

.card-header {
  font-weight: 600;
  background-color: #f9fafb;
  border-bottom: 1px solid #e5e7eb;
}

.card-body {
  padding: 1.5rem;
}
```

### Tabela
```css
.table-light thead th {
  background-color: #f9fafb;
  border-bottom: 2px solid #e5e7eb;
  font-weight: 600;
  color: #475569;
  padding: 1rem;
}

.table tbody tr {
  border-bottom: 1px solid #e5e7eb;
}

.table tbody tr:hover {
  background-color: #f9fafb;
  transition: background-color 0.15s ease;
}
```

### Botões
```css
.btn {
  border-radius: 8px;
  font-weight: 500;
  transition: all 0.2s ease;
}

.btn-primary {
  background: linear-gradient(135deg, #0ea5e9 0%, #0284c7 100%);
  border: none;
}

.btn-primary:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(14, 165, 233, 0.3);
}
```

### Loading Spinner
```css
.spinner-border {
  animation: spin 1s linear infinite;
  width: 2rem;
  height: 2rem;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
```

---

## 📝 Formulário de Filtros Detalhado

### HTML

```html
<form id="insumoFilters" class="row g-3">
  <!-- 1. Busca Principal (12 col no desktop) -->
  <div class="col-12 col-md-5">
    <label class="form-label fw-600">Buscar termo</label>
    <input type="text" class="form-control"
           id="fTermo"
           placeholder="Descrição, fabricante, código…"
           autocomplete="off">
    <small class="form-text text-muted">Procura em descrição e fabricante</small>
  </div>

  <!-- 2. Origem (6 col mobile, 2 col desktop) -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">Origem</label>
    <select id="fOrigem" class="form-select">
      <option value="">Todas</option>
      <option value="BRAS">Brasíndice</option>
      <option value="SIMPRO">SIMPRO</option>
    </select>
  </div>

  <!-- 3. Versão -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">Versão</label>
    <select id="fVersao" class="form-select">
      <option value="">Todas</option>
      <!-- Preenchido dinamicamente do backend -->
    </select>
  </div>

  <!-- 4. UF (6 col mobile, 2 col desktop) -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">UF</label>
    <select class="form-select" id="fUf">
      <option value="">Selecione…</option>
      <!-- Preenchido dinamicamente do backend -->
    </select>
  </div>

  <!-- 5. Código TUSS -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">TUSS</label>
    <input type="text" class="form-control"
           id="fTuss"
           placeholder="Código"
           autocomplete="off">
  </div>

  <!-- 6. Código TISS -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">TISS</label>
    <input type="text" class="form-control"
           id="fTiss"
           placeholder="Código"
           autocomplete="off">
  </div>

  <!-- 7. Registro ANVISA -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">ANVISA</label>
    <input type="text" class="form-control"
           id="fAnvisa"
           placeholder="Registro"
           autocomplete="off">
  </div>

  <!-- 8. Alíquota -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">Alíquota (%)</label>
    <input type="text" class="form-control"
           id="fAliquota"
           placeholder="Ex: 18"
           autocomplete="off">
  </div>

  <!-- 9. Fabricante -->
  <div class="col-12 col-md-3">
    <label class="form-label fw-600">Fabricante</label>
    <input type="text" class="form-control"
           id="fFabricante"
           placeholder="Nome ou código"
           autocomplete="off">
  </div>

  <!-- 10. Itens por Página -->
  <div class="col-6 col-md-2">
    <label class="form-label fw-600">Por página</label>
    <select id="fPerPage" class="form-select">
      <option value="25">25</option>
      <option value="50" selected>50</option>
      <option value="100">100</option>
      <option value="250">250</option>
    </select>
  </div>

  <!-- 11. Botões -->
  <div class="col-12 col-md-3 d-flex align-items-end gap-2">
    <button type="submit" class="btn btn-primary flex-grow-1" id="btnBuscar">
      <i class="bi bi-search me-2"></i>Buscar
    </button>
    <button type="button" class="btn btn-outline-secondary" id="btnLimpar"
            title="Limpar filtros">
      <i class="bi bi-eraser"></i>
    </button>
  </div>
</form>
```

### Layout Responsivo

```
Desktop (>= 768px):
┌──────────────────────────────────────────────────────────────┐
│ Buscar termo (5)  Origem (2)  Versão (2)  UF (2)  TUSS (2)   │
│ TISS (2)  ANVISA (2)  Alíquota (2)  Fabricante (3)  Por pág   │
│ Buscar (3)                                                    │
└──────────────────────────────────────────────────────────────┘

Tablet (< 768px):
┌──────────────────────────────────────┐
│ Buscar termo                         │
│ Origem (6) │ Versão (6)              │
│ UF (6)     │ TUSS (6)                │
│ TISS (6)   │ ANVISA (6)              │
│ Alíquota (6) │ Fabricante (6)        │
│ Por página │ Buscar | Limpar         │
└──────────────────────────────────────┘
```

---

## 📋 Tabela de Resultados

### Estrutura das Colunas

```html
<table class="table table-hover align-middle" id="resultadoTabela">
  <thead class="table-light">
    <tr>
      <th style="width: 90px;">Origem</th>
      <th style="width: 140px;">Códigos</th>
      <th>Descrição</th>
      <th style="width: 120px;" class="text-end">PMC</th>
      <th style="width: 120px;" class="text-end">PFB</th>
      <th style="width: 80px;" class="text-center">UF</th>
      <th style="width: 100px;" class="text-end">Alíquota</th>
      <th style="width: 120px;">Versão</th>
      <th style="width: 100px;">Atualiz.</th>
      <th style="width: 50px;" class="text-center">Ação</th>
    </tr>
  </thead>
  <tbody id="resultadoBody">
    <!-- Preenchido dinamicamente -->
  </tbody>
</table>
```

### Linha de Exemplo (Renderizada por JavaScript)

```html
<tr>
  <td class="fw-600 small">BRAS</td>
  <td class="small">
    <div class="fw-600">401010</div>
    <div class="text-muted small">TISS: 34028</div>
  </td>
  <td class="small">
    <div class="fw-600">Seringa 10mL</div>
    <div class="text-muted">Laboratório XYZ</div>
  </td>
  <td class="text-end small fw-600">R$ 15,50</td>
  <td class="text-end small fw-600">R$ 10,00</td>
  <td class="text-center small">SP</td>
  <td class="text-end small">18,00%</td>
  <td class="small">2024.01</td>
  <td class="small text-muted">2024-10-29</td>
  <td class="text-center">
    <button type="button" class="btn btn-sm btn-outline-primary visualizar-item"
            data-origem="BRAS"
            data-id="12345"
            data-uf="SP"
            title="Detalhes">
      <i class="bi bi-eye"></i>
    </button>
  </td>
</tr>
```

---

## 🎪 Modais

### Modal de Detalhes

```html
<div class="modal fade" id="detalheModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-lg modal-dialog-scrollable">
    <div class="modal-content">
      <div class="modal-header bg-light border-0">
        <h5 class="modal-title fw-600">Detalhes do item</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
      </div>
      <div class="modal-body" id="detalheConteudo">
        <!-- Preenchido dinamicamente -->
      </div>
    </div>
  </div>
</div>
```

### Conteúdo do Modal (Renderizado por JavaScript)

```html
<div>
  <div class="row mb-2 pb-2 border-bottom">
    <div class="col-4 text-muted small fw-600">Origem</div>
    <div class="col-8 small">BRAS</div>
  </div>
  <div class="row mb-2 pb-2 border-bottom">
    <div class="col-4 text-muted small fw-600">TUSS</div>
    <div class="col-8 small">401010</div>
  </div>
  <div class="row mb-2 pb-2 border-bottom">
    <div class="col-4 text-muted small fw-600">ANVISA</div>
    <div class="col-8 small">
      <a href="https://consultas.anvisa.gov.br/..."
         target="_blank" rel="noopener noreferrer"
         class="text-primary">1234567</a>
    </div>
  </div>
  <!-- ... mais campos ... -->
</div>
```

---

## 🎬 Animações

### Fade-In da Página
```css
@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.insumos-container {
  animation: fadeIn 0.5s ease;
}
```

### Spin do Spinner
```css
@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.spinner-border {
  animation: spin 1s linear infinite;
}
```

---

## 📱 Breakpoints e Responsividade

```css
/* Desktop: >= 1024px */
.insumos-container {
  padding: 0 3rem 2rem 3rem;
}

/* Tablet: 768px - 1023px */
@media (max-width: 1024px) {
  .insumos-container {
    padding: 0 1.5rem 1.5rem 1.5rem;
  }
}

/* Mobile: < 768px */
@media (max-width: 768px) {
  .insumos-container {
    padding: 0 1rem 1rem 1rem;
  }

  .table {
    font-size: 0.8rem;
  }

  .card-body {
    padding: 1rem;
  }
}
```

---

## 🔑 IDs de Elementos Críticos

| ID | Elemento | Função |
|----|----------|--------|
| `insumoFilters` | `<form>` | Formulário principal |
| `resultadoTabela` | `<table>` | Tabela de resultados |
| `resultadoBody` | `<tbody>` | Corpo da tabela |
| `resultadoLoading` | `<div>` | Spinner de carregamento |
| `resultadoResumo` | `<small>` | Texto de resumo |
| `fTermo` | `<input>` | Campo de busca |
| `fOrigem` | `<select>` | Dropdown origem |
| `fVersao` | `<select>` | Dropdown versão |
| `fUf` | `<select>` | Dropdown UF |
| `fTuss` | `<input>` | Campo TUSS |
| `fTiss` | `<input>` | Campo TISS |
| `fAnvisa` | `<input>` | Campo ANVISA |
| `fAliquota` | `<input>` | Campo alíquota |
| `fFabricante` | `<input>` | Campo fabricante |
| `fPerPage` | `<select>` | Dropdown itens/página |
| `btnBuscar` | `<button>` | Botão Buscar |
| `btnLimpar` | `<button>` | Botão Limpar |
| `paginacaoAnterior` | `<button>` | Página anterior |
| `paginacaoProximo` | `<button>` | Próxima página |
| `detalheModal` | `<div>` | Modal de detalhes |
| `detalheConteudo` | `<div>` | Corpo do modal |
| `deepDiveModal` | `<div>` | Modal de análise |

---

**Documentação:** HTML Completa v1.0
**Data:** 29 de outubro de 2024
