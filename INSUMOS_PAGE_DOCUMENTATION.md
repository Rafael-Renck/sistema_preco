# Documentação Completa - Página de Simpro & Brasíndice

## Visão Geral

A página "Simpro & Brasíndice" (`templates/insumos_index.html`) é um módulo de gerenciamento e busca de insumos com funcionalidades avançadas de filtro, paginação e visualização de detalhes.

**URL:** `/insumos/` (Dashboard de Insumos)

---

## 🏗️ Arquitetura da Página

### Estrutura HTML

```
.insumos-container (Wrapper principal com padding 3rem horizontal, 2rem vertical)
├── .insumos-wrapper (Container interno)
│   ├── Summary Cards (Brasíndice + SIMPRO)
│   ├── Botões Admin (Ajustar alíquotas)
│   ├── Card de Filtros
│   │   └── Formulário #insumoFilters
│   │       ├── Busca principal (termo)
│   │       ├── Origem (dropdown)
│   │       ├── Versão (dropdown)
│   │       ├── UF (dropdown)
│   │       ├── Códigos (TUSS, TISS, ANVISA)
│   │       ├── Alíquota (input)
│   │       ├── Fabricante (input)
│   │       ├── Itens por página (dropdown)
│   │       └── Botões (Buscar, Limpar)
│   └── Card de Resultados
│       ├── Header (Resumo + Paginação)
│       └── Tabela #resultadoTabela
│           ├── 10 colunas (Origem, Códigos, Descrição, etc)
│           └── Botão "Visualizar" por linha
│
├── Modal #detalheModal (Detalhes do item selecionado)
└── Modal #deepDiveModal (Análise detalhada - extensível)
```

---

## 📊 CSS - Classes e Estilos

### Classes Principais

| Classe | Descrição | Aplicação |
|--------|-----------|-----------|
| `.insumos-container` | Container principal com padding | Wrapper externo |
| `.insumos-wrapper` | Container interno | Conteúdo da página |
| `.card-primary` | Card com gradient azul | Cards de resumo |
| `.table-light` | Tabela com header destacado | Tabela de resultados |
| `.spinner-border` | Spinner de carregamento | Loader durante requisições |

### Cores e Paleta

- **Primary:** `#0ea5e9` (Azul claro)
- **Success:** `#10b981` (Verde)
- **Danger:** `#ef4444` (Vermelho)
- **Gray-50:** `#f9fafb` (Fundo claro)
- **Gray-900:** `#111827` (Texto escuro)
- **Border:** `#e5e7eb` (Cinza neutro)

### Padding e Espaçamento

```css
.insumos-container {
  padding: 0 3rem 2rem 3rem;  /* Desktop */
  /* Responsivo: 1.5rem no tablet, 1rem no mobile */
}
```

---

## 🎯 Funcionalidades JavaScript

### 1. **Gerenciamento de Estado**

```javascript
let page = 1;           // Página atual (começa em 1)
let totalPages = 0;     // Total de páginas disponíveis
let currentDetail = null;    // Item selecionado (modal)
let currentSimilares = [];   // Itens similares (extensível)
```

### 2. **Funções de Formatação**

#### `parseDecimal(value)`
Converte string de número para float, tratando separadores brasileiros (vírgula/ponto).

**Exemplo:**
```javascript
parseDecimal("1.234,56")  → 1234.56
parseDecimal("R$ 100,00") → 100
```

#### `formatMoney(value)`
Formata número como moeda BRL (R$).

**Exemplo:**
```javascript
formatMoney(1234.56)  → "R$ 1.234,56"
formatMoney(null)     → "—"
```

#### `formatAliquota(value)`
Formata número como percentual.

**Exemplo:**
```javascript
formatAliquota(18)    → "18,00%"
formatAliquota(null)  → "—"
```

#### `escapeHtml(str)`
Escapa caracteres HTML para evitar injeção XSS.

**Exemplo:**
```javascript
escapeHtml("<script>alert('XSS')</script>")
→ "&lt;script&gt;alert('XSS')&lt;/script&gt;"
```

### 3. **Funções de Renderização**

#### `renderRows(items)`
Renderiza array de insumos como linhas da tabela.

**Processo:**
1. Verifica se array está vazio
2. Itera cada item e cria `<tr>` com dados formatados
3. Insere HTML na `#resultadoBody`

**Colunas renderizadas:**
- Origem (Brasíndice/SIMPRO)
- Códigos (TUSS + TISS)
- Descrição + Fabricante
- PMC (Preço Máximo ao Consumidor)
- PFB (Preço Fábrica Base)
- UF
- Alíquota
- Versão
- Data de Atualização
- Botão de Ação (Visualizar)

#### `renderItemDetail(data)`
Renderiza modal com detalhes completos do item.

**Dados exibidos:**
- Origem
- TUSS/TISS
- ANVISA (com link)
- Descrição
- Fabricante
- Preços (PMC/PFB)
- Alíquota
- UF
- Versão
- Data de atualização

### 4. **Funções de Carregamento**

#### `getFilters()`
Coleta valores dos campos de filtro e retorna `URLSearchParams`.

**Parâmetros coletados:**
- `q` - Termo de busca
- `origem` - Brasíndice ou SIMPRO
- `versao_tabela` - Versão da tabela
- `tuss` - Código TUSS
- `tiss` - Código TISS
- `anvisa` - Registro ANVISA
- `uf_referencia` - UF
- `aliquota` - Alíquota percentual
- `fabricante` - Nome do fabricante
- `page` - Página atual
- `per_page` - Itens por página

#### `carregar()`
Função principal que:

1. **Valida** se UF foi selecionada (obrigatório)
2. **Coleta** filtros via `getFilters()`
3. **Mostra** spinner de carregamento
4. **Requisita** API: `GET /insumos/search`
5. **Processa** resposta JSON
6. **Renderiza** linhas na tabela
7. **Atualiza** paginação
8. **Trata** erros
9. **Oculta** spinner

**Fluxo:**
```
Clique em Buscar
        ↓
carregar() executada
        ↓
toggleLoading(true) - Mostra spinner
        ↓
fetch('/insumos/search?params...')
        ↓
JSON.parse() da resposta
        ↓
renderRows(items) - Insere linhas
        ↓
Atualiza resumo + paginação
        ↓
toggleLoading(false) - Oculta spinner
```

### 5. **Event Listeners**

#### Formulário de Filtros
```javascript
form.addEventListener('submit', (evt) => {
  evt.preventDefault();
  page = 1;  // Reset para primeira página
  carregar();
});
```

#### Mudança de itens por página
```javascript
perPageSelect.addEventListener('change', () => {
  page = 1;
  carregar();
});
```

#### Paginação
```javascript
btnPrev.addEventListener('click', () => {
  if(page > 1) {
    page--;
    carregar();
  }
});

btnNext.addEventListener('click', () => {
  if(totalPages && page < totalPages) {
    page++;
    carregar();
  }
});
```

#### Limpar Filtros
```javascript
btnLimpar.addEventListener('click', () => {
  form.reset();
  document.getElementById('fPerPage').value = '50';
});
```

#### Click em "Visualizar"
```javascript
tbody.addEventListener('click', function(evt) {
  const btn = evt.target.closest('.visualizar-item');
  if(!btn) return;

  // Coleta atributos
  const origem = btn.getAttribute('data-origem');
  const itemId = Number(btn.getAttribute('data-id'));
  const ufItem = btn.getAttribute('data-uf');

  // Requisita detalhes
  fetch(`/insumos/${origem}/${itemId}?uf=${ufItem}`)
    .then(r => r.json())
    .then(detail => {
      renderItemDetail(detail);
      modal.show();
    });
});
```

---

## 🔌 Endpoints da API

### Busca de Insumos
```
GET /insumos/search
Query Parameters:
  - q (string): Termo de busca (descrição/fabricante)
  - origem (string): BRAS | SIMPRO
  - versao_tabela (string): Versão da tabela
  - tuss (string): Código TUSS
  - tiss (string): Código TISS
  - anvisa (string): Registro ANVISA
  - uf_referencia (string): UF (obrigatório)
  - aliquota (string): Alíquota %
  - fabricante (string): Nome do fabricante
  - page (int): Página (padrão: 1)
  - per_page (int): Itens por página (padrão: 50)

Response:
{
  "items": [
    {
      "item_id": 123,
      "origem": "BRAS",
      "tuss_numero": "401010",
      "tiss": "34028",
      "anvisa": "1234567",
      "descricao": "Descrição do insumo",
      "fabricante": "Fabricante XYZ",
      "preco_pmc": 100.00,
      "preco_pfb": 80.00,
      "aliquota": 18.00,
      "uf_referencia": "SP",
      "versao_tabela": "1.0",
      "updated_at": "2024-10-29T10:30:00"
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 50,
    "total": 1234,
    "pages": 25
  }
}
```

### Detalhes do Insumo
```
GET /insumos/{origem}/{item_id}
Query Parameters:
  - uf (string): UF para contexto

Response:
{
  "item_id": 123,
  "origem": "BRAS",
  "tuss_numero": "401010",
  "tiss": "34028",
  "anvisa": "1234567",
  "descricao": "Descrição completa",
  "fabricante": "Fabricante",
  "preco_pmc": 100.00,
  "preco_pfb": 80.00,
  "aliquota": 18.00,
  "uf_referencia": "SP",
  "versao_tabela": "1.0",
  "updated_at": "2024-10-29T10:30:00"
}
```

---

## 🎨 Paleta de Cores

| Cor | Código | Uso |
|-----|--------|-----|
| Primary | `#0ea5e9` | Botões, links, highlights |
| Success | `#10b981` | Status positivo, SIMPRO |
| Danger | `#ef4444` | Erros, ações destrutivas |
| Warning | `#f59e0b` | Avisos, estados intermediários |
| Gray-50 | `#f9fafb` | Backgrounds claros |
| Gray-500 | `#6b7280` | Texto muted |
| Gray-900 | `#111827` | Texto principal |
| Border | `#e5e7eb` | Divisões |

---

## 📱 Responsividade

### Breakpoints

```css
Desktop (> 1024px)
├─ Padding: 3rem horizontal
├─ Colunas: full width
└─ Fonte: tamanho normal

Tablet (768px - 1024px)
├─ Padding: 1.5rem horizontal
├─ Tabela: scrollable
└─ Fonte: ligeiramente reduzida

Mobile (< 768px)
├─ Padding: 1rem horizontal
├─ Cards: stack verticalmente
├─ Tabela: fonte 0.8rem
└─ Inputs: full width
```

---

## 🛡️ Segurança

### XSS Prevention
- Todos os dados de usuário são escapados com `escapeHtml()`
- Links externos abrem em nova aba com `rel="noopener noreferrer"`
- HTML dinâmico tratado com cuidado

### Validação
- UF é obrigatório antes de buscar
- Números são parseados com `parseDecimal()` para evitar injeção
- Requisições validadas no backend

---

## 🔧 Extensibilidade

### Adicionar Nova Coluna na Tabela

1. Adicionar `<th>` no header da tabela
2. Mapear propriedade no `renderRows()`:
```javascript
// Adicione a linha no map
const novaColuna = escapeHtml(item.propriedade || '—');

// Adicione a célula no HTML:
<td>${novaColuna}</td>
```

### Adicionar Novo Filtro

1. Adicionar `<input>` ou `<select>` no formulário
2. Mapear em `getFilters()`:
```javascript
const novoFiltro = document.getElementById('fNovoFiltro').value.trim();
if(novoFiltro) data.set('nome_param', novoFiltro);
```

### Usar Deep Dive Modal

1. Implementar função `renderDeepDive(data)`
2. Chamar `deepDiveModalInstance.show()` após dados carregados

---

## 🐛 Troubleshooting

### "Nenhuma busca realizada"
- Clique em "Buscar" para ativar requisição
- Verifique se UF foi selecionada

### Spinner não some
- Verifique console para erros de requisição
- Verifique resposta da API

### Tabela não renderiza
- Verifique se `tbody` com id `resultadoBody` existe
- Verifique estrutura JSON da resposta

### Modal não abre
- Verifique se Bootstrap Modal está carregado
- Verifique ids dos modais

---

## 📝 Alterações Recentes

### Version 2.0 (29 de Outubro de 2024)

✅ **Melhorias de Layout:**
- Adicionado wrapper `.insumos-container` com padding 3rem
- Implementado `.insumos-wrapper` para estrutura interna
- Melhor responsividade em mobile (1rem padding)

✅ **Melhorias de CSS:**
- Estilos melhorados com transições suaves
- Card hover effects
- Animação fadeIn de página
- Spinner melhorado

✅ **Documentação:**
- JSDoc completo em todas as funções
- Comentários inline explicativos
- Estrutura clara de seções

✅ **UX:**
- Feedback visual melhorado
- Mensagens de erro mais claras
- Loading states bem definidos

---

## 📚 Referências

- **Bootstrap:** https://getbootstrap.com/
- **Bootstrap Icons:** https://icons.getbootstrap.com/
- **Fetch API:** https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API
- **Intl.NumberFormat:** https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat

---

**Versão:** 2.0
**Última Atualização:** 29 de outubro de 2024
**Status:** ✅ Completo e Funcional
