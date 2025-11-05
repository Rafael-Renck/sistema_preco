# 🎨 Plano de Refatoração Frontend - Sistema de Preços
## Especialista em Engenharia de Software Frontend

**Data**: 2025-10-24
**Versão**: 1.0
**Status**: 📋 PLANEJAMENTO

---

## 📊 ANÁLISE ATUAL

### Estrutura Existente
```
templates/
├── base.html (layout principal)
├── login.html
├── index.html (dashboard)
├── consulta-comparar.html (169KB - MUITO GRANDE!)
├── gerenciar-usuarios.html
├── gerenciar-tabelas.html
├── contratos_resumo.html
├── insumos_index.html (61KB)
└── health.html

static/
├── style.css (12KB - styles customizados)
├── logo-*.png (múltiplas variações)
└── favicon.ico
```

### Cores Atuais (MANTER)
```css
:root {
  --brand: #0ea5e9;        /* Azul cyan - primary */
  --sidebar-bg: #1c2530;   /* Azul escuro sidebar */
  --sidebar-bg-2: #2a3440; /* Azul escuro gradient */
  --ok: #22c55e;           /* Verde success */
  --warn: #f59e0b;         /* Laranja warning */
  --danger: #ef4444;       /* Vermelho danger */
  --accent: #3b82f6;       /* Azul accent */
}
```

### Problemas Identificados

1. **Performance**: Templates gigantes (consulta-comparar.html = 169KB)
2. **Inconsistência**: Dois arquivos CSS (inline no base.html + style.css)
3. **Responsividade**: Layout quebra em mobile (<820px)
4. **UX**: Sidebar esconde texto em telas médias sem toggle
5. **Acessibilidade**: Falta labels, ARIA, contraste
6. **Manutenibilidade**: CSS duplicado, sem componentes reutilizáveis

---

## 🎯 OBJETIVOS DA REFATORAÇÃO

### 1. Design Moderno (2025)
- ✨ **Glassmorphism** sutil nos cards
- 🌊 **Micro-animations** suaves
- 📱 **Mobile-first** approach
- 🎨 **Design System** consistente

### 2. Performance
- ⚡ **Code splitting** dos templates grandes
- 🗜️ **CSS minificado** e otimizado
- 🚀 **Lazy loading** de componentes pesados
- 📦 **Bundle size** reduzido

### 3. UX Melhorada
- 🔍 **Search** mais inteligente
- 📊 **Data visualization** aprimorada
- ⌨️ **Keyboard shortcuts**
- 🎯 **Loading states** e feedback visual

### 4. Acessibilidade (WCAG 2.1 AA)
- ♿ **Screen reader** friendly
- ⌨️ **Keyboard navigation**
- 🎨 **Contrast ratios** adequados
- 📱 **Touch targets** >44px

---

## 🏗️ ARQUITETURA NOVA

### Estrutura de Arquivos
```
static/
├── css/
│   ├── design-system.css       (variáveis + tokens)
│   ├── components.css          (componentes reutilizáveis)
│   ├── layouts.css             (grid + flex systems)
│   └── utilities.css           (classes utilitárias)
├── js/
│   ├── app.js                  (app principal)
│   ├── components/
│   │   ├── modal.js
│   │   ├── dropdown.js
│   │   ├── table-filters.js
│   │   └── toast.js
│   └── utils/
│       ├── api.js
│       └── formatters.js
└── img/
    └── logo.svg (otimizado)

templates/
├── base.html                   (layout master)
├── components/
│   ├── sidebar.html
│   ├── topbar.html
│   ├── card.html
│   ├── table.html
│   └── modal.html
├── auth/
│   └── login.html
└── pages/
    ├── dashboard.html
    ├── consulta/
    │   ├── index.html
    │   └── simulador.html
    └── admin/
        ├── usuarios.html
        ├── contratos.html
        └── tetos.html
```

---

## 🎨 DESIGN SYSTEM

### Tokens de Design
```css
/* design-system.css */
:root {
  /* ===== CORES (MANTER AS ATUAIS) ===== */
  --color-primary: #0ea5e9;
  --color-primary-dark: #0284c7;
  --color-primary-light: #38bdf8;

  --color-success: #22c55e;
  --color-warning: #f59e0b;
  --color-danger: #ef4444;
  --color-info: #3b82f6;

  --color-dark: #1c2530;
  --color-dark-2: #2a3440;

  /* ===== NEUTROS ===== */
  --color-slate-50: #f8fafc;
  --color-slate-100: #f1f5f9;
  --color-slate-200: #e2e8f0;
  --color-slate-700: #334155;
  --color-slate-900: #0f172a;

  /* ===== ESPAÇAMENTO ===== */
  --space-1: 0.25rem;  /* 4px */
  --space-2: 0.5rem;   /* 8px */
  --space-3: 0.75rem;  /* 12px */
  --space-4: 1rem;     /* 16px */
  --space-5: 1.25rem;  /* 20px */
  --space-6: 1.5rem;   /* 24px */
  --space-8: 2rem;     /* 32px */
  --space-12: 3rem;    /* 48px */

  /* ===== TIPOGRAFIA ===== */
  --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  --font-mono: 'JetBrains Mono', 'Fira Code', monospace;

  --text-xs: 0.75rem;   /* 12px */
  --text-sm: 0.875rem;  /* 14px */
  --text-base: 1rem;    /* 16px */
  --text-lg: 1.125rem;  /* 18px */
  --text-xl: 1.25rem;   /* 20px */
  --text-2xl: 1.5rem;   /* 24px */
  --text-3xl: 1.875rem; /* 30px */

  /* ===== BORDAS ===== */
  --radius-sm: 0.375rem;  /* 6px */
  --radius-md: 0.5rem;    /* 8px */
  --radius-lg: 0.75rem;   /* 12px */
  --radius-xl: 1rem;      /* 16px */
  --radius-2xl: 1.5rem;   /* 24px */
  --radius-full: 9999px;

  /* ===== SOMBRAS ===== */
  --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
  --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1);
  --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1);
  --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.1);
  --shadow-2xl: 0 25px 50px -12px rgb(0 0 0 / 0.25);

  /* ===== TRANSITIONS ===== */
  --transition-fast: 150ms cubic-bezier(0.4, 0, 0.2, 1);
  --transition-base: 250ms cubic-bezier(0.4, 0, 0.2, 1);
  --transition-slow: 350ms cubic-bezier(0.4, 0, 0.2, 1);

  /* ===== Z-INDEX ===== */
  --z-dropdown: 1000;
  --z-sticky: 1020;
  --z-fixed: 1030;
  --z-modal-backdrop: 1040;
  --z-modal: 1050;
  --z-popover: 1060;
  --z-tooltip: 1070;
}
```

---

## 📐 NOVO LAYOUT

### Estrutura Base
```html
<!-- base.html -->
<!DOCTYPE html>
<html lang="pt-BR" data-theme="light">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{% block title %}{% endblock %} | Murta PriceHealth</title>

  <!-- Fonts -->
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">

  <!-- Stylesheets -->
  <link href="{{ url_for('static', filename='css/design-system.css') }}" rel="stylesheet">
  <link href="{{ url_for('static', filename='css/components.css') }}" rel="stylesheet">
  <link href="{{ url_for('static', filename='css/layouts.css') }}" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">

  {% block head %}{% endblock %}
</head>
<body class="{% block body_class %}{% endblock %}">
  {% if request.endpoint in ['login', 'esqueci_senha'] %}
    <!-- Layout Auth -->
    {% block content_auth %}{% endblock %}
  {% else %}
    <!-- Layout App -->
    <div class="app-shell">
      <!-- Sidebar -->
      {% include 'components/sidebar.html' %}

      <!-- Main -->
      <main class="app-main">
        <!-- Topbar -->
        {% include 'components/topbar.html' %}

        <!-- Content -->
        <div class="app-content">
          {% block content %}{% endblock %}
        </div>

        <!-- Footer -->
        <footer class="app-footer">
          <p>&copy; 2025 Murta Consultoria. Todos os direitos reservados.</p>
        </footer>
      </main>
    </div>
  {% endif %}

  <!-- Scripts -->
  <script src="{{ url_for('static', filename='js/app.js') }}" defer></script>
  {% block scripts %}{% endblock %}
</body>
</html>
```

### Grid System
```css
/* layouts.css */
.app-shell {
  display: grid;
  grid-template-columns: 280px 1fr;
  min-height: 100vh;
  background: var(--color-slate-50);
}

.app-main {
  display: flex;
  flex-direction: column;
  min-width: 0; /* Previne overflow */
}

.app-content {
  flex: 1;
  padding: var(--space-6);
  max-width: 1600px;
  margin: 0 auto;
  width: 100%;
}

/* Responsive */
@media (max-width: 1024px) {
  .app-shell {
    grid-template-columns: 72px 1fr; /* Sidebar compacta */
  }
}

@media (max-width: 768px) {
  .app-shell {
    grid-template-columns: 1fr; /* Sidebar overlay */
  }

  .app-sidebar {
    position: fixed;
    left: -280px;
    top: 0;
    height: 100vh;
    z-index: var(--z-fixed);
    transition: left var(--transition-base);
  }

  .app-sidebar.is-open {
    left: 0;
  }

  .app-content {
    padding: var(--space-4);
  }
}
```

---

## 🧩 COMPONENTES

### 1. Card Component
```css
/* components.css */
.card {
  background: white;
  border-radius: var(--radius-xl);
  box-shadow: var(--shadow-md);
  border: 1px solid var(--color-slate-200);
  overflow: hidden;
  transition: all var(--transition-base);
}

.card:hover {
  box-shadow: var(--shadow-lg);
  transform: translateY(-2px);
}

.card-header {
  padding: var(--space-5);
  border-bottom: 1px solid var(--color-slate-200);
  background: linear-gradient(to bottom, #ffffff, #fafafa);
}

.card-body {
  padding: var(--space-6);
}

.card-footer {
  padding: var(--space-4);
  background: var(--color-slate-50);
  border-top: 1px solid var(--color-slate-200);
}

/* Glassmorphism variant */
.card-glass {
  background: rgba(255, 255, 255, 0.7);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.3);
}
```

### 2. Button System
```css
.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: var(--space-2);
  padding: var(--space-3) var(--space-5);
  font-size: var(--text-sm);
  font-weight: 600;
  border-radius: var(--radius-lg);
  border: 1px solid transparent;
  cursor: pointer;
  transition: all var(--transition-fast);
  text-decoration: none;
  white-space: nowrap;
  user-select: none;
}

.btn:active {
  transform: translateY(1px);
}

/* Variantes */
.btn-primary {
  background: var(--color-primary);
  color: white;
  box-shadow: 0 2px 4px rgba(14, 165, 233, 0.2);
}

.btn-primary:hover {
  background: var(--color-primary-dark);
  box-shadow: 0 4px 8px rgba(14, 165, 233, 0.3);
}

.btn-outline {
  border-color: var(--color-slate-300);
  background: white;
  color: var(--color-slate-700);
}

.btn-outline:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
  background: var(--color-slate-50);
}

.btn-sm { padding: var(--space-2) var(--space-3); font-size: var(--text-xs); }
.btn-lg { padding: var(--space-4) var(--space-6); font-size: var(--text-base); }
```

### 3. Table Component
```css
.data-table-container {
  background: white;
  border-radius: var(--radius-xl);
  box-shadow: var(--shadow-md);
  overflow: hidden;
}

.data-table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
}

.data-table thead th {
  position: sticky;
  top: 0;
  z-index: 10;
  background: linear-gradient(to bottom, #f8fafc, #f1f5f9);
  color: var(--color-slate-700);
  font-weight: 600;
  font-size: var(--text-sm);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding: var(--space-4) var(--space-4);
  border-bottom: 2px solid var(--color-slate-200);
}

.data-table tbody tr {
  transition: background var(--transition-fast);
}

.data-table tbody tr:hover {
  background: var(--color-slate-50);
}

.data-table tbody td {
  padding: var(--space-4);
  border-bottom: 1px solid var(--color-slate-100);
  font-size: var(--text-sm);
}

.data-table tbody tr:last-child td {
  border-bottom: none;
}

/* Zebra striping */
.data-table-striped tbody tr:nth-child(even) {
  background: rgba(248, 250, 252, 0.5);
}
```

### 4. Modal Component
```css
.modal-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.5);
  backdrop-filter: blur(4px);
  z-index: var(--z-modal-backdrop);
  opacity: 0;
  transition: opacity var(--transition-base);
}

.modal-backdrop.is-active {
  opacity: 1;
}

.modal-container {
  position: fixed;
  inset: 0;
  z-index: var(--z-modal);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: var(--space-4);
  pointer-events: none;
}

.modal {
  background: white;
  border-radius: var(--radius-2xl);
  box-shadow: var(--shadow-2xl);
  max-width: 800px;
  width: 100%;
  max-height: 90vh;
  display: flex;
  flex-direction: column;
  transform: scale(0.95) translateY(20px);
  opacity: 0;
  transition: all var(--transition-base);
  pointer-events: auto;
}

.modal.is-active {
  transform: scale(1) translateY(0);
  opacity: 1;
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--space-6);
  border-bottom: 1px solid var(--color-slate-200);
}

.modal-body {
  padding: var(--space-6);
  overflow-y: auto;
  flex: 1;
}

.modal-footer {
  padding: var(--space-5);
  border-top: 1px solid var(--color-slate-200);
  background: var(--color-slate-50);
  display: flex;
  justify-content: flex-end;
  gap: var(--space-3);
}
```

### 5. Form Components
```css
.form-group {
  margin-bottom: var(--space-5);
}

.form-label {
  display: block;
  font-size: var(--text-sm);
  font-weight: 600;
  color: var(--color-slate-700);
  margin-bottom: var(--space-2);
}

.form-control {
  width: 100%;
  padding: var(--space-3) var(--space-4);
  font-size: var(--text-base);
  border: 1px solid var(--color-slate-300);
  border-radius: var(--radius-lg);
  background: white;
  transition: all var(--transition-fast);
}

.form-control:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 4px rgba(14, 165, 233, 0.1);
}

.form-control::placeholder {
  color: var(--color-slate-400);
}

/* Select */
.form-select {
  appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='%23334155' viewBox='0 0 16 16'%3E%3Cpath d='M8 10.5l-4-4h8l-4 4z'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right var(--space-3) center;
  padding-right: var(--space-8);
}
```

### 6. Sidebar Moderna
```css
.app-sidebar {
  background: linear-gradient(180deg, var(--color-dark) 0%, var(--color-dark-2) 100%);
  color: white;
  padding: var(--space-6) var(--space-4);
  display: flex;
  flex-direction: column;
  height: 100vh;
  position: sticky;
  top: 0;
  overflow-y: auto;
}

.sidebar-brand {
  display: flex;
  align-items: center;
  gap: var(--space-3);
  padding: var(--space-4);
  margin-bottom: var(--space-6);
  text-decoration: none;
  color: white;
}

.sidebar-brand-logo {
  height: 48px;
  width: auto;
}

.sidebar-nav {
  flex: 1;
}

.nav-section {
  margin-bottom: var(--space-6);
}

.nav-section-title {
  font-size: var(--text-xs);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: rgba(255, 255, 255, 0.6);
  padding: 0 var(--space-3);
  margin-bottom: var(--space-3);
}

.nav-item {
  display: flex;
  align-items: center;
  gap: var(--space-3);
  padding: var(--space-3) var(--space-3);
  border-radius: var(--radius-lg);
  color: rgba(255, 255, 255, 0.8);
  text-decoration: none;
  transition: all var(--transition-fast);
  position: relative;
}

.nav-item:hover {
  background: rgba(255, 255, 255, 0.1);
  color: white;
}

.nav-item.is-active {
  background: rgba(14, 165, 233, 0.2);
  color: white;
}

.nav-item.is-active::before {
  content: '';
  position: absolute;
  left: 0;
  top: 20%;
  bottom: 20%;
  width: 3px;
  background: var(--color-primary);
  border-radius: 0 2px 2px 0;
}

.nav-item i {
  font-size: var(--text-lg);
  width: 24px;
  text-align: center;
}

.nav-item-text {
  font-size: var(--text-sm);
  font-weight: 500;
}

/* Compact mode */
@media (max-width: 1024px) {
  .nav-section-title,
  .nav-item-text {
    display: none;
  }

  .nav-item {
    justify-content: center;
  }
}
```

---

## 🎭 MICRO-ANIMATIONS

### Loading States
```css
@keyframes shimmer {
  0% { background-position: -468px 0; }
  100% { background-position: 468px 0; }
}

.skeleton {
  background: linear-gradient(
    to right,
    #f0f0f0 0%,
    #e0e0e0 20%,
    #f0f0f0 40%,
    #f0f0f0 100%
  );
  background-size: 800px 104px;
  animation: shimmer 1.5s infinite ease-out;
  border-radius: var(--radius-lg);
}

.skeleton-text {
  height: 12px;
  margin-bottom: var(--space-2);
}

.skeleton-button {
  height: 40px;
  width: 120px;
}
```

### Toast Notifications
```css
.toast-container {
  position: fixed;
  top: var(--space-6);
  right: var(--space-6);
  z-index: var(--z-tooltip);
  display: flex;
  flex-direction: column;
  gap: var(--space-3);
}

.toast {
  background: white;
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-xl);
  padding: var(--space-4) var(--space-5);
  display: flex;
  align-items: center;
  gap: var(--space-3);
  min-width: 300px;
  animation: slideIn 0.3s ease-out;
}

@keyframes slideIn {
  from {
    transform: translateX(100%);
    opacity: 0;
  }
  to {
    transform: translateX(0);
    opacity: 1;
  }
}

.toast-success { border-left: 4px solid var(--color-success); }
.toast-error { border-left: 4px solid var(--color-danger); }
.toast-warning { border-left: 4px solid var(--color-warning); }
.toast-info { border-left: 4px solid var(--color-info); }
```

---

## 📱 RESPONSIVIDADE

### Breakpoints
```css
/* Mobile first */
:root {
  --breakpoint-sm: 640px;
  --breakpoint-md: 768px;
  --breakpoint-lg: 1024px;
  --breakpoint-xl: 1280px;
  --breakpoint-2xl: 1536px;
}

/* Utility classes */
@media (max-width: 640px) {
  .hide-sm { display: none !important; }
  .stack-sm { flex-direction: column !important; }
}

@media (max-width: 768px) {
  .hide-md { display: none !important; }
}

@media (max-width: 1024px) {
  .hide-lg { display: none !important; }
}
```

---

## 🚀 IMPLEMENTAÇÃO FASEADA

### Fase 1: Fundação (Semana 1)
- ✅ Criar estrutura de arquivos CSS
- ✅ Design system tokens
- ✅ Refatorar base.html
- ✅ Criar componentes base (card, button, form)

### Fase 2: Layout (Semana 2)
- ✅ Nova sidebar
- ✅ Topbar moderna
- ✅ Grid system responsivo
- ✅ Login redesenhado

### Fase 3: Páginas Core (Semana 3)
- ✅ Dashboard
- ✅ Consulta & Comparar (split em componentes)
- ✅ Simulador CBHPM

### Fase 4: Admin (Semana 4)
- ✅ Usuários
- ✅ Contratos
- ✅ Tetos
- ✅ Tabelas

### Fase 5: Polish (Semana 5)
- ✅ Micro-animations
- ✅ Loading states
- ✅ Toast system
- ✅ Acessibilidade
- ✅ Performance optimization

---

## 📋 CHECKLIST DE IMPLEMENTAÇÃO

### Antes de Começar
- [ ] Backup completo do sistema atual
- [ ] Criar branch `frontend-refactor`
- [ ] Documentar fluxos de usuário atuais
- [ ] Screenshot de todas as páginas (antes/depois)

### Durante
- [ ] Testar em Chrome, Firefox, Safari, Edge
- [ ] Testar em mobile (iOS + Android)
- [ ] Validar acessibilidade (WAVE, Lighthouse)
- [ ] Verificar performance (PageSpeed)
- [ ] Code review com equipe

### Depois
- [ ] Documentação atualizada
- [ ] Style guide criado
- [ ] Treinamento da equipe
- [ ] Deployment gradual (beta → produção)
- [ ] Monitoramento de erros

---

## 🎯 MÉTRICAS DE SUCESSO

### Performance
- ⚡ **First Contentful Paint**: < 1.5s
- ⚡ **Time to Interactive**: < 3s
- ⚡ **Lighthouse Score**: > 90

### UX
- 📱 **Mobile Usage**: +30%
- ⏱️ **Task Completion Time**: -20%
- 😊 **User Satisfaction**: > 4.5/5

### Técnico
- 🗜️ **CSS Bundle Size**: < 50KB
- 📦 **HTML Reduction**: -30%
- ♿ **WCAG Compliance**: AA Level

---

**Desenvolvido por**: Especialista Frontend Claude
**Versão**: 1.0
**Data**: 2025-10-24
