# Guia de Uso do Frontend Refatorado

## 📁 Estrutura de Arquivos

```
templates/
├── base.html              # Template master (atualizado)
├── login.html            # Login refatorado
├── index.html            # Dashboard
├── components/           # NOVO - Componentes reutilizáveis
│   ├── _button.html
│   ├── _card.html
│   ├── _alert.html
│   ├── _form_group.html
│   ├── _badge.html
│   ├── _modal.html
│   ├── _pagination.html
│   └── _table.html
└── [demais páginas]

static/
├── css/
│   ├── main.css          # NOVO - Arquivo principal
│   ├── reset.css         # NOVO - Reset
│   ├── typography.css    # NOVO - Tipografia
│   ├── layout.css        # NOVO - Layout
│   ├── spacing.css       # NOVO - Espaçamento
│   ├── components-*.css  # NOVO - Componentes
│   ├── animations.css    # NOVO - Animações
│   └── [arquivos antigos mantidos]
├── js/
│   ├── core/
│   │   ├── main.js       # NOVO - Inicialização
│   │   ├── utils.js      # NOVO - Utilitários
│   │   └── api.js        # NOVO - HTTP Client
│   └── modules/
│       ├── toast.js      # NOVO - Notificações
│       ├── modal.js      # NOVO - Modais
│       └── sidebar.js    # NOVO - Sidebar
└── images/
```

---

## 🎨 Como Usar Componentes

### 1. Botões

**Sintaxe:**
```jinja2
{% from 'components/_button.html' import button %}

{{ button(
  text='Clique aqui',
  href='/url',               # Opcional - converte para <a>
  type='button',            # button, submit, reset
  variant='primary',        # primary, secondary, success, danger, warning, outline, etc
  size='md',               # sm, md (padrão), lg
  icon='search',           # ícone do Bootstrap Icons
  disabled=False,
  full_width=False,
  onclick='console.log("clicado")',
  data_attrs={'bs_toggle': 'modal', 'bs_target': '#myModal'}
) }}
```

**Exemplos:**
```jinja2
{# Botão primário com ícone #}
{{ button('Buscar', icon='search', variant='primary') }}

{# Botão link #}
{{ button('Voltar', href='/', variant='outline') }}

{# Botão perigo #}
{{ button('Deletar', variant='danger', onclick='confirm("Tem certeza?")') }}

{# Botão grande e largo #}
{{ button('Enviar', size='lg', full_width=True) }}
```

---

### 2. Cards

**Sintaxe:**
```jinja2
{% from 'components/_card.html' import card %}

{{ card(
  title='Título do Card',
  body='Conteúdo em HTML',
  footer='Rodapé opcional',
  icon='info-circle',
  variant='default',  # default, primary, success, danger, warning, info
  class='custom-class'
) }}
```

**Exemplos:**
```jinja2
{# Card simples #}
{{ card(title='Bem-vindo', body='<p>Olá usuário!</p>') }}

{# Card com ícone e rodapé #}
{{ card(
  title='Dados',
  icon='chart-bar',
  body='<table>...</table>',
  footer='<small>Atualizado agora</small>'
) }}

{# Card com variante #}
{{ card(title='Erro', variant='danger', body='Algo deu errado') }}
```

---

### 3. Alertas

**Sintaxe:**
```jinja2
{% from 'components/_alert.html' import alert %}

{{ alert(
  message='Mensagem de alerta',
  type='info',        # info, success, warning, danger
  dismissible=True,   # Permite fechar?
  icon='alert',       # Ícone customizado (opcional)
  class='custom-class'
) }}
```

**Exemplos:**
```jinja2
{# Alerta de sucesso #}
{{ alert('Operação realizada com sucesso!', type='success') }}

{# Aviso #}
{{ alert('Atenção: Alguns dados podem estar desatualizados', type='warning') }}

{# Erro sem fechar #}
{{ alert('Ocorreu um erro!', type='danger', dismissible=False) }}
```

---

### 4. Formulários

**Sintaxe:**
```jinja2
{% from 'components/_form_group.html' import form_group %}

{{ form_group(
  name='campo',
  label='Rótulo',
  type='text',        # text, email, password, number, date, select, textarea
  placeholder='...',
  value='valor_inicial',
  required=True,
  help_text='Texto de ajuda',
  error='Mensagem de erro',
  class='custom-class'
) }}
```

**Exemplos:**
```jinja2
{# Campo de texto #}
{{ form_group('nome', 'Nome Completo', placeholder='João Silva') }}

{# Email #}
{{ form_group('email', 'E-mail', type='email', required=True) }}

{# Textarea #}
{{ form_group('mensagem', 'Mensagem', type='textarea', placeholder='Digite sua mensagem') }}

{# Com erro #}
{{ form_group('email', 'E-mail', type='email', error='Email inválido') }}

{# Select #}
{% call form_group('categoria', 'Categoria', type='select') %}
  <option value="1">Opção 1</option>
  <option value="2">Opção 2</option>
{% endcall %}
```

---

### 5. Badges

**Sintaxe:**
```jinja2
{% from 'components/_badge.html' import badge %}

{{ badge(
  text='Badge',
  variant='primary',  # primary, secondary, success, danger, warning, info, light, dark
  icon='star',
  class='custom-class'
) }}
```

**Exemplos:**
```jinja2
{# Badge de status #}
{{ badge('Ativo', variant='success') }}

{# Com ícone #}
{{ badge('Novo', icon='star', variant='warning') }}

{# Múltiplos badges #}
{{ badge('Python', variant='info') }}
{{ badge('JavaScript', variant='dark') }}
```

---

### 6. Modais

**Sintaxe (Jinja2):**
```jinja2
{% from 'components/_modal.html' import modal %}

{{ modal(
  id='meuModal',
  title='Título da Modal',
  body='<p>Conteúdo</p>',
  footer='<button class="btn btn-primary">OK</button>',
  size='',            # '', sm, lg, xl
  icon='info-circle',
  centered=True
) }}
```

**Usar em JavaScript:**
```javascript
Modal.open('meuModal');      // Abrir
Modal.close('meuModal');     // Fechar
Modal.closeAll();            // Fechar todas
```

**Exemplo completo:**
```jinja2
{% from 'components/_modal.html' import modal %}

{{ modal(
  id='confirmDeleteModal',
  title='Confirmar Exclusão',
  icon='exclamation-triangle',
  body='<p>Tem certeza que deseja deletar este item?</p>',
  footer='
    <button class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
    <button class="btn btn-danger" onclick="deleteItem()">Deletar</button>
  '
) }}
```

---

### 7. Paginação

**Sintaxe:**
```jinja2
{% from 'components/_pagination.html' import pagination %}

{{ pagination(
  current_page=page,
  total_pages=total,
  base_url='/usuarios'
) }}
```

**Exemplo:**
```jinja2
{{ pagination(current_page=1, total_pages=10, base_url='/produtos') }}
```

---

### 8. Tabelas

**Sintaxe:**
```jinja2
{% from 'components/_table.html' import table %}

{% set columns = [
  {'key': 'nome', 'label': 'Nome'},
  {'key': 'email', 'label': 'Email'},
  {'key': 'status', 'label': 'Status'}
] %}

{{ table(columns=columns, rows=usuarios) }}
```

**Exemplo:**
```jinja2
{% set colunas = [
  {'key': 'id', 'label': 'ID'},
  {'key': 'nome', 'label': 'Nome'},
  {'key': 'email', 'label': 'Email'},
  {'key': 'acao', 'label': 'Ação'}
] %}

{{ table(columns=colunas, rows=usuarios, striped=True, hover=True) }}
```

---

## 🎯 JavaScript (Frontend)

### Toast (Notificações)

```javascript
// Sucesso
Toast.success('Cadastro realizado com sucesso!');

// Erro
Toast.error('Ocorreu um erro ao salvar');

// Aviso
Toast.warning('Alterações não salvas');

// Info
Toast.info('Verifique suas permissões');

// Customizado
Toast.show('Mensagem', type='info', duration=5000);
```

---

### API (Requisições HTTP)

```javascript
// GET
const usuarios = await API.get('/api/usuarios');

// POST
const novo = await API.post('/api/usuarios', {
  nome: 'João',
  email: 'joao@example.com'
});

// PUT
const atualizado = await API.put('/api/usuarios/1', {
  nome: 'João Silva'
});

// DELETE
await API.delete('/api/usuarios/1');

// PATCH
await API.patch('/api/usuarios/1', { status: 'ativo' });

// Upload com progresso
const form = new FormData();
form.append('arquivo', fileInput.files[0]);

await API.upload('/api/upload', form, (percentual) => {
  console.log(`Upload: ${percentual}%`);
});
```

---

### Utils (Funções Utilitárias)

```javascript
// Delays
await Utils.delay(1000);  // Espera 1 segundo

// Debounce (para busca em tempo real)
const search = Utils.debounce((query) => {
  API.get(`/api/search?q=${query}`);
}, 300);
input.addEventListener('input', (e) => search(e.target.value));

// Throttle (para scroll)
const onScroll = Utils.throttle(() => {
  console.log('Scrolling');
}, 300);
window.addEventListener('scroll', onScroll);

// Formatação
Utils.formatDate(new Date(), 'dd/MM/yyyy');        // 29/10/2025
Utils.formatDate(new Date(), 'dd/MM/yyyy HH:mm');  // 29/10/2025 14:30
Utils.formatCurrency(1234.56);                      // R$ 1.234,56
Utils.formatNumber(1234.567, 2);                    // 1.234,57

// Clipboard
await Utils.copyToClipboard('Texto para copiar');

// Validação
Utils.isValidEmail('user@example.com');   // true/false
Utils.isValidCPF('12345678901');         // true/false

// Máscaras
Utils.maskCPF('12345678901');            // 123.456.789-01
Utils.maskPhone('11987654321');          // (11) 98765-4321
Utils.maskCEP('12345678');               // 12345-678

// Query params
Utils.getQueryParam('page');             // Valor do parâmetro 'page'
```

---

### Modal (Gerenciador)

```javascript
// Abrir
Modal.open('minhaModal');

// Fechar uma
Modal.close('minhaModal');

// Fechar todas
Modal.closeAll();
```

---

### Sidebar (Menu)

```javascript
// Inicializar (automático ao carregar)
Sidebar.init();

// Toggle
Sidebar.toggle();

// Abrir
Sidebar.open();

// Fechar
Sidebar.close();
```

---

## 🎨 Variáveis CSS (Design Tokens)

Todos os valores podem ser customizados via CSS variables:

```css
:root {
  /* Cores primárias */
  --primary: #0ea5e9;
  --primary-light: #e0f2fe;
  --primary-dark: #0284c7;

  /* Status */
  --success: #10b981;
  --warning: #f59e0b;
  --danger: #ef4444;
  --info: #6366f1;

  /* Neutros */
  --gray-50: #f9fafb;
  --gray-100: #f3f4f6;
  --gray-200: #e5e7eb;
  --gray-300: #d1d5db;
  /* ... até gray-900 */

  /* Espaçamento */
  --spacing: 1rem;

  /* Sombras */
  --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
  --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
  --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
}
```

---

## 📱 Responsividade

Todos os componentes são responsivos:

```css
/* Breakpoints */
@media (max-width: 640px)  { /* Mobile */ }
@media (max-width: 768px)  { /* Tablet */ }
@media (max-width: 1024px) { /* Desktop pequeno */ }
@media (min-width: 1280px) { /* Desktop */ }
```

---

## ✨ Classe Utilitárias

```html
<!-- Spacing -->
<div class="m-3">Margin</div>
<div class="p-4">Padding</div>
<div class="mb-2">Margin bottom</div>

<!-- Flexbox -->
<div class="d-flex gap-3">
  <div>Item 1</div>
  <div>Item 2</div>
</div>

<!-- Display -->
<div class="d-none">Escondido</div>
<div class="d-block">Visível</div>

<!-- Animações -->
<div class="animate-fade-in">Fade in</div>
<div class="animate-slide-up">Slide up</div>
<div class="hover-scale">Hover scale</div>
<div class="hover-lift">Hover lift</div>
```

---

## 🔧 Customização

Para customizar cores, modifique `static/css/variables.css`:

```css
:root {
  --primary: #SEU_AZUL;
  --success: #SUA_COR_SUCESSO;
  /* ... etc */
}
```

---

## 📚 Exemplo Completo

```jinja2
{% extends 'base.html' %}

{% from 'components/_button.html' import button %}
{% from 'components/_card.html' import card %}
{% from 'components/_form_group.html' import form_group %}
{% from 'components/_alert.html' import alert %}

{% block title %}Novo Usuário{% endblock %}

{% block content %}
  {% if success %}
    {{ alert('Usuário criado com sucesso!', type='success') }}
  {% endif %}

  <div class="row">
    <div class="col-md-6">
      {{ card(
        title='Cadastro de Usuário',
        icon='user-plus',
        body='
          <form method="post">
            ' + form_group('nome', 'Nome Completo', required=True) + '
            ' + form_group('email', 'Email', type='email', required=True) + '
            ' + form_group('senha', 'Senha', type='password', required=True) + '
            ' + button('Salvar', type='submit', variant='primary') + '
          </form>
        '
      ) }}
    </div>
  </div>

  <script>
    // Validar email em tempo real
    const emailInput = document.querySelector('[name="email"]');
    emailInput.addEventListener('blur', () => {
      const valid = Utils.isValidEmail(emailInput.value);
      if (!valid) {
        Toast.warning('Email inválido');
      }
    });
  </script>
{% endblock %}
```

---

## 🚀 Performance Tips

1. **Use debounce para buscas:**
   ```javascript
   const search = Utils.debounce(async (query) => {
     const results = await API.get(`/api/search?q=${query}`);
   }, 300);
   ```

2. **Lazy load imagens:**
   ```html
   <img src="placeholder.jpg" loading="lazy" src="/real-image.jpg">
   ```

3. **Feche modais automaticamente:**
   ```javascript
   setTimeout(() => Modal.close('myModal'), 2000);
   ```

4. **Cache resultados:**
   ```javascript
   const cache = {};
   async function getData(url) {
     if (cache[url]) return cache[url];
     const data = await API.get(url);
     cache[url] = data;
     return data;
   }
   ```

---

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique `FRONTEND_REFACTORING_SUMMARY.md`
2. Consulte exemplos nos componentes
3. Verifique console do navegador para erros

---

**Última atualização:** 29/10/2025
