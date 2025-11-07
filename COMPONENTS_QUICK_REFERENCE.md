# 🎨 Referência Rápida de Componentes

## 📦 Todos os Componentes Disponíveis

### 1. Alert Component
**Arquivo:** `_alert.html`

```jinja2
{% from 'components/_alert.html' import alert %}

{{ alert('Sucesso!', type='success') }}
{{ alert('Erro!', type='danger', dismissible=True) }}
{{ alert('Aviso!', type='warning') }}
{{ alert('Info', type='info', icon='info-circle') }}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| message | str | - | Mensagem a exibir |
| type | str | 'info' | success, danger, warning, info |
| dismissible | bool | True | Mostrar botão de fechar |
| icon | str | - | Nome do ícone Bootstrap |
| class | str | '' | Classes CSS adicionais |

---

### 2. Auth Container Component
**Arquivo:** `_auth_container.html`

```jinja2
{% from 'components/_auth_container.html' import auth_container %}

{% call auth_container('Login', logo=url_for('static', filename='logo.png')) %}
  <form method="post">
    <!-- Conteúdo do formulário -->
  </form>
{% endcall %}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| title | str | - | Título do container |
| logo | str | None | URL da logo |
| class | str | '' | Classes CSS adicionais |

**CSS Classes:**
- `.auth-wrap` - Fundo gradiente
- `.auth-container` - Container branco
- `.auth-header` - Cabeçalho
- `.auth-title` - Título principal
- `.auth-subtitle` - Subtítulo (opcional)

---

### 3. Badge Component
**Arquivo:** `_badge.html`

```jinja2
{% from 'components/_badge.html' import badge %}

{{ badge('Ativo', variant='success') }}
{{ badge('Pendente', variant='warning') }}
{{ badge('Inativo', variant='danger') }}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| text | str | - | Texto da badge |
| variant | str | 'primary' | Variação de estilo |
| class | str | '' | Classes CSS adicionais |

---

### 4. Button Component
**Arquivo:** `_button.html`

```jinja2
{% from 'components/_button.html' import button %}

{{ button('Salvar', type='submit') }}
{{ button('Editar', href='/edit', icon='pencil') }}
{{ button('Deletar', variant='danger', icon='trash') }}
{{ button('Enviar', full_width=True) }}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| text | str | - | Texto do botão |
| href | str | None | URL (torna um link) |
| type | str | 'button' | button, submit, reset |
| variant | str | 'primary' | Variação de cor |
| size | str | 'md' | sm, md, lg |
| icon | str | None | Ícone Bootstrap |
| disabled | bool | False | Desabilitar |
| full_width | bool | False | Largura total |
| onclick | str | None | JavaScript |
| data_attrs | dict | {} | Atributos data-* |

---

### 5. Card Component
**Arquivo:** `_card.html`

```jinja2
{% from 'components/_card.html' import card %}

{{ card(title='Meu Card', body='Conteúdo') }}

{% call card(title='Card', icon='info-circle') %}
  <p>Conteúdo usando call</p>
{% endcall %}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| title | str | None | Título |
| body | str | None | Conteúdo |
| footer | str | None | Rodapé |
| icon | str | None | Ícone no título |
| variant | str | 'default' | Variação |
| class | str | '' | Classes CSS |

**CSS Classes:**
- `.card` - Container
- `.card-header` - Cabeçalho
- `.card-body` - Corpo
- `.card-footer` - Rodapé

---

### 6. Form Group Component
**Arquivo:** `_form_group.html`

```jinja2
{% from 'components/_form_group.html' import form_group %}

{{ form_group('email', 'E-mail', type='email', required=True) }}
{{ form_group('mensagem', 'Mensagem', type='textarea') }}

{% call form_group('tipo', 'Tipo', type='select') %}
  <option value="1">Opção 1</option>
  <option value="2">Opção 2</option>
{% endcall %}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| name | str | - | Nome do input |
| label | str | - | Rótulo |
| type | str | 'text' | text, email, password, textarea, select |
| placeholder | str | '' | Placeholder |
| value | str | '' | Valor inicial |
| required | bool | False | Obrigatório |
| help_text | str | '' | Texto de ajuda |
| error | str | None | Mensagem de erro |
| class | str | '' | Classes CSS |

---

### 7. Modal Component
**Arquivo:** `_modal.html`

```jinja2
{% from 'components/_modal.html' import modal %}

{% set body %}Tem certeza?{% endset %}
{% set footer %}<button class="btn btn-primary">Confirmar</button>{% endset %}

{{ modal('confirmModal', 'Confirmação', body, footer=footer) }}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| id | str | - | ID único |
| title | str | - | Título |
| body | str | - | Conteúdo |
| footer | str | None | Rodapé |
| size | str | '' | sm, lg, xl |
| icon | str | None | Ícone |
| centered | bool | True | Centralizar |

---

### 8. Password Input Component
**Arquivo:** `_password_input.html`

```jinja2
{% from 'components/_password_input.html' import password_input %}

{{ password_input('senha', 'Senha', required=True) }}
{{ password_input('confirmar', 'Confirmar', required=True) }}
```

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| name | str | - | Nome do input |
| label | str | - | Rótulo |
| placeholder | str | '••••••••' | Placeholder |
| value | str | '' | Valor |
| required | bool | False | Obrigatório |
| error | str | None | Erro |
| help_text | str | '' | Texto de ajuda |

**CSS Classes:**
- `.password-wrapper` - Container
- `.btn-toggle-password` - Botão toggle
- `.form-control` - Input

---

### 9. Table Component
**Arquivo:** `_table.html`

```jinja2
{% from 'components/_table.html' import table %}

{{ table(headers=['ID', 'Nome', 'Email'], rows=items) }}
```

---

## 🎯 Casos de Uso Comuns

### Formulário de Login Simples
```jinja2
{% from 'components/_auth_container.html' import auth_container %}
{% from 'components/_password_input.html' import password_input %}

{% call auth_container('Login') %}
  <form method="post">
    <div class="form-group">
      <label for="email" class="form-label">E-mail</label>
      <input type="email" id="email" name="email" class="form-control" required>
    </div>
    {{ password_input('senha', 'Senha', required=True) }}
    <button type="submit" class="btn-auth">Entrar</button>
  </form>
{% endcall %}
```

### Página com Cards
```jinja2
{% from 'components/_card.html' import card %}
{% from 'components/_button.html' import button %}

{{ card(title='Usuários', icon='people') }}
  {{ button('Adicionar', variant='primary') }}
  {{ button('Editar', variant='secondary') }}
{{ endcall }}
```

### Modal de Confirmação
```jinja2
{% from 'components/_modal.html' import modal %}

{% set body %}
  <p>Tem certeza que deseja deletar?</p>
{% endset %}

{% set footer %}
  <button class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
  <button class="btn btn-danger">Deletar</button>
{% endset %}

{{ modal('deleteModal', 'Confirmar Exclusão', body, footer=footer) }}
```

### Alertas com Ícones
```jinja2
{% from 'components/_alert.html' import alert %}

{{ alert('Operação realizada!', type='success', icon='check-circle') }}
{{ alert('Algo deu errado!', type='danger', icon='exclamation-circle') }}
{{ alert('Atenção!', type='warning', icon='exclamation-triangle') }}
{{ alert('Informação', type='info', icon='info-circle') }}
```

---

## 🎨 CSS Classes para Estilo Direto

Se preferir usar classes CSS diretamente (sem macros):

```html
<!-- Auth Container -->
<div class="auth-wrap">
  <div class="auth-container">
    <div class="auth-header">
      <img src="logo.png" class="auth-logo">
      <h1 class="auth-title">Login</h1>
      <p class="auth-subtitle">Bem-vindo de volta</p>
    </div>

    <!-- Alert -->
    <div class="auth-alert">
      <i class="bi bi-exclamation-circle"></i>
      <div>Mensagem de erro</div>
    </div>

    <!-- Form -->
    <form class="auth-content">
      <div class="form-group">
        <label for="email" class="form-label">E-mail</label>
        <input type="email" id="email" class="form-control">
      </div>

      <!-- Password with Toggle -->
      <div class="form-group">
        <label for="senha" class="form-label">Senha</label>
        <div class="password-wrapper">
          <input type="password" id="senha" class="form-control" data-toggle-password="senha">
          <button type="button" class="btn-toggle-password" data-target="#senha">
            <i class="bi bi-eye"></i>
          </button>
        </div>
      </div>

      <button type="submit" class="btn-auth">Entrar</button>
    </form>

    <!-- Footer -->
    <div class="auth-footer">
      <p class="auth-footer-text">
        Não tem conta?
        <a href="/register" class="auth-footer-link">Cadastre-se</a>
      </p>
    </div>
  </div>
</div>
```

---

## 🔌 JavaScript Hooks

O módulo `auth.js` fornece funcionalidades automáticas:

```javascript
// Adicione data-toggle-password a inputs de senha
<input type="password" data-toggle-password="fieldname">

// Adicione data-validate a formulários para validação
<form data-validate>...</form>

// Use nos botões de toggle
<button data-target="#fieldId" data-toggle-password="fieldname">...</button>
```

---

## 📱 Responsividade

Todos os componentes são responsive por padrão:
- Mobile (< 480px)
- Tablet (480px - 768px)
- Desktop (> 768px)

Use classes Bootstrap para controle adicional:
```html
<div class="d-none d-md-block">Visível só em desktop</div>
<div class="d-md-none">Visível só em mobile</div>
```

---

## 🎯 Importação de Componentes

Para usar qualquer componente, importe a macro:

```jinja2
{% from 'components/_nome.html' import nome %}

<!-- Ou múltiplos componentes -->
{% from 'components/_alert.html' import alert %}
{% from 'components/_button.html' import button %}
{% from 'components/_card.html' import card %}
```

---

## ✨ Dicas Profissionais

1. **Reutilize componentes** - Use macros em vez de duplicar código
2. **Customize com CSS** - Use classes adicionais para variações
3. **Valide inputs** - Use atributos `required`, `type`, etc.
4. **Acessibilidade** - Sempre inclua `aria-label` em botões
5. **Performance** - CSS externo é carregado uma vez

---

**Última atualização:** 2025-11-04
