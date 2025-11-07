# Guia de Componentes Reutilizáveis

Este arquivo documenta todos os componentes disponíveis para uso em seus templates. Eles foram criados para manter consistência e facilitar a manutenção.

## 📁 Estrutura

```
templates/components/
├── _alert.html              # Alertas (sucesso, erro, aviso, info)
├── _auth_container.html     # Container para páginas de autenticação
├── _badge.html              # Badges/etiquetas
├── _button.html             # Botões versáteis
├── _card.html               # Cards (contêineres)
├── _form_group.html         # Grupos de formulário
├── _modal.html              # Modais reutilizáveis
├── _pagination.html         # Paginação
├── _password_input.html     # Input de senha com toggle
└── _table.html              # Tabelas com estilos

static/css/
├── auth.css                 # Estilos para páginas de autenticação

static/js/modules/
├── auth.js                  # JavaScript para autenticação
```

## 🎨 Componentes Disponíveis

### 1. **Alert** (`_alert.html`)

Macro para exibir mensagens de alerta em diferentes variações.

```jinja2
{% from 'components/_alert.html' import alert %}

{{ alert('Operação realizada com sucesso!', type='success') }}
{{ alert('Ocorreu um erro', type='danger', dismissible=True) }}
{{ alert('Atenção!', type='warning', icon='warning-circle') }}
```

**Parâmetros:**
- `message` (str): Mensagem a exibir
- `type` (str): success, danger, warning, info (padrão: info)
- `dismissible` (bool): Mostrar botão de fechar (padrão: True)
- `icon` (str): Nome do ícone Bootstrap (opcional)
- `class` (str): Classes CSS adicionais

---

### 2. **Auth Container** (`_auth_container.html`)

Macro para criar containers de autenticação (login, recuperação de senha, etc).

```jinja2
{% from 'components/_auth_container.html' import auth_container %}

{% call auth_container('Bem-vindo', logo=url_for('static', filename='logo-login.png')) %}
  <!-- Seu conteúdo aqui -->
{% endcall %}
```

**Parâmetros:**
- `title` (str): Título do container
- `logo` (str): URL da logo (opcional)
- `class` (str): Classes CSS adicionais

**Exemplo completo (login.html):**
```jinja2
{% call auth_container('Bem-vindo ao Murta PriceHealth', logo=url_for('static', filename='logo-login.png')) %}
  <form method="post" action="{{ url_for('login') }}">
    <!-- Conteúdo do formulário -->
  </form>
{% endcall %}
```

---

### 3. **Badge** (`_badge.html`)

Etiquetas pequenas para status, categorias, etc.

```jinja2
{% from 'components/_badge.html' import badge %}

{{ badge('Ativo', variant='success') }}
{{ badge('Pendente', variant='warning') }}
{{ badge('Inativo', variant='danger') }}
```

---

### 4. **Button** (`_button.html`)

Botões reutilizáveis com múltiplas variações.

```jinja2
{% from 'components/_button.html' import button %}

{{ button('Salvar', variant='primary', size='md') }}
{{ button('Cancelar', variant='secondary', size='sm') }}
{{ button('Deletar', variant='danger', icon='trash') }}
{{ button('Editar', href='/edit/1', variant='primary', icon='pencil') }}
{{ button('Enviar', type='submit', full_width=True) }}
```

**Parâmetros:**
- `text` (str): Texto do botão
- `href` (str): URL para link (opcional, torna um `<a>`)
- `type` (str): button, submit, reset (padrão: button)
- `variant` (str): primary, secondary, success, danger, warning, outline
- `size` (str): sm, md, lg (padrão: md)
- `icon` (str): Nome do ícone Bootstrap (opcional)
- `disabled` (bool): Desabilitar botão
- `full_width` (bool): Botão em largura total
- `onclick` (str): JavaScript a executar
- `data_attrs` (dict): Atributos data-* adicionais

---

### 5. **Card** (`_card.html`)

Containers versáteis para agrupar conteúdo.

```jinja2
{% from 'components/_card.html' import card %}

{{ card(title='Informações', body='Conteúdo aqui') }}

{% call card(title='Meu Card', icon='info-circle') %}
  <p>Conteúdo usando call</p>
{% endcall %}

{{ card(title='Card', footer='Rodapé aqui', body='Conteúdo') }}
```

**Parâmetros:**
- `title` (str): Título do card (opcional)
- `body` (str): Conteúdo do corpo (opcional)
- `footer` (str): Conteúdo do rodapé (opcional)
- `icon` (str): Ícone no título (opcional)
- `variant` (str): Variação de estilo
- `class` (str): Classes CSS adicionais

---

### 6. **Form Group** (`_form_group.html`)

Grupos de formulário com label, input e mensagens de erro.

```jinja2
{% from 'components/_form_group.html' import form_group %}

{{ form_group('email', 'E-mail', type='email', placeholder='seu@email.com', required=True) }}
{{ form_group('nome', 'Nome Completo', required=True, help_text='Digite seu nome') }}
{{ form_group('descricao', 'Descrição', type='textarea', placeholder='...') }}

{% call form_group('tipo', 'Tipo', type='select') %}
  <option value="1">Opção 1</option>
  <option value="2">Opção 2</option>
{% endcall %}
```

**Parâmetros:**
- `name` (str): Nome do input
- `label` (str): Rótulo do campo
- `type` (str): text, email, password, textarea, select (padrão: text)
- `placeholder` (str): Placeholder do input
- `value` (str): Valor inicial
- `required` (bool): Campo obrigatório
- `help_text` (str): Texto de ajuda (opcional)
- `error` (str): Mensagem de erro (opcional)
- `class` (str): Classes CSS adicionais

---

### 7. **Modal** (`_modal.html`)

Modais reutilizáveis para diálogos e confirmações.

```jinja2
{% from 'components/_modal.html' import modal %}

{% set modal_body %}
  <p>Tem certeza que deseja prosseguir?</p>
{% endset %}

{% set modal_footer %}
  <button class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
  <button class="btn btn-primary">Confirmar</button>
{% endset %}

{{ modal('confirmDialog', 'Confirmação', modal_body, footer=modal_footer) }}
```

**Parâmetros:**
- `id` (str): ID único do modal
- `title` (str): Título do modal
- `body` (str): Conteúdo do corpo
- `footer` (str): Conteúdo do rodapé (opcional)
- `size` (str): sm, lg, xl (opcional)
- `icon` (str): Ícone no título (opcional)
- `centered` (bool): Centralizar modal (padrão: True)

---

### 8. **Password Input** (`_password_input.html`)

Input de senha com toggle de visibilidade.

```jinja2
{% from 'components/_password_input.html' import password_input %}

{{ password_input('senha', 'Senha', required=True) }}
{{ password_input('confirmar_senha', 'Confirmar Senha', required=True) }}
```

**Parâmetros:**
- `name` (str): Nome do input
- `label` (str): Rótulo
- `placeholder` (str): Placeholder (padrão: ••••••••)
- `value` (str): Valor inicial
- `required` (bool): Campo obrigatório
- `error` (str): Mensagem de erro
- `help_text` (str): Texto de ajuda

---

### 9. **Table** (`_table.html`)

Tabelas com estilos consistentes.

```jinja2
{% from 'components/_table.html' import table %}

{{ table(headers=['ID', 'Nome', 'Email'], rows=items) }}
```

---

## 📦 CSS Específico para Autenticação

O arquivo `static/css/auth.css` contém estilos para páginas de autenticação:

- `.auth-wrap` - Wrapper com gradiente de fundo
- `.auth-container` - Container branco com shadow
- `.auth-header` - Cabeçalho com logo e título
- `.auth-alert` - Mensagens de erro/alerta
- `.password-wrapper` - Container para input de senha
- `.btn-toggle-password` - Botão de toggle de senha
- `.btn-auth` - Botão de ação principal
- `.auth-footer` - Rodapé com links

## 🎯 JavaScript

### Auth Module (`static/js/modules/auth.js`)

Módulo JavaScript para autenticação que fornece:

#### Métodos Disponíveis:

```javascript
import { AuthModule } from '/static/js/modules/auth.js';

const auth = new AuthModule();

// Mostrar mensagem de erro
auth.showError('Credenciais inválidas', 'Erro de Login');

// Limpar mensagens de erro
auth.clearErrors();

// Validação de formulário automática
// Adicione data-validate ao formulário para validação automática
```

#### Atributos de Dados Utilizados:

- `data-toggle-password="fieldName"` - Input de senha para toggle automático
- `data-target="#fieldId"` - Botão que ativa toggle
- `data-validate` - Formulário com validação automática

---

## 🚀 Exemplo de Uso Completo (Página de Login)

```jinja2
{% extends "base.html" %}

{% set hide_chrome = True %}

{% block content_auth %}
  {% from 'components/_auth_container.html' import auth_container %}
  {% from 'components/_password_input.html' import password_input %}
  {% from 'components/_alert.html' import alert %}

  {% call auth_container('Bem-vindo', logo=url_for('static', filename='logo-login.png')) %}
    {% if erro %}
      {{ alert(erro, type='danger', dismissible=False) }}
    {% endif %}

    <form method="post" action="{{ url_for('login') }}" data-validate>
      {% from 'components/_form_group.html' import form_group %}

      {{ form_group('email', 'E-mail', type='email', placeholder='seu@email.com', required=True) }}
      {{ password_input('senha', 'Senha', required=True) }}

      <button type="submit" class="btn-auth">Entrar</button>
    </form>
  {% endcall %}
{% endblock %}
```

---

## ✅ Checklist de Refatoração

- [x] Criar `_auth_container.html` para páginas de autenticação
- [x] Criar `_password_input.html` com toggle automático
- [x] Criar `auth.css` com estilos completos
- [x] Criar `auth.js` com funcionalidades JavaScript
- [x] Refatorar `login.html` usando componentes
- [ ] Refatorar outras páginas de autenticação
- [ ] Implementar página de recuperação de senha
- [ ] Implementar página de dois fatores (2FA)

---

## 🎨 Sistema de Variáveis CSS

Todos os componentes utilizam variáveis CSS para fácil customização:

```css
:root {
  --brand-primary: #0ea5e9;
  --brand-primary-dark: #0284c7;
  --auth-bg-gradient-start: #001f3f;
  --auth-bg-gradient-end: #0ea5e9;
  --auth-container-radius: 16px;
  --auth-container-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
}
```

---

## 📝 Notas Importantes

1. Todos os componentes usam **Jinja2 macros** para máxima reutilização
2. Os estilos são modularizados em arquivos CSS separados
3. JavaScript é modularizado em arquivos ES6
4. Bootstrap 5 é a base para todos os estilos
5. Bootstrap Icons é usado para todos os ícones

---

## 🔄 Próximas Etapas

1. Criar página de recuperação de senha usando `_auth_container`
2. Criar página de registro/cadastro
3. Implementar validação frontend avançada
4. Criar tema escuro (dark mode)
5. Adicionar internacionalização (i18n)

