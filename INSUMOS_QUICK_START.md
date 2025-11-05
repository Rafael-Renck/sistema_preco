# Quick Start - Página Simpro & Brasíndice

## 🚀 Como Usar a Página

### 1️⃣ Acessar a Página

```
URL: http://seu-dominio/insumos/
```

Você verá:
- ✅ Header com "Simpro & Brasíndice"
- ✅ Dois cards de resumo (Brasíndice + SIMPRO)
- ✅ Formulário de filtros
- ✅ Tabela vazia (aguardando busca)

---

## 🔍 Realizar Uma Busca Básica

### Passo a Passo

1. **Selecione uma UF** (obrigatório)
   - Clique no dropdown "UF"
   - Escolha um estado (ex: SP, MG, RJ, etc)

2. **Opcionalmente, adicione filtros**
   - **Buscar termo:** Digite descrição, código ou fabricante
   - **Origem:** Selecione "Brasíndice" ou "SIMPRO"
   - **Versão:** Selecione versão específica
   - **Códigos:** Digite TUSS, TISS ou ANVISA
   - **Alíquota:** Digite percentual (ex: 18)
   - **Fabricante:** Digite nome do fabricante

3. **Clique no botão "Buscar"** 🔍
   - Você verá um spinner de carregamento
   - Após 1-2 segundos, resultados aparecerão

4. **Resultado aparecerá na tabela**
   ```
   Mostrando 1–50 de 1.234 itens
   ```

---

## 📊 Entender os Resultados

### Colunas da Tabela

| Coluna | Significado | Exemplo |
|--------|------------|---------|
| **Origem** | De qual base vem | BRAS / SIMPRO |
| **Códigos** | TUSS e TISS | 401010 / 34028 |
| **Descrição** | Nome do item | Seringa 10mL |
| **PMC** | Preço Máximo Consumidor | R$ 15,50 |
| **PFB** | Preço Fábrica Base | R$ 10,00 |
| **UF** | Estado | SP, MG, RJ |
| **Alíquota** | ICMS/PIS/COFINS | 18,00% |
| **Versão** | Versão da tabela | 2024.01 |
| **Atualiz.** | Data da última atualização | 2024-10-29 |
| **Ação** | Botão para ver detalhes | 👁 |

---

## 🔎 Visualizar Detalhes de Um Item

1. **Na tabela, clique no ícone de "olho"** na última coluna
2. **Um modal aparecerá** com:
   - Origem do item
   - Código TUSS
   - Código TISS
   - Registro ANVISA (clicável → consulta ANVISA)
   - Descrição completa
   - Fabricante
   - PMC (Preço Máximo)
   - PFB (Preço Fábrica)
   - Alíquota percentual
   - UF
   - Versão
   - Data de atualização

3. **Clique em "Fechar"** para voltar ou clique no X

---

## 📄 Controlar Exibição

### Quantidade de Itens por Página

- Padrão: **50 itens**
- Opções: 25, 50, 100, 250

```
Dropdown "Por página" no formulário de filtros
↓
Selecione quantidade desejada
↓
Tabela recarrega automaticamente com nova quantidade
```

### Navegação entre Páginas

**Se houver múltiplas páginas:**

```
Header da tabela → Botões de navegação
├─ ◀ Página anterior (desabilitado na página 1)
└─ Próxima página ▶ (desabilitado na última página)
```

**Exemplos:**
- Página 1 → clique ▶ → Página 2
- Página 5 → clique ◀ → Página 4

---

## 🗑️ Limpar Filtros

1. Clique no botão **"Limpar"** (ícone de borracha)
2. Todos os campos voltam ao padrão:
   - Buscas vazias
   - "Todas" selecionado onde aplicável
   - 50 itens por página

**Nota:** Você ainda precisa clicar "Buscar" novamente

---

## 🎯 Casos de Uso Práticos

### Caso 1: Encontrar um medicamento específico

```
1. Selecione UF: SP
2. No campo "Buscar termo": Digite "dipirona" ou código
3. Clique "Buscar"
4. Resultado mostrará todos com dipirona em SP
5. Clique no 👁 para ver detalhes e preços
```

### Caso 2: Comparar preços de um código TUSS

```
1. Selecione UF: SP
2. No campo "TUSS": Digite "401010"
3. Clique "Buscar"
4. Tabela mostra todas as variações desse TUSS
5. Compare PMC e PFB entre linhas
```

### Caso 3: Encontrar itens de um fabricante específico

```
1. Selecione UF: RJ
2. No campo "Fabricante": Digite "Laboratório XYZ"
3. Clique "Buscar"
4. Verá todos itens desse fabricante em RJ
```

### Caso 4: Conferir registros ANVISA

```
1. Selecione UF e preencha filtros
2. Clique "Buscar"
3. Na coluna de códigos, procure o link ANVISA
4. Clique para abrir consulta ANVISA em nova aba
5. Confira validade e status do registro
```

### Caso 5: Encontrar itens por alíquota

```
1. Selecione UF: MG
2. No campo "Alíquota": Digite "0" (isentos)
3. Clique "Buscar"
4. Verá todos itens com alíquota 0% em MG
```

---

## 💡 Dicas e Truques

### ⚡ Busca Rápida
- Escreva parte do nome: "seri" encontra "seringa"
- Escreva código TUSS/TISS direto no campo de busca principal

### 🔄 Alternar Origem
- Use o dropdown "Origem" para ver Brasíndice OU SIMPRO
- Deixe em branco para ver ambos

### 📋 Exportar Resultados
- Selecione itens manualmente de cada página
- Use "250 itens por página" para ver mais de uma vez

### 🔗 Link ANVISA
- Clique no código ANVISA para verificar registro oficial
- Abre em nova aba, não perde sua busca

### ⏱️ Busca Lenta?
- Reduza quantidade de itens por página
- Adicione mais filtros (UF + origem + versão)
- Específique um termo de busca

---

## ⚠️ Mensagens Comuns

### "Selecione uma UF para buscar"
```
❌ Significa: UF é obrigatória
✅ Solução: Clique em "Selecione..." no dropdown UF
```

### "Nenhum item encontrado"
```
❌ Significa: Nenhum resultado com seus filtros
✅ Solução: Tente filtros menos restritivos
          Mude a UF ou origem
          Limpe alguns filtros
```

### "Erro ao carregar itens"
```
❌ Significa: Problema no servidor
✅ Solução: Aguarde alguns segundos e tente novamente
          Recarregue a página (F5)
          Contate o administrador se persistir
```

### Spinner não some / Carregamento infinito
```
❌ Significa: Requisição travou
✅ Solução: Recarregue a página
          Limpe cache (Ctrl+Shift+Del)
          Tente com filtros diferentes
```

---

## 🎨 Interface Visual

### Cores e Significados

```
🔵 AZUL (#0ea5e9)
   ↓ Botões principais, links de ação
   ↓ Cards de resumo

🟢 VERDE (#10b981)
   ↓ Status positivo
   ↓ Card SIMPRO

🔴 VERMELHO (#ef4444)
   ↓ Erros, alertas
   ↓ Status crítico

⚫ CINZA (#6b7280)
   ↓ Texto secundário, muted
   ↓ Informações menos importantes
```

---

## 📱 Em Celular

A página é **responsiva** e funciona em:
- ✅ Desktop (1920px+)
- ✅ Tablet (768px-1024px)
- ✅ Celular (até 768px)

**Em celular:**
- Tabela fica mais compacta
- Você pode fazer scroll horizontal
- Todos os filtros ficam acessíveis

---

## 🔐 Permissões

### Usuários Comuns
- ✅ Acessar página
- ✅ Buscar insumos
- ✅ Ver detalhes
- ❌ Não veem botão "Ajustar alíquotas"

### Administradores
- ✅ Acesso completo
- ✅ Botão "Ajustar alíquotas" → `/insumos/aliquotas`
- ℹ️ Veem dica sobre importações em "Gerenciar Tabelas"

---

## 📞 Suporte

Se encontrar problemas:

1. **Tente recarregar a página** (F5)
2. **Limpe cache** (Ctrl+Shift+Del)
3. **Tente em outro navegador** (Chrome, Firefox, Edge)
4. **Abra o console** (F12) e procure por erros
5. **Contate o administrador** com screenshot do erro

---

## 🎓 Próximos Passos

- Leia a documentação completa em `INSUMOS_PAGE_DOCUMENTATION.md`
- Explore os diferentes filtros
- Familiarize-se com os códigos (TUSS, TISS, ANVISA)
- Teste as diferentes UFs e origens

**Versão:** 1.0
**Data:** 29 de outubro de 2024
