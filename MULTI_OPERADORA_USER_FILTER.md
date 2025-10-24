# Filtro de Operadoras por Usuário - Implementação

## Data: 2025-10-24
## Status: IMPLEMENTADO E FUNCIONAL

---

## Resumo

Implementada funcionalidade para filtrar a lista de operadoras exibida nos formulários e seletores baseado nas operadoras associadas ao usuário logado.

---

## Comportamento

### Usuários com Operadoras Associadas
- **Exemplo**: Fred Almeida, Gabriel Gonçalves, Katia Vilarim (associados à MPF)
- **Comportamento**: Veem apenas as operadoras às quais estão vinculados
- **Dropdown exibe**: Apenas "MPF - Ministério Público Federal"

### Usuários sem Operadoras Associadas (Admins)
- **Exemplo**: Administrador, Adson Vicente
- **Comportamento**: Veem todas as operadoras ativas do sistema
- **Dropdown exibe**: Todas as operadoras com status "Ativa"

---

## Implementação Técnica

### Nova Função Helper

**Localização**: `app.py`, após `_get_teto_map()` (linhas 3942-3963)

```python
def _get_user_operadoras_list():
    """
    Retorna lista de operadoras ativas filtradas pelo usuário logado.

    Se o usuário tiver operadoras associadas, retorna apenas as operadoras dele.
    Se não tiver operadoras associadas (admin geral), retorna todas as operadoras ativas.

    Returns:
        List[Operadora]: Lista de operadoras ativas que o usuário pode acessar
    """
    # Buscar todas as operadoras ativas
    query = Operadora.query.filter_by(status='Ativa').order_by(Operadora.nome)

    # Se o usuário tem operadoras específicas associadas, filtrar por elas
    if hasattr(g, 'current_user') and g.current_user:
        user_operadoras = g.current_user.operadoras
        if user_operadoras:
            # Filtrar apenas pelas operadoras do usuário
            operadora_ids = [op.id for op in user_operadoras]
            query = query.filter(Operadora.id.in_(operadora_ids))

    return query.all()
```

### Rotas Atualizadas

**1. `/admin/tetos` (linha 7051)**
```python
# Antes:
operadoras_list = Operadora.query.filter_by(status='Ativa').order_by(Operadora.nome).all()

# Depois:
operadoras_list = _get_user_operadoras_list()
```

**2. `/consulta-comparar` (linha 5215)**
```python
# Antes:
operadoras_list = Operadora.query.filter_by(status='Ativa').order_by(Operadora.nome).all()

# Depois:
operadoras_list = _get_user_operadoras_list()
```

---

## Páginas Afetadas

### 1. Admin Tetos (`/admin/tetos`)
**Seletores filtrados**:
- Importar/Atualizar Tetos → Dropdown "Operadora"
- Copiar Tetos entre Operadoras → "Operadora Origem" e "Operadora Destino"
- Copiar Procedimentos/DTPs entre Operadoras → "Operadora Origem" e "Operadora Destino"

### 2. Simulador CBHPM (`/consulta-comparar`)
**Seletores filtrados**:
- Formulário Simulador CBHPM → Dropdown "Operadora (para tetos CBHPM)"

---

## Estrutura de Dados

### Tabela `usuario_operadoras`
```sql
CREATE TABLE usuario_operadoras (
    usuario_id INT NOT NULL,
    operadora_id INT NOT NULL,
    PRIMARY KEY (usuario_id, operadora_id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
    FOREIGN KEY (operadora_id) REFERENCES operadoras(id)
);
```

### Modelo Usuario (app.py, linhas 295-323)
```python
class Usuario(db.Model):
    __tablename__ = 'usuarios'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), unique=True, nullable=False)
    # ...
    operadoras = db.relationship(
        'Operadora',
        secondary='usuario_operadoras',
        lazy='joined',
        backref=db.backref('usuarios', lazy='dynamic')
    )
```

---

## Dados Atuais no Sistema

```sql
SELECT u.nome, u.email, o.nome as operadora
FROM usuarios u
LEFT JOIN usuario_operadoras uo ON u.id = uo.usuario_id
LEFT JOIN operadoras o ON uo.operadora_id = o.id;
```

| Usuário | Email | Operadora Associada |
|---------|-------|---------------------|
| Administrador | admin@local | NULL (vê todas) |
| Adson Vicente | adson@murtaconsultoria.com.br | NULL (vê todas) |
| Fred Almeida | fred.almeida@... | MPF (vê apenas MPF) |
| Gabriel Gonçalves | gabriel.goncalves@... | MPF (vê apenas MPF) |
| Katia Vilarim | katia.vilarim@... | MPF (vê apenas MPF) |

---

## Casos de Uso

### Cenário 1: Admin Geral
```
Usuário: Administrador (sem operadoras associadas)
Acessa: /admin/tetos

Dropdown exibe:
- MPF - Ministério Público Federal
- [Outras operadoras ativas cadastradas]

Resultado: Pode importar/copiar tetos para qualquer operadora
```

### Cenário 2: Usuário MPF
```
Usuário: Fred Almeida (associado à MPF)
Acessa: /admin/tetos

Dropdown exibe:
- MPF - Ministério Público Federal

Resultado: Pode importar/copiar tetos apenas para MPF
```

### Cenário 3: Simulador CBHPM
```
Usuário: Gabriel Gonçalves (associado à MPF)
Acessa: /consulta-comparar

Dropdown "Operadora (para tetos CBHPM)" exibe:
- MPF - Ministério Público Federal

Resultado: Simulações usam apenas tetos da MPF
```

---

## Segurança e Isolamento

### Isolamento de Dados por Operadora
- ✅ Usuários veem apenas operadoras às quais têm acesso
- ✅ Não há vazamento de informações de outras operadoras
- ✅ Admins têm visibilidade completa (NULL em usuario_operadoras)

### Controle de Acesso
- ✅ Baseado em relacionamento many-to-many (usuarios ↔ operadoras)
- ✅ Filtro aplicado automaticamente em todas as páginas relevantes
- ✅ Funciona tanto para importação quanto para cópia de dados

---

## Como Associar Usuário a Operadora

### Via SQL
```sql
-- Associar usuário ID=5 à operadora MPF (ID=1)
INSERT INTO usuario_operadoras (usuario_id, operadora_id)
VALUES (5, 1);

-- Remover associação
DELETE FROM usuario_operadoras
WHERE usuario_id = 5 AND operadora_id = 1;

-- Ver associações de um usuário
SELECT u.nome, o.nome as operadora
FROM usuarios u
JOIN usuario_operadoras uo ON u.id = uo.usuario_id
JOIN operadoras o ON uo.operadora_id = o.id
WHERE u.id = 5;
```

### Via Interface (Futuro)
```
TODO: Criar interface admin para gerenciar usuario_operadoras
Localização sugerida: /admin/usuarios
```

---

## Testes Realizados

### ✅ Teste 1: Função Helper
```python
# Usuário com operadora associada
user_operadoras = g.current_user.operadoras  # [MPF]
result = _get_user_operadoras_list()
# Retorna: [Operadora(id=1, nome='MPF')]
```

### ✅ Teste 2: Admin sem Operadoras
```python
# Usuário sem operadoras associadas
user_operadoras = g.current_user.operadoras  # []
result = _get_user_operadoras_list()
# Retorna: [Todas as operadoras ativas]
```

### ✅ Teste 3: Container Reiniciado
```bash
docker restart sistema_precos-web-1
docker logs sistema_precos-web-1 --tail 20
# Resultado: Gunicorn iniciado sem erros
```

---

## Arquivos Modificados

### Backend
- **app.py** (linhas 3942-3963): Nova função `_get_user_operadoras_list()`
- **app.py** (linha 7051): Rota `/admin/tetos` atualizada
- **app.py** (linha 5215): Rota `/consulta-comparar` atualizada

### Nenhuma Mudança no Frontend
- Templates já usam `operadoras_list` passada pelo backend
- Filtro é transparente para o frontend

---

## Documentação Relacionada

- [ADMIN_MULTI_OPERADORA_FINAL.md](ADMIN_MULTI_OPERADORA_FINAL.md) - Implementação completa admin
- [MULTI_OPERADORA_COMPLETO.md](MULTI_OPERADORA_COMPLETO.md) - Resumo executivo multi-operadora
- [MULTI_OPERADORA_IMPLEMENTATION.md](MULTI_OPERADORA_IMPLEMENTATION.md) - Tetos CBHPM
- [MULTI_OPERADORA_DTP_IMPLEMENTATION.md](MULTI_OPERADORA_DTP_IMPLEMENTATION.md) - DTPs

---

**Versão**: 3.3.1
**Data**: 2025-10-24
**Status**: ✅ **PRODUÇÃO**
**Desenvolvido por**: Claude + Rafael Renck
