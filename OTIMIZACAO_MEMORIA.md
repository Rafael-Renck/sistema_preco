# Otimização de Memória - Sistema de Preços

## Data: 2025-10-24
## Status: ANÁLISE E RECOMENDAÇÕES

---

## Situação Atual

### Consumo de Memória do Sistema
```
Total:        7.6 GB
Usado:        3.8 GB (49.8%)
Disponível:   3.8 GB
Swap:         2.0 GB (524 KB usado)
```

### Consumo dos Containers Docker
```
Container              Memória        Limite       %
=====================  =============  ===========  =====
sistema_precos-web-1   168 MB         7.61 GB      2.16%
sistema_precos-db-1    424 MB         7.61 GB      5.44%
Total containers:      ~592 MB
```

### Maiores Consumidores (Fora dos Containers)
1. **VSCode Server Extension Host**: 1.14 GB (14.2%)
2. **Cloud Code CLI**: 365 MB (4.5%)
3. **Claude CLI**: 272 MB (3.4%)
4. **HTML Language Server**: 176 MB (2.2%)
5. **Gemini Code Assist**: 146 MB (1.8%)

**Total VSCode + Extensões**: ~2.1 GB

---

## Análise

### ✅ Containers Estão Otimizados
- Web: 168 MB é **excelente** para uma aplicação Flask
- Database: 424 MB é **razoável** para MySQL com 125 MB de dados
- Total: ~592 MB é muito eficiente

### ⚠️ Principais Consumidores de Memória

1. **VSCode + Extensões**: ~2.1 GB (27% da RAM total)
   - Extension Host: 1.14 GB
   - Cloud Code: 365 MB
   - Claude: 272 MB
   - Outras extensões: ~363 MB

2. **Sistema WSL2**: ~400 MB overhead

### 💡 Oportunidades de Otimização

**Impacto Alto (Economia: ~1-2 GB)**
1. Desabilitar extensões VSCode não utilizadas
2. Limitar workers do Gunicorn (atualmente 3)
3. Otimizar MySQL buffer pool

**Impacto Médio (Economia: ~200-500 MB)**
4. Implementar cache em memória limitado
5. Usar PyPy ao invés de CPython (opcional)
6. Limitar cache do SQLAlchemy

**Impacto Baixo (Economia: ~50-150 MB)**
7. Limitar memória dos containers Docker
8. Otimizar queries N+1
9. Lazy loading mais agressivo

---

## Recomendações por Prioridade

### 🔴 PRIORIDADE ALTA - Implementação Imediata

#### 1. Reduzir Workers do Gunicorn
**Economia esperada**: 50-100 MB

**Dockerfile atual**:
```dockerfile
CMD ["gunicorn", "-w", "3", "-b", "0.0.0.0:8000", "app:app"]
```

**Recomendação**:
```dockerfile
# Para uso em desenvolvimento/baixo tráfego
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8000", "app:app"]

# OU com timeout otimizado
CMD ["gunicorn", "-w", "2", "--timeout", "60", "--max-requests", "1000", "--max-requests-jitter", "50", "-b", "0.0.0.0:8000", "app:app"]
```

**Cálculo**:
- Fórmula recomendada: `workers = (2 x CPU_cores) + 1`
- Para 1-2 usuários simultâneos: 2 workers é suficiente
- Cada worker consome ~50-80 MB

---

#### 2. Otimizar MySQL Buffer Pool
**Economia esperada**: Previne crescimento desnecessário

**docker-compose.yml atual**: Sem limites definidos

**Recomendação** - Adicionar ao serviço `db`:
```yaml
db:
  profiles: ["dev"]
  image: mysql:8.0
  command: >
    --default-authentication-plugin=mysql_native_password
    --character-set-server=utf8mb4
    --collation-server=utf8mb4_unicode_ci
    --innodb-buffer-pool-size=256M
    --innodb-log-file-size=64M
    --max-connections=50
    --table-open-cache=400
    --tmp-table-size=32M
    --max-heap-table-size=32M
```

**Justificativa**:
- Database atual: 125 MB
- Buffer pool atual: 128 MB (padrão)
- 256 MB é 2x o tamanho do database (regra geral)

---

#### 3. Limitar Memória dos Containers
**Economia esperada**: Previne vazamentos, não reduz uso normal

**docker-compose.yml - Adicionar limits**:
```yaml
web:
  profiles: ["dev"]
  build: .
  deploy:
    resources:
      limits:
        memory: 512M
      reservations:
        memory: 256M
  # ... resto da configuração

db:
  profiles: ["dev"]
  image: mysql:8.0
  deploy:
    resources:
      limits:
        memory: 1G
      reservations:
        memory: 512M
  # ... resto da configuração
```

---

### 🟡 PRIORIDADE MÉDIA - Implementação Recomendada

#### 4. Desabilitar Extensões VSCode Não Utilizadas
**Economia esperada**: 500 MB - 1 GB

**Extensões detectadas**:
- Gemini Code Assist (146 MB) - se não usa, desabilitar
- Cloud Code CLI (365 MB) - se não usa, desabilitar
- ChatGPT (15 MB) - se usa Claude, talvez desnecessário

**Como fazer**:
1. VSCode → Extensions
2. Desabilitar extensões não usadas diariamente
3. Reiniciar VSCode

---

#### 5. Implementar Cache Limitado no Flask
**Economia esperada**: Previne crescimento, melhora performance

**app.py - Adicionar configuração**:
```python
# No início do arquivo, após imports
from functools import lru_cache
from cachetools import TTLCache

# Cache com limite de tamanho e tempo
_insumo_cache = TTLCache(maxsize=1000, ttl=300)  # 1000 items, 5 min
_teto_cache = TTLCache(maxsize=500, ttl=600)     # 500 items, 10 min

# Ou usar Redis para cache externo (recomendado para produção)
```

---

#### 6. Otimizar Queries SQLAlchemy
**Economia esperada**: 50-100 MB + melhora performance

**Padrões a implementar**:
```python
# 1. Lazy loading seletivo
class Procedimento(db.Model):
    # Usar lazy='select' ao invés de 'joined' quando não precisa
    operadora = db.relationship('Operadora', lazy='select', backref='procedimentos')

# 2. Usar query.options para carregar apenas o necessário
from sqlalchemy.orm import joinedload, selectinload

# Bom
procedimentos = Procedimento.query.options(
    selectinload(Procedimento.operadora)
).limit(100).all()

# Ruim (carrega tudo)
procedimentos = Procedimento.query.all()

# 3. Sempre usar paginação
def get_procedimentos_paginated(page=1, per_page=50):
    return Procedimento.query.paginate(
        page=page,
        per_page=per_page,
        error_out=False
    )
```

---

#### 7. Configurar SQLAlchemy Pool
**Economia esperada**: 30-50 MB

**app.py - Configuração de pool**:
```python
# Após app = Flask(__name__)
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_size': 5,           # Default é 10
    'pool_recycle': 3600,     # Recicla conexões após 1h
    'pool_pre_ping': True,    # Verifica conexão antes de usar
    'max_overflow': 5,        # Max conexões extras (default 10)
}
```

---

### 🟢 PRIORIDADE BAIXA - Otimizações Avançadas

#### 8. Implementar Garbage Collection Agressivo
**app.py - Adicionar**:
```python
import gc

# Forçar coleta após requests pesadas
@app.after_request
def cleanup(response):
    if request.endpoint in ['admin_insumos', 'consulta_comparar']:
        gc.collect()
    return response

# OU configurar GC threshold
gc.set_threshold(700, 10, 10)  # Mais agressivo que padrão (700, 10, 10)
```

---

#### 9. Usar Gunicorn com --preload
**Economia esperada**: 30-50 MB

**Dockerfile**:
```dockerfile
# Carrega app uma vez e faz fork (compartilha memória read-only)
CMD ["gunicorn", "-w", "2", "--preload", "-b", "0.0.0.0:8000", "app:app"]
```

**⚠️ Atenção**: Pode causar problemas com alguns recursos (websockets, etc)

---

#### 10. Monitorar Memória em Tempo Real
**Implementar endpoint de debug**:
```python
@app.route('/debug/memory')
@admin_required
def debug_memory():
    import sys
    import psutil
    import gc

    process = psutil.Process()
    mem_info = process.memory_info()

    return {
        'rss_mb': mem_info.rss / 1024 / 1024,
        'vms_mb': mem_info.vms / 1024 / 1024,
        'percent': process.memory_percent(),
        'gc_stats': gc.get_stats(),
        'object_count': len(gc.get_objects()),
    }
```

---

## Implementação Recomendada - Passo a Passo

### Fase 1: Mudanças Simples (5 minutos)
```bash
# 1. Reduzir workers do Gunicorn
# Editar Dockerfile, linha 27:
CMD ["gunicorn", "-w", "2", "--timeout", "60", "--max-requests", "1000", "-b", "0.0.0.0:8000", "app:app"]

# 2. Rebuild e restart
docker-compose --profile dev build web
docker restart sistema_precos-web-1
```

### Fase 2: Otimizar MySQL (10 minutos)
```bash
# Editar docker-compose.yml
# Adicionar parâmetros ao serviço db (conforme seção 2 acima)

# Restart database
docker restart sistema_precos-db-1
```

### Fase 3: Limitar Memória (5 minutos)
```bash
# Editar docker-compose.yml
# Adicionar deploy.resources (conforme seção 3 acima)

# Restart com nova config
docker-compose --profile dev down
docker-compose --profile dev up -d
```

### Fase 4: Otimizar Código (30-60 minutos)
- Implementar SQLAlchemy pool config
- Adicionar cache limitado
- Otimizar queries N+1

---

## Comparação: Antes vs Depois

### Cenário Atual
```
Web Container:       168 MB
DB Container:        424 MB
Total Containers:    592 MB
VSCode + Extensions: ~2.1 GB
Sistema Total:       3.8 GB usado
```

### Após Otimizações (Estimativa)
```
Web Container:       120 MB (-28%)  [2 workers + preload]
DB Container:        350 MB (-17%)  [buffer pool limitado]
Total Containers:    470 MB (-20%)
VSCode + Extensions: ~1.2 GB (-43%) [desabilitar extensões não usadas]
Sistema Total:       ~2.8 GB (-26%)
```

**Economia esperada total**: ~1 GB de RAM

---

## Monitoramento

### Verificar Uso Atual
```bash
# Containers
docker stats --no-stream sistema_precos-web-1 sistema_precos-db-1

# Sistema
free -h

# Top processos
ps aux --sort=-%mem | head -10
```

### Alertas Importantes
```
⚠️ Web container > 300 MB: Possível memory leak
⚠️ DB container > 800 MB: Queries muito pesadas
⚠️ Swap usage > 100 MB: Memória insuficiente
```

---

## Scripts Úteis

### Script de Monitoramento Contínuo
```bash
#!/bin/bash
# Salvar como: monitor_memory.sh

while true; do
    clear
    echo "=== $(date) ==="
    echo ""
    echo "Containers:"
    docker stats --no-stream sistema_precos-web-1 sistema_precos-db-1
    echo ""
    echo "Sistema:"
    free -h
    echo ""
    echo "Top 5 processos:"
    ps aux --sort=-%mem | head -6
    sleep 5
done
```

### Script de Limpeza
```bash
#!/bin/bash
# Salvar como: cleanup_memory.sh

echo "Limpando Docker..."
docker system prune -f

echo "Limpando cache apt..."
sudo apt-get clean

echo "Limpando cache pip..."
pip cache purge

echo "Dropando caches do sistema..."
sudo sync && sudo sysctl -w vm.drop_caches=3

echo "Memória liberada!"
free -h
```

---

## FAQ

### Q: 3.8 GB usado é muito?
**R**: Para um sistema com 7.6 GB total, 49% é aceitável. Porém, há margem para otimização considerando que os containers usam apenas 592 MB e o resto (~3.2 GB) é VSCode e overhead do sistema.

### Q: Devo aumentar a RAM?
**R**: Não necessariamente. Com as otimizações acima, você pode liberar ~1 GB. Se precisar rodar outros serviços pesados (Redis, ElasticSearch, etc), aí sim considere upgrade.

### Q: Posso usar swap mais agressivamente?
**R**: Swap em SSD é OK, mas evite. Melhor otimizar uso de RAM. Swap > 500 MB indica problema.

### Q: Redis vai ajudar?
**R**: Sim, mas vai consumir ~100-200 MB extras. Só vale a pena se você implementar cache distribuído para múltiplos workers/instâncias.

### Q: PyPy consome menos memória?
**R**: Às vezes sim, mas tem tradeoffs. Para este app, CPython 3.11 é mais estável.

---

## Próximos Passos Sugeridos

### Imediato (Hoje)
1. ✅ Reduzir workers Gunicorn para 2
2. ✅ Adicionar limits de memória nos containers
3. ✅ Desabilitar extensões VSCode não usadas

### Curto Prazo (Esta Semana)
4. ⏳ Otimizar MySQL buffer pool
5. ⏳ Configurar SQLAlchemy pool
6. ⏳ Implementar endpoint de monitoramento

### Médio Prazo (Este Mês)
7. ⏳ Implementar cache TTL limitado
8. ⏳ Otimizar queries N+1
9. ⏳ Adicionar testes de carga para validar otimizações

---

**Versão**: 1.0
**Data**: 2025-10-24
**Autor**: Claude + Rafael Renck
