# 🏥 Instalação Completa do Health Check

## 📋 Arquivos Criados:

1. ✅ `HEALTH_CHECK_UPGRADE.md` - Código Python atualizado
2. ✅ `HEALTH_TEMPLATE_UPGRADE.html` - Template HTML completo
3. ✅ Este arquivo - Instruções de instalação

---

## 🚀 PASSO A PASSO - Instalação:

### **1. Atualizar o Backend (app.py)**

```bash
# Abrir o arquivo
nano /home/rafaelrenck/code/sistema_precos/app.py

# Localizar a função health_check() (linha ~4615)
# Substituir TODO o conteúdo da função pelo código em HEALTH_CHECK_UPGRADE.md
```

**OU** copie e cole manualmente:
- Abra `HEALTH_CHECK_UPGRADE.md`
- Copie TODO o código Python
- Substitua a função `health_check()` em `app.py` (linhas 4615-4740)

---

### **2. Substituir o Template (health.html)**

```bash
# Backup do template antigo
cp templates/health.html templates/health.html.backup

# Substituir pelo novo
cp HEALTH_TEMPLATE_UPGRADE.html templates/health.html
```

**OU** manualmente:
- Abra `HEALTH_TEMPLATE_UPGRADE.html`
- Copie TODO o conteúdo
- Substitua `templates/health.html`

---

### **3. Reiniciar o Docker**

```bash
docker restart sistema_precos-web-1
```

---

### **4. Acessar o Health Check**

Abra no navegador:
```
http://localhost:8000/health
```

**OU** para JSON:
```
http://localhost:8000/health?format=json
```

---

## ✨ Recursos do Health Check Finalizado:

### **📊 Métricas do Sistema:**
- ✅ **CPU** - Uso %, cores, load average (1/5/15 min)
- ✅ **Memória** - RAM + Swap usage
- ✅ **Disco** - Espaço livre/usado
- ✅ **Database** - Conexão, tamanho, performance

### **📈 Métricas de Negócio:**
- ✅ **Tabelas** - Contadores (usuários, operadoras, tabelas, insumos)
- ✅ **Import Jobs** - Taxa de sucesso (24h), jobs rodando, falhas
- ✅ **Usuários Ativos** - Ativos (24h), logins hoje
- ✅ **Cache** - Tamanho, TTL, eficiência

### **🔔 Sistema de Alertas:**
- ❌ **ERRO** (vermelho) - Problemas críticos
- ⚠️ **WARNING** (amarelo) - Atenção necessária
- ℹ️ **INFO** (azul) - Informações

**Exemplos:**
- Disco > 95% → ERRO
- Memória > 95% → ERRO
- CPU > 95% → ERRO
- Query time > 100ms → WARNING
- Importações falhando → WARNING/ERRO

### **💡 Recomendações Automáticas:**
- Limpeza de arquivos antigos
- Dados desatualizados (> 60 dias)
- Taxa de sucesso baixa
- Uptime longo (> 90 dias)

### **⏱️ Outros:**
- ✅ **Uptime** - Tempo desde último restart
- ✅ **Auto-refresh** - Atualização automática (30s)
- ✅ **Export JSON** - Para monitoramento externo

---

## 🎨 Visual:

### **Status Geral:**
- 🟢 **Healthy** - Tudo OK
- 🟡 **Degraded** - Alguns warnings
- 🔴 **Unhealthy** - Erros críticos

### **Cores:**
- Verde (#10b981) - OK
- Amarelo (#f59e0b) - Warning
- Vermelho (#ef4444) - Error
- Azul (#667eea) - Métricas principais

### **Animações:**
- Pulse no status badge
- Shake quando unhealthy
- Hover effects nos cards

---

## 🔧 Troubleshooting:

### **Erro: "psutil not found"**
```bash
docker exec sistema_precos-web-1 pip install psutil
docker restart sistema_precos-web-1
```

### **Erro: "uptime not available"**
Normal - é um fallback. O uptime será calculado via `/proc/uptime`.

### **Página não carrega:**
1. Verificar logs: `docker logs sistema_precos-web-1 --tail 50`
2. Verificar se arquivo foi copiado: `ls templates/health.html`
3. Reiniciar: `docker restart sistema_precos-web-1`

---

## 📱 Uso Recomendado:

### **Monitoramento Manual:**
- Acesse `/health` diariamente
- Ative auto-refresh durante manutenção

### **Monitoramento Automático:**
```bash
# Curl para checagem programada
curl http://localhost:8000/health?format=json | jq '.status'

# Script de alerta
#!/bin/bash
STATUS=$(curl -s http://localhost:8000/health?format=json | jq -r '.status')
if [ "$STATUS" != "healthy" ]; then
    echo "ALERTA: Sistema em estado $STATUS"
    # Enviar email/notificação
fi
```

### **Integração com Monitoramento:**
- **Prometheus**: Use `/health?format=json` como exporter
- **Grafana**: Criar dashboard com métricas
- **Zabbix**: HTTP check em `/health`

---

## 🎯 Métricas Críticas para Observar:

| Métrica | Limite OK | Limite Warning | Limite Error |
|---------|-----------|----------------|--------------|
| Disco | < 85% | 85-95% | > 95% |
| Memória | < 85% | 85-95% | > 95% |
| CPU | < 80% | 80-95% | > 95% |
| Query Time | < 100ms | 100-500ms | > 500ms |
| Taxa Sucesso Imports | > 95% | 80-95% | < 80% |
| Jobs Simultâneos | < 3 | 3-5 | > 5 |

---

## ✅ Checklist Pós-Instalação:

- [ ] Código Python atualizado em `app.py`
- [ ] Template HTML substituído
- [ ] Docker reiniciado
- [ ] Página `/health` acessível
- [ ] Status mostrando "Healthy"
- [ ] Todas as métricas aparecendo
- [ ] Alertas funcionando (testar disco > 85%)
- [ ] Auto-refresh testado
- [ ] JSON export testado (`?format=json`)

---

## 📞 Próximos Passos:

1. **Adicionar ao menu principal** (opcional)
   ```html
   <!-- Em base.html ou navbar -->
   <a href="/health" class="nav-link">
       <i class="bi bi-heart-pulse"></i> System Health
   </a>
   ```

2. **Configurar alertas externos** (opcional)
   - Email quando unhealthy
   - Slack/Discord notifications
   - SMS para casos críticos

3. **Logging** (opcional)
   - Salvar histórico de métricas
   - Gráficos de tendência

---

**Status:** ✅ Pronto para produção!
**Versão:** 1.0
**Data:** 2024-10-23
