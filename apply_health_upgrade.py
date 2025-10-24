#!/usr/bin/env python3
"""
Script para aplicar upgrade do Health Check no app.py
"""

# Novo código da função health_check() completa
new_health_check = '''@app.route('/health')
def health_check():
    """
    Endpoint de health check / status do sistema COMPLETO
    Mostra: banco, CPU, memória, disco, cache, jobs, usuários, alertas
    Acesso público (sem login) para monitoramento externo
    """
    import psutil
    from datetime import datetime, timedelta

    health_data = {
        'timestamp': datetime.now().isoformat(),
        'status': 'healthy',
        'checks': {},
        'alerts': [],
        'recommendations': []
    }

    # 1. Database Connection & Performance
    try:
        start = time.perf_counter()
        db.session.execute(text('SELECT 1')).scalar()
        query_time = (time.perf_counter() - start) * 1000

        # Database size
        db_size_result = db.session.execute(text(
            "SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as size_mb "
            "FROM information_schema.tables WHERE table_schema = DATABASE()"
        )).scalar()

        health_data['checks']['database'] = {
            'status': 'ok' if query_time < 100 else 'warning',
            'message': f'Conexão OK ({query_time:.2f}ms)',
            'query_time_ms': round(query_time, 2),
            'size_mb': float(db_size_result) if db_size_result else 0
        }

        if query_time > 100:
            health_data['alerts'].append({
                'level': 'warning',
                'icon': 'exclamation-triangle',
                'message': f'Database response time alto: {query_time:.2f}ms'
            })
    except Exception as e:
        health_data['status'] = 'unhealthy'
        health_data['checks']['database'] = {
            'status': 'error',
            'message': f'Erro: {str(e)}'
        }
        health_data['alerts'].append({
            'level': 'error',
            'icon': 'x-circle',
            'message': 'Banco de dados inacessível!'
        })

    # 2. Disk Space
    try:
        disk = psutil.disk_usage('/')
        disk_percent = disk.percent
        health_data['checks']['disk'] = {
            'status': 'ok' if disk_percent < 85 else ('warning' if disk_percent < 95 else 'error'),
            'used_percent': round(disk_percent, 1),
            'free_gb': round(disk.free / (1024**3), 2),
            'total_gb': round(disk.total / (1024**3), 2),
            'used_gb': round(disk.used / (1024**3), 2)
        }

        if disk_percent > 95:
            health_data['alerts'].append({
                'level': 'error',
                'icon': 'hdd-fill',
                'message': f'Disco CRÍTICO: {disk_percent}% usado! Libere espaço urgente.'
            })
            health_data['recommendations'].append('Execute limpeza de arquivos temporários e logs antigos')
        elif disk_percent > 85:
            health_data['alerts'].append({
                'level': 'warning',
                'icon': 'hdd',
                'message': f'Disco com {disk_percent}% usado.'
            })
            health_data['recommendations'].append('Planeje limpeza de arquivos antigos em breve')
    except Exception as e:
        health_data['checks']['disk'] = {'status': 'error', 'message': str(e)}

    # 3. Memory
    try:
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        health_data['checks']['memory'] = {
            'status': 'ok' if mem.percent < 85 else ('warning' if mem.percent < 95 else 'error'),
            'used_percent': round(mem.percent, 1),
            'available_gb': round(mem.available / (1024**3), 2),
            'total_gb': round(mem.total / (1024**3), 2),
            'used_gb': round(mem.used / (1024**3), 2),
            'swap_used_percent': round(swap.percent, 1),
            'swap_used_gb': round(swap.used / (1024**3), 2)
        }

        if mem.percent > 95:
            health_data['alerts'].append({
                'level': 'error',
                'icon': 'memory',
                'message': f'Memória CRÍTICA: {mem.percent:.1f}%!'
            })
        elif mem.percent > 85:
            health_data['alerts'].append({
                'level': 'warning',
                'icon': 'memory',
                'message': f'Memória alta: {mem.percent:.1f}%'
            })
    except Exception as e:
        health_data['checks']['memory'] = {'status': 'error', 'message': str(e)}

    # 4. CPU Usage
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        try:
            load_avg = psutil.getloadavg()
        except:
            load_avg = (0, 0, 0)

        health_data['checks']['cpu'] = {
            'status': 'ok' if cpu_percent < 80 else ('warning' if cpu_percent < 95 else 'error'),
            'percent': round(cpu_percent, 1),
            'cores': cpu_count,
            'load_avg_1min': round(load_avg[0], 2),
            'load_avg_5min': round(load_avg[1], 2),
            'load_avg_15min': round(load_avg[2], 2)
        }

        if cpu_percent > 95:
            health_data['alerts'].append({
                'level': 'error',
                'icon': 'cpu',
                'message': f'CPU CRÍTICA: {cpu_percent}%!'
            })
    except Exception as e:
        health_data['checks']['cpu'] = {'status': 'error', 'message': str(e)}

    # 5. Table Counts & Last Updates
    try:
        counts = {}
        counts['usuarios'] = db.session.query(func.count(Usuario.id)).scalar() or 0
        counts['operadoras'] = db.session.query(func.count(Operadora.id)).scalar() or 0
        counts['tabelas'] = db.session.query(func.count(Tabela.id)).scalar() or 0
        counts['procedimentos'] = db.session.query(func.count(Procedimento.id)).scalar() or 0
        counts['simpro'] = db.session.query(func.count(SimproItemNormalized.id)).scalar() or 0
        counts['brasindice'] = db.session.query(func.count(BrasItemNormalized.id)).scalar() or 0
        counts['insumos_index'] = db.session.query(func.count(InsumoIndex.id)).scalar() or 0

        # Últimas atualizações
        last_simpro = db.session.query(func.max(SimproItemNormalized.imported_at)).scalar()
        last_bras = db.session.query(func.max(BrasItemNormalized.imported_at)).scalar()

        health_data['checks']['tables'] = {
            'status': 'ok',
            'counts': counts,
            'last_updates': {
                'simpro': last_simpro.isoformat() if last_simpro else None,
                'brasindice': last_bras.isoformat() if last_bras else None
            }
        }

        # Alertas
        if counts['simpro'] == 0:
            health_data['alerts'].append({
                'level': 'warning',
                'icon': 'database',
                'message': 'Nenhum registro SIMPRO importado'
            })
        if counts['brasindice'] == 0:
            health_data['alerts'].append({
                'level': 'warning',
                'icon': 'database',
                'message': 'Nenhum registro Brasíndice importado'
            })

        # Verificar dados desatualizados
        if last_simpro:
            days_old = (datetime.utcnow() - last_simpro.replace(tzinfo=None)).days
            if days_old > 60:
                health_data['recommendations'].append(f'SIMPRO está {days_old} dias desatualizado')
        if last_bras:
            days_old = (datetime.utcnow() - last_bras.replace(tzinfo=None)).days
            if days_old > 60:
                health_data['recommendations'].append(f'Brasíndice está {days_old} dias desatualizado')

    except Exception as e:
        health_data['checks']['tables'] = {'status': 'error', 'message': str(e)}

    # 6. Cache Status
    cache_size = len(_insumo_cache)
    health_data['checks']['cache'] = {
        'status': 'ok',
        'insumo_cache_size': cache_size,
        'ttl_seconds': _insumo_cache_ttl,
        'hit_potential': 'Alto' if cache_size > 0 else 'Baixo',
        'efficiency': f'{min(100, cache_size * 10)}%'
    }

    # 7. Import Jobs Analysis
    try:
        recent_jobs = ImportJob.query.order_by(ImportJob.created_at.desc()).limit(10).all()
        running_jobs = ImportJob.query.filter_by(status='RUNNING').count()

        yesterday = datetime.utcnow() - timedelta(hours=24)
        failed_24h = ImportJob.query.filter(
            ImportJob.status == 'FAILED',
            ImportJob.created_at >= yesterday
        ).count()

        total_24h = ImportJob.query.filter(ImportJob.created_at >= yesterday).count()
        success_rate = 0
        if total_24h > 0:
            success_24h = ImportJob.query.filter(
                ImportJob.status == 'SUCCESS',
                ImportJob.created_at >= yesterday
            ).count()
            success_rate = (success_24h / total_24h) * 100

        jobs_data = []
        for job in recent_jobs:
            duration = None
            if job.started_at and job.finished_at:
                duration = round((job.finished_at - job.started_at).total_seconds(), 1)

            jobs_data.append({
                'id': job.id,
                'origem': job.origem,
                'status': job.status,
                'versao': job.versao,
                'linhas': job.total_linhas,
                'created_at': job.created_at.isoformat()[:19] if job.created_at else None,
                'duration_seconds': duration
            })

        health_data['checks']['import_jobs'] = {
            'status': 'ok' if failed_24h == 0 else ('warning' if failed_24h < 5 else 'error'),
            'running_now': running_jobs,
            'failed_24h': failed_24h,
            'success_rate_24h': round(success_rate, 1),
            'total_24h': total_24h,
            'recent_jobs': jobs_data
        }

        if running_jobs > 3:
            health_data['alerts'].append({
                'level': 'warning',
                'icon': 'cloud-upload',
                'message': f'{running_jobs} importações simultâneas'
            })

        if failed_24h > 5:
            health_data['alerts'].append({
                'level': 'error',
                'icon': 'exclamation-circle',
                'message': f'{failed_24h} importações falharam (24h)'
            })

        if success_rate < 80 and total_24h > 0:
            health_data['recommendations'].append(f'Taxa de sucesso baixa: {success_rate:.1f}% - Investigue erros')

    except Exception as e:
        health_data['checks']['import_jobs'] = {'status': 'error', 'message': str(e)}

    # 8. Active Users
    try:
        yesterday = datetime.utcnow() - timedelta(hours=24)
        active_24h = db.session.query(func.count(func.distinct(AuditLog.usuario_id))).filter(
            AuditLog.timestamp >= yesterday
        ).scalar() or 0

        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        logins_today = db.session.query(func.count(AuditLog.id)).filter(
            AuditLog.acao == 'login',
            AuditLog.timestamp >= today
        ).scalar() or 0

        total_users = db.session.query(func.count(Usuario.id)).scalar() or 0

        health_data['checks']['users'] = {
            'status': 'ok',
            'active_24h': active_24h,
            'logins_today': logins_today,
            'total_users': total_users,
            'activity_rate': f'{round((active_24h/total_users)*100, 1)}%' if total_users > 0 else '0%'
        }
    except Exception as e:
        health_data['checks']['users'] = {'status': 'error', 'message': str(e)}

    # 9. System Uptime
    try:
        with open('/proc/uptime', 'r') as f:
            uptime_seconds = float(f.readline().split()[0])
        uptime_days = uptime_seconds / 86400

        health_data['checks']['uptime'] = {
            'status': 'ok',
            'seconds': round(uptime_seconds, 0),
            'days': round(uptime_days, 2),
            'formatted': f'{int(uptime_days)}d {int((uptime_seconds % 86400) / 3600)}h {int((uptime_seconds % 3600) / 60)}m'
        }

        if uptime_days > 90:
            health_data['recommendations'].append('Sistema há mais de 90 dias sem restart - Considere manutenção')
    except:
        health_data['checks']['uptime'] = {
            'status': 'ok',
            'message': 'N/A'
        }

    # Status geral
    has_errors = any(c.get('status') == 'error' for c in health_data['checks'].values() if isinstance(c, dict))
    has_warnings = any(c.get('status') == 'warning' for c in health_data['checks'].values() if isinstance(c, dict))

    if has_errors:
        health_data['status'] = 'unhealthy'
    elif has_warnings:
        health_data['status'] = 'degraded'

    # JSON ou HTML
    if request.args.get('format') == 'json':
        return jsonify(health_data)

    return render_template('health.html', health=health_data)'''

# Ler arquivo app.py
with open('/home/rafaelrenck/code/sistema_precos/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Encontrar início e fim da função health_check atual
import re
pattern = r"@app\.route\('/health'\)\ndef health_check\(\):.*?(?=\n@app\.route|$)"
match = re.search(pattern, content, re.DOTALL)

if match:
    # Substituir
    new_content = content[:match.start()] + new_health_check + '\n\n' + content[match.end():]

    # Salvar
    with open('/home/rafaelrenck/code/sistema_precos/app.py', 'w', encoding='utf-8') as f:
        f.write(new_content)

    print("✅ Função health_check() atualizada com sucesso!")
    print(f"📍 Localização: linha {content[:match.start()].count(chr(10)) + 1}")
else:
    print("❌ Erro: Não foi possível encontrar a função health_check()")
