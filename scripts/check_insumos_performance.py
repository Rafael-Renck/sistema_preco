#!/usr/bin/env python3
"""
Script para verificar a performance das otimizações do módulo de insumos.

Uso:
    python scripts/check_insumos_performance.py
"""
import sys
import time
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app import app, db, BrasItemNormalized, SimproItemNormalized
from app import _insumo_summary, _insumo_distinct_versions, _clear_insumo_cache
from sqlalchemy import text


def check_table_counts():
    """Verifica quantidade de registros nas tabelas"""
    print("=" * 60)
    print("📊 CONTAGEM DE REGISTROS")
    print("=" * 60)

    with app.app_context():
        bras_count = db.session.execute(
            text("SELECT COUNT(*) FROM bras_item_n")
        ).scalar()
        simpro_count = db.session.execute(
            text("SELECT COUNT(*) FROM simpro_item_norm")
        ).scalar()

        print(f"BrasItemNormalized:   {bras_count:>10,} registros")
        print(f"SimproItemNormalized: {simpro_count:>10,} registros")
        print(f"Total:                {(bras_count + simpro_count):>10,} registros")
    print()


def check_indexes():
    """Verifica se os índices de performance estão criados"""
    print("=" * 60)
    print("🗂️  VERIFICAÇÃO DE ÍNDICES")
    print("=" * 60)

    indexes_to_check = [
        ('bras_item_n', 'idx_bras_item_n_imported_at'),
        ('bras_item_n', 'idx_bras_item_n_edicao_sorted'),
        ('simpro_item_norm', 'idx_simpro_item_norm_imported_at'),
        ('simpro_item_norm', 'idx_simpro_item_norm_versao_data'),
        ('simpro_item_norm', 'idx_simpro_item_norm_uf_versao'),
    ]

    with app.app_context():
        for table_name, index_name in indexes_to_check:
            result = db.session.execute(text(
                f"SELECT COUNT(*) FROM information_schema.statistics "
                f"WHERE table_schema = DATABASE() "
                f"AND table_name = :table AND index_name = :idx"
            ), {"table": table_name, "idx": index_name}).scalar()

            status = "✅ OK" if result > 0 else "❌ FALTANDO"
            print(f"{status} - {table_name}.{index_name}")
    print()


def benchmark_summary_queries():
    """Testa performance das queries de summary"""
    print("=" * 60)
    print("⚡ BENCHMARK - QUERIES DE SUMMARY")
    print("=" * 60)

    with app.app_context():
        # Limpa cache para teste com cache frio
        _clear_insumo_cache()

        # Teste 1: Cache frio (primeira execução)
        print("\n🧊 Teste 1: Cache FRIO (primeira execução)")
        start = time.perf_counter()
        bras_summary = _insumo_summary(BrasItemNormalized)
        elapsed_bras_cold = time.perf_counter() - start
        print(f"   BrasItemNormalized:   {elapsed_bras_cold*1000:>8.2f} ms")

        start = time.perf_counter()
        simpro_summary = _insumo_summary(SimproItemNormalized)
        elapsed_simpro_cold = time.perf_counter() - start
        print(f"   SimproItemNormalized: {elapsed_simpro_cold*1000:>8.2f} ms")
        print(f"   TOTAL:                {(elapsed_bras_cold + elapsed_simpro_cold)*1000:>8.2f} ms")

        # Teste 2: Cache quente (segunda execução)
        print("\n🔥 Teste 2: Cache QUENTE (segunda execução)")
        start = time.perf_counter()
        _insumo_summary(BrasItemNormalized)
        elapsed_bras_hot = time.perf_counter() - start
        print(f"   BrasItemNormalized:   {elapsed_bras_hot*1000:>8.2f} ms")

        start = time.perf_counter()
        _insumo_summary(SimproItemNormalized)
        elapsed_simpro_hot = time.perf_counter() - start
        print(f"   SimproItemNormalized: {elapsed_simpro_hot*1000:>8.2f} ms")
        print(f"   TOTAL:                {(elapsed_bras_hot + elapsed_simpro_hot)*1000:>8.2f} ms")

        # Calcula melhoria
        total_cold = (elapsed_bras_cold + elapsed_simpro_cold) * 1000
        total_hot = (elapsed_bras_hot + elapsed_simpro_hot) * 1000
        improvement = ((total_cold - total_hot) / total_cold) * 100 if total_cold > 0 else 0

        print(f"\n📈 Melhoria com cache: {improvement:>6.1f}% mais rápido")

    print()


def benchmark_versions_queries():
    """Testa performance das queries de versões"""
    print("=" * 60)
    print("⚡ BENCHMARK - QUERIES DE VERSÕES")
    print("=" * 60)

    with app.app_context():
        # Limpa cache
        _clear_insumo_cache()

        # Cache frio
        print("\n🧊 Cache FRIO:")
        start = time.perf_counter()
        bras_versions = _insumo_distinct_versions(BrasItemNormalized)
        elapsed_cold = time.perf_counter() - start
        print(f"   BrasItemNormalized:   {elapsed_cold*1000:>8.2f} ms ({len(bras_versions)} versões)")

        start = time.perf_counter()
        simpro_versions = _insumo_distinct_versions(SimproItemNormalized)
        elapsed_cold_simpro = time.perf_counter() - start
        print(f"   SimproItemNormalized: {elapsed_cold_simpro*1000:>8.2f} ms ({len(simpro_versions)} versões)")

        # Cache quente
        print("\n🔥 Cache QUENTE:")
        start = time.perf_counter()
        _insumo_distinct_versions(BrasItemNormalized)
        elapsed_hot = time.perf_counter() - start
        print(f"   BrasItemNormalized:   {elapsed_hot*1000:>8.2f} ms")

        start = time.perf_counter()
        _insumo_distinct_versions(SimproItemNormalized)
        elapsed_hot_simpro = time.perf_counter() - start
        print(f"   SimproItemNormalized: {elapsed_hot_simpro*1000:>8.2f} ms")

    print()


def show_summary_data():
    """Mostra os dados retornados pelas funções de summary"""
    print("=" * 60)
    print("📋 DADOS DE SUMMARY")
    print("=" * 60)

    with app.app_context():
        _clear_insumo_cache()

        bras = _insumo_summary(BrasItemNormalized)
        print("\nBrasItemNormalized:")
        print(f"   Total:           {bras['total']:,}")
        print(f"   Última versão:   {bras.get('latest_version', 'N/A')}")
        print(f"   Última atualiz:  {bras.get('last_updated', 'N/A')}")
        print(f"   Última data ref: {bras.get('last_data_ref', 'N/A')}")

        simpro = _insumo_summary(SimproItemNormalized)
        print("\nSimproItemNormalized:")
        print(f"   Total:           {simpro['total']:,}")
        print(f"   Última versão:   {simpro.get('latest_version', 'N/A')}")
        print(f"   Última atualiz:  {simpro.get('last_updated', 'N/A')}")
        print(f"   Última data ref: {simpro.get('last_data_ref', 'N/A')}")

    print()


def main():
    """Executa todos os testes"""
    print("\n" + "=" * 60)
    print("🚀 VERIFICAÇÃO DE PERFORMANCE - MÓDULO DE INSUMOS")
    print("=" * 60)
    print()

    try:
        check_table_counts()
        check_indexes()
        benchmark_summary_queries()
        benchmark_versions_queries()
        show_summary_data()

        print("=" * 60)
        print("✅ VERIFICAÇÃO CONCLUÍDA")
        print("=" * 60)
        print()
        print("💡 Dicas:")
        print("   - Se índices estão faltando, rode: flask db upgrade")
        print("   - Se cache não melhora: verifique _insumo_cache_ttl em app.py")
        print("   - Para limpar cache: reinicie a aplicação")
        print()

    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
