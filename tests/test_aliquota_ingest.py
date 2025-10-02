from decimal import Decimal
from datetime import datetime

import pytest


def test_ingestir_e_publicar_lote_bras(app_ctx):
    db = app_ctx.db
    BrasItem = app_ctx.BrasItemNormalized

    arquivo_label = 'BRASINDICE_202501_1_1700'
    item = BrasItem(
        arquivo=arquivo_label,
        linha_num=1,
        laboratorio_codigo='LAB001',
        laboratorio_nome='Laboratório Teste',
        produto_codigo='P001',
        produto_nome='Produto Teste',
        apresentacao_codigo='AP001',
        apresentacao_descricao='Apresentação',
        ean='7891234567890',
        registro_anvisa='123456',
        edicao='2025-01',
        preco_pmc_unit=Decimal('32.46'),
        preco_pfb_unit=Decimal('30.00'),
        preco_pmc_pacote=Decimal('0'),
        preco_pfb_pacote=Decimal('0'),
        imported_at=datetime.utcnow(),
    )
    db.session.add(item)
    db.session.commit()

    lote = app_ctx.ingestir_arquivo(
        fornecedor='Brasindice',
        origem='BRAS',
        aliquota_bp=1700,
        periodo='202501',
        sequencia=1,
        arquivo_label=arquivo_label,
        commit=True,
    )

    assert lote.status == app_ctx.LoteStatus.VALIDADO
    assert lote.total_itens == 1
    assert lote.hash_arquivo

    linhas = list(app_ctx.LinhaHash.query.filter_by(lote_id=lote.id))
    assert len(linhas) == 1
    assert linhas[0].hash_linha

    # idempotente
    lote_again = app_ctx.ingestir_arquivo(
        fornecedor='BRASINDICE',
        origem='BRAS',
        aliquota_bp='17',
        periodo='202501',
        sequencia=1,
        arquivo_label=arquivo_label,
        commit=True,
    )
    assert lote_again.id == lote.id
    assert app_ctx.LinhaHash.query.filter_by(lote_id=lote.id).count() == 1

    publicacao = app_ctx.publicar_lote(
        fornecedor='BRASíndice',
        aliquota_bp='17.0',
        periodo='202501',
        sequencia=1,
    )
    assert publicacao.lote_id == lote.id
    assert publicacao.etag_versao == 'BRASINDICE:202501:1'
    assert app_ctx.Lote.query.get(lote.id).status == app_ctx.LoteStatus.PUBLICADO


@pytest.mark.parametrize('aliquota_in,expected', [
    ('17', 1700),
    ('17.5', 1750),
    ('19,5', 1950),
    (Decimal('12.34'), 1234),
])
def test_normalize_aliquota_bp_variants(app_ctx, aliquota_in, expected):
    assert app_ctx._normalize_aliquota_bp(aliquota_in) == expected
