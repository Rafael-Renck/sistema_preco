from datetime import date, datetime
from decimal import Decimal


def test_insumos_search_filters(app_ctx):
    session = app_ctx.db.session

    bras_item = app_ctx.BrasItemNormalized(
        id=1,
        arquivo='Brasindice_2025',
        linha_num=1,
        laboratorio_codigo='LAB01',
        laboratorio_nome='ACME',
        produto_codigo='12345',
        produto_nome='Seringa descartável 5ml',
        apresentacao_codigo='AP01',
        apresentacao_descricao='5ml',
        ean='7891234567890',
        registro_anvisa='789',
        edicao='2025-01',
        preco_pmc_pacote=Decimal('25.00'),
        preco_pfb_pacote=Decimal('23.00'),
        preco_pmc_unit=Decimal('12.34'),
        preco_pfb_unit=Decimal('11.00'),
        aliquota_ou_ipi=Decimal('18.0'),
        quantidade_embalagem=10,
        imported_at=datetime.utcnow(),
    )
    session.add(bras_item)
    session.add(app_ctx.InsumoIndex(
        origem='BRAS',
        item_id=bras_item.id,
        descricao='Seringa descartável 5ml',
        preco=Decimal('12.34'),
        aliquota=Decimal('18.0'),
        fabricante='ACME',
        anvisa='789',
        versao_tabela='2025-01',
        updated_at=datetime.utcnow(),
    ))

    simpro_item = app_ctx.SimproItem(
        tuss='54321',
        tiss='B2',
        anvisa='321',
        descricao='Agulha cirúrgica 10mm',
        preco=Decimal('8.50'),
        aliquota=Decimal('12.5'),
        fabricante='Medicorp',
        versao_tabela='2025-02',
        data_atualizacao=date(2025, 2, 15),
        updated_at=datetime.utcnow(),
    )
    session.add(simpro_item)
    session.flush()
    session.add(app_ctx.InsumoIndex(
        origem='SIMPRO',
        item_id=simpro_item.id,
        tuss='54321',
        tiss='B2',
        anvisa='321',
        descricao='Agulha cirúrgica 10mm',
        preco=Decimal('8.50'),
        aliquota=Decimal('12.5'),
        uf_referencia='RJ',
        fabricante='Medicorp',
        versao_tabela='2025-02',
        data_atualizacao=date(2025, 2, 15),
        updated_at=datetime.utcnow(),
    ))
    session.commit()

    client = app_ctx.app.test_client()

    response = client.get('/insumos/search', query_string={'origem': 'BRAS', 'q': 'seringa'})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload['pagination']['total'] == 1
    assert payload['items'][0]['origem'] == 'BRAS'
    assert 'Seringa' in payload['items'][0]['descricao']

    response = client.get('/insumos/search', query_string={'q': 'agulha', 'per_page': 1, 'uf_referencia': 'rj'})
    payload = response.get_json()
    assert payload['pagination']['total'] == 1
    assert payload['items'][0]['origem'] == 'SIMPRO'
    assert payload['items'][0]['uf_referencia'] == 'RJ'
    assert payload['items'][0]['aliquota'] == '12.5'


def test_insumo_detail_route(app_ctx):
    session = app_ctx.db.session

    bras_item = app_ctx.BrasItemNormalized(
        id=200,
        arquivo='Brasindice_2024',
        linha_num=1,
        laboratorio_codigo='LAB02',
        laboratorio_nome='Cirurgia Brasil',
        produto_codigo='1111',
        produto_nome='Fio cirúrgico absorvível',
        apresentacao_codigo='AP02',
        apresentacao_descricao='Pacote',
        ean='ANV123',
        registro_anvisa='ANV123',
        edicao='2024-07',
        preco_pmc_pacote=Decimal('45.00'),
        preco_pfb_pacote=Decimal('40.00'),
        preco_pmc_unit=Decimal('45.00'),
        preco_pfb_unit=Decimal('40.00'),
        aliquota_ou_ipi=Decimal('20'),
        quantidade_embalagem=5,
        imported_at=datetime.utcnow(),
    )
    session.add(bras_item)
    session.commit()

    client = app_ctx.app.test_client()
    response = client.get(f'/insumos/BRAS/{bras_item.id}')
    assert response.status_code == 200
    data = response.get_json()
    assert 'Fio cirúrgico absorvível' in data['descricao']
    assert data['origem'] == 'BRAS'
    assert data['item_id'] == bras_item.id
    assert data['aliquota'] == '20'
