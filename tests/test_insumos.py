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


def test_simpro_fixed_postprocess_pipeline(app_ctx):
    session = app_ctx.db.session

    def place(segment: list[str], start: int, length: int, value: str) -> None:
        text = str(value)[:length].ljust(length)
        segment[start - 1:start - 1 + length] = list(text)

    line_chars = [' '] * 600
    place(line_chars, 1, 10, '1234567890')            # codigo_interno
    place(line_chars, 16, 10, 'ALT0001111')           # codigo_alternativo
    place(line_chars, 30, 92, 'Produto teste SIMPRO linha completa')
    place(line_chars, 123, 8, '15032025')             # data_vigencia
    place(line_chars, 131, 1, '1')                    # tipo_registro
    place(line_chars, 132, 12, '000000012345')        # preco_pf
    place(line_chars, 144, 12, '000000067890')        # preco_pmc
    place(line_chars, 156, 12, '000000000000')        # preco_ph
    place(line_chars, 168, 12, '000000001234')        # preco_outro
    place(line_chars, 200, 8, 'CX10')
    place(line_chars, 209, 6, '000010')               # qtd_unidade
    place(line_chars, 230, 24, 'FABRICANTE TESTE')
    place(line_chars, 280, 20, 'ANV1234567890123456')
    place(line_chars, 301, 8, '31122026')
    place(line_chars, 310, 16, '580076#NN7827608')
    place(line_chars, 330, 20, 'ATIVO')
    place(line_chars, 350, 200, '   NS12345678   NN')
    fixed_line = ''.join(line_chars).rstrip()

    stage = app_ctx.SimproFixedStage(
        arquivo='SIMPRO_TESTE',
        linha_num=1,
        linha=fixed_line,
    )
    session.add(stage)
    session.commit()

    map_config = {
        "encoding": "latin-1",
        "lines_terminated": "\n",
        "skip_header": False,
        "decimal_divisor": 100,
        "columns": [
            {"name": "codigo_interno", "start": 1, "length": 10},
            {"name": "codigo_alternativo", "start": 16, "length": 10},
            {"name": "descricao_completa", "start": 30, "length": 92},
            {"name": "data_vigencia", "start": 123, "length": 8, "type": "date", "date_fmt": "DDMMYYYY"},
            {"name": "tipo_registro", "start": 131, "length": 1},
            {"name": "preco_pf", "start": 132, "length": 12, "type": "decimal"},
            {"name": "preco_pmc", "start": 144, "length": 12, "type": "decimal"},
            {"name": "preco_ph", "start": 156, "length": 12, "type": "decimal"},
            {"name": "preco_outro", "start": 168, "length": 12, "type": "decimal"},
            {"name": "unidade_comercial", "start": 200, "length": 8},
            {"name": "qtd_unidade", "start": 209, "length": 6, "type": "int"},
            {"name": "fabricante", "start": 230, "length": 24},
            {"name": "registro_anvisa", "start": 280, "length": 20},
            {"name": "validade_anvisa", "start": 301, "length": 8, "type": "date", "date_fmt": "DDMMYYYY"},
            {"name": "ean", "start": 310, "length": 16, "strip": ["+"]},
            {"name": "situacao", "start": 330, "length": 20},
            {"name": "sufixo_livre", "start": 350, "length": 200, "rtrim": True},
        ],
        "postprocess": {
            "extract": [
                {
                    "from": "sufixo_livre",
                    "regex": "(?:[#\\-\\s]?)(N[SNRA])\\s*([0-9]{6,12})",
                    "fields": {"tuss_prefix": 1, "tuss_numero": 2},
                },
                {
                    "from": "sufixo_livre",
                    "regex": "([A-Z]{2})\\s*$",
                    "fields": {"status_final": 1},
                },
            ],
            "derive": [
                {"name": "tuss", "expr": "tuss_prefix && tuss_numero ? tuss_prefix + tuss_numero : null"},
            ],
            "cleanup": ["sufixo_livre"],
        },
    }

    materialized = app_ctx._materialize_simpro_items(
        arquivo_label='SIMPRO_TESTE',
        map_config=map_config,
        versao='2025-09',
        uf_default='RJ',
    )

    assert materialized == 1
    item = session.get(app_ctx.SimproItemNormalized, stage.id)
    assert item is not None
    assert item.codigo_interno == '1234567890'
    assert item.codigo_alt == 'ALT0001111'
    assert item.codigo == '7827608'
    assert item.tuss_prefix == 'NN'
    assert item.tuss_numero == '7827608'
    assert item.status_final == 'NN'
    assert item.descricao.startswith('Produto teste SIMPRO')
    assert item.data_ref == date(2025, 3, 15)
    assert item.preco1 == Decimal('123.45')
    assert item.preco2 == Decimal('678.90')
    assert item.preco3 == Decimal('0')
    assert item.preco4 == Decimal('12.34')
    assert item.qtd_unidade == 10
    assert item.fabricante == 'FABRICANTE TESTE'
    assert item.anvisa == 'ANV1234567890123456'
    assert item.validade_anvisa == date(2026, 12, 31)
    assert item.ean == '580076'
    assert item.situacao == 'ATIVO'
    assert item.uf_referencia == 'RJ'

    # Caso com TUSS embutido no EAN (prefixo SN) e sem sufixo dedicado.
    line_chars_sn = [' '] * 600
    place(line_chars_sn, 1, 10, '8888888888')
    place(line_chars_sn, 16, 10, 'ALT9990000')
    place(line_chars_sn, 30, 92, 'Produto com TUSS no campo EAN')
    place(line_chars_sn, 123, 8, '01012026')
    place(line_chars_sn, 131, 1, '1')
    place(line_chars_sn, 132, 12, '000000050000')
    place(line_chars_sn, 144, 12, '000000070000')
    place(line_chars_sn, 156, 12, '000000090000')
    place(line_chars_sn, 168, 12, '000000010000')
    place(line_chars_sn, 200, 8, 'UNID')
    place(line_chars_sn, 209, 6, '000001')
    place(line_chars_sn, 230, 24, 'FABRICANTE EAN')
    place(line_chars_sn, 280, 20, 'EAN1234567890123')
    place(line_chars_sn, 301, 8, '01012027')
    place(line_chars_sn, 310, 16, '7896004710471+SN')
    place(line_chars_sn, 326, 11, '+SN90434668')
    place(line_chars_sn, 350, 200, '')
    fixed_line_sn = ''.join(line_chars_sn).rstrip()

    stage_sn = app_ctx.SimproFixedStage(
        arquivo='SIMPRO_SN',
        linha_num=1,
        linha=fixed_line_sn,
    )
    session.add(stage_sn)
    session.commit()

    materialized_sn = app_ctx._materialize_simpro_items(
        arquivo_label='SIMPRO_SN',
        map_config=map_config,
        versao='2026-01',
        uf_default='SP',
    )

    assert materialized_sn == 1
    item_sn = session.get(app_ctx.SimproItemNormalized, stage_sn.id)
    assert item_sn is not None
    assert item_sn.codigo == '90434668'
    assert item_sn.tuss_prefix == 'SN'
    assert item_sn.tuss_numero == '90434668'
    assert item_sn.ean == '7896004710471'
