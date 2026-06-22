from decimal import Decimal


def test_simulacao_cbhpm_teto_alert(app_ctx):
    session = app_ctx.db.session

    operadora = app_ctx.Operadora(nome='Teste', status='Ativa')
    session.add(operadora)
    session.flush()

    tabela = app_ctx.Tabela(
        nome='CBHPM Test',
        tipo_tabela='cbhpm',
        id_operadora=operadora.id,
        uco_valor=Decimal('10.00'),
    )
    session.add(tabela)
    session.flush()

    item = app_ctx.CBHPMItem(
        codigo='123',
        procedimento='Exame XYZ',
        valor_porte=Decimal('100.00'),
        total_porte=Decimal('100.00'),
        filme=Decimal('5.00'),
        incidencias='1',
        total_filme=Decimal('5.00'),
        uco=Decimal('2'),
        total_uco=Decimal('20.00'),
        valor_porte_anestesico=Decimal('30.00'),
        total_porte_anestesico=Decimal('30.00'),
        total_auxiliares=Decimal('10.00'),
        id_tabela=tabela.id,
    )
    session.add(item)
    session.add(app_ctx.CbhpmTeto(
        codigo='123',
        operadora_id=operadora.id,
        descricao='Teto referência',
        valor_total=Decimal('120.00'),
    ))
    session.commit()

    with app_ctx.app.test_request_context():
        payload, status = app_ctx._compute_simulacao_cbhpm({
            'codigos': ['123'],
            'versao': 'CBHPM Test',
            'operadora_id': operadora.id,
        })
    assert status == 200
    assert payload['teto_status'] == 'ULTRAPASSA'
    assert payload['teto_alertas']
    alerta = payload['teto_alertas'][0]
    assert alerta['codigo'] == '123'
    assert Decimal(alerta['excedente']) > 0
    item_payload = payload['itens'][0]
    assert item_payload['teto_valor_total'] == '120.00'
    assert item_payload['teto_excedido'] is True


def test_simulacao_cbhpm_sem_auxiliares_quando_nao_informado(app_ctx):
    item = app_ctx.CBHPMItem(
        codigo='10101012',
        procedimento='Consulta em horário normal ou preestabelecido',
        porte='2B',
        valor_porte=Decimal('224.90'),
        total_porte=Decimal('224.90'),
        numero_auxiliares=None,
        id_tabela=1,
    )
    tabela = app_ctx.Tabela(nome='CBHPM Consulta', tipo_tabela='cbhpm', id_operadora=1)

    br = app_ctx.compute_cbhpm_breakdown(item, tabela, rules=app_ctx.DEFAULT_CBHPM_RULES)
    assert br['total_auxiliares'] == Decimal('0')
    assert br['total'] == Decimal('224.90')


def test_simulacao_cbhpm_calcula_auxiliares_quando_informado(app_ctx):
    item = app_ctx.CBHPMItem(
        codigo='30101039',
        procedimento='Procedimento cirúrgico teste',
        porte='6',
        valor_porte=Decimal('100.00'),
        total_porte=Decimal('100.00'),
        numero_auxiliares=2,
        id_tabela=1,
    )
    tabela = app_ctx.Tabela(nome='CBHPM Cirurgia', tipo_tabela='cbhpm', id_operadora=1)

    br = app_ctx.compute_cbhpm_breakdown(item, tabela, rules=app_ctx.DEFAULT_CBHPM_RULES)
    assert br['total_auxiliares'] == Decimal('50.00')
    assert br['total'] == Decimal('150.00')


def test_simulacao_cbhpm_quatro_auxiliares_porte_alfanumerico(app_ctx):
    """Porte 11B não deve ser limitado pelo default numérico do ruleset."""
    item = app_ctx.CBHPMItem(
        codigo='30401020',
        procedimento='Exérese de tumor com abordagem craniofacial oncológica pavilhão auricular (tempo facial)',
        porte='11B',
        valor_porte=Decimal('3756.00'),
        total_porte=Decimal('3756.00'),
        valor_porte_anestesico=Decimal('3490.67'),
        total_porte_anestesico=Decimal('3490.67'),
        numero_auxiliares=4,
        id_tabela=1,
    )
    tabela = app_ctx.Tabela(nome='CBHPM 2021', tipo_tabela='cbhpm', id_operadora=1)

    br = app_ctx.compute_cbhpm_breakdown(item, tabela, rules=app_ctx.DEFAULT_CBHPM_RULES)
    assert br['total_auxiliares'] == Decimal('2629.20')
    assert br['total_porte_an'] == Decimal('3490.67')
    assert br['total'] == Decimal('9875.87')
    assert len(br.get('auxiliares_detalhe') or []) == 4


def test_simulacao_cbhpm_cap_auxiliares_apenas_porte_numerico(app_ctx):
    """Porte numérico 6 limita a 3 auxiliares mesmo se o catálogo informar 4."""
    item = app_ctx.CBHPMItem(
        codigo='99999999',
        procedimento='Cirurgia porte 6',
        porte='6',
        valor_porte=Decimal('100.00'),
        total_porte=Decimal('100.00'),
        numero_auxiliares=4,
        id_tabela=1,
    )
    tabela = app_ctx.Tabela(nome='CBHPM Test', tipo_tabela='cbhpm', id_operadora=1)

    br = app_ctx.compute_cbhpm_breakdown(item, tabela, rules=app_ctx.DEFAULT_CBHPM_RULES)
    assert br['total_auxiliares'] == Decimal('60.00')
    assert br['total'] == Decimal('160.00')
    assert len(br.get('auxiliares_detalhe') or []) == 3


def test_simulacao_cbhpm_porte_numerico_zero_auxiliares(app_ctx):
    item = app_ctx.CBHPMItem(
        codigo='88888888',
        procedimento='Procedimento porte 1',
        porte='1',
        valor_porte=Decimal('100.00'),
        total_porte=Decimal('100.00'),
        numero_auxiliares=2,
        id_tabela=1,
    )
    tabela = app_ctx.Tabela(nome='CBHPM Test', tipo_tabela='cbhpm', id_operadora=1)

    br = app_ctx.compute_cbhpm_breakdown(item, tabela, rules=app_ctx.DEFAULT_CBHPM_RULES)
    assert br['total_auxiliares'] == Decimal('0')
    assert br['total'] == Decimal('100.00')


def test_simulacao_cbhpm_zero_auxiliares_explicito(app_ctx):
    item = app_ctx.CBHPMItem(
        codigo='10101012',
        procedimento='Consulta',
        porte='2B',
        valor_porte=Decimal('224.90'),
        total_porte=Decimal('224.90'),
        numero_auxiliares=0,
        id_tabela=1,
    )
    tabela = app_ctx.Tabela(nome='CBHPM Consulta', tipo_tabela='cbhpm', id_operadora=1)

    br = app_ctx.compute_cbhpm_breakdown(item, tabela, rules=app_ctx.DEFAULT_CBHPM_RULES)
    assert br['total_auxiliares'] == Decimal('0')
    assert br['total'] == Decimal('224.90')
