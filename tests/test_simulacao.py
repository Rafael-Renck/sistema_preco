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
    session.add(app_ctx.CBHPMTeto(
        codigo='123',
        descricao='Teto referência',
        valor_total=Decimal('120.00'),
        versao_ref='2024',
    ))
    session.commit()

    payload, status = app_ctx._compute_simulacao_cbhpm({
        'codigos': ['123'],
        'versao': 'CBHPM Test',
    })
    assert status == 200
    assert payload['teto_status'] == 'EXCEDIDO'
    assert payload['teto_alertas']
    alerta = payload['teto_alertas'][0]
    assert alerta['codigo'] == '123'
    assert Decimal(alerta['excedente']) > 0
    item_payload = payload['itens'][0]
    assert item_payload['teto_valor_total'] == '120.00'
    assert item_payload['teto_excedido'] is True
