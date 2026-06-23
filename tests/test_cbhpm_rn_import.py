"""Testes do importador de Resoluções Normativas CBHPM."""
from pathlib import Path

import cbhpm_rn_import


ROOT = Path(__file__).resolve().parents[1]
RN_DIR = ROOT / 'docs' / 'CBHPM' / 'rn'


def test_parse_rn_059_inclusao():
    pdf = RN_DIR / 'RN_CNHM_059.pdf'
    if not pdf.is_file():
        return
    changes = cbhpm_rn_import.parse_rn_pdf(pdf)
    by_code = {c.codigo: c for c in changes}
    assert '3.02.04.11-9' in by_code
    item = by_code['3.02.04.11-9']
    assert item.decisao == 'inclusao'
    assert 'Sialoendoscopia' in item.descricao
    assert item.porte == '7B'


def test_parse_rn_071_robotica():
    pdf = RN_DIR / 'RN_CNHM_071.pdf'
    if not pdf.is_file():
        return
    changes = cbhpm_rn_import.parse_rn_pdf(pdf)
    codes = {c.codigo for c in changes if c.decisao == 'inclusao'}
    assert '3.10.03.80-0' in codes
    assert '3.12.01.18-0' in codes


def test_parse_rn_055_incidencia():
    body = (
        'Inclusão de Procedimento US - Estudo sonográfico dinâmico das vias urinárias (masculino), '
        'Porte 4C, UCO 13,240, Filme: 0,6800 e Incidência: 4'
    )
    parsed = cbhpm_rn_import._parse_inclusion_body('4.09.01.58-0', body)
    assert parsed['porte'] == '4C'
    assert parsed['uco'] == '13,240'
    assert parsed['filme'] == '0,6800'
    assert parsed['incidencia'] == '4'


def test_parse_rn_055_porte_uco_filme():
    body = 'Inclusão de Procedimento RM - Tractografia, Porte 4A, UCO 51,964 e Filme 4,0000'
    parsed = cbhpm_rn_import._parse_inclusion_body('4.11.01.44-8', body)
    assert parsed['porte'] == '4A'
    assert parsed['uco'] == '51,964'
    assert parsed['filme'] == '4,0000'
    assert 'Tractografia' in parsed['descricao']


def test_parse_rn_porte_sem_uco_sem_filme():
    body = 'Inclusão de Procedimento Cirurgia teste, Porte 9B, Número de Auxiliar 1 e Porte Anestésico 3'
    parsed = cbhpm_rn_import._parse_inclusion_body('3.12.01.17-2', body)
    assert parsed['porte'] == '9B'
    assert parsed.get('uco') in (None, '')
    assert parsed.get('filme') in (None, '')
    assert parsed['num_auxiliares'] == '1'
    assert parsed['porte_anestesico'] == '3'


def test_parse_rn_071_auxiliar_after_label():
    body = (
        'Inclusão de Procedimento Artroplastia de joelho infectada (limpeza articular) '
        'Porte 9C Auxiliar 2 Porte Anestésico 5'
    )
    parsed = cbhpm_rn_import._parse_inclusion_body('3.07.26.41-7', body)
    assert parsed['porte'] == '9C'
    assert parsed['num_auxiliares'] == '2'
    assert parsed['porte_anestesico'] == '5'
    assert parsed['layout'] == 'cirurgico'
    assert 'Porte' not in parsed['descricao']


def test_parse_rn_071_from_pdf():
    pdf = RN_DIR / 'RN_CNHM_071.pdf'
    if not pdf.is_file():
        return
    changes = cbhpm_rn_import.parse_rn_pdf(pdf)
    by_code = {c.codigo: c for c in changes}
    item = by_code['3.07.26.41-7']
    assert item.num_auxiliares == '2'
    assert item.porte_anestesico == '5'
    assert item.layout == 'cirurgico'
    item2 = by_code['3.07.26.42-5']
    assert item2.num_auxiliares == '2'
    assert item2.porte_anestesico == '6'


def test_apply_inclusao_on_catalog():
    catalog = {'1.01.01.01-2': {
        'codigo': '1.01.01.01-2', 'descricao': 'Consulta', 'porte': '2B', 'uco': '',
        'num_auxiliares': '', 'porte_anestesico': '', 'filme': '', 'layout': 'consulta',
        'capitulo': '', 'grupo': '', 'subgrupo': '', 'pagina': '26',
    }}
    change = cbhpm_rn_import.CbhpmRnChange(
        codigo='9.99.99.99-9',
        decisao='inclusao',
        rn_num=71,
        descricao='Procedimento teste',
        porte='3A',
        layout='consulta',
    )
    updated, log = cbhpm_rn_import.apply_rn_changes(catalog, [change])
    assert '9.99.99.99-9' in updated
    assert updated['9.99.99.99-9']['rn_ultima_alteracao'] == 'RN 071'
    assert len(log) == 1
