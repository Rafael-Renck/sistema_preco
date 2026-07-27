"""Testes do importador Rol ANS."""
import io
from pathlib import Path

from openpyxl import Workbook

import rol_import


def _build_anexo_i_xlsx(rows: list[list]) -> bytes:
    wb = Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


SAMPLE_ANEXO_I = [
    [
        'PROCEDIMENTO', 'RN (alteração)', 'VIGÊNCIA', 'OD', 'AMB', 'HCO', 'HSO', 'REF',
        'PAC', 'DUT', 'SUBGRUPO', 'GRUPO', 'CAPÍTULO',
    ],
    [
        'CONSULTA MÉDICA', '', '', '', 'AMB', 'HCO', 'HSO', 'REF',
        '', '', 'CONSULTAS, VISITAS HOSPITALARES OU ACOMPANHAMENTO DE PACIENTES',
        'PROCEDIMENTOS GERAIS', 'PROCEDIMENTOS GERAIS',
    ],
    [
        'RESSONÂNCIA MAGNÉTICA DE CRÂNIO (COM DIRETRIZ DE UTILIZAÇÃO)', '', '', '', '', 'HCO', 'HSO', 'REF',
        'PAC', '64', 'DIAGNÓSTICO POR IMAGEM', 'PROCEDIMENTOS DIAGNÓSTICOS', 'PROCEDIMENTOS CLÍNICOS',
    ],
]

SAMPLE_TUSS_ROL_PLANILHA = [
    [
        'Código',
        'Terminologia de Procedimentos e Eventos em Saúde (Tab. 22 202403)',
        'Correlação (Sim/Não)',
        'PROCEDIMENTO',
        'Resolução Normativa (alteração)',
        'VIGÊNCIA',
        'OD', 'AMB', 'HCO', 'HSO', 'PAC', 'DUT', 'SUBGRUPO', 'GRUPO', 'CAPÍTULO',
    ],
    [
        '41101014',
        'RESSONÂNCIA MAGNÉTICA DE CRÂNIO',
        'Sim',
        'RESSONÂNCIA MAGNÉTICA DE CRÂNIO (COM DIRETRIZ DE UTILIZAÇÃO)',
        '541/2022', '01/08/2022',
        '', '', 'HCO', 'HSO', 'PAC', '64',
        'DIAGNÓSTICO POR IMAGEM', 'PROCEDIMENTOS DIAGNÓSTICOS', 'PROCEDIMENTOS CLÍNICOS',
    ],
    [
        '99999999',
        'PROCEDIMENTO FORA DO ROL',
        'Não',
        '',
        '', '',
        '', '', '', '', '', '', '',
        '', '', '',
    ],
]

SAMPLE_ANEXO_II = """
DIRETRIZES DE UTILIZAÇÃO

64 - RESSONÂNCIA MAGNÉTICA
1. Cobertura obrigatória quando houver indicação clínica documentada.
2. Deve haver solicitação médica com justificativa clínica.
3. Exame anterior inconclusivo ou contraindicação à tomografia computadorizada.

109 - ATENDIMENTO EM HOSPITAL-DIA PSIQUIÁTRICO
1. Paciente com transtorno mental grave em estabilização.
2. Plano terapêutico definido por equipe multiprofissional.
"""


def test_parse_anexo_i_xlsx():
    data = _build_anexo_i_xlsx(SAMPLE_ANEXO_I)
    bundle = rol_import.parse_anexo_i_file(data, '.xlsx')
    assert not bundle.erros
    assert len(bundle.procedimentos) == 2
    assert bundle.procedimentos[0].descricao == 'CONSULTA MÉDICA'
    assert bundle.procedimentos[0].seg_amb is True
    assert bundle.procedimentos[0].seg_hco is True
    assert bundle.procedimentos[1].dut_numero == '64'
    assert bundle.procedimentos[1].pac is True
    assert bundle.procedimentos[1].capitulo == 'PROCEDIMENTOS CLÍNICOS'
    assert bundle.formato == 'anexo_i'
    assert not bundle.correlacoes


def test_parse_tuss_rol_planilha_oficial():
    data = _build_anexo_i_xlsx(SAMPLE_TUSS_ROL_PLANILHA)
    bundle = rol_import.parse_anexo_i_file(data, '.xlsx')
    assert not bundle.erros
    assert bundle.formato == 'tuss_rol_planilha'
    assert len(bundle.correlacoes) == 2
    assert bundle.correlacoes[0].codigo == '41101014'
    assert bundle.correlacoes[0].consta_rol is True
    assert bundle.correlacoes[1].consta_rol is False
    assert len(bundle.procedimentos) == 1
    assert bundle.procedimentos[0].rn_alteracao == '541/2022'


def test_dedupe_tuss_correlacoes_keeps_last():
    rows = [
        rol_import.TussCorrelacaoRow('10101012', 'Primeira', True, 'CONSULTA MÉDICA'),
        rol_import.TussCorrelacaoRow('10101012', 'Segunda versão', False, None),
        rol_import.TussCorrelacaoRow('20202020', 'Outro', True, None),
    ]
    deduped = rol_import.dedupe_tuss_correlacoes(rows)
    assert len(deduped) == 2
    by_code = {r.codigo: r for r in deduped}
    assert by_code['10101012'].descricao == 'Segunda versão'
    assert by_code['10101012'].consta_rol is False


def test_import_tuss_correlacoes_duplicate_codigo(app_ctx):
    app = app_ctx
    models = {
        'RolCapitulo': app.RolCapitulo,
        'RolGrupo': app.RolGrupo,
        'RolSubgrupo': app.RolSubgrupo,
        'RolProcedimento': app.RolProcedimento,
        'RolDut': app.RolDut,
        'TussRolCorrelacao': app.TussRolCorrelacao,
    }
    rows = [
        rol_import.TussCorrelacaoRow('10101012', 'Linha A', True, None),
        rol_import.TussCorrelacaoRow('10101012', 'Linha B', True, None),
    ]
    stats = rol_import.import_tuss_correlacoes_to_db(app.db.session, models, rows)
    app.db.session.commit()
    assert stats.criados == 1
    assert stats.processados == 1
    assert app.TussRolCorrelacao.query.filter_by(codigo='10101012').count() == 1


def test_import_tuss_correlacoes_updates_existing(app_ctx):
    app = app_ctx
    models = {
        'RolCapitulo': app.RolCapitulo,
        'RolGrupo': app.RolGrupo,
        'RolSubgrupo': app.RolSubgrupo,
        'RolProcedimento': app.RolProcedimento,
        'RolDut': app.RolDut,
        'TussRolCorrelacao': app.TussRolCorrelacao,
    }
    app.db.session.add(app.TussRolCorrelacao(
        codigo='10101012',
        descricao='Antiga',
        consta_rol=False,
    ))
    app.db.session.commit()

    rows = [rol_import.TussCorrelacaoRow('10101012', 'Nova descrição', True, None)]
    stats = rol_import.import_tuss_correlacoes_to_db(app.db.session, models, rows)
    app.db.session.commit()

    assert stats.processados == 1
    row = app.TussRolCorrelacao.query.filter_by(codigo='10101012').one()
    assert row.descricao == 'Nova descrição'
    assert row.consta_rol is True
    assert app.TussRolCorrelacao.query.filter_by(codigo='10101012').count() == 1


def test_normalize_tuss_codigo_zero_pad():
    assert rol_import._normalize_tuss_codigo(10101012) == '10101012'
    assert rol_import._normalize_tuss_codigo('4030110') == '04030110'


def test_parse_anexo_ii_text():
    rows, erros = rol_import.parse_anexo_ii_text(SAMPLE_ANEXO_II)
    assert not erros
    assert len(rows) == 2
    assert rows[0].numero == '64'
    assert 'indicação clínica' in rows[0].texto_completo.lower()
    assert rows[0].resumo
    assert 'indicação clínica' in rows[0].resumo.lower()


SAMPLE_ANEXO_II_ANS = """
SUMÁRIO
64. TERAPIA ANTINEOPLÁSICA ORAL ................................ 67
65.1  ARTRITE REUMATÓIDE ................................ 76

64. TERAPIA ANTINEOPLÁSICA ORAL PARA TRATAMENTO DO CÂNCER
1. Cobertura obrigatória para pacientes com indicação oncológica documentada.
2. Deve haver laudo anatomopatológico confirmando diagnóstico.

65.1  ARTRITE REUMATÓIDE
1. Cobertura obrigatória para artrite reumatoide ativa refratária.
"""


def test_parse_anexo_ii_ans_pdf_format():
    rows, erros = rol_import.parse_anexo_ii_text(SAMPLE_ANEXO_II_ANS)
    assert not erros
    assert len(rows) == 2
    nums = {r.numero for r in rows}
    assert nums == {'64', '65.1'}
    dut64 = next(r for r in rows if r.numero == '64')
    assert 'indicação oncológica' in dut64.texto_completo.lower()


def test_parse_anexo_ii_real_pdf_if_present():
    pdf_path = Path(__file__).resolve().parents[1] / 'tmp' / 'Anexo_II_DUT.pdf'
    if not pdf_path.is_file():
        return
    rows, erros = rol_import.parse_anexo_ii_pdf(pdf_path.read_bytes())
    assert not erros
    assert len(rows) >= 100
    assert any(r.numero == '64' for r in rows)


def test_format_vigencia_excel_serial():
    assert rol_import.format_vigencia_display('44856') == '22/10/2022'


def test_format_rn_display():
    assert rol_import.format_rn_display('538/2022') == 'RN 538/2022'
    assert rol_import.format_rn_display('RN 538/2022') == 'RN 538/2022'


def test_generate_dut_resumo_lists_criteria():
    texto = "1. Primeiro critério importante.\n2. Segundo critério relevante.\n3. Terceiro critério adicional."
    resumo = rol_import.generate_dut_resumo(texto)
    assert 'Primeiro critério' in resumo
    assert 'critérios adicionais' in resumo or 'Segundo critério' in resumo


def test_import_anexo_i_and_duts_db(app_ctx):
    app = app_ctx
    models = {
        'RolCapitulo': app.RolCapitulo,
        'RolGrupo': app.RolGrupo,
        'RolSubgrupo': app.RolSubgrupo,
        'RolProcedimento': app.RolProcedimento,
        'RolDut': app.RolDut,
        'TussRolCorrelacao': app.TussRolCorrelacao,
    }

    rows_i, _, _ = rol_import.parse_anexo_i_rows(SAMPLE_ANEXO_I)
    stats_i = rol_import.import_anexo_i_to_db(app.db.session, models, rows_i, versao_label='TEST/2026')
    assert stats_i.processados == 2

    rows_dut, _ = rol_import.parse_anexo_ii_text(SAMPLE_ANEXO_II)
    stats_d = rol_import.import_duts_to_db(app.db.session, models, rows_dut)
    assert stats_d.processados == 2

    app.db.session.add(app.TussRolCorrelacao(
        codigo='41101014',
        descricao='RESSONÂNCIA MAGNÉTICA DE CRÂNIO (COM DIRETRIZ DE UTILIZAÇÃO)',
        consta_rol=True,
    ))
    app.db.session.commit()

    linked = rol_import.link_tuss_correlacoes(app.db.session, models)
    assert linked == 1

    corr = app.TussRolCorrelacao.query.filter_by(codigo='41101014').first()
    assert corr.rol_procedimento_id is not None
    proc = app.RolProcedimento.query.get(corr.rol_procedimento_id)
    assert proc.dut_numero == '64'

    dut = app.RolDut.query.filter_by(numero='64').first()
    assert dut is not None
    assert dut.resumo


def test_serialize_rol_procedimento(app_ctx):
    app = app_ctx
    cap = app.RolCapitulo(nome='CAP TESTE')
    app.db.session.add(cap)
    app.db.session.flush()
    grp = app.RolGrupo(capitulo_id=cap.id, nome='GRUPO TESTE')
    app.db.session.add(grp)
    app.db.session.flush()
    sub = app.RolSubgrupo(grupo_id=grp.id, nome='SUB TESTE')
    app.db.session.add(sub)
    app.db.session.flush()
    proc = app.RolProcedimento(
        descricao='PROC TESTE',
        descricao_norm='PROC TESTE',
        capitulo_id=cap.id,
        grupo_id=grp.id,
        subgrupo_id=sub.id,
        seg_amb=True,
        pac=True,
        dut_numero='64',
    )
    app.db.session.add(proc)
    app.db.session.add(app.RolDut(
        numero='64',
        titulo='RM',
        texto_completo='Texto completo',
        resumo='Resumo curto',
    ))
    app.db.session.commit()

    payload = rol_import.serialize_rol_procedimento(proc, app.RolDut.query.get('64'))
    assert payload['segmentacao'] == ['AMB']
    assert payload['pac'] is True
    assert payload['dut']['resumo'] == 'Resumo curto'
    assert payload['capitulo'] == 'CAP TESTE'
