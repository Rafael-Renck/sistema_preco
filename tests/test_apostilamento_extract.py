"""Testes do parser de apostilamentos (layout Honorário + Pacote Hospitalar)."""
from __future__ import annotations

from decimal import Decimal

import apostilamento_extract as extract
from app import _parse_money


def _parse(linha: str):
    return extract.parse_item_bloco(linha, 1, "teste.pdf", "", "ESCLERA")


def test_biopsia_esclera_remove_tabela_acordada():
    item = _parse("10001024 Biopsia de esclera Tabela acordada R$ 700,77")
    assert item is not None
    assert item["codigo"] == "10001024"
    assert item["descricao"] == "Biopsia de esclera"
    assert item["valor_numero"] == 700.77
    assert "Tabela acordada" in (item["observacao_linha"] or "")


def test_codigo_curto_falso_positivo_promove_hospital():
    item = _parse("1.000 1017 Remocao de Hifema Tabela acordada R$ 4.115,00")
    assert item is not None
    assert item["codigo"] == "1017"
    assert item["descricao"] == "Remocao de Hifema"
    assert item["valor_numero"] == 4115.0


def test_dois_valores_prefere_ultimo_pacote():
    item = _parse("10001024 Biopsia de esclera R$ 150,00 R$ 700,77")
    assert item is not None
    assert item["codigo"] == "10001024"
    assert item["descricao"] == "Biopsia de esclera"
    assert item["valor_numero"] == 700.77


def test_capsulotomia_sem_lixo_honorario():
    item = _parse("10001018 Capsulotomia YAG Tabela acordada R$ 1.859,00")
    assert item is not None
    assert item["codigo"] == "10001018"
    assert item["descricao"] == "Capsulotomia YAG"
    assert item["valor_numero"] == 1859.0


def test_asterisco_honorario_removido_da_descricao():
    item = _parse(
        "30307147 Implante intravireo de polimero - EYLA – MONOCULAR * R$ 5.277,56"
    )
    assert item is not None
    assert item["codigo"] == "30307147"
    assert "*" not in item["descricao"]
    assert item["valor_numero"] == 5277.56


def test_dois_valores_honorario_e_pacote():
    item = _parse("30304083 Implante de anel intra-estromal R$ 2.329,37 R$ 8.230,49")
    assert item is not None
    assert item["codigo"] == "30304083"
    assert item["valor_numero"] == 8230.49


def test_parse_money_nao_multiplica_decimal_ponto():
    # Regressão: preview grava "5277.56" e import não pode virar 527756
    assert _parse_money("5277.56") == Decimal("5277.56")
    assert _parse_money("R$ 5.277,56") == Decimal("5277.56")
    assert _parse_money("5.277,56") == Decimal("5277.56")


def test_codigo_decimal_hospital_mantem_completo():
    item = _parse("0.02041390 APARELHO PARA BIOPSIA R$ 503,36")
    assert item is not None
    assert item["codigo"] == "0.02041390"
    assert "APARELHO PARA BIOPSIA" in item["descricao"].upper()
    assert item["valor_numero"] == 503.36


def test_codigo_decimal_hospital_curativo():
    item = _parse("0.02050266 TAXA POR UNIDADE DE CURATIVO OFTALMOLOGICO R$ 10,00")
    assert item is not None
    assert item["codigo"] == "0.02050266"
    assert "CURATIVO" in item["descricao"].upper()


def test_normalizar_codigo_mantem_decimal_hospital():
    assert extract.normalizar_codigo_extraido("0.02041390") == "0.02041390"
    assert extract.normalizar_codigo_extraido("0.02050266") == "0.02050266"
    assert extract.normalizar_codigo_extraido("60025158") == "60025158"


def test_taxa_fralda_nao_perde_prefixo_taxa():
    item = _parse("795010400 TAXA/FRALDA/DIA R$ 59,98")
    assert item is not None
    assert item["codigo"] == "795010400"
    assert item["descricao"].upper().startswith("TAXA/")
    assert "FRALDA" in item["descricao"].upper()
    assert not item["descricao"].startswith("/")


def test_taxa_fralda_alto_risco():
    item = _parse("795010399 TAXA/FRALDA/ DIA (ALTO RISCO) R$ 149,93")
    assert item is not None
    assert item["codigo"] == "795010399"
    assert "FRALDA" in item["descricao"].upper()
    assert "ALTO RISCO" in item["descricao"].upper()
    assert not item["descricao"].startswith("/")


def test_aluguel_taxa_nao_parte_unidade_errada():
    item = _parse("60025158 ALUGUEL/TAXA DE ASPIRADOR A VACUO, POR USO R$ 143,07")
    assert item is not None
    assert item["codigo"] == "60025158"
    assert item["descricao"].upper().startswith("ALUGUEL")
    assert item["valor_numero"] == 143.07


def test_normalizar_codigo_mantem_decimal_hospital():
    assert extract.normalizar_codigo_extraido("0.02041390") == "0.02041390"
    assert extract.normalizar_codigo_extraido("0.02050266") == "0.02050266"
    assert extract.normalizar_codigo_extraido("60025158") == "60025158"


def test_codigo_amb_xx_xx_xxxx_avaliacao():
    item = _parse("00.01.1200 AVALIACAO - PSICOLOGIA R$ 74,60")
    assert item is not None
    assert item["codigo"] == "00.01.1200"
    assert "PSICOLOGIA" in item["descricao"].upper()
    assert not item["descricao"].startswith("00")


def test_codigo_amb_nutricionista():
    item = _parse("17.01.0011 AVALIACAO - NUTRICIONISTA R$ 74,60")
    assert item is not None
    assert item["codigo"] == "17.01.0011"
    assert "NUTRICIONISTA" in item["descricao"].upper()


def test_descricao_com_hifen_inicial():
    item = _parse("41389 - FISIOTERAPIA R$ 324,75")
    assert item is not None
    assert item["codigo"] == "41389"
    assert item["descricao"].upper().startswith("FISIOTERAPIA")
    assert not item["descricao"].startswith("-")


def test_codigo_incompleto_nao_captura_0_02_05():
    item = _parse("0.02.05.0 APARELHO XYZ R$ 10,00")
    assert item is not None
    # não deve fixar código truncado 0.02.05
    assert item["codigo"] != "0.02.05"
    assert "APARELHO" in item["descricao"].upper() or "0.02" in (item["descricao"] + item["codigo"])


def test_codigo_cbhpm_3_10_02_390():
    item = _parse(
        "3.10.02.390 PACOTE DE GASTROPLASTIA POR VIDEOLAPAROSCOPIA R$ 1.000,00"
    )
    assert item is not None
    assert item["codigo"] == "3.10.02.390"
    assert "GASTROPLASTIA" in item["descricao"].upper()
    assert item["valor_numero"] == 1000.0


def test_valor_sem_separador_de_milhar():
    item = _parse("60025778 BOMBA DE SUCCAO R$ 1000,00")
    assert item is not None
    assert item["valor_numero"] == 1000.0


def test_juntar_descricao_cortada_em_por():
    texto = (
        "60028640 TAXA DE ASSISTENCIA DE ENFERMAGEM NA CLINICA CIRURGICA, POR\n"
        "USO/SESSAO R$ 69,54"
    )
    itens = extract.extrair_itens_do_texto(texto, 1, "t.pdf")
    assert len(itens) >= 1
    assert any("USO" in (i["descricao"] or "").upper() for i in itens)
    assert any(i["codigo"] == "60028640" for i in itens)


def test_rejeita_somente_codigo_sem_descricao():
    assert _parse("60017046 R$ 16,82") is None


def test_por_uso_permanece_na_descricao():
    item = _parse("60025158 ALUGUEL/TAXA DE ASPIRADOR A VACUO, POR USO R$ 143,07")
    assert item is not None
    assert item["codigo"] == "60025158"
    assert item["descricao"].upper().endswith("POR USO")
    assert (item["unidade_cobranca"] or "") == ""


def test_por_dia_permanece_na_descricao():
    item = _parse("60027835 RESPIRADOR, POR DIA R$ 188,61")
    assert item is not None
    assert item["codigo"] == "60027835"
    assert "POR DIA" in item["descricao"].upper()
    assert (item["unidade_cobranca"] or "") == ""


def test_por_hora_permanece_na_descricao():
    item = _parse("60025301 BALAO INTRA AORTICO POR HORA R$ 1.304,37")
    assert item is not None
    assert "POR HORA" in item["descricao"].upper()
    assert (item["unidade_cobranca"] or "") == ""


def test_codigo_com_parentese_lixo():
    item = _parse("7950104( TAXA/FRALDA/DIA R$ 59,98")
    assert item is not None
    assert "(" not in item["codigo"]
    assert item["descricao"].upper().startswith("TAXA/")
