"""Testes dos comunicados CBHPM (valores de porte e UCO)."""

from decimal import Decimal
from pathlib import Path

import cbhpm_comunicado_import
import cbhpm_pricing

ROOT = Path(__file__).resolve().parents[1]


def test_parse_comunicado_2020_2021():
    pdf = ROOT / "Comunicado-CBHPM-2020-2021.pdf"
    if not pdf.is_file():
        return
    com = cbhpm_comunicado_import.parse_comunicado_file(
        pdf,
        ano=2021,
        periodo="2020-2021",
    )
    assert len(com.portes) == 42
    assert com.portes["1A"] == Decimal("24.37")
    assert com.portes["14C"] == Decimal("5082.69")
    assert com.uco_valor == Decimal("21.89")


def test_parse_comunicado_2023_original():
    pdf = ROOT / "COMUNICADO-CBHPM-2023_2024.pdf"
    if not pdf.is_file():
        return
    com = cbhpm_comunicado_import.parse_comunicado_file(
        pdf,
        ano=2024,
        periodo="2023-2024",
        faixa="original",
    )
    assert len(com.portes) == 42
    assert com.portes["1A"] == Decimal("24.36")
    assert com.uco_valor == Decimal("27.15")


def test_load_comunicado_2022_png():
    png = ROOT / "Comunicado-CBHPM-2021-2022.png"
    if not png.is_file():
        return
    com = cbhpm_comunicado_import.load_comunicado_for_year(2022, ROOT)
    assert com.portes["14C"] == Decimal("5630.60")
    assert com.uco_valor == Decimal("24.24")


def test_load_comunicado_2023_inpc_chain_from_2022():
    """2023+ usa reajuste INPC sobre 2022, não a coluna Original do multifaixa."""
    com2022 = cbhpm_comunicado_import.load_comunicado_for_year(2022, ROOT)
    com2023 = cbhpm_comunicado_import.load_comunicado_for_year(2023, ROOT)
    com2024 = cbhpm_comunicado_import.load_comunicado_for_year(2024, ROOT)

    assert com2022.portes["11B"] == Decimal("4160.90")
    assert com2023.portes["11B"] == Decimal("4460.07")
    assert com2024.portes["11B"] == Decimal("4661.22")
    assert com2023.portes["11B"] > com2022.portes["11B"]
    assert com2024.portes["11B"] > com2023.portes["11B"]
    assert "INPC" in (com2023.observacao or "")
    assert com2023.uco_valor == Decimal("25.98")


def test_load_comunicado_2026_inpc_chain():
    com2026 = cbhpm_comunicado_import.load_comunicado_for_year(2026, ROOT)
    assert com2026.portes["11B"] > Decimal("5000")
    assert com2026.portes["11B"] > cbhpm_comunicado_import.load_comunicado_for_year(2025, ROOT).portes["11B"]


def test_price_row_uco_filme_anest():
    com = cbhpm_comunicado_import.CbhpmComunicadoValores(
        ano=2024,
        periodo="2023-2024",
        faixa="original",
        uco_valor=Decimal("27.15"),
        portes={"4C": Decimal("447.70"), "6B": Decimal("661.89"), "3A": Decimal("207.82")},
    )
    row = {
        "codigo": "4.09.01.58-0",
        "descricao": "US teste",
        "porte": "4C",
        "uco": "13,240",
        "filme": "0,6800",
        "incidencia": "4",
    }
    priced = cbhpm_pricing.price_catalog_row(row, com, ano=2024)
    assert priced["valor_porte"] == "447,70"
    assert priced["total_uco"] == "359,47"
    assert priced["total_filme"] == "104,83"  # 0.68 * 38.54 * 4

    row2 = {
        "codigo": "3.02.04.08-9",
        "descricao": "Parotidectomia",
        "porte": "7C",
        "porte_anestesico": "4",
        "uco": "–",
    }
    com2 = cbhpm_comunicado_import.load_comunicado_for_year(2024, ROOT)
    priced2 = cbhpm_pricing.price_catalog_row(row2, com2, ano=2024)
    assert priced2["valor_porte_anestesico"] == priced2["total_porte_anestesico"]
    assert priced2["valor_porte_anestesico"]  # 6B do comunicado 2024
