from pathlib import Path
from decimal import Decimal


def test_bras_catalog_snapshot_delta(app_ctx, tmp_path: Path):
    old_file = tmp_path / "bras_catalog_old.txt"
    old_file.write_text(
        "\n".join(
            [
                '"048";"ACHE";"18829";"PRODUTO A";"FXHC";"20 mg bl. 60 cprs.";"048.18829.FXHC";"7896658052767";"0000095175";"90609204"',
                '"048";"ACHE";"28769";"PRODUTO B";"FXGM";"Susp. oral fr. x 150 ml";"048.28769.FXGM";"";"0000095151";""',
            ]
        ),
        encoding="latin-1",
    )

    new_file = tmp_path / "bras_catalog_new.txt"
    new_file.write_text(
        "\n".join(
            [
                '"048";"ACHE";"18829";"PRODUTO A ALTERADO";"FXHC";"20 mg bl. 60 cprs.";"048.18829.FXHC";"7896658052767";"0000095175";"90609204"',
                '"1133";"TRUE";"28770";"PRODUTO C";"FXGO";"Liquido 30 ml";"1133.28770.FXGO";"618341318090";"0000095153";""',
            ]
        ),
        encoding="latin-1",
    )

    app_ctx._sync_bras_catalog_snapshot(file_path=old_file, versao="2026-01")
    app_ctx._sync_bras_catalog_snapshot(file_path=new_file, versao="2026-02")

    delta = app_ctx._analyze_bras_catalog_delta(
        current_version="2026-02",
        previous_version="2026-01",
    )

    assert delta["current_version"] == "2026-02"
    assert delta["previous_version"] == "2026-01"
    assert delta["new_count"] == 1
    assert delta["changed_count"] == 1
    assert delta["removed_count"] == 1
    assert any(sample["ean"] == "618341318090" for sample in delta["sample_new"])
    assert any(sample["ean"] == "7896658052767" for sample in delta["sample_changed"])


def test_bras_main_row_key_matches_catalog_identity(app_ctx):
    giant_row = [""] * 20
    giant_row[0] = "048"
    giant_row[16] = "7896658052767"
    giant_row[17] = "FXHC"
    giant_row[19] = "18829"

    item_key, key_kind = app_ctx._build_bras_main_row_key(giant_row)

    assert key_kind == "ean"
    assert item_key == "ean:7896658052767"


def test_bras_sync_split_from_bras_n_creates_cadastro_e_preco(app_ctx):
    row = app_ctx.BrasItemNormalized(
        id=1,
        arquivo="1091_SP",
        linha_num=1,
        laboratorio_codigo="048",
        laboratorio_nome="ACHE",
        produto_codigo="90604482",
        produto_nome="PRODUTO TESTE",
        apresentacao_codigo="FXHC",
        apresentacao_descricao="20 mg bl. 60 cprs.",
        ean="7896658052767",
        registro_anvisa="123456789",
        edicao="1091",
        preco_pmc_pacote=Decimal("120.55"),
        preco_pfb_pacote=Decimal("100.10"),
        preco_pmc_unit=Decimal("2.00"),
        preco_pfb_unit=Decimal("1.75"),
        aliquota_ou_ipi=Decimal("20.5"),
        quantidade_embalagem=60,
    )
    app_ctx.db.session.add(row)
    app_ctx.db.session.commit()

    stats = app_ctx._sync_bras_split_from_bras_n(n_ids=[row.id])

    cadastro = app_ctx.BrasItemCadastro.query.one()
    preco = app_ctx.BrasItemPreco.query.one()

    assert stats["cadastros_criados"] == 1
    assert stats["precos_criados"] == 1
    assert cadastro.edicao == "1091"
    assert cadastro.ean == "7896658052767"
    assert cadastro.produto_codigo == "90604482"
    assert preco.cadastro_id == cadastro.id
    assert preco.aliquota == Decimal("20.5")
    assert preco.preco_pmc_pacote == Decimal("120.55")


def test_bras_sync_split_from_bras_n_respeita_aliquota_override(app_ctx):
    row = app_ctx.BrasItemNormalized(
        id=2,
        arquivo="1091_BA",
        linha_num=2,
        laboratorio_codigo="048",
        laboratorio_nome="ACHE",
        produto_codigo="11111111",
        produto_nome="PRODUTO PRECO",
        apresentacao_codigo="FXGM",
        apresentacao_descricao="Susp. oral fr. x 150 ml",
        ean="7896658052000",
        registro_anvisa="987654321",
        edicao="1091",
        preco_pmc_pacote=Decimal("88.00"),
        preco_pfb_pacote=Decimal("77.00"),
        preco_pmc_unit=Decimal("1.10"),
        preco_pfb_unit=Decimal("1.00"),
        aliquota_ou_ipi=None,
        quantidade_embalagem=1,
    )
    app_ctx.db.session.add(row)
    app_ctx.db.session.commit()

    stats = app_ctx._sync_bras_split_from_bras_n(
        n_ids=[row.id],
        aliquota_override=Decimal("20.5"),
    )

    cadastro = app_ctx.BrasItemCadastro.query.one()
    preco = app_ctx.BrasItemPreco.query.one()

    assert stats["puladas_sem_aliquota"] == 0
    assert cadastro.edicao == "1091"
    assert preco.aliquota == Decimal("20.5")
    assert preco.preco_pfb_pacote == Decimal("77.00")
