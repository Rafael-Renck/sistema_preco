from datetime import datetime
from decimal import Decimal


PRICE_KEYS = {"preco", "preco_pmc", "preco_pfb", "aliquota"}


def _seed_insumo(app_ctx):
    session = app_ctx.db.session
    bras_item = app_ctx.BrasItemNormalized(
        id=501,
        arquivo="Brasindice_2025",
        linha_num=1,
        laboratorio_codigo="LAB01",
        laboratorio_nome="ACME",
        produto_codigo="12345",
        produto_nome="Seringa descartável 5ml",
        apresentacao_codigo="AP01",
        apresentacao_descricao="5ml",
        ean="7891234567890",
        registro_anvisa="789",
        edicao="2025-01",
        preco_pmc_pacote=Decimal("25.00"),
        preco_pfb_pacote=Decimal("23.00"),
        preco_pmc_unit=Decimal("12.34"),
        preco_pfb_unit=Decimal("11.00"),
        aliquota_ou_ipi=Decimal("18.0"),
        quantidade_embalagem=10,
        imported_at=datetime.utcnow(),
    )
    session.add(bras_item)
    session.add(
        app_ctx.InsumoIndex(
            origem="BRAS",
            item_id=bras_item.id,
            tuss="12345",
            tiss="AP01",
            descricao="Seringa descartável 5ml",
            preco=Decimal("12.34"),
            aliquota=Decimal("18.0"),
            fabricante="ACME",
            anvisa="789",
            versao_tabela="2025-01",
            updated_at=datetime.utcnow(),
        )
    )
    session.commit()


def test_public_insumos_requires_token(app_ctx):
    app_ctx._PUBLIC_API_TOKENS = {"secret-token"}
    _seed_insumo(app_ctx)
    client = app_ctx.app.test_client()

    response = client.get("/api/v1/insumos/itens", query_string={"q": "seringa"})
    assert response.status_code == 401

    response = client.get(
        "/api/v1/insumos/itens",
        query_string={"q": "seringa"},
        headers={"Authorization": "Bearer wrong"},
    )
    assert response.status_code == 403


def test_public_insumos_search_without_prices(app_ctx):
    app_ctx._PUBLIC_API_TOKENS = {"secret-token"}
    _seed_insumo(app_ctx)
    client = app_ctx.app.test_client()
    headers = {"Authorization": "Bearer secret-token"}

    response = client.get(
        "/api/v1/insumos/itens",
        query_string={"q": "seringa", "limit": 10},
        headers=headers,
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total"] >= 1
    item = payload["items"][0]
    assert item["origem"] == "BRAS"
    assert item["item_id"] == 501
    assert "Seringa" in (item["descricao"] or "")
    assert item["tuss"] == "12345"
    assert item["tiss"] == "AP01"
    assert item["anvisa"] == "789"
    assert item["fabricante"] == "ACME"
    for key in PRICE_KEYS:
        assert key not in item

    by_tuss = client.get(
        "/api/v1/insumos/itens",
        query_string={"tuss": "12345"},
        headers=headers,
    )
    assert by_tuss.status_code == 200
    assert by_tuss.get_json()["total"] >= 1

    by_anvisa = client.get(
        "/api/v1/insumos/itens",
        query_string={"anvisa": "789"},
        headers=headers,
    )
    assert by_anvisa.status_code == 200
    assert by_anvisa.get_json()["total"] >= 1


def test_public_insumos_requires_query(app_ctx):
    app_ctx._PUBLIC_API_TOKENS = {"secret-token"}
    client = app_ctx.app.test_client()
    response = client.get(
        "/api/v1/insumos/itens",
        headers={"Authorization": "Bearer secret-token"},
    )
    assert response.status_code == 400
