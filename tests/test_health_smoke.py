"""Health check e smoke tests da API — validados no CI em cada PR."""

import pytest


@pytest.mark.smoke
def test_health_endpoint_returns_json_structure(app_ctx):
    client = app_ctx.app.test_client()
    response = client.get("/health?format=json")

    assert response.status_code == 200
    data = response.get_json()
    assert data is not None
    assert data["status"] in ("healthy", "degraded", "unhealthy")
    assert "checks" in data
    assert "timestamp" in data
    assert "database" in data["checks"]


@pytest.mark.smoke
def test_login_page_smoke(app_ctx):
    client = app_ctx.app.test_client()
    response = client.get("/login")

    assert response.status_code == 200
    assert b"email" in response.data.lower()


@pytest.mark.smoke
def test_root_redirects_or_responds(app_ctx):
    client = app_ctx.app.test_client()
    response = client.get("/", follow_redirects=False)

    assert response.status_code in (200, 302, 401, 403)


@pytest.mark.smoke
def test_app_imports_without_side_effects():
    """Garante que o módulo principal é importável (build smoke)."""
    import app as app_module

    assert app_module.app is not None
    assert app_module.app.name == "app"
