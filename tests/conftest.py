import importlib
import sys
from pathlib import Path

import pytest
from sqlalchemy import BigInteger
from sqlalchemy.ext.compiler import compiles


# SQLite não autoincrementa BIGINT — mapear para INTEGER nos testes
@compiles(BigInteger, "sqlite")
def _compile_bigint_sqlite(_type, _compiler, **_kw):
    return "INTEGER"


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(autouse=True)
def _test_env_defaults(monkeypatch, tmp_path_factory):
    """Evita bootstrap pesado e garante SQLite isolado em todos os testes."""
    monkeypatch.setenv("SKIP_ENSURE_DB", "1")
    db_dir = tmp_path_factory.mktemp("pytest_db")
    db_path = db_dir / "default.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")


@pytest.fixture
def app_ctx(monkeypatch, tmp_path):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    if "app" in sys.modules:
        app_module = importlib.reload(sys.modules["app"])
    else:
        app_module = importlib.import_module("app")

    application = app_module.app
    application.config.update(TESTING=True)

    with application.app_context():
        app_module.db.drop_all()
        app_module.db.create_all()
        yield app_module
        app_module.db.session.remove()
        app_module.db.drop_all()


@pytest.fixture
def auth_client(app_ctx):
    """Cliente de teste com sessão autenticada e acesso a insumos."""
    application = app_ctx.app
    client = application.test_client()
    Usuario = app_ctx.Usuario
    db = app_ctx.db

    with application.app_context():
        user = Usuario.query.filter_by(email="test@local").first()
        if user is None:
            user = Usuario(
                nome="Test User",
                email="test@local",
                senha=app_ctx._hash_password("Test@1234"),
                perfil="adm",
            )
            db.session.add(user)
        user.acesso_insumos = True
        user.acesso_consulta = True
        user.acesso_contratos = True
        user.acesso_tuss_rol = True
        user.must_reset_senha = False
        db.session.commit()
        user_id = user.id

    with client.session_transaction() as sess:
        sess["user_id"] = user_id
        sess["nome"] = "Test User"
        sess["perfil"] = "adm"
        sess["must_change_senha"] = False

    return client
