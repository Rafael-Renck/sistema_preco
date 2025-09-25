import importlib
import sys

import pytest


@pytest.fixture
def app_ctx(monkeypatch, tmp_path):
    db_path = tmp_path / 'test.db'
    monkeypatch.setenv('DATABASE_URL', f'sqlite:///{db_path}')

    if 'app' in sys.modules:
        app_module = importlib.reload(sys.modules['app'])
    else:
        app_module = importlib.import_module('app')

    application = app_module.app
    application.config.update(TESTING=True)

    with application.app_context():
        app_module.db.drop_all()
        app_module.db.create_all()
        yield app_module
        app_module.db.session.remove()
        app_module.db.drop_all()
