from app import app, _ensure_bras_item_view_exists
from app import db

with app.app_context():
    _ensure_bras_item_view_exists()
    db.session.commit()
    print('bras_item_v rebuilt')
