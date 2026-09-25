import os
import sys

from cryptography.fernet import Fernet

# security_utils exits at import without a key; models imports it.
os.environ.setdefault("ENCRYPTION_KEY", Fernet.generate_key().decode())
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from flask import Flask

from models import db


@pytest.fixture
def app_ctx():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite://"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()
