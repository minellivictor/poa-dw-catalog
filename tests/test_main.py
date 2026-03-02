from pathlib import Path
import sys

from sqlalchemy import inspect

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database import DB_PATH, engine, init_db
from src.main import app


def test_index_route_registered() -> None:
    paths = {route.path for route in app.routes}
    assert "/" in paths


def test_init_db_creates_catalog_file() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()

    init_db()

    assert DB_PATH.exists()


def test_init_db_creates_catalog_table() -> None:
    init_db()
    inspector = inspect(engine)

    assert "catalog_table" in inspector.get_table_names()
