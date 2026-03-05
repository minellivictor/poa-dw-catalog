from pathlib import Path
import sys

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.database as database
from src.main import app, search, table_detail
from src.seed import seed_metadata


def _make_request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "query_string": b"",
            "router": app.router,
            "app": app,
        }
    )


def _configure_test_db(tmp_path, monkeypatch):
    test_db_path = tmp_path / "catalog_test.db"
    test_engine = create_engine(
        f"sqlite:///{test_db_path}",
        connect_args={"check_same_thread": False},
    )
    test_session_local = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

    monkeypatch.setattr(database, "DB_PATH", test_db_path)
    monkeypatch.setattr(database, "DATABASE_URL", f"sqlite:///{test_db_path}")
    monkeypatch.setattr(database, "engine", test_engine)
    monkeypatch.setattr(database, "SessionLocal", test_session_local)

    return test_db_path, test_engine


def test_index_route_registered() -> None:
    paths = {route.path for route in app.routes}
    assert "/" in paths
    assert "/search" in paths
    assert "/table" in paths


def test_init_db_creates_catalog_file(tmp_path, monkeypatch) -> None:
    db_path, _ = _configure_test_db(tmp_path, monkeypatch)

    database.init_db()

    assert db_path.exists()


def test_init_db_creates_catalog_table(tmp_path, monkeypatch) -> None:
    _, test_engine = _configure_test_db(tmp_path, monkeypatch)

    database.init_db()
    inspector = inspect(test_engine)

    assert "catalog_table" in inspector.get_table_names()


def test_search_with_layer_all_returns_multiple_layers(tmp_path, monkeypatch) -> None:
    _configure_test_db(tmp_path, monkeypatch)
    seed_metadata()

    request = _make_request("/search")
    with database.SessionLocal() as db:
        response = search(request=request, q="cliente", scope="all", layer="all", db=db)

    assert response.status_code == 200
    html = response.body.decode("utf-8")
    assert "Total de resultados" in html
    assert "bronze.raw_cliente" in html
    assert "silver.dim_cliente" in html
    assert "gold.fato_pedido.cliente_sk" in html
    table_layers = {table.resolved_layer for table in response.context["table_results"]}
    column_layers = {column.resolved_layer for column in response.context["column_results"]}
    assert {"bronze", "silver"}.issubset(table_layers)
    assert "gold" in column_layers


def test_search_with_specific_layer_filters_results(tmp_path, monkeypatch) -> None:
    _configure_test_db(tmp_path, monkeypatch)
    seed_metadata()

    request = _make_request("/search")
    with database.SessionLocal() as db:
        response = search(request=request, q="cliente", scope="all", layer="silver", db=db)

    assert response.status_code == 200
    html = response.body.decode("utf-8")
    assert "silver.dim_cliente" in html
    assert "silver.dim_cliente.nome_cliente" in html
    assert "bronze.raw_cliente" not in html
    assert "gold.fato_pedido.cliente_sk" not in html
    assert all(
        table.resolved_layer == "silver" for table in response.context["table_results"]
    )
    assert all(
        column.resolved_layer == "silver" for column in response.context["column_results"]
    )


def test_search_mock_mode_returns_demo_banner(tmp_path, monkeypatch) -> None:
    _configure_test_db(tmp_path, monkeypatch)

    request = _make_request("/search")
    with database.SessionLocal() as db:
        response = search(request=request, q="", scope="all", layer="all", mock=1, db=db)

    assert response.status_code == 200
    html = response.body.decode("utf-8")
    assert "Modo DEMO (dados simulados)" in html
    assert "bronze.raw_pedidos" in html
    assert "silver.dim_cliente" in html
    assert "gold.fato_vendas_diaria" in html


def test_table_detail_shows_columns(tmp_path, monkeypatch) -> None:
    _configure_test_db(tmp_path, monkeypatch)
    seed_metadata()

    request = _make_request("/table")
    with database.SessionLocal() as db:
        response = table_detail(request=request, schema="silver", table="dim_cliente", db=db)

    assert response.status_code == 200
    html = response.body.decode("utf-8")
    assert "silver.dim_cliente" in html
    assert "nome_cliente" in html
    assert "Dimensão de clientes padronizada" in html
