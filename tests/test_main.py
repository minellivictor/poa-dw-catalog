from pathlib import Path
import sys
from urllib.parse import urlencode
import asyncio

from pydantic import TypeAdapter
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.database as database
from src.main import (
    app,
    atualizar_descricao_negocio_coluna,
    atualizar_descricao_negocio_tabela,
    column_detail,
    search,
    table_detail,
)
from src.models import CatalogColumn, CatalogTable, HistoricoCuradoria
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


def _make_post_request(path: str, data: dict[str, str]) -> Request:
    body = urlencode(data).encode("utf-8")

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": path,
            "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
            "query_string": b"",
            "router": app.router,
            "app": app,
        },
        receive=receive,
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
    assert "/column" in paths


def test_init_db_creates_catalog_file(tmp_path, monkeypatch) -> None:
    db_path, _ = _configure_test_db(tmp_path, monkeypatch)

    database.init_db()

    assert db_path.exists()


def test_init_db_creates_catalog_table(tmp_path, monkeypatch) -> None:
    _, test_engine = _configure_test_db(tmp_path, monkeypatch)

    database.init_db()
    inspector = inspect(test_engine)

    assert "catalog_table" in inspector.get_table_names()
    assert "historico_curadoria" in inspector.get_table_names()
    assert {
        column["name"] for column in inspector.get_columns("catalog_table")
    } >= {"descricao_negocio"}
    assert {
        column["name"] for column in inspector.get_columns("catalog_column")
    } >= {"descricao_negocio"}


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
        response = search(request=request, q="", scope="all", layer="all", mock=True, db=db)

    assert response.status_code == 200
    html = response.body.decode("utf-8")
    assert "Modo DEMO (dados simulados)" in html
    assert "bronze.nfse_raw" in html
    assert "silver.ods_cadastro_contribuinte" in html
    assert len(response.context["table_results"]) >= 1


def test_search_mock_mode_keywords_return_results(tmp_path, monkeypatch) -> None:
    _configure_test_db(tmp_path, monkeypatch)

    request = _make_request("/search")
    with database.SessionLocal() as db:
        contribuinte_response = search(
            request=request,
            q="contribuinte",
            scope="all",
            layer="all",
            mock=True,
            db=db,
        )
        divida_response = search(
            request=request,
            q="divida",
            scope="all",
            layer="all",
            mock=True,
            db=db,
        )
        lancamento_response = search(
            request=request,
            q="lancamento",
            scope="all",
            layer="all",
            mock=True,
            db=db,
        )

    assert len(contribuinte_response.context["table_results"]) >= 1
    assert len(contribuinte_response.context["column_results"]) >= 1
    assert (
        len(divida_response.context["table_results"])
        + len(divida_response.context["column_results"])
        >= 1
    )
    assert (
        len(lancamento_response.context["table_results"])
        + len(lancamento_response.context["column_results"])
        >= 1
    )


def test_mock_query_values_are_coerced_to_boolean() -> None:
    parser = TypeAdapter(bool)

    assert parser.validate_python("true") is True
    assert parser.validate_python("1") is True
    assert parser.validate_python("false") is False
    assert parser.validate_python("0") is False


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
    assert "Descrição de Negócio" in html


def test_update_table_business_description_creates_history(tmp_path, monkeypatch) -> None:
    _configure_test_db(tmp_path, monkeypatch)
    seed_metadata()

    with database.SessionLocal() as db:
        tabela = (
            db.query(CatalogTable)
            .filter(
                CatalogTable.dw_schema == "silver",
                CatalogTable.dw_table == "dim_cliente",
            )
            .one()
        )
        table_id = tabela.id

    with database.SessionLocal() as db:
        response = asyncio.run(
            atualizar_descricao_negocio_tabela(
                table_id=table_id,
                request=_make_post_request(
                    f"/table/{table_id}/descricao-negocio",
                    {
                        "descricao_negocio": "Tabela usada como dimensão mestre de clientes",
                        "usuario": "analista_dw",
                    },
                ),
                db=db,
            )
        )

    assert response.status_code == 303
    assert "mensagem=Descricao+de+negocio+da+tabela+atualizada" in response.headers["location"]

    with database.SessionLocal() as db:
        catalog_table = db.query(CatalogTable).filter(CatalogTable.id == table_id).one()
        historico = db.query(HistoricoCuradoria).filter_by(
            tipo_entidade="tabela",
            entidade_id=table_id,
        ).one()

    assert catalog_table.descricao_negocio == "Tabela usada como dimensão mestre de clientes"
    assert historico.valor_anterior is None
    assert historico.valor_novo == "Tabela usada como dimensão mestre de clientes"
    assert historico.usuario == "analista_dw"

    request = _make_request("/table")
    with database.SessionLocal() as db:
        detail_response = table_detail(
            request=request,
            schema="silver",
            table="dim_cliente",
            mensagem="Descrição atualizada",
            db=db,
        )

    assert "Tabela usada como dimensão mestre de clientes" in detail_response.body.decode("utf-8")
    assert "analista_dw" in detail_response.body.decode("utf-8")


def test_update_column_business_description_creates_history(tmp_path, monkeypatch) -> None:
    _configure_test_db(tmp_path, monkeypatch)
    seed_metadata()

    with database.SessionLocal() as db:
        coluna = (
            db.query(CatalogColumn)
            .join(CatalogTable)
            .filter(
                CatalogTable.dw_schema == "silver",
                CatalogTable.dw_table == "dim_cliente",
                CatalogColumn.column_name == "nome_cliente",
            )
            .one()
        )
        coluna_id = coluna.id

    with database.SessionLocal() as db:
        response = asyncio.run(
            atualizar_descricao_negocio_coluna(
                column_id=coluna_id,
                request=_make_post_request(
                    f"/column/{coluna_id}/descricao-negocio",
                    {
                        "descricao_negocio": "Nome principal exibido nos relatórios de clientes",
                        "usuario": "governanca_dados",
                    },
                ),
                db=db,
            )
        )

    assert response.status_code == 303
    assert "mensagem=Descricao+de+negocio+da+coluna+atualizada" in response.headers["location"]

    with database.SessionLocal() as db:
        coluna = db.query(CatalogColumn).filter(CatalogColumn.id == coluna_id).one()
        historico = db.query(HistoricoCuradoria).filter_by(
            tipo_entidade="coluna",
            entidade_id=coluna_id,
        ).one()

    assert coluna.descricao_negocio == "Nome principal exibido nos relatórios de clientes"
    assert historico.valor_anterior is None
    assert historico.valor_novo == "Nome principal exibido nos relatórios de clientes"
    assert historico.usuario == "governanca_dados"

    request = _make_request("/column")
    with database.SessionLocal() as db:
        detail_response = column_detail(
            request=request,
            schema="silver",
            table="dim_cliente",
            column="nome_cliente",
            mensagem="Descrição atualizada",
            db=db,
        )

    assert "Nome principal exibido nos relatórios de clientes" in detail_response.body.decode(
        "utf-8"
    )
    assert "governanca_dados" in detail_response.body.decode("utf-8")
