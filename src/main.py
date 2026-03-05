from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.database import get_db, init_db
from src.models import CatalogColumn, CatalogTable

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    init_db()
    yield


app = FastAPI(title="POA DW Catalog", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def _resolve_table_layer(table: CatalogTable) -> str:
    layer_value = (table.layer or table.dw_schema or "").strip().lower()
    if layer_value in {"bronze", "silver", "gold"}:
        return layer_value
    return "unknown"


def _annotate_result_layers(
    table_results: list[CatalogTable], column_results: list[CatalogColumn]
) -> None:
    for table in table_results:
        table.resolved_layer = _resolve_table_layer(table)
    for column in column_results:
        resolved_layer = _resolve_table_layer(column.table)
        column.table.resolved_layer = resolved_layer
        column.resolved_layer = resolved_layer


def _build_mock_results() -> tuple[list[SimpleNamespace], list[SimpleNamespace]]:
    table_bronze = SimpleNamespace(
        dw_schema="bronze",
        dw_table="raw_pedidos",
        layer="bronze",
        table_comment="Carga bruta de pedidos vindos do ERP",
    )
    table_silver = SimpleNamespace(
        dw_schema="silver",
        dw_table="dim_cliente",
        layer="silver",
        table_comment="Dimensão de clientes padronizada",
    )
    table_gold = SimpleNamespace(
        dw_schema="gold",
        dw_table="fato_vendas_diaria",
        layer="gold",
        table_comment="Fato agregado diário para indicadores de vendas",
    )
    table_results = [table_bronze, table_silver, table_gold]

    column_results = [
        SimpleNamespace(
            table=table_bronze,
            column_name="pedido_id",
            data_type="INTEGER",
            column_comment="Identificador bruto do pedido na origem",
        ),
        SimpleNamespace(
            table=table_silver,
            column_name="cliente_nome",
            data_type="TEXT",
            column_comment="Nome do cliente após padronização",
        ),
        SimpleNamespace(
            table=table_gold,
            column_name="valor_total_dia",
            data_type="NUMERIC",
            column_comment="Soma diária dos valores de venda",
        ),
    ]
    return table_results, column_results


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request=request, name="index.html", context={})


@app.get("/search", response_class=HTMLResponse)
def search(
    request: Request,
    q: str = Query("", min_length=0),
    layer: Annotated[
        Literal["all", "bronze", "silver", "gold", ""] | None,
        Query(),
    ] = "all",
    scope: Annotated[Literal["all", "tables", "columns"], Query()] = "all",
    mock: Annotated[Literal[0, 1], Query()] = 0,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    is_mock = mock == 1
    query = q.strip()
    effective_layer = layer if layer in {"bronze", "silver", "gold"} else None
    selected_layer = effective_layer or "all"
    table_results: list[CatalogTable] = []
    column_results: list[CatalogColumn] = []

    if is_mock:
        mock_tables, mock_columns = _build_mock_results()
        table_results = mock_tables
        column_results = mock_columns
    elif query:
        if scope in {"all", "tables"}:
            table_query = db.query(CatalogTable)
            if effective_layer:
                table_query = table_query.filter(CatalogTable.layer == effective_layer)
            table_results = (
                table_query.filter(
                    or_(
                        CatalogTable.dw_schema.ilike(f"%{query}%"),
                        CatalogTable.dw_table.ilike(f"%{query}%"),
                        CatalogTable.table_comment.ilike(f"%{query}%"),
                    )
                )
                .order_by(CatalogTable.dw_schema.asc(), CatalogTable.dw_table.asc())
                .all()
            )

        if scope in {"all", "columns"}:
            column_query = db.query(CatalogColumn).join(CatalogTable)
            if effective_layer:
                column_query = column_query.filter(CatalogTable.layer == effective_layer)
            column_results = (
                column_query.filter(
                    or_(
                        CatalogColumn.column_name.ilike(f"%{query}%"),
                        CatalogColumn.column_comment.ilike(f"%{query}%"),
                        CatalogColumn.data_type.ilike(f"%{query}%"),
                        CatalogTable.dw_schema.ilike(f"%{query}%"),
                        CatalogTable.dw_table.ilike(f"%{query}%"),
                    )
                )
                .order_by(
                    CatalogTable.dw_schema.asc(),
                    CatalogTable.dw_table.asc(),
                    CatalogColumn.ordinal_position.asc(),
                )
                .all()
            )

    _annotate_result_layers(table_results, column_results)
    if effective_layer:
        table_results = [t for t in table_results if t.resolved_layer == effective_layer]
        column_results = [c for c in column_results if c.resolved_layer == effective_layer]

    context = {
        "query": query,
        "layer": selected_layer,
        "scope": scope,
        "is_mock": is_mock,
        "table_results": table_results,
        "column_results": column_results,
        "result_count": len(table_results) + len(column_results),
    }
    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context=context,
    )


@app.get("/table", response_class=HTMLResponse)
def table_detail(
    request: Request,
    schema: str = Query(..., min_length=1),
    table: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    catalog_table = (
        db.query(CatalogTable)
        .filter(CatalogTable.dw_schema == schema, CatalogTable.dw_table == table)
        .one_or_none()
    )
    if catalog_table is None:
        raise HTTPException(status_code=404, detail="Tabela não encontrada")

    catalog_table.resolved_layer = _resolve_table_layer(catalog_table)
    columns = sorted(catalog_table.columns, key=lambda column: column.ordinal_position)

    return templates.TemplateResponse(
        request=request,
        name="table.html",
        context={"table": catalog_table, "columns": columns},
    )
