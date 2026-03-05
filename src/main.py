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
    table_nfse_raw = SimpleNamespace(
        dw_schema="bronze",
        dw_table="nfse_raw",
        layer="bronze",
        table_comment="Documentos de NFS-e recebidos dos provedores municipais",
    )
    table_divida_ativa_raw = SimpleNamespace(
        dw_schema="bronze",
        dw_table="divida_ativa_raw",
        layer="bronze",
        table_comment="Extrato bruto das inscricoes em divida ativa",
    )
    table_ods_cadastro_contribuinte = SimpleNamespace(
        dw_schema="silver",
        dw_table="ods_cadastro_contribuinte",
        layer="silver",
        table_comment="Cadastro padronizado de contribuintes municipais",
    )
    table_ods_divida_ativa = SimpleNamespace(
        dw_schema="silver",
        dw_table="ods_divida_ativa",
        layer="silver",
        table_comment="Situacao consolidada de titulos em divida ativa",
    )
    table_ods_lancamentos_tributarios = SimpleNamespace(
        dw_schema="silver",
        dw_table="ods_lancamentos_tributarios",
        layer="silver",
        table_comment="Lancamentos tributarios com base de calculo e vencimento",
    )
    table_fato_arrecadacao_iss_mensal = SimpleNamespace(
        dw_schema="gold",
        dw_table="fato_arrecadacao_iss_mensal",
        layer="gold",
        table_comment="Fato mensal de arrecadacao de ISS por contribuinte",
    )
    table_fato_recuperacao_divida_ativa = SimpleNamespace(
        dw_schema="gold",
        dw_table="fato_recuperacao_divida_ativa",
        layer="gold",
        table_comment="Indicadores mensais de recuperacao da divida ativa",
    )
    table_results = [
        table_nfse_raw,
        table_divida_ativa_raw,
        table_ods_cadastro_contribuinte,
        table_ods_divida_ativa,
        table_ods_lancamentos_tributarios,
        table_fato_arrecadacao_iss_mensal,
        table_fato_recuperacao_divida_ativa,
    ]

    column_results = [
        SimpleNamespace(
            table=table_nfse_raw,
            column_name="inscricao_municipal",
            data_type="TEXT",
            column_comment="Inscricao municipal informada no documento NFS-e",
        ),
        SimpleNamespace(
            table=table_nfse_raw,
            column_name="codigo_servico",
            data_type="TEXT",
            column_comment="Codigo do servico conforme lista tributavel",
        ),
        SimpleNamespace(
            table=table_nfse_raw,
            column_name="competencia",
            data_type="DATE",
            column_comment="Competencia da emissao da NFS-e",
        ),
        SimpleNamespace(
            table=table_ods_cadastro_contribuinte,
            column_name="cnpj_cpf",
            data_type="TEXT",
            column_comment="Documento principal do contribuinte",
        ),
        SimpleNamespace(
            table=table_ods_cadastro_contribuinte,
            column_name="razao_social",
            data_type="TEXT",
            column_comment="Razao social normalizada do contribuinte",
        ),
        SimpleNamespace(
            table=table_ods_lancamentos_tributarios,
            column_name="valor_iss",
            data_type="NUMERIC(14,2)",
            column_comment="Valor de ISS calculado no lancamento tributario",
        ),
        SimpleNamespace(
            table=table_ods_lancamentos_tributarios,
            column_name="data_vencimento",
            data_type="DATE",
            column_comment="Data de vencimento do lancamento",
        ),
        SimpleNamespace(
            table=table_ods_divida_ativa,
            column_name="valor_principal",
            data_type="NUMERIC(14,2)",
            column_comment="Valor principal inscrito em divida ativa",
        ),
        SimpleNamespace(
            table=table_ods_divida_ativa,
            column_name="valor_multa",
            data_type="NUMERIC(14,2)",
            column_comment="Valor de multa aplicado na inscricao",
        ),
        SimpleNamespace(
            table=table_ods_divida_ativa,
            column_name="valor_juros",
            data_type="NUMERIC(14,2)",
            column_comment="Valor de juros acumulado da divida",
        ),
        SimpleNamespace(
            table=table_ods_divida_ativa,
            column_name="data_inscricao",
            data_type="DATE",
            column_comment="Data de inscricao da divida ativa",
        ),
        SimpleNamespace(
            table=table_ods_divida_ativa,
            column_name="situacao",
            data_type="TEXT",
            column_comment="Situacao administrativa da cobranca",
        ),
        SimpleNamespace(
            table=table_fato_recuperacao_divida_ativa,
            column_name="canal_cobranca",
            data_type="TEXT",
            column_comment="Canal de cobranca utilizado na recuperacao",
        ),
        SimpleNamespace(
            table=table_fato_recuperacao_divida_ativa,
            column_name="parcelamento_ativo",
            data_type="BOOLEAN",
            column_comment="Indica se a divida esta em parcelamento ativo",
        ),
        SimpleNamespace(
            table=table_fato_arrecadacao_iss_mensal,
            column_name="valor_arrecadado_iss",
            data_type="NUMERIC",
            column_comment="Total mensal de ISS arrecadado",
        ),
    ]
    return table_results, column_results


def _match_mock_query(values: list[str], query: str) -> bool:
    normalized_query = query.lower()
    return any(normalized_query in value.lower() for value in values if value)


def _filter_mock_results(
    query: str,
    scope: Literal["all", "tables", "columns"],
    table_results: list[SimpleNamespace],
    column_results: list[SimpleNamespace],
) -> tuple[list[SimpleNamespace], list[SimpleNamespace]]:
    filtered_tables = table_results
    filtered_columns = column_results

    if query:
        filtered_tables = [
            table
            for table in table_results
            if _match_mock_query(
                [table.dw_schema, table.dw_table, table.table_comment],
                query,
            )
        ]
        filtered_columns = [
            column
            for column in column_results
            if _match_mock_query(
                [
                    column.column_name,
                    column.column_comment,
                    column.data_type,
                    column.table.dw_schema,
                    column.table.dw_table,
                    column.table.table_comment,
                ],
                query,
            )
        ]

    if scope == "tables":
        return filtered_tables, []
    if scope == "columns":
        return [], filtered_columns
    return filtered_tables, filtered_columns


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
    mock: bool = False,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    is_mock = mock
    query = q.strip()
    effective_layer = layer if layer in {"bronze", "silver", "gold"} else None
    selected_layer = effective_layer or "all"
    table_results: list[CatalogTable] = []
    column_results: list[CatalogColumn] = []

    if is_mock:
        mock_tables, mock_columns = _build_mock_results()
        table_results, column_results = _filter_mock_results(
            query=query,
            scope=scope,
            table_results=mock_tables,
            column_results=mock_columns,
        )
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
