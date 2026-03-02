from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import HTMLResponse
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
    db: Session = Depends(get_db),
) -> HTMLResponse:
    query = q.strip()
    effective_layer = layer if layer in {"bronze", "silver", "gold"} else None
    selected_layer = effective_layer or "all"
    table_results: list[CatalogTable] = []
    column_results: list[CatalogColumn] = []

    if query:
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

    context = {
        "query": query,
        "layer": selected_layer,
        "scope": scope,
        "table_results": table_results,
        "column_results": column_results,
        "result_count": len(table_results) + len(column_results),
    }
    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context=context,
    )
