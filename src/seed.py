from sqlalchemy.orm import Session

import src.database as database
from src.models import CatalogColumn, CatalogTable

SEED_DATA = [
    {
        "layer": "bronze",
        "dw_schema": "bronze",
        "dw_table": "raw_cliente",
        "table_comment": "Dados brutos de clientes",
        "columns": [
            {
                "column_name": "cliente_id",
                "data_type": "INTEGER",
                "is_nullable": False,
                "ordinal_position": 1,
                "column_comment": "Identificador do cliente",
            },
            {
                "column_name": "nome",
                "data_type": "TEXT",
                "is_nullable": True,
                "ordinal_position": 2,
                "column_comment": "Nome informado na origem",
            },
            {
                "column_name": "email",
                "data_type": "TEXT",
                "is_nullable": True,
                "ordinal_position": 3,
                "column_comment": "Email capturado na origem",
            },
        ],
    },
    {
        "layer": "silver",
        "dw_schema": "silver",
        "dw_table": "dim_cliente",
        "table_comment": "Dimensão de clientes padronizada",
        "columns": [
            {
                "column_name": "cliente_sk",
                "data_type": "INTEGER",
                "is_nullable": False,
                "ordinal_position": 1,
                "column_comment": "Chave substituta do cliente",
            },
            {
                "column_name": "cliente_id",
                "data_type": "INTEGER",
                "is_nullable": False,
                "ordinal_position": 2,
                "column_comment": "Chave natural do cliente",
            },
            {
                "column_name": "nome_cliente",
                "data_type": "TEXT",
                "is_nullable": True,
                "ordinal_position": 3,
                "column_comment": "Nome consolidado do cliente",
            },
        ],
    },
    {
        "layer": "gold",
        "dw_schema": "gold",
        "dw_table": "fato_pedido",
        "table_comment": "Fato de pedidos para analytics",
        "columns": [
            {
                "column_name": "pedido_id",
                "data_type": "INTEGER",
                "is_nullable": False,
                "ordinal_position": 1,
                "column_comment": "Identificador do pedido",
            },
            {
                "column_name": "cliente_sk",
                "data_type": "INTEGER",
                "is_nullable": False,
                "ordinal_position": 2,
                "column_comment": "Chave do cliente na dimensão",
            },
            {
                "column_name": "valor_total",
                "data_type": "NUMERIC",
                "is_nullable": True,
                "ordinal_position": 3,
                "column_comment": "Valor total do pedido",
            },
        ],
    },
]


def _upsert_table(session: Session, table_data: dict) -> None:
    catalog_table = (
        session.query(CatalogTable)
        .filter(
            CatalogTable.dw_schema == table_data["dw_schema"],
            CatalogTable.dw_table == table_data["dw_table"],
        )
        .one_or_none()
    )

    if catalog_table is None:
        catalog_table = CatalogTable(
            dw_schema=table_data["dw_schema"],
            dw_table=table_data["dw_table"],
            layer=table_data["layer"],
            table_comment=table_data["table_comment"],
        )
        session.add(catalog_table)
        session.flush()
    else:
        catalog_table.layer = table_data["layer"]
        catalog_table.table_comment = table_data["table_comment"]

    existing_columns = {column.column_name: column for column in catalog_table.columns}
    for column_data in table_data["columns"]:
        catalog_column = existing_columns.get(column_data["column_name"])
        if catalog_column is None:
            catalog_column = CatalogColumn(table_id=catalog_table.id, **column_data)
            session.add(catalog_column)
            continue

        catalog_column.data_type = column_data["data_type"]
        catalog_column.is_nullable = column_data["is_nullable"]
        catalog_column.ordinal_position = column_data["ordinal_position"]
        catalog_column.column_comment = column_data["column_comment"]


def seed_metadata() -> None:
    database.init_db()
    with database.SessionLocal() as session:
        for table_data in SEED_DATA:
            _upsert_table(session, table_data)
        session.commit()


if __name__ == "__main__":
    seed_metadata()
    print("Seed concluído com sucesso.")
