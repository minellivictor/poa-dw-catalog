from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "catalog.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    from src.models import Base

    Base.metadata.create_all(bind=engine)
    _ensure_curadoria_schema()


def _ensure_curadoria_schema() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    if "catalog_table" in existing_tables:
        table_columns = {column["name"] for column in inspector.get_columns("catalog_table")}
        if "descricao_negocio" not in table_columns:
            with engine.begin() as connection:
                connection.execute(
                    text("ALTER TABLE catalog_table ADD COLUMN descricao_negocio TEXT")
                )

    if "catalog_column" in existing_tables:
        column_columns = {
            column["name"] for column in inspector.get_columns("catalog_column")
        }
        if "descricao_negocio" not in column_columns:
            with engine.begin() as connection:
                connection.execute(
                    text("ALTER TABLE catalog_column ADD COLUMN descricao_negocio TEXT")
                )


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
