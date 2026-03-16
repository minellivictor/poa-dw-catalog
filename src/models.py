from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class CatalogTable(Base):
    __tablename__ = "catalog_table"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    dw_schema: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    dw_table: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    layer: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    table_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    descricao_negocio: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    columns: Mapped[list["CatalogColumn"]] = relationship(
        back_populates="table",
        cascade="all, delete-orphan",
    )


class CatalogColumn(Base):
    __tablename__ = "catalog_column"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    table_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("catalog_table.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    column_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    data_type: Mapped[str] = mapped_column(String(100), nullable=False)
    is_nullable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ordinal_position: Mapped[int] = mapped_column(Integer, nullable=False)
    column_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    descricao_negocio: Mapped[str | None] = mapped_column(Text, nullable=True)

    table: Mapped[CatalogTable] = relationship(back_populates="columns")


class HistoricoCuradoria(Base):
    __tablename__ = "historico_curadoria"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    tipo_entidade: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    entidade_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    campo_alterado: Mapped[str] = mapped_column(String(100), nullable=False)
    valor_anterior: Mapped[str | None] = mapped_column(Text, nullable=True)
    valor_novo: Mapped[str | None] = mapped_column(Text, nullable=True)
    usuario: Mapped[str] = mapped_column(String(255), nullable=False)
    data_alteracao: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC),
        index=True,
    )
