"""Add SIMPRO split tables (cadastro + preco por aliquota).

Revision ID: 20260428_01_add_simpro_split_tables
Revises: 20260427_01_add_simpro_fracionavel
Create Date: 2026-04-28 09:05:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "20260428_01_add_simpro_split_tables"
down_revision: Union[str, None] = "20260428_00_widever"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        return table_name in inspector.get_table_names()
    except Exception:
        return False


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        indexes = inspector.get_indexes(table_name)
    except Exception:
        return False
    return any((idx.get("name") or "") == index_name for idx in indexes)


def upgrade() -> None:
    if not _has_table("simpro_item_cadastro"):
        op.create_table(
            "simpro_item_cadastro",
            sa.Column("id", sa.BigInteger(), nullable=False),
            sa.Column("versao", sa.String(length=100), nullable=False),
            sa.Column("item_key", sa.String(length=64), nullable=False),
            sa.Column("tuss_numero", sa.String(length=16), nullable=True),
            sa.Column("codigo", sa.String(length=20), nullable=True),
            sa.Column("codigo_interno", sa.String(length=20), nullable=True),
            sa.Column("codigo_alt", sa.String(length=20), nullable=True),
            sa.Column("descricao", sa.String(length=255), nullable=True),
            sa.Column("fabricante", sa.String(length=80), nullable=True),
            sa.Column("referencia", sa.String(length=120), nullable=True),
            sa.Column("anvisa", sa.String(length=20), nullable=True),
            sa.Column("ean", sa.String(length=32), nullable=True),
            sa.Column("unidade", sa.String(length=16), nullable=True),
            sa.Column("qtd_unidade", sa.Integer(), nullable=True),
            sa.Column("fracionavel", sa.String(length=1), nullable=True),
            sa.Column("status_final", sa.String(length=8), nullable=True),
            sa.Column("data_ref", sa.Date(), nullable=True),
            sa.Column("linha_num", sa.Integer(), nullable=True),
            sa.Column("imported_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=True),
            sa.PrimaryKeyConstraint("id", name="pk_simpro_item_cadastro"),
            sa.UniqueConstraint("versao", "item_key", name="uq_simpro_cadastro_versao_item"),
        )
    if not _has_index("simpro_item_cadastro", "idx_simpro_cad_versao_item"):
        op.create_index("idx_simpro_cad_versao_item", "simpro_item_cadastro", ["versao", "item_key"], unique=False)
    if not _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_versao"):
        op.create_index("ix_simpro_item_cadastro_versao", "simpro_item_cadastro", ["versao"], unique=False)
    if not _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_item_key"):
        op.create_index("ix_simpro_item_cadastro_item_key", "simpro_item_cadastro", ["item_key"], unique=False)
    if not _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_tuss_numero"):
        op.create_index("ix_simpro_item_cadastro_tuss_numero", "simpro_item_cadastro", ["tuss_numero"], unique=False)
    if not _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_codigo"):
        op.create_index("ix_simpro_item_cadastro_codigo", "simpro_item_cadastro", ["codigo"], unique=False)
    if not _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_descricao"):
        op.create_index("ix_simpro_item_cadastro_descricao", "simpro_item_cadastro", ["descricao"], unique=False)
    if not _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_anvisa"):
        op.create_index("ix_simpro_item_cadastro_anvisa", "simpro_item_cadastro", ["anvisa"], unique=False)
    if not _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_ean"):
        op.create_index("ix_simpro_item_cadastro_ean", "simpro_item_cadastro", ["ean"], unique=False)

    if not _has_table("simpro_item_preco"):
        op.create_table(
            "simpro_item_preco",
            sa.Column("id", sa.BigInteger(), nullable=False),
            sa.Column("cadastro_id", sa.BigInteger(), nullable=False),
            sa.Column("aliquota", sa.Numeric(6, 2), nullable=False),
            sa.Column("preco1", sa.Numeric(15, 4), nullable=True),
            sa.Column("preco2", sa.Numeric(15, 4), nullable=True),
            sa.Column("preco3", sa.Numeric(15, 4), nullable=True),
            sa.Column("preco4", sa.Numeric(15, 4), nullable=True),
            sa.Column("arquivo_fonte", sa.String(length=255), nullable=True),
            sa.Column("imported_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=True),
            sa.ForeignKeyConstraint(["cadastro_id"], ["simpro_item_cadastro.id"]),
            sa.PrimaryKeyConstraint("id", name="pk_simpro_item_preco"),
            sa.UniqueConstraint("cadastro_id", "aliquota", name="uq_simpro_preco_cadastro_aliquota"),
        )
    if not _has_index("simpro_item_preco", "idx_simpro_preco_cad"):
        op.create_index("idx_simpro_preco_cad", "simpro_item_preco", ["cadastro_id", "aliquota"], unique=False)
    if not _has_index("simpro_item_preco", "ix_simpro_item_preco_cadastro_id"):
        op.create_index("ix_simpro_item_preco_cadastro_id", "simpro_item_preco", ["cadastro_id"], unique=False)
    if not _has_index("simpro_item_preco", "ix_simpro_item_preco_aliquota"):
        op.create_index("ix_simpro_item_preco_aliquota", "simpro_item_preco", ["aliquota"], unique=False)


def downgrade() -> None:
    if _has_table("simpro_item_preco"):
        if _has_index("simpro_item_preco", "ix_simpro_item_preco_aliquota"):
            op.drop_index("ix_simpro_item_preco_aliquota", table_name="simpro_item_preco")
        if _has_index("simpro_item_preco", "ix_simpro_item_preco_cadastro_id"):
            op.drop_index("ix_simpro_item_preco_cadastro_id", table_name="simpro_item_preco")
        if _has_index("simpro_item_preco", "idx_simpro_preco_cad"):
            op.drop_index("idx_simpro_preco_cad", table_name="simpro_item_preco")
        op.drop_table("simpro_item_preco")

    if _has_table("simpro_item_cadastro"):
        if _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_ean"):
            op.drop_index("ix_simpro_item_cadastro_ean", table_name="simpro_item_cadastro")
        if _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_anvisa"):
            op.drop_index("ix_simpro_item_cadastro_anvisa", table_name="simpro_item_cadastro")
        if _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_descricao"):
            op.drop_index("ix_simpro_item_cadastro_descricao", table_name="simpro_item_cadastro")
        if _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_codigo"):
            op.drop_index("ix_simpro_item_cadastro_codigo", table_name="simpro_item_cadastro")
        if _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_tuss_numero"):
            op.drop_index("ix_simpro_item_cadastro_tuss_numero", table_name="simpro_item_cadastro")
        if _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_item_key"):
            op.drop_index("ix_simpro_item_cadastro_item_key", table_name="simpro_item_cadastro")
        if _has_index("simpro_item_cadastro", "ix_simpro_item_cadastro_versao"):
            op.drop_index("ix_simpro_item_cadastro_versao", table_name="simpro_item_cadastro")
        if _has_index("simpro_item_cadastro", "idx_simpro_cad_versao_item"):
            op.drop_index("idx_simpro_cad_versao_item", table_name="simpro_item_cadastro")
        op.drop_table("simpro_item_cadastro")
