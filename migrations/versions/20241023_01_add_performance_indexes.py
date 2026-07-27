"""Add performance indexes for insumos module

Revision ID: 20241023_01_add_performance_indexes
Revises: 20241013_01_extend_simpro_norm_fields
Create Date: 2024-10-23 15:00:00

"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect

revision: str = "20241023_01_add_performance_indexes"
down_revision: Union[str, None] = "20241013_01_extend_simpro_norm_fields"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _index_names(table: str) -> set[str]:
    bind = op.get_bind()
    insp = inspect(bind)
    if not insp.has_table(table):
        return set()
    return {idx["name"] for idx in insp.get_indexes(table)}


def _create_index(table: str, index_name: str, columns: list[str]) -> None:
    if index_name in _index_names(table):
        return
    op.create_index(index_name, table, columns)


def _drop_index(table: str, index_name: str) -> None:
    if index_name not in _index_names(table):
        return
    op.drop_index(index_name, table_name=table)


def upgrade() -> None:
    """Índices para summary/versões do módulo de insumos (MySQL-compatible)."""
    if "idx_bras_item_n_imported_at" not in _index_names("bras_item_n"):
        op.execute("CREATE INDEX idx_bras_item_n_imported_at ON bras_item_n (imported_at DESC)")

    _create_index("bras_item_n", "idx_bras_item_n_edicao_sorted", ["edicao"])

    if "idx_simpro_item_norm_imported_at" not in _index_names("simpro_item_norm"):
        op.execute("CREATE INDEX idx_simpro_item_norm_imported_at ON simpro_item_norm (imported_at DESC)")

    _create_index("simpro_item_norm", "idx_simpro_item_norm_versao_data", ["versao", "data_ref"])
    _create_index("simpro_item_norm", "idx_simpro_item_norm_uf_versao", ["uf_referencia", "versao"])


def downgrade() -> None:
    _drop_index("bras_item_n", "idx_bras_item_n_imported_at")
    _drop_index("bras_item_n", "idx_bras_item_n_edicao_sorted")
    _drop_index("simpro_item_norm", "idx_simpro_item_norm_imported_at")
    _drop_index("simpro_item_norm", "idx_simpro_item_norm_versao_data")
    _drop_index("simpro_item_norm", "idx_simpro_item_norm_uf_versao")
