"""Add BRAS purge batch indexes

Revision ID: 20260511_02_bras_purge_batch_indexes
Revises: 20260511_01_idx_insumos_origem_versao
Create Date: 2026-05-11 17:35:00

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect


revision: str = '20260511_02_bras_purge_batch_indexes'
down_revision: Union[str, None] = '20260511_01_idx_insumos_origem_versao'
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
    op.create_index(index_name, table, columns, unique=False)


def _drop_index(table: str, index_name: str) -> None:
    if index_name not in _index_names(table):
        return
    op.drop_index(index_name, table_name=table)


def upgrade() -> None:
    _create_index("bras_item_n", "idx_bras_item_n_edicao_id", ["edicao", "id"])
    # bras_item_cadastro é criada via create_all em prod; pode não existir no CI só com migrations
    _create_index("bras_item_cadastro", "idx_bras_item_cadastro_edicao_id", ["edicao", "id"])


def downgrade() -> None:
    _drop_index("bras_item_cadastro", "idx_bras_item_cadastro_edicao_id")
    _drop_index("bras_item_n", "idx_bras_item_n_edicao_id")
