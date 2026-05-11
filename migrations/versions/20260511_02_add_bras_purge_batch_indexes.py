"""Add BRAS purge batch indexes

Revision ID: 20260511_02_bras_purge_batch_indexes
Revises: 20260511_01_idx_insumos_origem_versao
Create Date: 2026-05-11 17:35:00

"""
from typing import Sequence, Union

from alembic import op


revision: str = '20260511_02_bras_purge_batch_indexes'
down_revision: Union[str, None] = '20260511_01_idx_insumos_origem_versao'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'idx_bras_item_n_edicao_id',
        'bras_item_n',
        ['edicao', 'id'],
        unique=False,
    )
    op.create_index(
        'idx_bras_item_cadastro_edicao_id',
        'bras_item_cadastro',
        ['edicao', 'id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('idx_bras_item_cadastro_edicao_id', table_name='bras_item_cadastro')
    op.drop_index('idx_bras_item_n_edicao_id', table_name='bras_item_n')
