"""Expand SIMPRO normalized item fields

Revision ID: 20241013_01_extend_simpro_norm_fields
Revises: 20241012_02_expand_uf_storage
Create Date: 2024-10-13 12:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241013_01_extend_simpro_norm_fields'
down_revision: Union[str, None] = '20241012_02_expand_uf_storage'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('simpro_item_norm', sa.Column('codigo_interno', sa.String(length=20), nullable=True))
    op.add_column('simpro_item_norm', sa.Column('tuss_prefix', sa.String(length=4), nullable=True))
    op.add_column('simpro_item_norm', sa.Column('tuss_numero', sa.String(length=16), nullable=True))
    op.add_column('simpro_item_norm', sa.Column('status_final', sa.String(length=8), nullable=True))

    op.create_index('idx_simpro_item_norm_cod_interno', 'simpro_item_norm', ['codigo_interno'], unique=False)
    op.create_index('idx_simpro_item_norm_tuss_numero', 'simpro_item_norm', ['tuss_numero'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_simpro_item_norm_tuss_numero', table_name='simpro_item_norm')
    op.drop_index('idx_simpro_item_norm_cod_interno', table_name='simpro_item_norm')

    op.drop_column('simpro_item_norm', 'status_final')
    op.drop_column('simpro_item_norm', 'tuss_numero')
    op.drop_column('simpro_item_norm', 'tuss_prefix')
    op.drop_column('simpro_item_norm', 'codigo_interno')

