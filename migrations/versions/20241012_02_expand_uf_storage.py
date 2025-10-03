"""expand uf storage columns

Revision ID: 20241012_02_expand_uf_storage
Revises: 20241012_01_add_cbhpm_teto_admin
Create Date: 2024-10-12 00:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20241012_02_expand_uf_storage'
down_revision: Union[str, None] = '20241012_01_add_cbhpm_teto_admin'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


NEW_LENGTH = 64
OLD_LENGTH = 5


def upgrade() -> None:
    op.alter_column(
        'insumos_index',
        'uf_referencia',
        existing_type=sa.String(length=OLD_LENGTH),
        type_=sa.String(length=NEW_LENGTH),
        existing_nullable=True,
    )
    op.alter_column(
        'simpro_item',
        'uf_referencia',
        existing_type=sa.String(length=OLD_LENGTH),
        type_=sa.String(length=NEW_LENGTH),
        existing_nullable=True,
    )
    op.alter_column(
        'simpro_item_norm',
        'uf_referencia',
        existing_type=sa.String(length=OLD_LENGTH),
        type_=sa.String(length=NEW_LENGTH),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        'simpro_item_norm',
        'uf_referencia',
        existing_type=sa.String(length=NEW_LENGTH),
        type_=sa.String(length=OLD_LENGTH),
        existing_nullable=True,
    )
    op.alter_column(
        'simpro_item',
        'uf_referencia',
        existing_type=sa.String(length=NEW_LENGTH),
        type_=sa.String(length=OLD_LENGTH),
        existing_nullable=True,
    )
    op.alter_column(
        'insumos_index',
        'uf_referencia',
        existing_type=sa.String(length=NEW_LENGTH),
        type_=sa.String(length=OLD_LENGTH),
        existing_nullable=True,
    )
