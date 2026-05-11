"""Add insumos_index version index for BRAS purge

Revision ID: 20260511_01_idx_insumos_origem_versao
Revises: 20260428_02_pk_aliquota
Create Date: 2026-05-11 17:05:00

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '20260511_01_idx_insumos_origem_versao'
down_revision: Union[str, None] = '20260428_02_pk_aliquota'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'idx_insumos_origem_versao',
        'insumos_index',
        ['origem', 'versao_tabela'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('idx_insumos_origem_versao', table_name='insumos_index')
