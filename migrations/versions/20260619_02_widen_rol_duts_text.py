"""widen rol_duts text columns to LONGTEXT

Revision ID: 20260619_02_widen_rol_duts
Revises: 20260619_01_add_rol_ans
Create Date: 2026-06-19

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import LONGTEXT


revision: str = '20260619_02_widen_rol_duts'
down_revision: Union[str, None] = '20260619_01_add_rol_ans'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(name: str) -> bool:
    bind = op.get_bind()
    return inspect(bind).has_table(name)


def upgrade() -> None:
    if not _has_table('rol_duts'):
        return
    bind = op.get_bind()
    if bind.dialect.name != 'mysql':
        return
    op.alter_column(
        'rol_duts',
        'texto_completo',
        existing_type=sa.Text(),
        type_=LONGTEXT(),
        existing_nullable=False,
    )
    op.alter_column(
        'rol_duts',
        'resumo',
        existing_type=sa.Text(),
        type_=LONGTEXT(),
        existing_nullable=True,
    )


def downgrade() -> None:
    if not _has_table('rol_duts'):
        return
    bind = op.get_bind()
    if bind.dialect.name != 'mysql':
        return
    op.alter_column(
        'rol_duts',
        'texto_completo',
        existing_type=LONGTEXT(),
        type_=sa.Text(),
        existing_nullable=False,
    )
    op.alter_column(
        'rol_duts',
        'resumo',
        existing_type=LONGTEXT(),
        type_=sa.Text(),
        existing_nullable=True,
    )
