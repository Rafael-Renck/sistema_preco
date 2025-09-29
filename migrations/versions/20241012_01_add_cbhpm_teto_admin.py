"""add cbhpm teto admin support

Revision ID: 20241012_01_add_cbhpm_teto_admin
Revises: 20241009_04_add_simpro_pipeline
Create Date: 2025-09-29 10:22:36

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


revision: str = '20241012_01_add_cbhpm_teto_admin'
down_revision: Union[str, None] = '20241009_04_add_simpro_pipeline'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'cbhpm_teto',
        'codigo',
        existing_type=sa.String(length=100),
        type_=sa.String(length=20),
        existing_nullable=False,
    )
    op.alter_column(
        'cbhpm_teto',
        'descricao',
        existing_type=sa.String(length=500),
        type_=sa.String(length=255),
        existing_nullable=True,
        nullable=False,
    )
    op.alter_column(
        'cbhpm_teto',
        'valor_total',
        existing_type=sa.Numeric(precision=12, scale=2),
        type_=sa.Numeric(precision=15, scale=2),
        existing_nullable=False,
    )
    op.alter_column(
        'cbhpm_teto',
        'updated_at',
        existing_type=sa.DateTime(),
        type_=mysql.TIMESTAMP(),
        existing_nullable=False,
        existing_server_default=sa.text('CURRENT_TIMESTAMP'),
        server_default=sa.text('CURRENT_TIMESTAMP'),
        existing_onupdate=sa.text('CURRENT_TIMESTAMP'),
        server_onupdate=sa.text('CURRENT_TIMESTAMP'),
    )
    op.drop_column('cbhpm_teto', 'versao_ref')
    op.create_index('idx_cbhpm_teto_descricao', 'cbhpm_teto', ['descricao'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_cbhpm_teto_descricao', table_name='cbhpm_teto')
    op.add_column('cbhpm_teto', sa.Column('versao_ref', sa.String(length=100), nullable=False))
    op.alter_column(
        'cbhpm_teto',
        'updated_at',
        existing_type=mysql.TIMESTAMP(),
        type_=sa.DateTime(),
        existing_nullable=False,
        existing_server_default=sa.text('CURRENT_TIMESTAMP'),
        server_default=sa.text('CURRENT_TIMESTAMP'),
        existing_onupdate=sa.text('CURRENT_TIMESTAMP'),
        server_onupdate=sa.text('CURRENT_TIMESTAMP'),
    )
    op.alter_column(
        'cbhpm_teto',
        'valor_total',
        existing_type=sa.Numeric(precision=15, scale=2),
        type_=sa.Numeric(precision=12, scale=2),
        existing_nullable=False,
    )
    op.alter_column(
        'cbhpm_teto',
        'descricao',
        existing_type=sa.String(length=255),
        type_=sa.String(length=500),
        existing_nullable=False,
        nullable=True,
    )
    op.alter_column(
        'cbhpm_teto',
        'codigo',
        existing_type=sa.String(length=20),
        type_=sa.String(length=100),
        existing_nullable=False,
    )
