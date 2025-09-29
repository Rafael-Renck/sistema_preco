"""Add SIMPRO fixed-width pipeline tables

Revision ID: 20241009_04
Revises: 20241009_03
Create Date: 2024-10-10 12:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241009_04'
down_revision: Union[str, None] = '20241009_03'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'simpro_fixed_stage',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('arquivo', sa.String(length=255), nullable=False),
        sa.Column('linha_num', sa.Integer(), nullable=False),
        sa.Column('linha', sa.Text(), nullable=False),
        sa.Column('imported_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_simpro_fixed_stage')
    )
    op.create_index('idx_simpro_fixed_arquivo', 'simpro_fixed_stage', ['arquivo'], unique=False)

    op.create_table(
        'simpro_item_norm',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('arquivo', sa.String(length=255), nullable=False),
        sa.Column('linha_num', sa.Integer(), nullable=False),
        sa.Column('codigo', sa.String(length=20), nullable=False),
        sa.Column('codigo_alt', sa.String(length=20), nullable=True),
        sa.Column('descricao', sa.String(length=255), nullable=False),
        sa.Column('data_ref', sa.Date(), nullable=True),
        sa.Column('tipo_reg', sa.String(length=4), nullable=True),
        sa.Column('preco1', sa.Numeric(15, 4), nullable=True),
        sa.Column('preco2', sa.Numeric(15, 4), nullable=True),
        sa.Column('preco3', sa.Numeric(15, 4), nullable=True),
        sa.Column('preco4', sa.Numeric(15, 4), nullable=True),
        sa.Column('unidade', sa.String(length=16), nullable=True),
        sa.Column('qtd_unidade', sa.Integer(), nullable=True),
        sa.Column('fabricante', sa.String(length=80), nullable=True),
        sa.Column('anvisa', sa.String(length=20), nullable=True),
        sa.Column('validade_anvisa', sa.Date(), nullable=True),
        sa.Column('ean', sa.String(length=32), nullable=True),
        sa.Column('situacao', sa.String(length=40), nullable=True),
        sa.Column('versao', sa.String(length=100), nullable=True),
        sa.Column('uf_referencia', sa.String(length=5), nullable=True),
        sa.Column('imported_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_simpro_item_norm')
    )
    op.create_index('idx_simpro_item_norm_desc', 'simpro_item_norm', ['descricao'], unique=False)
    op.create_index('idx_simpro_item_norm_ean', 'simpro_item_norm', ['ean'], unique=False)
    op.create_index('idx_simpro_item_norm_anvisa', 'simpro_item_norm', ['anvisa'], unique=False)
    op.create_index('idx_simpro_item_norm_versao', 'simpro_item_norm', ['versao'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_simpro_item_norm_versao', table_name='simpro_item_norm')
    op.drop_index('idx_simpro_item_norm_anvisa', table_name='simpro_item_norm')
    op.drop_index('idx_simpro_item_norm_ean', table_name='simpro_item_norm')
    op.drop_index('idx_simpro_item_norm_desc', table_name='simpro_item_norm')
    op.drop_table('simpro_item_norm')

    op.drop_index('idx_simpro_fixed_arquivo', table_name='simpro_fixed_stage')
    op.drop_table('simpro_fixed_stage')
