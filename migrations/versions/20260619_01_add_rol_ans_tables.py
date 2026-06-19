"""add Rol ANS tables (hierarquia, procedimentos, DUTs)

Revision ID: 20260619_01_add_rol_ans
Revises: 20260511_02_bras_purge_batch_indexes
Create Date: 2026-06-19

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260619_01_add_rol_ans'
down_revision: Union[str, None] = '20260511_02_bras_purge_batch_indexes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(name: str) -> bool:
    bind = op.get_bind()
    return inspect(bind).has_table(name)


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    return column in {col['name'] for col in inspect(bind).get_columns(table)}


def _has_index(table: str, index: str) -> bool:
    bind = op.get_bind()
    return index in {idx['name'] for idx in inspect(bind).get_indexes(table)}


def _has_fk(table: str, fk_name: str) -> bool:
    bind = op.get_bind()
    return fk_name in {fk['name'] for fk in inspect(bind).get_foreign_keys(table)}


def upgrade() -> None:
    if not _has_table('rol_capitulos'):
        op.create_table(
            'rol_capitulos',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('nome', sa.String(length=255), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('nome'),
        )

    if not _has_table('rol_grupos'):
        op.create_table(
            'rol_grupos',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('capitulo_id', sa.Integer(), nullable=False),
            sa.Column('nome', sa.String(length=255), nullable=False),
            sa.ForeignKeyConstraint(['capitulo_id'], ['rol_capitulos.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('capitulo_id', 'nome', name='uq_rol_grupo_capitulo_nome'),
        )
    if _has_table('rol_grupos') and not _has_index('rol_grupos', 'idx_rol_grupo_capitulo'):
        op.create_index('idx_rol_grupo_capitulo', 'rol_grupos', ['capitulo_id'], unique=False)

    if not _has_table('rol_subgrupos'):
        op.create_table(
            'rol_subgrupos',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('grupo_id', sa.Integer(), nullable=False),
            sa.Column('nome', sa.String(length=255), nullable=False),
            sa.ForeignKeyConstraint(['grupo_id'], ['rol_grupos.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('grupo_id', 'nome', name='uq_rol_subgrupo_grupo_nome'),
        )
    if _has_table('rol_subgrupos') and not _has_index('rol_subgrupos', 'idx_rol_subgrupo_grupo'):
        op.create_index('idx_rol_subgrupo_grupo', 'rol_subgrupos', ['grupo_id'], unique=False)

    if not _has_table('rol_duts'):
        op.create_table(
            'rol_duts',
            sa.Column('numero', sa.String(length=10), nullable=False),
            sa.Column('titulo', sa.String(length=500), nullable=False),
            sa.Column('texto_completo', sa.Text(), nullable=False),
            sa.Column('resumo', sa.Text(), nullable=True),
            sa.Column('resumo_tipo', sa.String(length=20), nullable=False, server_default='automatico'),
            sa.Column(
                'atualizado_em',
                sa.DateTime(),
                nullable=False,
                server_default=sa.text('CURRENT_TIMESTAMP'),
            ),
            sa.PrimaryKeyConstraint('numero'),
        )

    if not _has_table('rol_procedimentos'):
        op.create_table(
            'rol_procedimentos',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('descricao', sa.String(length=500), nullable=False),
            sa.Column('descricao_norm', sa.String(length=500), nullable=False),
            sa.Column('capitulo_id', sa.Integer(), nullable=True),
            sa.Column('grupo_id', sa.Integer(), nullable=True),
            sa.Column('subgrupo_id', sa.Integer(), nullable=True),
            sa.Column('rn_alteracao', sa.String(length=50), nullable=True),
            sa.Column('vigencia', sa.String(length=30), nullable=True),
            sa.Column('seg_od', sa.Boolean(), nullable=False, server_default=sa.text('0')),
            sa.Column('seg_amb', sa.Boolean(), nullable=False, server_default=sa.text('0')),
            sa.Column('seg_hco', sa.Boolean(), nullable=False, server_default=sa.text('0')),
            sa.Column('seg_hso', sa.Boolean(), nullable=False, server_default=sa.text('0')),
            sa.Column('seg_ref', sa.Boolean(), nullable=False, server_default=sa.text('0')),
            sa.Column('pac', sa.Boolean(), nullable=False, server_default=sa.text('0')),
            sa.Column('dut_numero', sa.String(length=10), nullable=True),
            sa.Column('versao_label', sa.String(length=120), nullable=True),
            sa.Column(
                'atualizado_em',
                sa.DateTime(),
                nullable=False,
                server_default=sa.text('CURRENT_TIMESTAMP'),
            ),
            sa.ForeignKeyConstraint(['capitulo_id'], ['rol_capitulos.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['grupo_id'], ['rol_grupos.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['subgrupo_id'], ['rol_subgrupos.id'], ondelete='SET NULL'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('descricao_norm'),
        )
    if _has_table('rol_procedimentos') and not _has_index('rol_procedimentos', 'idx_rol_proc_dut'):
        op.create_index('idx_rol_proc_dut', 'rol_procedimentos', ['dut_numero'], unique=False)
    if _has_table('rol_procedimentos') and not _has_index('rol_procedimentos', 'idx_rol_proc_capitulo'):
        op.create_index('idx_rol_proc_capitulo', 'rol_procedimentos', ['capitulo_id'], unique=False)

    if _has_table('tuss_rol_correlacoes') and not _has_column('tuss_rol_correlacoes', 'rol_procedimento_id'):
        op.add_column(
            'tuss_rol_correlacoes',
            sa.Column('rol_procedimento_id', sa.Integer(), nullable=True),
        )
    if (
        _has_table('tuss_rol_correlacoes')
        and _has_column('tuss_rol_correlacoes', 'rol_procedimento_id')
        and not _has_fk('tuss_rol_correlacoes', 'fk_tuss_rol_proc')
    ):
        op.create_foreign_key(
            'fk_tuss_rol_proc',
            'tuss_rol_correlacoes',
            'rol_procedimentos',
            ['rol_procedimento_id'],
            ['id'],
            ondelete='SET NULL',
        )
    if (
        _has_table('tuss_rol_correlacoes')
        and _has_column('tuss_rol_correlacoes', 'rol_procedimento_id')
        and not _has_index('tuss_rol_correlacoes', 'idx_tuss_rol_proc')
    ):
        op.create_index(
            'idx_tuss_rol_proc',
            'tuss_rol_correlacoes',
            ['rol_procedimento_id'],
            unique=False,
        )


def downgrade() -> None:
    if _has_table('tuss_rol_correlacoes'):
        if _has_index('tuss_rol_correlacoes', 'idx_tuss_rol_proc'):
            op.drop_index('idx_tuss_rol_proc', table_name='tuss_rol_correlacoes')
        if _has_fk('tuss_rol_correlacoes', 'fk_tuss_rol_proc'):
            op.drop_constraint('fk_tuss_rol_proc', 'tuss_rol_correlacoes', type_='foreignkey')
        if _has_column('tuss_rol_correlacoes', 'rol_procedimento_id'):
            op.drop_column('tuss_rol_correlacoes', 'rol_procedimento_id')

    if _has_table('rol_procedimentos'):
        if _has_index('rol_procedimentos', 'idx_rol_proc_capitulo'):
            op.drop_index('idx_rol_proc_capitulo', table_name='rol_procedimentos')
        if _has_index('rol_procedimentos', 'idx_rol_proc_dut'):
            op.drop_index('idx_rol_proc_dut', table_name='rol_procedimentos')
        op.drop_table('rol_procedimentos')
    if _has_table('rol_duts'):
        op.drop_table('rol_duts')
    if _has_table('rol_subgrupos'):
        if _has_index('rol_subgrupos', 'idx_rol_subgrupo_grupo'):
            op.drop_index('idx_rol_subgrupo_grupo', table_name='rol_subgrupos')
        op.drop_table('rol_subgrupos')
    if _has_table('rol_grupos'):
        if _has_index('rol_grupos', 'idx_rol_grupo_capitulo'):
            op.drop_index('idx_rol_grupo_capitulo', table_name='rol_grupos')
        op.drop_table('rol_grupos')
    if _has_table('rol_capitulos'):
        op.drop_table('rol_capitulos')
