"""Add operadora support to CBHPM teto

Revision ID: 20241024_01_add_operadora_to_teto
Revises: 20241023_01_add_performance_indexes
Create Date: 2024-10-24 10:30:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = '20241024_01_add_operadora_to_teto'
down_revision: Union[str, None] = '20241023_01_add_performance_indexes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Adiciona suporte para múltiplas operadoras na tabela de tetos CBHPM.

    Mudanças:
    1. Adiciona coluna operadora_id (nullable para compatibilidade com dados existentes)
    2. Remove PRIMARY KEY atual (codigo)
    3. Adiciona PRIMARY KEY composta (codigo, operadora_id)
    4. Adiciona foreign key para operadoras
    5. Define operadora_id = 1 (MPF) para registros existentes

    ATENÇÃO: Esta migration assume que já existe uma operadora com ID=1 (MPF).
    Se não existir, crie antes de rodar esta migration!
    """

    # 1. Adicionar coluna operadora_id (nullable temporariamente)
    op.add_column('cbhpm_teto', sa.Column('operadora_id', sa.Integer(), nullable=True))

    # 2. Setar operadora_id = 1 (MPF) para todos os registros existentes
    op.execute("UPDATE cbhpm_teto SET operadora_id = 1 WHERE operadora_id IS NULL")

    # 3. Tornar operadora_id NOT NULL
    op.alter_column(
        'cbhpm_teto',
        'operadora_id',
        existing_type=sa.Integer(),
        nullable=False,
    )

    # 4. Dropar PRIMARY KEY antiga e criar nova composta
    # NOTA: No MySQL, precisamos recriar a PK
    op.execute("ALTER TABLE cbhpm_teto DROP PRIMARY KEY")
    op.execute("ALTER TABLE cbhpm_teto ADD PRIMARY KEY (codigo, operadora_id)")

    # 5. Garantir tabela operadoras e seed MPF (CI / installs limpos)
    bind = op.get_bind()
    insp = inspect(bind)
    if not insp.has_table('operadoras'):
        op.create_table(
            'operadoras',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('nome', sa.String(length=255), nullable=False),
            sa.Column('uf', sa.String(length=2), nullable=True),
            sa.Column('cnpj', sa.String(length=20), nullable=True),
            sa.Column('status', sa.String(length=50), nullable=False),
            sa.PrimaryKeyConstraint('id'),
        )
    op.execute("INSERT IGNORE INTO operadoras (id, nome, status) VALUES (1, 'MPF', 'ativa')")

    # 6. Adicionar FOREIGN KEY
    op.create_foreign_key(
        'fk_cbhpm_teto_operadora',
        'cbhpm_teto',
        'operadoras',
        ['operadora_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # 6. Adicionar índice para busca por operadora
    op.create_index('idx_cbhpm_teto_operadora', 'cbhpm_teto', ['operadora_id'], unique=False)


def downgrade() -> None:
    """Reverter mudanças"""

    # 1. Remover índice
    op.drop_index('idx_cbhpm_teto_operadora', table_name='cbhpm_teto')

    # 2. Remover FK
    op.drop_constraint('fk_cbhpm_teto_operadora', 'cbhpm_teto', type_='foreignkey')

    # 3. Remover PK composta e recriar PK simples
    op.execute("ALTER TABLE cbhpm_teto DROP PRIMARY KEY")
    op.execute("ALTER TABLE cbhpm_teto ADD PRIMARY KEY (codigo)")

    # 4. Remover coluna
    op.drop_column('cbhpm_teto', 'operadora_id')
