"""
Add operadora_id to procedimentos table for multi-operadora support

Revision ID: 20241024_02
Revises: 20241024_01
Create Date: 2025-10-24

Esta migration adiciona suporte para múltiplas operadoras na tabela procedimentos.
Cada procedimento (DTP, etc) agora pode ter valores específicos por operadora.

Mudanças:
1. Adiciona coluna operadora_id (INT NOT NULL)
2. Cria foreign key para operadoras(id) com CASCADE delete
3. Associa todos os procedimentos existentes à operadora_id = 1 (MPF)
4. Cria índice para performance
5. Altera PRIMARY KEY de (id) para (id, operadora_id) - REVISAR: pode não ser necessário

IMPORTANTE: Esta migration pressupõe que existe uma operadora com id=1 (MPF)
"""

from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    """
    Adiciona suporte multi-operadora à tabela procedimentos
    """
    # 1. Adicionar coluna operadora_id (nullable inicialmente)
    op.add_column('procedimentos',
                  sa.Column('operadora_id', sa.Integer(), nullable=True))

    # 2. Setar operadora_id = 1 (MPF) para todos os registros existentes
    op.execute("UPDATE procedimentos SET operadora_id = 1 WHERE operadora_id IS NULL")

    # 3. Tornar operadora_id NOT NULL
    op.alter_column('procedimentos', 'operadora_id', nullable=False)

    # 4. Criar foreign key para operadoras
    op.create_foreign_key(
        'fk_procedimentos_operadora',
        'procedimentos',
        'operadoras',
        ['operadora_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # 5. Criar índice para melhorar performance de consultas por operadora
    op.create_index('idx_procedimentos_operadora', 'procedimentos', ['operadora_id'])

    # NOTA: Diferente de cbhpm_teto, NÃO alteramos a PRIMARY KEY aqui
    # porque procedimentos já tem id auto_increment como PK
    # A combinação única será garantida pela lógica de negócio


def downgrade() -> None:
    """
    Remove suporte multi-operadora da tabela procedimentos
    """
    # 1. Remover índice
    op.drop_index('idx_procedimentos_operadora', table_name='procedimentos')

    # 2. Remover foreign key
    op.drop_constraint('fk_procedimentos_operadora', 'procedimentos', type_='foreignkey')

    # 3. Remover coluna operadora_id
    op.drop_column('procedimentos', 'operadora_id')
