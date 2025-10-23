"""Add performance indexes for insumos module

Revision ID: 20241023_01_add_performance_indexes
Revises: 20241013_01_extend_simpro_norm_fields
Create Date: 2024-10-23 15:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241023_01_add_performance_indexes'
down_revision: Union[str, None] = '20241013_01_extend_simpro_norm_fields'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Adiciona índices para otimizar as queries de summary e versões do módulo de insumos.

    Melhora performance em:
    - Carregamento da página /insumos (queries de agregação)
    - Busca por versões distintas
    - Filtros de UF e alíquota
    """

    # Índices para BrasItemNormalized (bras_item_n)
    # Acelera MAX(imported_at) na função _insumo_summary
    op.execute("CREATE INDEX IF NOT EXISTS idx_bras_item_n_imported_at ON bras_item_n(imported_at DESC)")

    # Acelera DISTINCT versao/edicao (já existe idx mas vamos garantir order otimizado)
    op.execute("CREATE INDEX IF NOT EXISTS idx_bras_item_n_edicao_sorted ON bras_item_n(edicao) WHERE edicao IS NOT NULL")

    # Índices para SimproItemNormalized (simpro_item_norm)
    # Acelera MAX(imported_at) na função _insumo_summary
    op.execute("CREATE INDEX IF NOT EXISTS idx_simpro_item_norm_imported_at ON simpro_item_norm(imported_at DESC)")

    # Acelera queries por versao + data_ref combinadas
    op.execute("CREATE INDEX IF NOT EXISTS idx_simpro_item_norm_versao_data ON simpro_item_norm(versao, data_ref DESC)")

    # Índice composto para buscas com UF (muito comum no módulo)
    op.execute("CREATE INDEX IF NOT EXISTS idx_simpro_item_norm_uf_versao ON simpro_item_norm(uf_referencia, versao)")


def downgrade() -> None:
    """Remove os índices adicionados"""

    # Remove índices do BrasItemNormalized
    op.execute("DROP INDEX IF EXISTS idx_bras_item_n_imported_at ON bras_item_n")
    op.execute("DROP INDEX IF EXISTS idx_bras_item_n_edicao_sorted ON bras_item_n")

    # Remove índices do SimproItemNormalized
    op.execute("DROP INDEX IF EXISTS idx_simpro_item_norm_imported_at ON simpro_item_norm")
    op.execute("DROP INDEX IF EXISTS idx_simpro_item_norm_versao_data ON simpro_item_norm")
    op.execute("DROP INDEX IF EXISTS idx_simpro_item_norm_uf_versao ON simpro_item_norm")
