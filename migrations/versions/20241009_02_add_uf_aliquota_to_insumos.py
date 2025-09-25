"""add uf and aliquota columns to insumos

Revision ID: 20241009_02
Revises: 20241009_01
Create Date: 2024-10-09 12:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241009_02'
down_revision: Union[str, None] = '20241009_01'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('bras_item', sa.Column('aliquota', sa.Numeric(12, 4), nullable=True))
    op.add_column('bras_item', sa.Column('uf_referencia', sa.String(length=5), nullable=True))

    op.add_column('simpro_item', sa.Column('aliquota', sa.Numeric(12, 4), nullable=True))
    op.add_column('simpro_item', sa.Column('uf_referencia', sa.String(length=5), nullable=True))

    op.add_column('insumos_index', sa.Column('aliquota', sa.Numeric(12, 4), nullable=True))
    op.add_column('insumos_index', sa.Column('uf_referencia', sa.String(length=5), nullable=True))

    op.execute("DROP TRIGGER IF EXISTS trg_bras_item_au")
    op.execute("DROP TRIGGER IF EXISTS trg_bras_item_ai")
    op.execute("DROP TRIGGER IF EXISTS trg_simpro_item_au")
    op.execute("DROP TRIGGER IF EXISTS trg_simpro_item_ai")

    op.execute(
        """
        CREATE TRIGGER trg_bras_item_ai
        AFTER INSERT ON bras_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, aliquota, fabricante,
                anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
            )
            VALUES (
                'BRAS', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco, NEW.aliquota,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao,
                NEW.uf_referencia, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                aliquota = NEW.aliquota,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                uf_referencia = NEW.uf_referencia,
                updated_at = NOW();
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_bras_item_au
        AFTER UPDATE ON bras_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, aliquota, fabricante,
                anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
            )
            VALUES (
                'BRAS', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco, NEW.aliquota,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao,
                NEW.uf_referencia, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                aliquota = NEW.aliquota,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                uf_referencia = NEW.uf_referencia,
                updated_at = NOW();
        END
        """
    )

    op.execute(
        """
        CREATE TRIGGER trg_simpro_item_ai
        AFTER INSERT ON simpro_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, aliquota, fabricante,
                anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
            )
            VALUES (
                'SIMPRO', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco, NEW.aliquota,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao,
                NEW.uf_referencia, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                aliquota = NEW.aliquota,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                uf_referencia = NEW.uf_referencia,
                updated_at = NOW();
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_simpro_item_au
        AFTER UPDATE ON simpro_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, aliquota, fabricante,
                anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
            )
            VALUES (
                'SIMPRO', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco, NEW.aliquota,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao,
                NEW.uf_referencia, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                aliquota = NEW.aliquota,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                uf_referencia = NEW.uf_referencia,
                updated_at = NOW();
        END
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_simpro_item_au")
    op.execute("DROP TRIGGER IF EXISTS trg_simpro_item_ai")
    op.execute("DROP TRIGGER IF EXISTS trg_bras_item_au")
    op.execute("DROP TRIGGER IF EXISTS trg_bras_item_ai")

    op.drop_column('insumos_index', 'uf_referencia')
    op.drop_column('insumos_index', 'aliquota')

    op.drop_column('simpro_item', 'uf_referencia')
    op.drop_column('simpro_item', 'aliquota')

    op.drop_column('bras_item', 'uf_referencia')
    op.drop_column('bras_item', 'aliquota')

    op.execute(
        """
        CREATE TRIGGER trg_bras_item_ai
        AFTER INSERT ON bras_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, fabricante,
                anvisa, versao_tabela, data_atualizacao, updated_at
            )
            VALUES (
                'BRAS', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                updated_at = NOW();
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_bras_item_au
        AFTER UPDATE ON bras_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, fabricante,
                anvisa, versao_tabela, data_atualizacao, updated_at
            )
            VALUES (
                'BRAS', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                updated_at = NOW();
        END
        """
    )

    op.execute(
        """
        CREATE TRIGGER trg_simpro_item_ai
        AFTER INSERT ON simpro_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, fabricante,
                anvisa, versao_tabela, data_atualizacao, updated_at
            )
            VALUES (
                'SIMPRO', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                updated_at = NOW();
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_simpro_item_au
        AFTER UPDATE ON simpro_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (
                origem, item_id, tuss, tiss, descricao, preco, fabricante,
                anvisa, versao_tabela, data_atualizacao, updated_at
            )
            VALUES (
                'SIMPRO', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco,
                NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao, NOW()
            )
            ON DUPLICATE KEY UPDATE
                tuss = NEW.tuss,
                tiss = NEW.tiss,
                descricao = NEW.descricao,
                preco = NEW.preco,
                fabricante = NEW.fabricante,
                anvisa = NEW.anvisa,
                versao_tabela = NEW.versao_tabela,
                data_atualizacao = NEW.data_atualizacao,
                updated_at = NOW();
        END
        """
    )
