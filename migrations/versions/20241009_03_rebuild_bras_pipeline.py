"""Rebuild Brasindice pipeline with staging and materialized tables

Revision ID: 20241009_03
Revises: 20241009_02
Create Date: 2024-10-10 00:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241009_03'
down_revision: Union[str, None] = '20241009_02'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute('DROP TRIGGER IF EXISTS trg_bras_item_ai')
    op.execute('DROP TRIGGER IF EXISTS trg_bras_item_au')

    op.execute('DROP TABLE IF EXISTS bras_item')

    op.create_table(
        'bras_raw',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('arquivo', sa.String(length=255), nullable=False),
        sa.Column('linha_num', sa.Integer(), nullable=False),
        sa.Column('col01', sa.String(length=255), nullable=True),
        sa.Column('col02', sa.String(length=255), nullable=True),
        sa.Column('col03', sa.String(length=255), nullable=True),
        sa.Column('col04', sa.String(length=255), nullable=True),
        sa.Column('col05', sa.String(length=255), nullable=True),
        sa.Column('col06', sa.String(length=255), nullable=True),
        sa.Column('col07', sa.String(length=255), nullable=True),
        sa.Column('col08', sa.String(length=255), nullable=True),
        sa.Column('col09', sa.String(length=255), nullable=True),
        sa.Column('col10', sa.String(length=255), nullable=True),
        sa.Column('col11', sa.String(length=255), nullable=True),
        sa.Column('col12', sa.String(length=255), nullable=True),
        sa.Column('col13', sa.String(length=255), nullable=True),
        sa.Column('col14', sa.String(length=255), nullable=True),
        sa.Column('col15', sa.String(length=255), nullable=True),
        sa.Column('col16', sa.String(length=255), nullable=True),
        sa.Column('col17', sa.String(length=255), nullable=True),
        sa.Column('col18', sa.String(length=255), nullable=True),
        sa.Column('col19', sa.String(length=255), nullable=True),
        sa.Column('col20', sa.String(length=255), nullable=True),
        sa.Column('col21', sa.String(length=255), nullable=True),
        sa.Column('col22', sa.String(length=255), nullable=True),
        sa.Column('col23', sa.String(length=255), nullable=True),
        sa.Column('imported_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_bras_raw')
    )
    op.create_index('idx_bras_raw_arquivo', 'bras_raw', ['arquivo'], unique=False)
    op.create_index('idx_bras_raw_col17', 'bras_raw', ['col17'], unique=False)
    op.create_index('idx_bras_raw_col03', 'bras_raw', ['col03'], unique=False)
    op.create_index('idx_bras_raw_col06', 'bras_raw', ['col06'], unique=False)

    op.create_table(
        'bras_fixed_stage',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('arquivo', sa.String(length=255), nullable=False),
        sa.Column('linha_num', sa.Integer(), nullable=False),
        sa.Column('linha', sa.Text(), nullable=False),
        sa.Column('imported_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_bras_fixed_stage')
    )
    op.create_index('idx_bras_fixed_arquivo', 'bras_fixed_stage', ['arquivo'], unique=False)

    op.create_table(
        'bras_item_n',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('arquivo', sa.String(length=255), nullable=False),
        sa.Column('linha_num', sa.Integer(), nullable=False),
        sa.Column('laboratorio_codigo', sa.String(length=50), nullable=True),
        sa.Column('laboratorio_nome', sa.String(length=255), nullable=True),
        sa.Column('produto_codigo', sa.String(length=50), nullable=True),
        sa.Column('produto_nome', sa.String(length=255), nullable=True),
        sa.Column('apresentacao_codigo', sa.String(length=50), nullable=True),
        sa.Column('apresentacao_descricao', sa.String(length=255), nullable=True),
        sa.Column('ean', sa.String(length=20), nullable=True),
        sa.Column('registro_anvisa', sa.String(length=50), nullable=True),
        sa.Column('edicao', sa.String(length=50), nullable=True),
        sa.Column('preco_pmc_pacote', sa.Numeric(15, 4), nullable=True),
        sa.Column('preco_pfb_pacote', sa.Numeric(15, 4), nullable=True),
        sa.Column('preco_pmc_unit', sa.Numeric(15, 4), nullable=True),
        sa.Column('preco_pfb_unit', sa.Numeric(15, 4), nullable=True),
        sa.Column('aliquota_ou_ipi', sa.Numeric(15, 4), nullable=True),
        sa.Column('quantidade_embalagem', sa.Integer(), nullable=True),
        sa.Column('imported_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_bras_item_n')
    )
    op.create_index('idx_bras_item_n_ean', 'bras_item_n', ['ean'], unique=False)
    op.create_index('idx_bras_item_n_prod', 'bras_item_n', ['produto_codigo'], unique=False)
    op.create_index('idx_bras_item_n_desc', 'bras_item_n', ['produto_nome', 'apresentacao_descricao'], unique=False)
    op.create_index('idx_bras_item_n_anvisa', 'bras_item_n', ['registro_anvisa'], unique=False)
    op.create_index('idx_bras_item_n_edicao', 'bras_item_n', ['edicao'], unique=False)

    op.execute('DROP VIEW IF EXISTS bras_item_v')
    op.execute(
        """
        CREATE VIEW bras_item_v AS
        SELECT
            r.id,
            r.arquivo,
            r.linha_num,
            r.col01 AS laboratorio_codigo,
            r.col02 AS laboratorio_nome,
            r.col03 AS produto_codigo,
            r.col04 AS produto_nome,
            r.col05 AS apresentacao_codigo,
            r.col06 AS apresentacao_descricao,
            r.col17 AS ean,
            r.col14 AS registro_anvisa,
            r.col19 AS edicao,
            CAST(NULLIF(REPLACE(r.col21, ',', '.'), '') AS DECIMAL(15,4)) AS preco_pmc_pacote,
            CAST(NULLIF(REPLACE(r.col22, ',', '.'), '') AS DECIMAL(15,4)) AS preco_pfb_pacote,
            CAST(NULLIF(REPLACE(r.col23, ',', '.'), '') AS DECIMAL(15,4)) AS preco_pmc_unit,
            CAST(NULLIF(REPLACE(r.col22, ',', '.'), '') AS DECIMAL(15,4)) AS preco_pfb_unit,
            CAST(NULLIF(REPLACE(r.col20, ',', '.'), '') AS DECIMAL(15,4)) AS aliquota_ou_ipi,
            CAST(NULLIF(r.col05, '') AS UNSIGNED) AS quantidade_embalagem,
            r.imported_at
        FROM bras_raw r
        """
    )

    op.execute("DELETE FROM insumos_index WHERE origem = 'BRAS'")


def downgrade() -> None:
    op.execute('DROP VIEW IF EXISTS bras_item_v')
    op.drop_index('idx_bras_item_n_edicao', table_name='bras_item_n')
    op.drop_index('idx_bras_item_n_anvisa', table_name='bras_item_n')
    op.drop_index('idx_bras_item_n_desc', table_name='bras_item_n')
    op.drop_index('idx_bras_item_n_prod', table_name='bras_item_n')
    op.drop_index('idx_bras_item_n_ean', table_name='bras_item_n')
    op.drop_table('bras_item_n')

    op.drop_index('idx_bras_fixed_arquivo', table_name='bras_fixed_stage')
    op.drop_table('bras_fixed_stage')

    op.drop_index('idx_bras_raw_col06', table_name='bras_raw')
    op.drop_index('idx_bras_raw_col03', table_name='bras_raw')
    op.drop_index('idx_bras_raw_col17', table_name='bras_raw')
    op.drop_index('idx_bras_raw_arquivo', table_name='bras_raw')
    op.drop_table('bras_raw')

    op.create_table(
        'bras_item',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tuss', sa.String(length=50), nullable=True),
        sa.Column('tiss', sa.String(length=50), nullable=True),
        sa.Column('anvisa', sa.String(length=50), nullable=True),
        sa.Column('descricao', sa.String(length=500), nullable=False),
        sa.Column('preco', sa.Numeric(12, 4), nullable=True),
        sa.Column('aliquota', sa.Numeric(12, 4), nullable=True),
        sa.Column('fabricante', sa.String(length=255), nullable=True),
        sa.Column('versao_tabela', sa.String(length=100), nullable=True),
        sa.Column('data_atualizacao', sa.Date(), nullable=True),
        sa.Column('uf_referencia', sa.String(length=5), nullable=True),
        sa.Column('tipo_preco', sa.String(length=50), nullable=True),
        sa.Column('ean', sa.String(length=64), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_bras_item')
    )
    op.create_index('ix_bras_item_tuss', 'bras_item', ['tuss'], unique=False)
    op.create_index('ix_bras_item_tiss', 'bras_item', ['tiss'], unique=False)
    op.create_index('ix_bras_item_anvisa', 'bras_item', ['anvisa'], unique=False)
    op.create_index('ix_bras_item_descricao', 'bras_item', ['descricao'], unique=False)

    op.execute(
        """
        CREATE TRIGGER trg_bras_item_ai
        AFTER INSERT ON bras_item
        FOR EACH ROW
        BEGIN
            INSERT INTO insumos_index (origem, item_id, tuss, tiss, descricao, preco, aliquota, fabricante, anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at)
            VALUES ('BRAS', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco, NEW.aliquota, NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao, NEW.uf_referencia, NOW())
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
            INSERT INTO insumos_index (origem, item_id, tuss, tiss, descricao, preco, aliquota, fabricante, anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at)
            VALUES ('BRAS', NEW.id, NEW.tuss, NEW.tiss, NEW.descricao, NEW.preco, NEW.aliquota, NEW.fabricante, NEW.anvisa, NEW.versao_tabela, NEW.data_atualizacao, NEW.uf_referencia, NOW())
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
