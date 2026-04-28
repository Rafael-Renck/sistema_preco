"""include aliquota in insumos_index primary key (multi-aliquota per item_id)

SIMPRO split import gravava apenas uma linha por cadastro em `insumos_index` porque a PK
era (origem, item_id). Inclui `aliquota` na PK e ajusta triggers legacy (BRAS/SIMPRO)
para substituir a linha do item em vez de acumular linhas ao mudar a alíquota.

Revision ID: 20260428_02_insumos_index_pk_aliquota
Revises: 20260428_01_add_simpro_split_tables
Create Date: 2026-04-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


revision: str = "20260428_02_pk_aliquota"
down_revision: Union[str, None] = "20260428_01_add_simpro_split_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _dedupe_insumos_index_for_new_pk() -> None:
    bind = op.get_bind()
    bind.execute(text("UPDATE insumos_index SET aliquota = 0 WHERE aliquota IS NULL"))

    # Remove duplicatas (origem, item_id, aliquota) iterando até estabilizar.
    delete_sql = text(
        """
        DELETE t1 FROM insumos_index t1
        INNER JOIN insumos_index t2
          ON t1.origem = t2.origem
         AND t1.item_id = t2.item_id
         AND t1.aliquota = t2.aliquota
        WHERE
          (t2.updated_at > t1.updated_at)
          OR (
            t2.updated_at = t1.updated_at AND (
              COALESCE(t2.tuss, '') > COALESCE(t1.tuss, '')
              OR (
                COALESCE(t2.tuss, '') = COALESCE(t1.tuss, '')
                AND COALESCE(t2.tiss, '') > COALESCE(t1.tiss, '')
              )
              OR (
                COALESCE(t2.tuss, '') = COALESCE(t1.tuss, '')
                AND COALESCE(t2.tiss, '') = COALESCE(t1.tiss, '')
                AND COALESCE(t2.descricao, '') > COALESCE(t1.descricao, '')
              )
            )
          )
        """
    )
    for _ in range(128):
        r = bind.execute(delete_sql)
        if r.rowcount == 0:
            break


_TRIGGERS_BRA_AI = """
CREATE TRIGGER trg_bras_item_ai
AFTER INSERT ON bras_item
FOR EACH ROW
BEGIN
    DELETE FROM insumos_index WHERE origem = 'BRAS' AND item_id = NEW.id;
    INSERT INTO insumos_index (
        origem, item_id, tuss, tiss, descricao, preco, aliquota,
        fabricante, anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
    )
    VALUES (
        'BRAS',
        NEW.id,
        NEW.tuss,
        NEW.tiss,
        NEW.descricao,
        NEW.preco,
        COALESCE(NEW.aliquota, 0),
        NEW.fabricante,
        NEW.anvisa,
        NEW.versao_tabela,
        NEW.data_atualizacao,
        NEW.uf_referencia,
        NOW()
    );
END
"""

_TRIGGERS_BRA_AU = """
CREATE TRIGGER trg_bras_item_au
AFTER UPDATE ON bras_item
FOR EACH ROW
BEGIN
    DELETE FROM insumos_index WHERE origem = 'BRAS' AND item_id = NEW.id;
    INSERT INTO insumos_index (
        origem, item_id, tuss, tiss, descricao, preco, aliquota,
        fabricante, anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
    )
    VALUES (
        'BRAS',
        NEW.id,
        NEW.tuss,
        NEW.tiss,
        NEW.descricao,
        NEW.preco,
        COALESCE(NEW.aliquota, 0),
        NEW.fabricante,
        NEW.anvisa,
        NEW.versao_tabela,
        NEW.data_atualizacao,
        NEW.uf_referencia,
        NOW()
    );
END
"""

_TRIGGERS_SIM_AI = """
CREATE TRIGGER trg_simpro_item_ai
AFTER INSERT ON simpro_item
FOR EACH ROW
BEGIN
    DELETE FROM insumos_index WHERE origem = 'SIMPRO' AND item_id = NEW.id;
    INSERT INTO insumos_index (
        origem, item_id, tuss, tiss, descricao, preco, aliquota,
        fabricante, anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
    )
    VALUES (
        'SIMPRO',
        NEW.id,
        NEW.tuss,
        NEW.tiss,
        NEW.descricao,
        NEW.preco,
        COALESCE(NEW.aliquota, 0),
        NEW.fabricante,
        NEW.anvisa,
        NEW.versao_tabela,
        NEW.data_atualizacao,
        NEW.uf_referencia,
        NOW()
    );
END
"""

_TRIGGERS_SIM_AU = """
CREATE TRIGGER trg_simpro_item_au
AFTER UPDATE ON simpro_item
FOR EACH ROW
BEGIN
    DELETE FROM insumos_index WHERE origem = 'SIMPRO' AND item_id = NEW.id;
    INSERT INTO insumos_index (
        origem, item_id, tuss, tiss, descricao, preco, aliquota,
        fabricante, anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at
    )
    VALUES (
        'SIMPRO',
        NEW.id,
        NEW.tuss,
        NEW.tiss,
        NEW.descricao,
        NEW.preco,
        COALESCE(NEW.aliquota, 0),
        NEW.fabricante,
        NEW.anvisa,
        NEW.versao_tabela,
        NEW.data_atualizacao,
        NEW.uf_referencia,
        NOW()
    );
END
"""


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tbls = set(insp.get_table_names() or [])

    op.execute("DROP TRIGGER IF EXISTS trg_simpro_item_au")
    op.execute("DROP TRIGGER IF EXISTS trg_simpro_item_ai")
    op.execute("DROP TRIGGER IF EXISTS trg_bras_item_au")
    op.execute("DROP TRIGGER IF EXISTS trg_bras_item_ai")

    _dedupe_insumos_index_for_new_pk()

    pk_cons = insp.get_pk_constraint("insumos_index") or {}
    pk_cols = list(pk_cons.get("constrained_columns") or [])
    if set(pk_cols) != {"origem", "item_id", "aliquota"}:
        op.drop_constraint("pk_insumos_index", "insumos_index", type_="primary")
        op.create_primary_key(
            "pk_insumos_index",
            "insumos_index",
            ["origem", "item_id", "aliquota"],
        )

    aliquota_nullable = True
    for col in insp.get_columns("insumos_index"):
        if col.get("name") == "aliquota":
            aliquota_nullable = bool(col.get("nullable", True))
            break
    if aliquota_nullable:
        op.alter_column(
            "insumos_index",
            "aliquota",
            existing_type=sa.Numeric(12, 4),
            nullable=False,
            server_default=sa.text("0"),
            existing_nullable=True,
            existing_server_default=None,
        )

    if "bras_item" in tbls:
        op.execute(_TRIGGERS_BRA_AI)
        op.execute(_TRIGGERS_BRA_AU)
    if "simpro_item" in tbls:
        op.execute(_TRIGGERS_SIM_AI)
        op.execute(_TRIGGERS_SIM_AU)


def downgrade() -> None:
    raise NotImplementedError(
        "Reverter PK de insumos_index exigiria colapsar múltiplas alíquotas por item; não suportado."
    )
