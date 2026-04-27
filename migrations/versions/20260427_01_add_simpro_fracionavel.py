"""Add fracionavel field to SIMPRO tables.

Revision ID: 20260427_01_add_simpro_fracionavel
Revises: 20241024_02
Create Date: 2026-04-27 10:40:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "20260427_01_add_simpro_fracionavel"
down_revision: Union[str, None] = "20241024_02"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        columns = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(col.get("name") == column_name for col in columns)


def upgrade() -> None:
    if not _has_column("simpro_item_norm", "fracionavel"):
        op.add_column("simpro_item_norm", sa.Column("fracionavel", sa.String(length=1), nullable=True))

    if not _has_column("mv_catalogo_vigente_simpro", "fracionavel"):
        op.add_column("mv_catalogo_vigente_simpro", sa.Column("fracionavel", sa.String(length=1), nullable=True))


def downgrade() -> None:
    if _has_column("mv_catalogo_vigente_simpro", "fracionavel"):
        op.drop_column("mv_catalogo_vigente_simpro", "fracionavel")

    if _has_column("simpro_item_norm", "fracionavel"):
        op.drop_column("simpro_item_norm", "fracionavel")
