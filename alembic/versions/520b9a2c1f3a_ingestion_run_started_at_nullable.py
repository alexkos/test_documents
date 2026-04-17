"""ingestion_run started_at nullable for queued runs

Revision ID: 520b9a2c1f3a
Revises: 410451835799
Create Date: 2026-04-17

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "520b9a2c1f3a"
down_revision: Union[str, Sequence[str], None] = "410451835799"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "ingestion_runs",
        "started_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE ingestion_runs SET started_at = CURRENT_TIMESTAMP "
            "WHERE started_at IS NULL"
        )
    )
    op.alter_column(
        "ingestion_runs",
        "started_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )
