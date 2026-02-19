"""
Schema documentation router — system data model definitions.
"""

from typing import List

from fastapi import APIRouter, Depends

from ..dependencies import get_current_user
from ..models import SchemaColumn, SchemaTable

router = APIRouter(tags=["schema"], dependencies=[Depends(get_current_user)])


@router.get("/api/schema", response_model=List[SchemaTable])
def get_schema_definitions():
    """Get the system data model definitions."""
    from idr_core.schema_defs import SYSTEM_TABLES

    tables = []
    for t in SYSTEM_TABLES:
        cols = [
            SchemaColumn(
                name=c.name,
                type=c.type.value,  # Use enum value
                is_pk=c.is_pk,
                description=c.description,
            )
            for c in t.columns
        ]

        tables.append(
            SchemaTable(
                schema_name=t.schema,
                table_name=t.name,
                fqn=t.fqn,
                description=t.description,
                columns=cols,
            )
        )

    return tables
