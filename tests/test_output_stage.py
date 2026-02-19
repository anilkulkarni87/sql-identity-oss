"""
Tests for idr_core.stages.output.OutputStage.
"""

import logging
import os
import sys
from unittest.mock import MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.adapters.base import get_dialect_config
from idr_core.runner import RunConfig
from idr_core.stages.base import StageContext
from idr_core.stages.output import OutputStage


def make_output_stage(dialect: str = "snowflake") -> tuple[OutputStage, MagicMock]:
    adapter = MagicMock()
    adapter.dialect = dialect
    # No survivorship rules -> profile builder exits quickly.
    adapter.query.return_value = []
    ctx = StageContext(
        adapter=adapter,
        dialect=get_dialect_config(dialect),
        run_id="run_test_output",
        logger=logging.getLogger("test-output"),
        warnings=[],
        generate_evidence=False,
    )
    stage = OutputStage(ctx, config=RunConfig(run_mode="INCR"))
    return stage, adapter


def test_non_duckdb_edge_merge_has_single_not_matched_clause():
    stage, adapter = make_output_stage("snowflake")
    stage.generate_output()

    edge_merge_sql = [
        call.args[0]
        for call in adapter.execute.call_args_list
        if "MERGE INTO idr_out.identity_edges_current" in call.args[0]
    ]

    assert len(edge_merge_sql) == 1
    assert edge_merge_sql[0].upper().count("WHEN NOT MATCHED THEN") == 1
