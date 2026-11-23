"""
Command-line interface for IDR.

Provides unified CLI commands for running identity resolution
across all supported platforms.
"""

import argparse
import os
import sys
from typing import TYPE_CHECKING, Any, Dict, Optional

from idr_core.logger_utils import configure_logging

if TYPE_CHECKING:
    from idr_core.llm import LLMClient


def main(args: Optional[list] = None) -> int:
    """Main CLI entry point."""
    # Configure logging (defaults to text, JSON if IDR_JSON_LOGS=1)
    configure_logging()

    # print(f"DEBUG: CLI main entered with args: {args or sys.argv}", file=sys.stderr)
    parser = argparse.ArgumentParser(
        prog="idr",
        description="SQL Identity Resolution - Unified CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with DuckDB
    idr run --platform=duckdb --db=my_database.duckdb --mode=FULL

    # Dry run with BigQuery
    idr run --platform=bigquery --project=my-project --mode=INCR --dry-run

    # Validate configuration
    idr config validate --file=config.yaml

    # Generate SQL from YAML config
    idr config generate --file=config.yaml
""",
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # === idr version ===
    subparsers.add_parser("version", help="Show version info")

    # === idr run ===
    run_parser = subparsers.add_parser("run", help="Run identity resolution")
    run_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=False,
        help="Target platform (auto-detected if omitted)",
    )
    run_parser.add_argument("--db", help="DuckDB database path")
    run_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    run_parser.add_argument("--location", default="US", help="BigQuery location")
    run_parser.add_argument(
        "--dataset", default="idr_out", help="BigQuery output dataset (default: idr_out)"
    )
    run_parser.add_argument(
        "--meta-dataset", default="idr_meta", help="BigQuery metadata dataset (default: idr_meta)"
    )
    run_parser.add_argument(
        "--work-dataset", default="idr_work", help="BigQuery work dataset (default: idr_work)"
    )
    run_parser.add_argument("--config", help="YAML configuration file")
    run_parser.add_argument(
        "--mode",
        "-m",
        choices=["FULL", "INCR", "INCREMENTAL"],
        default="FULL",
        help="Run mode",
    )
    run_parser.add_argument(
        "--max-iters", type=int, default=30, help="Maximum label propagation iterations"
    )
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Preview changes without committing"
    )
    run_parser.add_argument("--evaluation-config", help="JSON label mapping and acceptance configuration for a dry run")
    run_parser.add_argument("--ground-truth-table", help="Optional local warehouse entity_key/truth_id labels for dry-run evaluation")
    run_parser.add_argument(
        "--critique",
        action="store_true",
        help="Run zero-PII AI Graph Critique after a --dry-run to diagnose mega-clusters, singletons, and unused rules",
    )
    run_parser.add_argument(
        "--strict",
        action="store_true",
        help="Deterministic mode: disable fuzzy matching for exact-match-only resolution",
    )
    run_parser.add_argument(
        "--max-group-size",
        type=int,
        default=None,
        help="Global maximum identifier group size safeguard (default: 10000)",
    )
    run_parser.add_argument(
        "--max-moved-pct",
        type=float,
        default=None,
        help="Blast-radius circuit breaker: abort live Stage 8 commit if moved entity percentage exceeds threshold (0-100]",
    )
    run_parser.add_argument(
        "--max-cluster-delta-pct",
        type=float,
        default=None,
        help="Blast-radius circuit breaker: abort live Stage 8 commit if absolute cluster count delta percentage exceeds threshold (0-100]",
    )
    run_parser.add_argument(
        "--lens",
        default=None,
        help="Optional named lens ID from idr_meta.lens_definition to materialize (or 'none' to skip lenses)",
    )
    run_parser.add_argument(
        "--skip-lenses",
        action="store_true",
        default=False,
        help="Run only the primary pipeline and skip all configured lenses",
    )

    # === idr config ===
    config_parser = subparsers.add_parser("config", help="Configuration management")
    config_subparsers = config_parser.add_subparsers(dest="config_cmd")

    validate_parser = config_subparsers.add_parser("validate", help="Validate config file")
    validate_parser.add_argument("--file", "-f", required=True, help="Config file path")

    generate_parser = config_subparsers.add_parser("generate", help="Generate SQL from config")
    generate_parser.add_argument("--file", "-f", required=True, help="Config file path")
    generate_parser.add_argument(
        "--dialect",
        default="duckdb",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        help="Target SQL dialect",
    )

    export_dbt_parser = config_subparsers.add_parser(
        "export-dbt", help="Export YAML or live metadata config to dbt_idr seed CSVs"
    )
    export_dbt_parser.add_argument("--file", "-f", help="YAML config file path")
    export_dbt_parser.add_argument(
        "--output-dir", "-o", default="dbt_idr/seeds", help="Target directory for dbt seed CSVs"
    )
    export_dbt_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        help="Export from live warehouse metadata instead of YAML file",
    )
    export_dbt_parser.add_argument("--db", help="DuckDB database path")
    export_dbt_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    export_dbt_parser.add_argument("--location", default="US", help="BigQuery location")
    export_dbt_parser.add_argument(
        "--meta-dataset", default="idr_meta", help="BigQuery metadata dataset"
    )
    export_dbt_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")

    # === idr dbt ===
    dbt_parser = subparsers.add_parser("dbt", help="dbt_idr package utilities")
    dbt_subparsers = dbt_parser.add_subparsers(dest="dbt_cmd")
    dbt_export = dbt_subparsers.add_parser(
        "export", help="Export YAML or live metadata config to dbt_idr seed CSVs"
    )
    dbt_export.add_argument("--file", "-f", help="YAML config file path")
    dbt_export.add_argument(
        "--output-dir", "-o", default="dbt_idr/seeds", help="Target directory for dbt seed CSVs"
    )
    dbt_export.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        help="Export from live warehouse metadata instead of YAML file",
    )
    dbt_export.add_argument("--db", help="DuckDB database path")
    dbt_export.add_argument("--project", help="GCP project ID (BigQuery)")
    dbt_export.add_argument("--location", default="US", help="BigQuery location")
    dbt_export.add_argument(
        "--meta-dataset", default="idr_meta", help="BigQuery metadata dataset"
    )
    dbt_export.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")

    # === idr init ===
    init_parser = subparsers.add_parser("init", help="Initialize metadata tables")
    init_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=False,
        help="Target platform (auto-detected if omitted)",
    )
    init_parser.add_argument("--db", help="DuckDB database path")
    init_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    init_parser.add_argument("--location", default="US", help="BigQuery location")
    init_parser.add_argument("--dataset", default="idr_out", help="BigQuery output dataset")
    init_parser.add_argument("--meta-dataset", default="idr_meta", help="BigQuery metadata dataset")
    init_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")
    init_parser.add_argument("--reset", action="store_true", help="Drop and recreate tables")
    init_parser.add_argument(
        "--no-default-exclusions",
        action="store_true",
        help="Skip seeding the default toxic identifier exclusion pack",
    )

    # === idr privacy ===
    privacy_parser = subparsers.add_parser(
        "privacy", help="GDPR / CCPA Right-to-be-Forgotten (RTBF) graph operations"
    )
    privacy_subparsers = privacy_parser.add_subparsers(dest="privacy_cmd")
    forget_parser = privacy_subparsers.add_parser(
        "forget",
        help="Cascading deletion of an identity across all IDR output & work tables with SHA-256 audit log",
    )
    forget_parser.add_argument("--identifier", help="Identifier value (e.g. email or phone) to forget")
    forget_parser.add_argument("--entity-key", help="Specific entity_key (e.g. crm:101) to forget")
    forget_parser.add_argument("--resolved-id", help="Specific cluster resolved_id to forget")
    forget_parser.add_argument(
        "--dry-run", action="store_true", help="Preview impacted entities and tables without deleting"
    )
    forget_parser.add_argument(
        "--reason", default="GDPR_CCPA_RTBF", help="Audit reason (default: GDPR_CCPA_RTBF)"
    )
    forget_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=False,
        help="Target platform (auto-detected if omitted)",
    )
    forget_parser.add_argument("--db", help="DuckDB database path")
    forget_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    forget_parser.add_argument("--location", default="US", help="BigQuery location")
    forget_parser.add_argument("--dataset", default="idr_out", help="BigQuery output dataset")
    forget_parser.add_argument("--meta-dataset", default="idr_meta", help="BigQuery metadata dataset")
    forget_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")

    def _add_connection_args(sub_p, include_dataset: bool = True) -> None:
        sub_p.add_argument(
            "--platform",
            "-p",
            choices=["duckdb", "bigquery", "snowflake", "databricks"],
            required=False,
            help="Target platform (auto-detected if omitted)",
        )
        sub_p.add_argument("--db", help="DuckDB database path")
        sub_p.add_argument("--project", help="GCP project ID (BigQuery)")
        sub_p.add_argument("--location", default="US", help="BigQuery location")
        if include_dataset:
            sub_p.add_argument("--dataset", default="idr_out", help="BigQuery output dataset")
        sub_p.add_argument(
            "--meta-dataset", default="idr_meta", help="BigQuery metadata dataset"
        )
        sub_p.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")

    apply_parser = config_subparsers.add_parser("apply", help="Apply config to metadata tables")
    apply_parser.add_argument("--file", "-f", required=True, help="Config file path")
    _add_connection_args(apply_parser, include_dataset=True)

    history_parser = config_subparsers.add_parser(
        "history", help="List saved configuration snapshots from idr_out.config_snapshot"
    )
    history_parser.add_argument("--limit", type=int, default=15, help="Max snapshots to show")
    _add_connection_args(history_parser, include_dataset=True)

    rollback_parser = config_subparsers.add_parser(
        "rollback", help="Roll back idr_meta to a previously saved configuration snapshot"
    )
    rollback_parser.add_argument(
        "--hash",
        dest="config_hash",
        default=None,
        help="Snapshot config_hash (or unique prefix) to restore; defaults to the previous saved version",
    )
    _add_connection_args(rollback_parser, include_dataset=True)

    # === idr serve ===
    serve_parser = subparsers.add_parser("serve", help="Start API & UI server")
    serve_parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    serve_parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    serve_parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    serve_parser.add_argument(
        "--open", action="store_true", help="Open the UI in the default browser"
    )
    serve_parser.add_argument(
        "--quiet", action="store_true", help="Hide per-request HTTP access logs"
    )

    # === idr ui ===
    ui_parser = subparsers.add_parser(
        "ui", help="Launch the interactive IDR Studio UI & API server"
    )
    ui_parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    ui_parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    ui_parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    ui_parser.add_argument(
        "--no-browser", action="store_true", help="Do not automatically open the browser"
    )
    ui_parser.add_argument(
        "--quiet", action="store_true", help="Hide per-request HTTP access logs"
    )

    # === idr mcp ===
    mcp_parser = subparsers.add_parser("mcp", help="Start MCP Server")
    mcp_parser.add_argument(
        "--transport", default="stdio", choices=["stdio", "sse"], help="Transport mode"
    )

    # === idr quickstart ===
    qs_parser = subparsers.add_parser(
        "quickstart",
        help="Generate demo data, run IDR, and see results — zero config needed",
    )
    qs_parser.add_argument(
        "--rows", type=int, default=10000, help="Number of demo records (default: 10000)"
    )
    qs_parser.add_argument(
        "--output", "-o", default="quickstart_demo.duckdb", help="Output DuckDB path"
    )
    qs_parser.add_argument("--seed", type=int, default=42, help="Random seed")

    # === idr demo ===
    demo_parser = subparsers.add_parser(
        "demo",
        help="Generate demo data, run IDR, and launch the Explorer UI + /mcp in one command",
    )
    demo_parser.add_argument(
        "--rows", type=int, default=10000, help="Number of demo records (default: 10000)"
    )
    demo_parser.add_argument(
        "--output", "-o", default="quickstart_demo.duckdb", help="Output DuckDB path"
    )
    demo_parser.add_argument("--seed", type=int, default=42, help="Random seed")
    demo_parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    demo_parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    demo_parser.add_argument(
        "--no-browser", action="store_true", help="Do not automatically open the browser"
    )
    demo_parser.add_argument(
        "--quiet", action="store_true", help="Hide per-request HTTP access logs"
    )

    # === idr bootstrap ===
    bootstrap_parser = subparsers.add_parser(
        "bootstrap",
        help="Auto-profile warehouse tables and synthesize production YAML configuration",
    )
    bootstrap_parser.add_argument(
        "--auto", action="store_true", default=True, help="Automatically profile and infer all rules (default: true)"
    )
    bootstrap_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=False,
        help="Target platform (auto-detected if omitted)",
    )
    bootstrap_parser.add_argument("--db", help="DuckDB database path")
    bootstrap_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    bootstrap_parser.add_argument("--location", default="US", help="BigQuery location")
    bootstrap_parser.add_argument(
        "--schema", "--dataset", dest="schema", help="Source schema or dataset to profile (e.g. ecommerce_synth)"
    )
    bootstrap_parser.add_argument("--tables", help="Comma-separated list of candidate tables to profile")
    bootstrap_parser.add_argument(
        "--output", "-o", default="idr_config.yaml", help="Path to write the generated YAML manifest"
    )
    bootstrap_parser.add_argument(
        "--sample-size", type=int, default=500, help="Number of rows to sample per table (default: 500)"
    )
    bootstrap_parser.add_argument(
        "--apply", action="store_true", help="Immediately apply the generated configuration to idr_meta.* tables"
    )

    # === idr doctor ===
    doctor_parser = subparsers.add_parser(
        "doctor", help="Run production readiness diagnostic and health checks"
    )
    doctor_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=False,
        help="Target platform (auto-detected if omitted)",
    )
    doctor_parser.add_argument("--db", help="DuckDB database path")
    doctor_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    doctor_parser.add_argument("--location", default="US", help="BigQuery location")
    doctor_parser.add_argument("--dataset", default="idr_out", help="BigQuery output dataset")
    doctor_parser.add_argument("--meta-dataset", default="idr_meta", help="BigQuery metadata dataset")
    doctor_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")
    doctor_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed diagnostic output")

    # === idr evaluate ===
    eval_parser = subparsers.add_parser(
        "evaluate", help="Evaluate entity resolution Precision/Recall or generate stratified audit samples"
    )
    eval_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=False,
        help="Target platform (auto-detected if omitted)",
    )
    eval_parser.add_argument("--db", help="DuckDB database path")
    eval_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    eval_parser.add_argument("--location", default="US", help="BigQuery location")
    eval_parser.add_argument("--dataset", default="idr_out", help="BigQuery output dataset")
    eval_parser.add_argument("--meta-dataset", default="idr_meta", help="BigQuery metadata dataset")
    eval_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")
    eval_parser.add_argument(
        "--ground-truth-table", help="FQN of ground-truth table for Pairwise Precision/Recall/F1"
    )
    eval_parser.add_argument(
        "--entity-key-col", default="entity_key", help="Entity key column in ground truth table"
    )
    eval_parser.add_argument(
        "--truth-id-col", default="truth_id", help="Ground truth cluster/person ID column"
    )
    eval_parser.add_argument(
        "--sample-size", type=int, default=20, help="Number of pairs to extract per audit stratum"
    )
    eval_parser.add_argument(
        "--no-persist", action="store_true", help="Do not write sampled pairs to idr_out.evaluation_sample"
    )

    eval_parser.add_argument("--evaluation-config", help="JSON label mapping and acceptance criteria; evaluates existing outputs without rebuilding the graph")

    # === idr plugins ===
    plugins_parser = subparsers.add_parser("plugins", help="Inspect registered plugins and extensions")
    plugins_subparsers = plugins_parser.add_subparsers(dest="plugins_cmd", help="Plugin commands")
    plugins_subparsers.add_parser("list", help="List registered canonicalizers, survivorship strategies, and adapters")

    # === idr ai ===
    ai_parser = subparsers.add_parser("ai", help="Local Ollama & BYOK AI assistant commands")
    ai_subparsers = ai_parser.add_subparsers(dest="ai_cmd", help="AI commands")

    for sub_name, sub_help in [
        ("test", "Test connection to configured AI model (defaults to local Ollama)"),
        ("models", "List locally available Ollama models"),
        ("ask", "Ask a natural-language question about the identity graph"),
        ("generate-config", "Profile warehouse tables (zero-PII) and generate a validated IDR config with AI reasoning"),
        ("review", "Read a saved run-bound aggregate review"),
        ("profile-preview", "Validate profiles without scanning or explicitly preview bounded complete clusters; no AI call"),
        ("compare", "Compare one rule or lens change on frozen preview inputs"),
        ("purge-reviews", "Purge this CLI owner's saved reviews and retained inputs"),
    ]:
        p = ai_subparsers.add_parser(sub_name, help=sub_help)
        p.add_argument("--provider", help="ollama | openai | anthropic | gemini | openai_compatible")
        p.add_argument("--model", help="Model name (e.g. llama3.2, qwen2.5:14b, gpt-4o-mini)")
        p.add_argument("--base-url", help="Custom base URL (e.g. http://localhost:11434/v1)")
        p.add_argument("--api-key", help="API key (or set IDR_LLM_API_KEY)")
        mask_grp = p.add_mutually_exclusive_group()
        mask_grp.add_argument("--mask-pii", action="store_true", help="Force PII masking before sending")
        mask_grp.add_argument("--no-mask-pii", action="store_true", help="Disable PII masking (local only)")
        if sub_name in ("review", "compare", "purge-reviews", "profile-preview"):
            _add_connection_args(p)
            if sub_name != "purge-reviews":
                p.add_argument("--run-id", required=sub_name != "profile-preview")
            if sub_name == "profile-preview":
                p.add_argument("--config", required=True)
                p.add_argument("--preview-mode", choices=["validate","targeted","full"], default="validate")
                p.add_argument("--attributes", default="", help="Comma-separated profile fields")
                p.add_argument("--grouped-outputs", default="", help="Comma-separated grouped output IDs")
                p.add_argument("--max-clusters", type=int, default=25)
                p.add_argument("--max-members", type=int, default=10000)
                p.add_argument("--timeout-seconds", type=int, default=15)
                p.add_argument("--max-bytes-billed", type=int, default=None, help="Optional BigQuery scan cost cap in bytes (min 1048576)")
                p.add_argument("--allow-source-scan", action="store_true", help="Explicitly accept unknown source scan cost")
                p.add_argument("--allow-full-population", action="store_true")
                p.add_argument("--lens", default="default")
            if sub_name == "compare":
                p.add_argument("--config", required=True, help="Current draft YAML")
                p.add_argument("--action", required=True, choices=["remove_rule", "set_group_limit", "raise_fuzzy_threshold", "set_lens_types"])
                p.add_argument("--target", required=True, help="Exact rule or lens ID")
                p.add_argument("--value", default="null", help="JSON value, e.g. 10, 0.95, or a list of identifier types")
                p.add_argument("--lens", default="default")
        elif sub_name == "ask":
            p.add_argument("question", help="Natural-language question")
            p.add_argument("--platform", "-p", choices=["duckdb", "bigquery", "snowflake", "databricks"])
            p.add_argument("--db", help="DuckDB database path")
            p.add_argument("--project", help="GCP project ID (BigQuery)")
        elif sub_name == "generate-config":
            _add_connection_args(p)
            p.add_argument("--current-config", help="Current draft YAML for rules-only mode or locked decisions")
            p.add_argument("--rules-only", action="store_true")
            p.add_argument("--allow-match", help="Comma-separated approved identifier types")
            p.add_argument("--allow-fuzzy", action="store_true", default=None)
            p.add_argument(
                "--tables",
                help="Comma-separated table FQNs to include (if omitted, auto-discovers non-system tables)",
            )
            p.add_argument("--schema", help="Optional schema/dataset filter when auto-discovering tables")
            p.add_argument(
                "--goal",
                default="Unify customer records accurately across sources with safe supernode guardrails and practical golden-profile survivorship.",
                help="Local objective hints: precision, coverage, cross-source, auditability. Other text is not sent to AI.",
            )
            p.add_argument(
                "--grain",
                choices=["individual", "household", "account"],
                default=None,
                help="Primary resolution grain policy (new drafts default to individual)",
            )
            p.add_argument(
                "--event-policy",
                choices=["bridge", "include", "exclude"],
                default=None,
                help="How to treat EVENT_FACT tables (bridge=link only without profile attrs, include=full, exclude=skip)",
            )
            p.add_argument(
                "--system-of-record",
                default="",
                help="Optional table FQN to prioritize #1 for Survivorship tie-breaking",
            )
            p.add_argument(
                "--output",
                "-o",
                default="idr_config.yaml",
                help="Output YAML file path (default: idr_config.yaml)",
            )
            p.add_argument(
                "--reasoning-out",
                help="Optional path to write full JSON reasoning & validation report",
            )
            p.add_argument(
                "--sample-size",
                type=int,
                default=2000,
                help="Sample size per table for statistical profiling (default: 2000)",
            )
            p.add_argument(
                "--no-verify-sql",
                action="store_true",
                help="Skip live warehouse SQL dry-parse verification",
            )

    # === idr explain ===
    explain_parser = subparsers.add_parser("explain", help="Explain why an identity cluster merged using AI")
    explain_parser.add_argument("resolved_id", help="Resolved cluster ID to explain")
    explain_parser.add_argument("--platform", "-p", choices=["duckdb", "bigquery", "snowflake", "databricks"])
    explain_parser.add_argument("--db", help="DuckDB database path")
    explain_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    explain_parser.add_argument("--provider", help="LLM provider (default: IDR_LLM_PROVIDER or ollama)")
    explain_parser.add_argument("--model", help="Model name")
    explain_parser.add_argument("--base-url", help="Custom base URL")
    explain_parser.add_argument("--prompt", help="Optional custom question/prompt about this cluster")

    # === idr triage ===
    triage_parser = subparsers.add_parser("triage", help="3-Tier AI-assisted triage of borderline clusters")
    triage_parser.add_argument("--platform", "-p", choices=["duckdb", "bigquery", "snowflake", "databricks"])
    triage_parser.add_argument("--db", help="DuckDB database path")
    triage_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    triage_parser.add_argument("--max-confidence", type=float, default=0.75, help="Tier-0 SQL max confidence threshold (default: 0.75)")
    triage_parser.add_argument("--min-subject-fanout", type=int, default=2, help="Tier-0 SQL min subject fanout to flag (default: 2)")
    triage_parser.add_argument("--limit", type=int, default=8, help="Max flagged clusters to judge")
    triage_parser.add_argument("--provider", help="Tier-1 LLM provider (ollama, warehouse, openai, anthropic, gemini)")
    triage_parser.add_argument("--model", help="Tier-1 model name")
    triage_parser.add_argument("--base-url", help="Custom base URL")
    triage_parser.add_argument("--escalate-provider", help="Optional Tier-2 escalation LLM provider")
    triage_parser.add_argument("--escalate-model", help="Optional Tier-2 escalation model name")
    triage_parser.add_argument("--escalation-threshold", type=float, default=0.80, help="Escalate Tier-1 verdicts below this confidence (default: 0.80)")
    triage_parser.add_argument("--json", action="store_true", dest="json_output", help="Emit JSON output")

    # === idr lenses ===
    lenses_parser = subparsers.add_parser("lenses", help="Multi-lens identity graph utilities")
    lenses_subparsers = lenses_parser.add_subparsers(dest="lenses_cmd")

    lenses_list_p = lenses_subparsers.add_parser("list", help="List configured identity lenses")
    _add_connection_args(lenses_list_p, include_dataset=True)

    lenses_diff_p = lenses_subparsers.add_parser(
        "diff", help="Compare cluster boundaries between two lenses (or 'default' vs a lens)"
    )
    lenses_diff_p.add_argument("lens_a", help="First lens ID (or 'default')")
    lenses_diff_p.add_argument("lens_b", help="Second lens ID (or 'default')")
    lenses_diff_p.add_argument("--limit", type=int, default=25, help="Max differing clusters to show")
    _add_connection_args(lenses_diff_p, include_dataset=True)

    # Parse arguments
    parsed_args = parser.parse_args(args)

    if not parsed_args.command:
        parser.print_help()
        return 0

    if parsed_args.command == "version":
        from idr_core import __version__

        print(f"sql-identity-resolution v{__version__}")
        return 0

    if parsed_args.command == "config":
        return handle_config(parsed_args)

    if parsed_args.command == "dbt":
        if getattr(parsed_args, "dbt_cmd", None) == "export":
            return handle_dbt_export(parsed_args)
        dbt_parser.print_help()
        return 0

    if parsed_args.command == "init":
        return handle_init(parsed_args)

    if parsed_args.command == "privacy":
        return handle_privacy(parsed_args)

    if parsed_args.command == "run":
        return handle_run(parsed_args)

    if parsed_args.command == "serve":
        return handle_serve(parsed_args)

    if parsed_args.command == "ui":
        return handle_ui(parsed_args)

    if parsed_args.command == "mcp":
        return handle_mcp(parsed_args)

    if parsed_args.command == "quickstart":
        return handle_quickstart(parsed_args)

    if parsed_args.command == "demo":
        return handle_demo(parsed_args)

    if parsed_args.command == "bootstrap":
        return handle_bootstrap(parsed_args)

    if parsed_args.command == "doctor":
        return handle_doctor(parsed_args)

    if parsed_args.command == "evaluate":
        return handle_evaluate(parsed_args)

    if parsed_args.command == "plugins":
        if getattr(parsed_args, "plugins_cmd", None) == "list":
            return handle_plugins_list()
        plugins_parser.print_help()
        return 0

    if parsed_args.command == "ai":
        if not getattr(parsed_args, "ai_cmd", None):
            ai_parser.print_help()
            return 0
        return handle_ai(parsed_args)

    if parsed_args.command == "explain":
        return handle_explain(parsed_args)

    if parsed_args.command == "triage":
        return handle_triage(parsed_args)

    if parsed_args.command == "lenses":
        if not getattr(parsed_args, "lenses_cmd", None):
            lenses_parser.print_help()
            return 0
        return handle_lenses(parsed_args)

    return 0


def _resolve_platform(args) -> str:
    """Resolve target platform from CLI args, environment, or local credential discovery."""
    platform = getattr(args, "platform", None)
    if platform:
        return platform

    if getattr(args, "db", None):
        return "duckdb"
    if getattr(args, "project", None):
        return "bigquery"

    env_platform = os.environ.get("IDR_PLATFORM")
    if env_platform:
        return env_platform.lower()

    if os.environ.get("IDR_DATABASE") or os.environ.get("IDR_DB_PATH"):
        return "duckdb"

    try:
        from idr_core.credential_discovery import discover_all

        discovered_platforms = {c.get("platform") for c in discover_all()}
        for candidate in ("snowflake", "bigquery", "databricks", "duckdb"):
            if candidate in discovered_platforms:
                return candidate
    except Exception:
        pass

    raise ValueError(
        "Could not auto-detect warehouse platform. Pass --platform (-p) or set IDR_PLATFORM."
    )


def get_adapter(args):
    """Helper to create adapter from args."""
    platform = _resolve_platform(args)
    args.platform = platform

    if platform == "duckdb":
        db_path = args.db or os.environ.get("IDR_DATABASE") or os.environ.get("IDR_DB_PATH")
        if not db_path:
            raise ValueError("--db required for DuckDB (or set IDR_DATABASE / IDR_DB_PATH)")
        from idr_core.adapters.duckdb import DuckDBAdapter

        return DuckDBAdapter(db_path)

    elif args.platform == "bigquery":
        if not args.project:
            raise ValueError("--project required for BigQuery")
        from google.cloud import bigquery

        from idr_core.adapters.bigquery import BigQueryAdapter

        client = bigquery.Client(project=args.project)
        dataset_mapping = {
            "idr_out": args.dataset,
            "idr_meta": args.meta_dataset,
            "idr_work": args.work_dataset,
        }
        return BigQueryAdapter(client, args.project, args.location, dataset_mapping=dataset_mapping)

    elif args.platform == "snowflake":
        print("Connecting to Snowflake...")
        try:
            from snowflake.snowpark import Session

            from idr_core.adapters.snowflake import SnowflakeAdapter

            session = Session.builder.getOrCreate()
            return SnowflakeAdapter(session)
        except Exception:
            import snowflake.connector

            from idr_core.adapters.snowflake import SnowflakeAdapter
            from idr_core.connection_manager import build_snowflake_connect_kwargs

            conn = snowflake.connector.connect(**build_snowflake_connect_kwargs({}))
            return SnowflakeAdapter(conn)

    elif args.platform == "databricks":
        try:
            from databricks import sql

            from idr_core.adapters.databricks import DatabricksAdapter
            from idr_core.connection_manager import build_databricks_connect_kwargs

            # Resolve credentials: env vars first (PAT or OAuth M2M), then ~/.databrickscfg / dbt profiles
            host = os.environ.get("DATABRICKS_HOST")
            http_path = os.environ.get("DATABRICKS_HTTP_PATH")
            token = os.environ.get("DATABRICKS_TOKEN")
            client_id = os.environ.get("DATABRICKS_CLIENT_ID")
            client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
            catalog = os.environ.get("DATABRICKS_CATALOG")

            discovered_params: Dict[str, Any] = {}
            if not (host and http_path and (token or (client_id and client_secret))):
                from idr_core.credential_discovery import discover_all

                for conn_info in discover_all():
                    if conn_info.get("platform") != "databricks":
                        continue
                    p = conn_info.get("params", {})
                    if p.get("server_hostname") and p.get("http_path") and (
                        p.get("access_token") or (p.get("client_id") and p.get("client_secret"))
                    ):
                        discovered_params = dict(p)
                        host = host or p["server_hostname"]
                        http_path = http_path or p["http_path"]
                        token = token or p.get("access_token")
                        catalog = catalog or p.get("catalog")
                        print(f"Using discovered credentials: {conn_info.get('name')}")
                        break

            if not (host and http_path and (token or (client_id and client_secret) or (discovered_params.get("client_id") and discovered_params.get("client_secret")))):
                raise ValueError(
                    "Databricks connection requires DATABRICKS_HOST, DATABRICKS_HTTP_PATH, "
                    "and DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET"
                )

            dbx_kwargs, resolved_catalog = build_databricks_connect_kwargs(discovered_params)
            print(f"Connecting to Databricks ({dbx_kwargs['server_hostname']})...")
            conn = sql.connect(**dbx_kwargs)
            return DatabricksAdapter(conn, catalog=resolved_catalog)

        except ImportError:
            raise ImportError(
                "'databricks-sql-connector' not installed. Run pip install 'sql-identity-resolution[databricks]'"
            )

    else:
        raise ValueError(f"Unknown platform {args.platform}")


def handle_init(args) -> int:
    """Handle init command."""
    from idr_core.schema_manager import SchemaManager

    try:
        adapter = get_adapter(args)
    except Exception as e:
        print(f"Error creating adapter: {e}")
        return 1

    try:
        print("Initializing metadata tables...")
        manager = SchemaManager(adapter)
        seed_defaults = not getattr(args, "no_default_exclusions", False)
        manager.initialize(reset=args.reset, seed_default_exclusions=seed_defaults)

        print("✅ Metadata tables initialized.")
        return 0
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        return 1
    finally:
        adapter.close()


def handle_privacy(args) -> int:
    """Handle privacy RTBF subcommands."""
    from idr_core.privacy import PrivacyManager

    if getattr(args, "privacy_cmd", None) != "forget":
        print("Usage: idr privacy forget --identifier=<val> | --entity-key=<key> | --resolved-id=<id>")
        return 1

    if not any([args.identifier, args.entity_key, args.resolved_id]):
        print("❌ Specify at least one of --identifier, --entity-key, or --resolved-id.")
        return 1

    try:
        adapter = get_adapter(args)
    except Exception as e:
        print(f"Error creating adapter: {e}")
        return 1

    try:
        pm = PrivacyManager(adapter)
        res = pm.forget(
            identifier_value=args.identifier,
            entity_key=args.entity_key,
            resolved_id=args.resolved_id,
            dry_run=args.dry_run,
            requested_by="cli",
            reason=args.reason,
        )
        mode_label = "DRY RUN PREVIEW" if args.dry_run else "RTBF PURGE COMPLETE"
        print(f"\n{'=' * 60}")
        print(f"  IDR Privacy ({mode_label}) - {res['status']}")
        print(f"{'=' * 60}")
        print(f"  Request ID:      {res['request_id']}")
        print(f"  Target SHA-256:  {res['target_hash'][:16]}...")
        print(f"  Clusters Found:  {len(res['resolved_ids'])} {res['resolved_ids']}")
        print(f"  Entities Purged: {res['entities_purged']} {res['entity_keys']}")
        print(f"  Edges Purged:    {res['edges_purged']}")
        print(f"  Profiles Purged: {res['profiles_purged']}")
        print(f"{'=' * 60}\n")
        return 0
    except Exception as e:
        print(f"❌ Privacy forget failed: {e}")
        return 1
    finally:
        adapter.close()


def handle_config(args) -> int:
    """Handle config subcommands."""
    from idr_core.config import (
        apply_config_to_warehouse,
        config_to_sql,
        get_config_snapshots,
        load_config,
    )

    if args.config_cmd == "validate":
        try:
            load_config(args.file)
            print(f"✅ Configuration valid: {args.file}")
            return 0
        except Exception as e:
            print(f"❌ Configuration invalid: {e}")
            return 1

    if args.config_cmd == "generate":
        try:
            config = load_config(args.file)
            statements = config_to_sql(config, dialect=getattr(args, "dialect", "duckdb"))
            print("-- Generated SQL from YAML configuration")
            print("-- " + "=" * 58)
            for stmt in statements:
                print(stmt)
                print()
            return 0
        except Exception as e:
            print(f"Error: {e}")
            return 1

    if args.config_cmd == "apply":
        adapter = None
        try:
            adapter = get_adapter(args)
            print(f"Applying configuration from {args.file}...")
            config = load_config(args.file)
            res = apply_config_to_warehouse(adapter, config, record_snapshot=True)
            print(
                f"✅ Configuration applied ({res['batches_executed']} batches executed, "
                f"snapshot={res['config_hash'][:10]})."
            )
            return 0
        except Exception as e:
            print(f"❌ Failed to apply configuration: {e}")
            return 1
        finally:
            if adapter is not None:
                adapter.close()

    if args.config_cmd == "history":
        adapter = None
        try:
            adapter = get_adapter(args)
            snapshots = get_config_snapshots(adapter, limit=getattr(args, "limit", 15))
            if not snapshots:
                print("No saved configuration snapshots found in idr_out.config_snapshot.")
                return 0
            print(f"\nSaved Configuration Snapshots ({len(snapshots)}):")
            print("-" * 76)
            for idx, snap in enumerate(snapshots):
                marker = " (latest)" if idx == 0 else ""
                print(
                    f"  {snap['config_hash'][:12]}  |  {snap['created_at']}  |  "
                    f"{snap['sources_count']} src, {snap['rules_count']} rules, "
                    f"{snap['exclusions_count']} excl{marker}"
                )
            print("-" * 76)
            return 0
        except Exception as e:
            print(f"❌ Failed to read config history: {e}")
            return 1
        finally:
            if adapter is not None:
                adapter.close()

    if args.config_cmd == "rollback":
        adapter = None
        try:
            adapter = get_adapter(args)
            snapshots = get_config_snapshots(adapter, limit=50)
            if not snapshots:
                print("❌ No saved configuration snapshots available to roll back to.")
                return 1

            requested_hash = (getattr(args, "config_hash", None) or "").strip()
            target = None
            if requested_hash:
                matches = [
                    s for s in snapshots if s["config_hash"].startswith(requested_hash)
                ]
                if not matches:
                    print(f"❌ No saved snapshot found matching hash prefix '{requested_hash}'.")
                    return 1
                target = matches[0]
            else:
                if len(snapshots) < 2:
                    print("❌ At least 2 saved configuration snapshots are needed for automatic rollback (or pass --hash).")
                    return 1
                target = snapshots[1]

            res = apply_config_to_warehouse(adapter, target["config"], record_snapshot=True)
            print(
                f"✅ Rolled back configuration to snapshot {target['config_hash'][:12]} "
                f"(saved_at={target['created_at']}, new_snapshot={res['config_hash'][:10]})."
            )
            return 0
        except Exception as e:
            print(f"❌ Failed to roll back configuration: {e}")
            return 1
        finally:
            if adapter is not None:
                adapter.close()

    if args.config_cmd == "export-dbt":
        return handle_dbt_export(args)

    return 0


def handle_dbt_export(args) -> int:
    """Export YAML configuration or active warehouse metadata into dbt_idr seed CSVs."""
    from idr_core.config import config_to_dbt_seeds, load_config, sql_to_config

    try:
        if getattr(args, "file", None):
            config = load_config(args.file)
        else:
            adapter = get_adapter(args)
            try:
                config = sql_to_config(adapter)
            finally:
                adapter.close()
            if not config.get("sources"):
                print("❌ No active sources found in warehouse metadata.")
                return 1

        written = config_to_dbt_seeds(config, output_dir=args.output_dir)
        print(f"✅ Exported {len(written)} dbt_idr seed CSVs to {os.path.abspath(args.output_dir)}:")
        for name, path in written.items():
            print(f"   - {name} -> {path}")
        return 0
    except Exception as e:
        print(f"❌ Failed to export dbt seeds: {e}")
        return 1


def handle_run(args) -> int:
    """Handle run command."""
    from idr_core import IDRRunner, load_config
    from idr_core.config import (
        apply_config_to_warehouse,
        compute_dry_run_summary,
    )
    from idr_core.runner import RunConfig

    try:
        adapter = get_adapter(args)
    except Exception as e:
        print(f"Error: {e}")
        return 1

    ephemeral_dry_run = False
    try:
        runner = IDRRunner(adapter)
        run_mode = "INCR" if str(args.mode).upper() == "INCREMENTAL" else str(args.mode).upper()

        # Apply YAML config if provided: ephemeral & rolled back on --dry-run, persisted otherwise
        if args.config:
            config_data = load_config(args.config)
            if args.dry_run:
                ephemeral_dry_run = True
            else:
                apply_config_to_warehouse(adapter, config_data, record_snapshot=True)

        config = RunConfig(
            run_mode=run_mode,
            max_iters=args.max_iters,
            dry_run=args.dry_run,
            strict=getattr(args, "strict", False),
            max_group_size=getattr(args, "max_group_size", None),
            max_moved_entities_pct=getattr(args, "max_moved_pct", None),
            max_cluster_delta_pct=getattr(args, "max_cluster_delta_pct", None),
            lens=getattr(args, "lens", None),
            skip_lenses=getattr(args, "skip_lenses", False),
        )
        dry_summary = None
        if args.dry_run:
            from types import SimpleNamespace

            from idr_core.ai_review import AIReviewService
            from idr_core.config import sql_to_config
            draft_cfg = config_data if args.config else sql_to_config(adapter)
            options = dict(run_mode=config.run_mode, max_iters=config.max_iters, strict=config.strict,
                           max_group_size=config.max_group_size, lens=config.lens or "default")
            truth = {"ground_truth_table": args.ground_truth_table} if getattr(args, "ground_truth_table", None) else None
            if getattr(args, "evaluation_config", None):
                import json
                with open(args.evaluation_config) as label_file:
                    truth = json.load(label_file)
            preview = AIReviewService(adapter).preview(draft_cfg, options, ground_truth=truth)
            result = SimpleNamespace(**{k:v for k,v in preview.items() if k not in ("review", "draft_hash", "dry_run_summary")})
            dry_summary = preview["dry_run_summary"]
        else:
            result = runner.run(config)

        if args.dry_run and dry_summary is None and result.status == "DRY_RUN_COMPLETE":
            dry_summary = compute_dry_run_summary(
                adapter,
                result.run_id,
                run_mode,
                edges_created=result.edges_created,
                clusters_impacted=result.clusters_impacted,
            )

        # Print summary
        print(f"\n{'=' * 60}")
        print(f"  IDR Run Complete - {result.status}")
        print(f"{'=' * 60}")
        print(f"  Run ID:          {result.run_id}")
        print(f"  Entities:        {result.entities_processed:,}")
        print(f"  Edges:           {result.edges_created:,}")
        print(f"  Clusters:        {result.clusters_impacted:,}")
        print(f"  Duration:        {result.duration_seconds:.1f}s")
        if dry_summary is not None:
            print(f"  {'-' * 56}")
            print(f"  New Entities:    {dry_summary.get('new_entities', 0):,}")
            print(f"  Moved Entities:  {dry_summary.get('moved_entities', 0):,}")
            print(f"  Unchanged:       {dry_summary.get('unchanged_entities', 0):,}")
            edges_d = dry_summary.get("edges_delta", 0)
            clusters_d = dry_summary.get("clusters_delta", 0)
            print(f"  Edges Delta:     {edges_d:+d}")
            print(f"  Clusters Delta:  {clusters_d:+d}")
            by_type = dry_summary.get("edges_delta_by_type") or {}
            if by_type:
                type_summary = ", ".join(f"{k}: {v:+d}" for k, v in by_type.items())
                print(f"  Edges by Type:   {type_summary}")
            shadow_kept = dry_summary.get("shadow_preserved_entities", 0) or 0
            if shadow_kept > 0:
                corr_types = ", ".join(dry_summary.get("corroborating_identifier_types") or [])
                corr_suffix = f" (corroborated by: {corr_types})" if corr_types else ""
                print(f"  Shadow-Kept:     {shadow_kept:,} entities{corr_suffix}")
            if ephemeral_dry_run:
                print("  Config Mode:     Isolated (--dry-run left active idr_meta unchanged)")
        if getattr(result, "warnings", None):
            print(f"  {'-' * 56}")
            print("  Warnings:")
            for w in result.warnings:
                print(f"    ! {w}")
        if getattr(result, "error", None):
            print(f"  Error:           {result.error}")
        print(f"{'=' * 60}\n")

        if getattr(args, "critique", False) and args.dry_run and result.status == "DRY_RUN_COMPLETE":
            from idr_core.ai_review import AIReviewService
            critique = AIReviewService(adapter).get(result.run_id)
            tel = critique.get("telemetry") or {}
            print(f"{'=' * 60}")
            print(
                f"  AI Graph Critique (Beta) — {str(critique.get('health', 'unknown')).upper().replace('_', ' ')}"
            )
            print(
                f"  Provider: {critique.get('provider')}:{critique.get('model')} | "
                f"Singletons: {tel.get('singleton_pct', 0)}% | Max Cluster: {tel.get('max_cluster_size', 0):,}"
            )
            print(f"{'=' * 60}")
            print(f"  {critique.get('headline', '')}")
            for f in critique.get("findings") or []:
                sev = str(f.get("severity", "info")).upper()
                print(f"\n  [{sev}] {f.get('title')} ({f.get('metric', '')})")
                print(f"    {f.get('detail')}")
                if f.get("patch"):
                    print(f"    -> Suggested fix: {f['patch'].get('label')}")
            print(f"{'=' * 60}\n")

        return 0 if ("SUCCESS" in result.status or result.status == "DRY_RUN_COMPLETE") else 1

    finally:
        adapter.close()


def handle_serve(args) -> int:
    """Handle serve command."""
    # Ensure dev auth is allowed by default for local CLI serving
    os.environ.setdefault("IDR_ALLOW_INSECURE_DEV_AUTH", "true")

    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    try:
        import uvicorn
    except ImportError:
        print(
            "❌ 'api' extra not installed. Run: pip install 'sql-identity-resolution[api]'",
            file=sys.stderr,
        )
        return 1

    try:
        import idr_api.main  # noqa: F401
    except ImportError as exc:
        print(
            f"❌ Failed to load 'idr_api': {exc}\n"
            "Ensure the package is installed in your active environment: pip install -e '.[all]'",
            file=sys.stderr,
        )
        return 1

    print(f"🚀 Starting IDR API Server on {args.host}:{args.port}")
    if getattr(args, "open", False):
        import threading
        import webbrowser

        browse_host = "localhost" if args.host in ("0.0.0.0", "127.0.0.1") else args.host
        url = f"http://{browse_host}:{args.port}"
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()

    uvicorn.run(
        "idr_api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        access_log=not getattr(args, "quiet", False),
    )
    return 0


def handle_ui(args) -> int:
    """Handle ui command (starts server and opens browser by default)."""
    args.open = not getattr(args, "no_browser", False)
    return handle_serve(args)


def handle_mcp(args) -> int:
    """Handle mcp command."""
    try:
        from idr_mcp.server import connect_from_env, mcp
    except ImportError:
        print(
            "❌ 'mcp' extra not installed. Run: pip install 'sql-identity-resolution[mcp]'",
            file=sys.stderr,
        )
        return 1

    # Ensure connection logic runs
    connect_from_env()

    # Run the MCP server
    # FastMCP uses click/typer internals usually, but we can invoke run() directly.
    # If args.transport == 'stdio':
    #     mcp.run(transport='stdio')
    # but FastMCP.run() typically parses CLI args itself.
    # Since we are inside a CLI already, we should manually invoke the desired transport.
    if args.transport == "sse":
        # FastMCP doesn't expose a simple run_sse() public API easily without 'uvicorn',
        # but mcp.run() usually defaults to stdio or parses args.
        # For simplicity, we just call mcp.run() and let it handle stdio (default).
        # If SSE is needed, user might need to run via `uvicorn idr_mcp.server:mcp.sse`.
        print(
            "Note: SSE mode requires running via uvicorn directly: uvicorn idr_mcp.server:mcp.sse_app",
            file=sys.stderr,
        )
        return 1

    # Stdio mode
    mcp.run()
    return 0


def handle_quickstart(args) -> int:
    """Handle quickstart command."""
    from idr_core.quickstart import run_quickstart

    return run_quickstart(
        output=args.output,
        rows=args.rows,
        seed=args.seed,
    )


def handle_demo(args) -> int:
    """Generate demo DuckDB dataset, run IDR, and launch the bundled Explorer UI + /mcp."""
    from idr_core.quickstart import run_quickstart

    rc = run_quickstart(
        output=args.output,
        rows=args.rows,
        seed=args.seed,
    )
    if rc != 0:
        return rc

    args.platform = "duckdb"
    args.db = args.output
    args.project = None
    args.location = "US"
    args.account = None
    args.user = None
    args.password = None
    args.warehouse = None
    args.database = None
    args.schema = "PUBLIC"
    args.role = None
    args.reload = False
    args.open = not getattr(args, "no_browser", False)
    return handle_serve(args)


def handle_doctor(args) -> int:
    """Execute IDR Doctor production readiness diagnostics."""
    adapter = get_adapter(args)
    from idr_core.doctor import CheckStatus, IDRDoctor

    doctor = IDRDoctor(adapter)
    print("=" * 70)
    print(f"🩺  SQL Identity Resolution — Production Doctor ({doctor.platform.upper()})")
    print("=" * 70)

    report = doctor.run_diagnostics(verbose=getattr(args, "verbose", False))

    current_cat = None
    for check in report.checks:
        if check.category != current_cat:
            current_cat = check.category
            print(f"\n[{current_cat}]")

        if check.status == CheckStatus.PASS:
            icon = "✓"
        elif check.status == CheckStatus.WARN:
            icon = "!"
        elif check.status == CheckStatus.INFO:
            icon = "ℹ"
        else:
            icon = "✗"

        print(f"  {icon} {check.name:<32} {check.message}")
        if check.remediation and (check.status in (CheckStatus.WARN, CheckStatus.FAIL) or getattr(args, "verbose", False)):
            for line in check.remediation.split("\n"):
                print(f"    👉 {line}")

    print("\n" + "-" * 70)
    print(f"Status: {report.overall_status.value} ({report.passed_count} passed, {report.warn_count} warnings, {report.fail_count} failed)")
    if report.overall_status == CheckStatus.PASS:
        print(f"🎉 {report.summary}")
        return 0
    elif report.overall_status == CheckStatus.WARN:
        print(f"⚠️  {report.summary}")
        return 0
    else:
        print(f"❌ {report.summary}")
        return 1


def handle_bootstrap(args) -> int:
    """Execute autonomous dataset profiling and YAML manifest generation."""
    adapter = get_adapter(args)
    from idr_core.bootstrap import AutoBootstrapper

    print("=" * 70)
    print("⚡  SQL Identity Resolution — Autonomous Bootstrapper")
    print("=" * 70)

    schema = getattr(args, "schema", None)
    tables_arg = getattr(args, "tables", None)
    candidate_tables = [t.strip() for t in tables_arg.split(",")] if tables_arg else None

    target_display = f"schema/dataset '{schema}'" if schema else "warehouse catalog"
    print(f"Profiling {target_display} via {getattr(adapter, 'dialect', 'warehouse').upper()}...")

    bootstrapper = AutoBootstrapper(adapter)
    res = bootstrapper.bootstrap(
        schema=schema,
        candidate_tables=candidate_tables,
        sample_size=getattr(args, "sample_size", 500),
        output_path=getattr(args, "output", "idr_config.yaml"),
        apply_to_warehouse=getattr(args, "apply", False),
    )

    if res["status"] != "SUCCESS":
        print(f"\n❌ Bootstrap halted: {res.get('message', res['status'])}")
        return 1

    print(f"\n✓ Scanned {res['tables_scanned']} candidate tables")
    print(f"✓ Configured {res['sources_configured']} source tables with detected customer identifiers")
    print(f"✓ Synthesized {res['rules_generated']} deterministic matching rules")
    print(f"✓ Generated {res['survivorship_rules']} attribute survivorship rules")
    if res.get("exclusions_discovered"):
        print(f"✓ Discovered {res['exclusions_discovered']} high-frequency poison-pill exclusions")

    print(f"\n📁 Manifest written to: {res['output_path']}")
    if res.get("applied_to_warehouse"):
        print("🚀 Successfully applied configuration directly to idr_meta.* tables!")
    elif res.get("apply_error"):
        print(f"⚠️  Failed to apply to warehouse: {res['apply_error']}")
    else:
        print("💡 Next step: Run 'idr run --dry-run' to preview cluster resolutions.")

    return 0


def handle_evaluate(args) -> int:
    """Execute entity resolution quality evaluation or stratified audit sampling."""
    adapter = get_adapter(args)
    from idr_core.evaluator import IDREvaluator

    evaluator = IDREvaluator(adapter)
    print("=" * 70)
    print(f"📊  SQL Identity Resolution — Quality Evaluation ({evaluator.dialect_name.upper()})")
    print("=" * 70)

    if getattr(args, "evaluation_config", None):
        import json
        import uuid

        from idr_core.config import sql_to_config
        from idr_core.evaluation_config import EvaluationOptions, acceptance, materialize_labels
        target = "idr_work.eval_labels_" + uuid.uuid4().hex[:12]
        try:
            with open(args.evaluation_config) as file:
                options = EvaluationOptions.model_validate(json.load(file))
            normalized = materialize_labels(adapter, sql_to_config(adapter), options, target)
            result = evaluator.evaluate_ground_truth(**normalized)
            check = acceptance(result, options)
            print(json.dumps({"evaluation": result, "acceptance": check}, default=str, indent=2))
            return 0 if check["status"] == "passed" else 1
        except Exception as exc:
            print(f"Evaluation configuration failed: {exc}")
            return 1
        finally:
            adapter.execute(f"DROP TABLE IF EXISTS {target}")
            adapter.close()

    gt_table = getattr(args, "ground_truth_table", None)
    if gt_table:
        res = evaluator.evaluate_ground_truth(
            ground_truth_table=gt_table,
            entity_key_col=getattr(args, "entity_key_col", "entity_key"),
            truth_id_col=getattr(args, "truth_id_col", "truth_id"),
        )
        if res.get("status") != "SUCCESS":
            print(f"❌ Evaluation failed: {res.get('message')}")
            return 1

        print(f"Evaluated Entities : {res['evaluated_entities']:,}")
        print(f"Predicted Clusters : {res['predicted_clusters']:,}")
        print(f"True Clusters      : {res['true_clusters']:,}")
        print("-" * 70)
        print(f"Pairwise Precision : {format(res['precision'], '.4f') if res['precision'] is not None else 'N/A'}  (TP={res['tp_pairs']:,}, FP={res['fp_pairs']:,})")
        print(f"Pairwise Recall    : {format(res['recall'], '.4f') if res['recall'] is not None else 'N/A'}  (FN={res['fn_pairs']:,})")
        print(f"Pairwise F1 Score  : {format(res['f1'], '.4f') if res['f1'] is not None else 'N/A'}")
        if res.get("top_overmerged_clusters"):
            print(f"\n⚠️  Top Over-Merged Clusters ({len(res['top_overmerged_clusters'])} shown):")
            for c in res["top_overmerged_clusters"][:5]:
                print(f"   - {c['resolved_id']}: {c['distinct_truth_ids']} truth IDs merged across {c['cluster_entities']} rows")
        if res.get("top_split_entities"):
            print(f"\n⚠️  Top Split Entities ({len(res['top_split_entities'])} shown):")
            for s in res["top_split_entities"][:5]:
                print(f"   - {s['truth_id']}: fractured into {s['split_into_clusters']} clusters")
        return 0

    # Unsupervised Stratified Audit Mode
    res = evaluator.audit_quality_and_sample(
        sample_per_stratum=getattr(args, "sample_size", 20),
        persist_sample=not getattr(args, "no_persist", False),
    )
    d = res["distribution"]
    print(f"Total Clusters             : {int(d.get('total_clusters') or 0):,}")
    print(f"Singleton Rate             : {float(d.get('singleton_rate') or 0) * 100:.1f}% ({int(d.get('singleton_clusters') or 0):,} singletons)")
    print(f"Multi-Entity Clusters      : {int(d.get('multi_entity_clusters') or 0):,}")
    print(f"  ├─ Corroborated (2+ keys): {int(d.get('corroborated_clusters') or 0):,}")
    print(f"  ├─ Single-Key Deterministic: {int(d.get('single_key_clusters') or 0):,}")
    print(f"  └─ Low-Confidence Risk     : {int(d.get('low_confidence_risk_clusters') or 0):,}")
    print(f"Avg Multi-Entity Conf      : {d.get('avg_multi_entity_confidence') or 'N/A'}")
    print(f"Edge Evidence Rows         : {int(d.get('edge_evidence_rows') or 0):,}")
    print(f"Near-Miss Singletons Found : {res['near_miss_candidates_found']:,}")
    print("-" * 70)
    print(f"Stratified Audit Samples   : {res['samples_extracted']} pairs extracted ({res['samples_persisted']} saved to idr_out.evaluation_sample)")
    return 0


def handle_plugins_list() -> int:
    """List registered canonicalizers, survivorship strategies, and warehouse adapters."""
    from idr_core.adapters import list_adapters
    from idr_core.canonicalizers import list_canonicalizers
    from idr_core.survivorship import list_survivorship_strategies

    print("Registered IDR Plugins & Extensions")
    print("=" * 50)
    print(f"Adapters ({len(list_adapters())}):")
    for a in list_adapters():
        print(f"  - {a}")
    print(f"\nCanonicalizers ({len(list_canonicalizers())}):")
    for c in list_canonicalizers():
        print(f"  - {c}")
    print(f"\nSurvivorship Strategies ({len(list_survivorship_strategies())}):")
    for s in list_survivorship_strategies():
        print(f"  - {s}")
    return 0


def _build_llm_client_from_args(args, adapter=None) -> "LLMClient":
    from idr_core.llm import LLMClient, LLMConfig

    cfg = LLMConfig.from_env()
    if getattr(args, "provider", None):
        cfg.provider = args.provider
    elif not cfg.is_enabled:
        cfg.provider = "ollama"
    if getattr(args, "model", None):
        cfg.model = args.model
    if getattr(args, "base_url", None):
        cfg.base_url = args.base_url
    if getattr(args, "api_key", None):
        cfg.api_key = args.api_key
    if getattr(args, "mask_pii", False):
        cfg.mask_pii = True
    elif getattr(args, "no_mask_pii", False):
        cfg.mask_pii = False
    return LLMClient(cfg, adapter=adapter)


def handle_ai(args) -> int:
    """Handle `idr ai test|models|ask` commands."""
    from idr_core.ai_assistant import ask_identity_graph, test_llm_connection
    from idr_core.llm import list_ollama_models

    if args.ai_cmd == "models":
        models = list_ollama_models(getattr(args, "base_url", None))
        if not models:
            print("No local Ollama models found (is `ollama serve` running on localhost:11434?).")
            return 1
        print(f"Local Ollama Models ({len(models)}):")
        for m in models:
            print(f"  - {m}")
        return 0

    if args.ai_cmd in ("review", "compare", "purge-reviews", "profile-preview"):
        import json

        from .ai_review import AIReviewService
        from .config import load_config
        adapter = get_adapter(args)
        try:
            service = AIReviewService(adapter)
            if args.ai_cmd == "profile-preview":
                from .ai_config import draft_hash
                from .profile_preview import ProfilePreviewOptions, preview_profiles
                cfg = load_config(args.config)
                options = ProfilePreviewOptions(mode=args.preview_mode, attributes=[v.strip() for v in args.attributes.split(',') if v.strip()],
                    grouped_outputs=[v.strip() for v in args.grouped_outputs.split(',') if v.strip()],
                    max_clusters=args.max_clusters, max_members=args.max_members, timeout_seconds=args.timeout_seconds,
                    max_bytes_billed=args.max_bytes_billed,
                    allow_unknown_scan_cost=args.allow_source_scan, allow_full_population=args.allow_full_population)
                result = preview_profiles(service, cfg, args.run_id, draft_hash(cfg), args.lens, options)
                print(json.dumps(result, indent=2, default=str))
                return 0 if result['status'] in ('complete','validated') else 1
            if args.ai_cmd == "purge-reviews":
                print(json.dumps({"purged": service.purge(all_owned=True, limit=100)}))
            elif args.ai_cmd == "review":
                print(json.dumps(service.get(args.run_id), indent=2))
            else:
                cfg = load_config(args.config)
                patch = service._patch(cfg, args.action, args.target, json.loads(args.value))
                print(json.dumps(service.compare(args.run_id, cfg, patch, args.lens), indent=2))
            return 0
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        finally:
            adapter.close()

    client = _build_llm_client_from_args(args)
    if args.ai_cmd == "test":
        res = test_llm_connection(client)
        print(
            f"✓ Connected to {res['provider']} ({res['model']}) in {res['latency_ms']}ms "
            f"[local={res['is_local']}, pii_masking={res['mask_pii']}]"
        )
        return 0

    if args.ai_cmd == "ask":
        adapter = get_adapter(args)
        res = ask_identity_graph(adapter, args.question, client=client)
        print(f"[{res['provider']}:{res['model']} | pii_masked={res['pii_masked']}]\n")
        print(res["answer"])
        return 0

    if args.ai_cmd == "generate-config":
        import json

        from idr_core.bootstrap import AutoBootstrapper
        from idr_core.config import load_config
        from idr_core.rule_generator import AIRuleGenerator, render_config_yaml_with_reasoning

        adapter = get_adapter(args)
        current = load_config(args.current_config) if args.current_config else {}
        tables_arg = getattr(args, "tables", None)
        if args.rules_only and current:
            tables = [source["table"] for source in current.get("sources", [])]
        elif tables_arg:
            tables = [t.strip() for t in tables_arg.split(",") if t.strip()]
        else:
            tables = AutoBootstrapper(adapter)._discover_tables(getattr(args, "schema", None))

        if not tables:
            print("❌ No candidate tables found. Pass --tables schema.table1,schema.table2 explicitly.")
            return 1

        print("=" * 72)
        print("⚡ SQL Identity Resolution — AI Rule & Config Generator (Zero-PII · Beta)")
        print("=" * 72)
        print(f"Tables ({len(tables)}): {', '.join(tables)}")
        print(f"Model:  {client.config.provider}:{client.config.resolved_model()}")

        from idr_core.ai_config import AIConfigContext
        ctx = AIConfigContext.model_validate(current.get("ai_assistance") or {})
        ctx.grain = args.grain or ctx.grain
        ctx.event_policy = args.event_policy or ctx.event_policy
        ctx.system_of_record = args.system_of_record or ctx.system_of_record
        ctx.fuzzy = args.allow_fuzzy if args.allow_fuzzy is not None else ctx.fuzzy
        if args.allow_match is not None:
            ctx.allowed_matching_identifiers = [k.strip() for k in args.allow_match.split(',') if k.strip()]
        if args.rules_only and not current:
            print("--rules-only requires --current-config", file=sys.stderr)
            adapter.close()
            return 1

        def _cli_progress(evt: dict) -> None:
            print(f"  [{evt.get('step', '?')}/{evt.get('total_steps', 5)}] {evt.get('message', '')}", flush=True)

        plan = AIRuleGenerator(adapter, client=client).generate(
            tables=tables,
            goal=getattr(args, "goal", ""), context=ctx, current_config=current, mode="rules" if args.rules_only else "config",
            sample_size=getattr(args, "sample_size", 2000),
            verify_sql=not getattr(args, "no_verify_sql", False),
            on_progress=_cli_progress,
        )

        yaml_text = render_config_yaml_with_reasoning(plan)
        out_path = getattr(args, "output", "idr_config.yaml")
        if plan.validation_report.valid:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(yaml_text)

        reasoning_out = getattr(args, "reasoning_out", None)
        if reasoning_out:
            with open(reasoning_out, "w", encoding="utf-8") as f:
                json.dump(plan.model_dump(), f, indent=2, default=str)

        cfg = plan.suggested_config or {}
        print(f"\n✓ Mode: {plan.generation_mode} | Validated: {plan.validation_report.valid} | SQL Verified: {plan.validation_report.sql_verified}")
        print(f"✓ Sources: {len(cfg.get('sources') or [])} | Rules: {len(cfg.get('rules') or [])} | Survivorship: {len(cfg.get('survivorship') or [])} | Lenses: {len(cfg.get('lenses') or [])}")

        overlaps = (plan.profile_summary or {}).get("cross_table_overlaps") or []
        if overlaps:
            print("\nCross-Table Key Overlaps Detected:")
            for ov in overlaps[:8]:
                print(
                    f"  • {ov['table_a']}.{ov['column_a']} ↔ {ov['table_b']}.{ov['column_b']} "
                    f"[{ov['identifier_type']}]: {ov['overlap_a_in_b']:.0%} / {ov['overlap_b_in_a']:.0%}"
                )

        if plan.explanations.rules:
            print("\nProposed Rules & Reasoning:")
            for item in plan.explanations.rules:
                flag = " ⚠ NEEDS REVIEW" if item.needs_review else ""
                print(f"  • {item.target} ({item.confidence}{flag}): {item.summary}")
                for r in item.risks:
                    print(f"      Risk: {r}")

        if plan.validation_report.warnings:
            print("\nWarnings:")
            for w in plan.validation_report.warnings:
                print(f"  ! {w}")

        adapter.close()
        if not plan.validation_report.valid:
            print("Invalid proposal; no usable config file written.", file=sys.stderr)
            for error in plan.validation_report.errors:
                print(error, file=sys.stderr)
            return 1
        print(f"\n📁 Config written to: {out_path}")
        if reasoning_out:
            print(f"📄 Reasoning report written to: {reasoning_out}")
        print(f"💡 Next step: Review {out_path}, then run `idr config apply --config {out_path}`.")
        return 0 if plan.validation_report.valid else 1

    return 0


def handle_explain(args) -> int:
    """Handle `idr explain <resolved_id>`."""
    from idr_core.ai_assistant import explain_cluster_with_ai

    adapter = get_adapter(args)
    client = _build_llm_client_from_args(args)
    res = explain_cluster_with_ai(
        adapter,
        args.resolved_id,
        client=client,
        custom_prompt=getattr(args, "prompt", None),
    )
    print(f"Cluster {res['resolved_id']} ({res['provider']}:{res['model']} | pii_masked={res['pii_masked']})")
    print("-" * 60)
    print(res["explanation"])
    return 0


def handle_triage(args) -> int:
    """Handle `idr triage`."""
    import json

    from idr_core.ai_assistant import triage_with_ai

    adapter = get_adapter(args)
    client = _build_llm_client_from_args(args, adapter=adapter)
    res = triage_with_ai(
        adapter,
        client=client,
        max_confidence=getattr(args, "max_confidence", 0.75),
        limit=getattr(args, "limit", 8),
        escalate_provider=getattr(args, "escalate_provider", None),
        escalate_model=getattr(args, "escalate_model", None),
        escalation_threshold=getattr(args, "escalation_threshold", 0.80),
        min_subject_fanout=getattr(args, "min_subject_fanout", 2),
    )
    if getattr(args, "json_output", False):
        print(json.dumps(res, default=str, indent=2))
        return 0

    tier2_info = (
        f" -> Tier-2 {res.get('escalate_provider')}:{res.get('escalate_model')}"
        if res.get("escalate_model")
        else ""
    )
    print(
        f"3-Tier AI Triage ({res.get('provider', 'n/a')}:{res.get('model', 'n/a')}"
        f"{tier2_info} | pii_masked={res['pii_masked']})"
    )
    funnel = res.get("cascade_summary") or {}
    if funnel:
        print(
            f"  Tier 0 (SQL, $0) : {funnel.get('total_multi_entity_clusters', 0):,} multi-entity clusters -> "
            f"{funnel.get('tier0_auto_passed', 0):,} auto-passed ({funnel.get('sql_filter_savings_pct', 0)}%) | "
            f"{funnel.get('tier0_flagged', 0):,} flagged"
        )
        print(
            f"  Tier 1 (LLM)     : {funnel.get('tier1_reviewed', 0):,} judged | "
            f"Tier 2 escalated: {funnel.get('tier2_escalated', 0):,}"
        )
    print("-" * 72)
    proposals = res.get("proposals", [])
    if not proposals:
        print(res.get("message", "No proposals."))
        return 0
    for p in proposals:
        tier = str(p.get("judge_tier") or "tier1").upper()
        jconf = p.get("judge_confidence")
        conf_str = f" conf={jconf:.2f}" if isinstance(jconf, (int, float)) else ""
        fanout = p.get("subject_fanout")
        fanout_str = f" fanout={fanout}" if fanout is not None else ""
        print(
            f"- {p.get('resolved_id')}: [{p.get('action')}] ({tier}{conf_str}{fanout_str}) {p.get('reason')}"
        )
    return 0


def handle_lenses(args) -> int:
    """Handle `idr lenses list|diff` commands."""
    from idr_core.config import compute_lens_diff, sql_to_config

    try:
        adapter = get_adapter(args)
    except Exception as e:
        print(f"Error creating adapter: {e}")
        return 1

    try:
        if args.lenses_cmd == "list":
            cfg = sql_to_config(adapter)
            lenses = cfg.get("lenses") or []
            if not lenses:
                print("No active lenses configured in idr_meta.lens_definition.")
                return 0
            print(f"Configured Identity Lenses ({len(lenses)}):")
            for ldef in lenses:
                inc = ", ".join(ldef.get("include_types") or ["*"])
                exc = ", ".join(ldef.get("exclude_types") or []) or "none"
                src = ", ".join(ldef.get("source_filter") or ["*"])
                print(f"  - {ldef['id']}: include=[{inc}] exclude=[{exc}] sources=[{src}]")
            return 0

        if args.lenses_cmd == "diff":
            diff = compute_lens_diff(adapter, args.lens_a, args.lens_b, limit=getattr(args, "limit", 25))
            print(f"\n{'=' * 60}")
            print(f"  Lens Boundary Diff: {diff['lens_a']} vs {diff['lens_b']}")
            print(f"{'=' * 60}")
            print(f"  {diff['lens_a']} clusters:   {diff['lens_a_clusters']:,}")
            print(f"  {diff['lens_b']} clusters:   {diff['lens_b_clusters']:,}")
            print(f"  Entities compared:   {diff['entities_compared']:,}")
            print(f"  Merged in {diff['lens_b']} only: {len(diff['merged_in_b_only'])} cluster(s)")
            for row in diff["merged_in_b_only"][:10]:
                print(
                    f"    - {row['resolved_b']}: combines {row['distinct_a_clusters']} {diff['lens_a']} clusters ({row['entity_count']} entities)"
                )
            print(f"  Merged in {diff['lens_a']} only: {len(diff['merged_in_a_only'])} cluster(s)")
            for row in diff["merged_in_a_only"][:10]:
                print(
                    f"    - {row['resolved_a']}: combines {row['distinct_b_clusters']} {diff['lens_b']} clusters ({row['entity_count']} entities)"
                )
            print(f"{'=' * 60}\n")
            return 0
        return 0
    finally:
        adapter.close()


if __name__ == "__main__":
    sys.exit(main())
