"""
Command-line interface for IDR.

Provides unified CLI commands for running identity resolution
across all supported platforms.
"""

import argparse
import os
import sys
from typing import Optional

from idr_core.logger_utils import configure_logging


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

    apply_parser = config_subparsers.add_parser("apply", help="Apply config to metadata tables")
    apply_parser.add_argument("--file", "-f", required=True, help="Config file path")
    # We need connection args here too since we write to DB
    apply_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=False,
        help="Target platform (auto-detected if omitted)",
    )
    apply_parser.add_argument("--db", help="DuckDB database path")
    apply_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    apply_parser.add_argument("--location", default="US", help="BigQuery location")
    apply_parser.add_argument(
        "--meta-dataset", default="idr_meta", help="BigQuery metadata dataset"
    )
    apply_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")

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

    if parsed_args.command == "bootstrap":
        return handle_bootstrap(parsed_args)

    if parsed_args.command == "doctor":
        return handle_doctor(parsed_args)

    if parsed_args.command == "evaluate":
        return handle_evaluate(parsed_args)

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
        try:
            from snowflake.snowpark import Session

            from idr_core.adapters.snowflake import SnowflakeAdapter

            print("Connecting to Snowflake...")
            # Snowflake Session builder automatically picks up config from:
            # 1. SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc. env vars
            # 2. ~/.snowflake/connections.toml (if using 'connection_name')
            # For now, we rely on implicit env vars or default connection.
            # Passing {} is risky if no default is configured.
            # Better to allow it to fail naturally if no config found.
            session = Session.builder.getOrCreate()
            return SnowflakeAdapter(session)
        except ImportError:
            raise ImportError("'snowflake-snowpark-python' not installed")

    elif args.platform == "databricks":
        try:
            from databricks import sql

            from idr_core.adapters.databricks import DatabricksAdapter

            # Resolve credentials: env vars first, then ~/.databrickscfg / dbt profiles
            host = os.environ.get("DATABRICKS_HOST")
            http_path = os.environ.get("DATABRICKS_HTTP_PATH")
            token = os.environ.get("DATABRICKS_TOKEN")
            catalog = os.environ.get("DATABRICKS_CATALOG")

            if not (host and http_path and token):
                from idr_core.credential_discovery import discover_all

                for conn_info in discover_all():
                    if conn_info.get("platform") != "databricks":
                        continue
                    p = conn_info.get("params", {})
                    if p.get("server_hostname") and p.get("http_path") and p.get("access_token"):
                        host = host or p["server_hostname"]
                        http_path = http_path or p["http_path"]
                        token = token or p["access_token"]
                        catalog = catalog or p.get("catalog")
                        print(f"Using discovered credentials: {conn_info.get('name')}")
                        break

            if not (host and http_path and token):
                raise ValueError(
                    "Databricks connection requires DATABRICKS_HOST, DATABRICKS_HTTP_PATH, "
                    "DATABRICKS_TOKEN (env vars) or a ~/.databrickscfg profile with host, http_path, token"
                )

            host = host.replace("https://", "").replace("http://", "").split("/")[0]
            print(f"Connecting to Databricks ({host})...")
            conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
            return DatabricksAdapter(conn, catalog=catalog)

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
    from idr_core.config import config_to_sql, load_config

    # Debug print removed
    # print(f"DEBUG: config_cmd={args.config_cmd}")

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
        try:
            # Need adapter to execute SQL
            adapter = get_adapter(args)

            print(f"Applying configuration from {args.file}...")
            config = load_config(args.file)
            statements = config_to_sql(config, dialect=adapter.dialect)

            # Execute statements in transaction if possible (DuckDB/Snowflake)
            # For now just execute sequentially
            count = 0
            for stmt in statements:
                adapter.execute(stmt)
                count += 1

            print(f"✅ Configuration applied ({count} statements executed).")
            adapter.close()
            return 0
        except Exception as e:
            print(f"❌ Failed to apply configuration: {e}")
            return 1

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
    from idr_core.config import config_to_sql
    from idr_core.runner import RunConfig

    try:
        adapter = get_adapter(args)
    except Exception as e:
        print(f"Error: {e}")
        return 1

    try:
        # Create runner and execute
        runner = IDRRunner(adapter)

        # Apply YAML config if provided
        if args.config:
            # Note: We enforce that `idr init` should have been called first.
            # We insert the config data into the metadata tables.
            config_data = load_config(args.config)
            for stmt in config_to_sql(config_data, dialect=adapter.dialect):
                adapter.execute(stmt)

        config = RunConfig(
            run_mode=args.mode,
            max_iters=args.max_iters,
            dry_run=args.dry_run,
            strict=getattr(args, "strict", False),
            max_group_size=getattr(args, "max_group_size", None),
        )
        result = runner.run(config)

        # Print summary
        print(f"\n{'=' * 60}")
        print(f"  IDR Run Complete - {result.status}")
        print(f"{'=' * 60}")
        print(f"  Run ID:          {result.run_id}")
        print(f"  Entities:        {result.entities_processed:,}")
        print(f"  Edges:           {result.edges_created:,}")
        print(f"  Clusters:        {result.clusters_impacted:,}")
        print(f"  Duration:        {result.duration_seconds:.1f}s")
        if result.error:
            print(f"  Error:           {result.error}")
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
    print(f"⚡  SQL Identity Resolution — Autonomous Bootstrapper")
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
        print(f"Pairwise Precision : {res['precision']:.4f}  (TP={res['tp_pairs']:,}, FP={res['fp_pairs']:,})")
        print(f"Pairwise Recall    : {res['recall']:.4f}  (FN={res['fn_pairs']:,})")
        print(f"Pairwise F1 Score  : {res['f1']:.4f}")
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


if __name__ == "__main__":
    sys.exit(main())
