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
        required=True,
        help="Target platform",
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

    # === idr init ===
    init_parser = subparsers.add_parser("init", help="Initialize metadata tables")
    init_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=True,
        help="Target platform",
    )
    init_parser.add_argument("--db", help="DuckDB database path")
    init_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    init_parser.add_argument("--location", default="US", help="BigQuery location")
    init_parser.add_argument("--dataset", default="idr_out", help="BigQuery output dataset")
    init_parser.add_argument("--meta-dataset", default="idr_meta", help="BigQuery metadata dataset")
    init_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")
    init_parser.add_argument("--reset", action="store_true", help="Drop and recreate tables")

    apply_parser = config_subparsers.add_parser("apply", help="Apply config to metadata tables")
    apply_parser.add_argument("--file", "-f", required=True, help="Config file path")
    # We need connection args here too since we write to DB
    apply_parser.add_argument(
        "--platform",
        "-p",
        choices=["duckdb", "bigquery", "snowflake", "databricks"],
        required=True,
        help="Target platform",
    )
    apply_parser.add_argument("--db", help="DuckDB database path")
    apply_parser.add_argument("--project", help="GCP project ID (BigQuery)")
    apply_parser.add_argument("--location", default="US", help="BigQuery location")
    apply_parser.add_argument(
        "--meta-dataset", default="idr_meta", help="BigQuery metadata dataset"
    )
    apply_parser.add_argument("--work-dataset", default="idr_work", help="BigQuery work dataset")

    # === idr serve ===
    serve_parser = subparsers.add_parser("serve", help="Start API server")
    serve_parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    serve_parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    serve_parser.add_argument("--reload", action="store_true", help="Enable auto-reload")

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

    if parsed_args.command == "init":
        return handle_init(parsed_args)

    if parsed_args.command == "run":
        return handle_run(parsed_args)

    if parsed_args.command == "serve":
        return handle_serve(parsed_args)

    if parsed_args.command == "mcp":
        return handle_mcp(parsed_args)

    if parsed_args.command == "quickstart":
        return handle_quickstart(parsed_args)

    return 0


def get_adapter(args):
    """Helper to create adapter from args."""
    if args.platform == "duckdb":
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

            # Check for required env vars (CLI doesn't have args for these)
            host = os.environ.get("DATABRICKS_HOST")
            http_path = os.environ.get("DATABRICKS_HTTP_PATH")
            token = os.environ.get("DATABRICKS_TOKEN")

            if not (host and http_path and token):
                raise ValueError(
                    "Databricks connection requires environment variables: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN"
                )

            print("Connecting to Databricks...")
            conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
            catalog = os.environ.get("DATABRICKS_CATALOG")
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
        manager.initialize(reset=args.reset)

        print("✅ Metadata tables initialized.")
        return 0
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
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
            statements = config_to_sql(config)
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

    return 0


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

        return 0 if "SUCCESS" in result.status else 1

    finally:
        adapter.close()


def handle_serve(args) -> int:
    """Handle serve command."""
    try:
        import uvicorn
    except ImportError:
        print(
            "❌ 'api' extra not installed. Run: pip install 'sql-identity-resolution[api]'",
            file=sys.stderr,
        )
        return 1

    print(f"🚀 Starting IDR API Server on {args.host}:{args.port}")
    uvicorn.run("idr_api.main:app", host=args.host, port=args.port, reload=args.reload)
    return 0


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


if __name__ == "__main__":
    sys.exit(main())
