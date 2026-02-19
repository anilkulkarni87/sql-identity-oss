import argparse
import os
import sys
import zipfile

# Add project root to path to import idr_core
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.config import config_to_sql, load_config


def create_zip_package(output_path: str):
    """Create zip package of idr_core."""
    print(f"Creating runner package: {output_path}")
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk("idr_core"):
            # Skip pycache
            if "__pycache__" in root:
                continue

            for file in files:
                if file.endswith(".pyc"):
                    continue
                file_path = os.path.join(root, file)
                zf.write(file_path, file_path)  # Preserve path inside zip


def main():
    parser = argparse.ArgumentParser(description="Build IDR Deployment Script")
    parser.add_argument("--config", required=True, help="YAML configuration file")
    parser.add_argument(
        "--platform",
        default="snowflake",
        choices=["snowflake", "duckdb", "bigquery", "databricks"],
        help="Target platform",
    )
    parser.add_argument("--output-sql", help="Output SQL script (default: deploy_{platform}.sql)")
    parser.add_argument(
        "--output-zip", default="idr_core.zip", help="Output Zip package (Snowflake only)"
    )
    args = parser.parse_args()

    # Default output filename if not provided
    if not args.output_sql:
        args.output_sql = f"deploy_{args.platform}.sql"

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)

    # 1. Create Zip (Snowflake Only)
    if args.platform == "snowflake":
        create_zip_package(args.output_zip)

    # 2. Generate Deployment SQL
    print(f"Generating deployment script for {args.platform.upper()}: {args.output_sql}")
    with open(args.output_sql, "w") as out_f:
        # Header
        out_f.write("-- ========================================================\n")
        out_f.write(f"-- IDR Deployment Script for {args.platform.upper()}\n")
        out_f.write(f"-- Config: {args.config}\n")
        out_f.write("-- ========================================================\n\n")

        # A. DDL (Base Tables)
        out_f.write("-- [Step 1] Base Schema & Tables\n")
        ddl_path = os.path.join("sql", "ddl", f"{args.platform}.sql")

        if not os.path.exists(ddl_path):
            print(f"Error: DDL file not found: {ddl_path}")
            sys.exit(1)

        with open(ddl_path, "r") as ddl_f:
            out_f.write(ddl_f.read())
        out_f.write("\n\n")

        # B. Config Rules (Dynamic)
        out_f.write("-- [Step 2] Configuration Rules\n")
        try:
            config = load_config(args.config)
            # Pass platform as dialect
            statements = config_to_sql(config, dialect=args.platform)
            for stmt in statements:
                out_f.write(stmt.strip())
                out_f.write(";\n")
        except Exception as e:
            print(f"Error loading config: {e}")
            sys.exit(1)
        out_f.write("\n\n")

        # C. Stored Procedure (Snowflake Only)
        if args.platform == "snowflake":
            out_f.write("-- [Step 3] Stored Procedure Registry\n")
            runner_sql_path = os.path.join("runners", "deploy_snowflake.sql")
            with open(runner_sql_path, "r") as run_f:
                out_f.write(run_f.read())

    print("\nBuild Complete!")
    print("-------------------------------------------------------------")
    if args.platform == "snowflake":
        print(
            f"1. UPLOAD: PUT file://{args.output_zip} @idr_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
        )
        print(f"2. DEPLOY: Run file '{args.output_sql}' in Snowflake.")
    elif args.platform == "duckdb":
        print(f"1. DEPLOY: duckdb your_db.db < {args.output_sql}")
    elif args.platform == "bigquery":
        print(f"1. DEPLOY: Run '{args.output_sql}' in Cloud Console or bq query.")
    elif args.platform == "databricks":
        print(f"1. DEPLOY: Run '{args.output_sql}' in a Notebook.")
    print("-------------------------------------------------------------")


if __name__ == "__main__":
    main()
