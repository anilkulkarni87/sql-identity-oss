import glob
import os

import duckdb


def load_data():
    db_path = "fuzzy_retail.duckdb"
    data_dir = "test_data/fuzzy_10k"

    if os.path.exists(db_path):
        os.remove(db_path)

    con = duckdb.connect(db_path)

    print(f"Creating database at {db_path}...")
    con.execute("CREATE SCHEMA IF NOT EXISTS retail")

    # Check if data exists
    if not os.path.exists(data_dir):
        print(f"Error: Data directory {data_dir} not found. Run generation script first.")
        return

    tables = {
        "digital_customer_account": "digital_customer_account",
        "pos_customer": "pos_customer",
    }

    for table_name, folder in tables.items():
        path = os.path.join(data_dir, folder, "*.parquet")
        # specific check to see if files exist, as read_parquet might throw if empty
        files = glob.glob(path)
        if not files:
            print(f"Warning: No parquet files found for {table_name} at {path}")
            continue

        print(f"Loading {table_name}...")
        con.execute(f"""
            CREATE OR REPLACE TABLE retail.{table_name} AS
            SELECT * FROM read_parquet('{path}')
        """)

        count = con.execute(f"SELECT COUNT(*) FROM retail.{table_name}").fetchone()[0]
        print(f"  - Loaded {count} rows")

    con.close()
    print("Database ready!")


if __name__ == "__main__":
    load_data()
