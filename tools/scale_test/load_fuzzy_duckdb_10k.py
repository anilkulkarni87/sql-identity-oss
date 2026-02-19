import os

import duckdb


def load_data():
    db_path = "fuzzy_test.db"

    # We don't necessarily need to delete the DB if we just DROP/CREATE schemas/tables,
    # but for a clean slate verification, removing it is safer.
    if os.path.exists(db_path):
        os.remove(db_path)

    con = duckdb.connect(db_path)

    print("Creating schema 'retail'...")
    con.execute("CREATE SCHEMA IF NOT EXISTS retail")

    print("Loading digital_customer_account...")
    con.execute("""
        CREATE OR REPLACE TABLE retail.digital_customer_account AS
        SELECT * FROM read_parquet('test_data/fuzzy_10k/digital_customer_account/*.parquet')
    """)

    print("Loading pos_customer...")
    con.execute("""
        CREATE OR REPLACE TABLE retail.pos_customer AS
        SELECT * FROM read_parquet('test_data/fuzzy_10k/pos_customer/*.parquet')
    """)

    print("Verifying counts:")
    print(
        "Digital:",
        con.execute("SELECT COUNT(*) FROM retail.digital_customer_account").fetchone()[0],
    )
    print("POS:", con.execute("SELECT COUNT(*) FROM retail.pos_customer").fetchone()[0])

    con.close()
    print(f"Database created at {db_path}")


if __name__ == "__main__":
    load_data()
