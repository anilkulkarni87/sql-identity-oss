import duckdb


def check_error():
    con = duckdb.connect("fuzzy_test.db")
    try:
        # Get the most recent run
        run = con.execute("""
            SELECT run_id, status, error_message, started_at
            FROM idr_out.run_history
            ORDER BY started_at DESC
            LIMIT 1
        """).fetchone()

        if run:
            print(f"Run ID: {run[0]}")
            print(f"Status: {run[1]}")
            print(f"Error: {run[2]}")
        else:
            print("No runs found in history.")
    except Exception as e:
        print(f"Failed to read history: {e}")


if __name__ == "__main__":
    check_error()
