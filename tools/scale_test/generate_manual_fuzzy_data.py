import os
import shutil
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq


def generate_manual_data():
    out_dir = "test_data/manual_fuzzy"
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    os.makedirs(f"{out_dir}/digital_customer_account", exist_ok=True)
    os.makedirs(f"{out_dir}/pos_customer", exist_ok=True)

    # --- Data Definition ---
    # Case 1: Standard Exact Match (Control)
    # Robert Smith -> Robert Smith (Same Email)

    # Case 2: Fuzzy Name Match (James vs Jim, Different Email, Different Phone)
    # This MUST match via Jaro-Winkler on First Name (James vs Jim is > 0.70 probably, or we use a better pair)
    # Let's use "Robert" vs "Robertt" (Typo) for safety or "James" vs "Jim" if threshold allows.
    # Config threshold is 0.70.
    # James vs Jim Jaro-Winkler is ~0.78. So it should match.

    # Case 3: Fuzzy Email Match (Typo in Email)
    # Sarah Jones -> Sarah Jones (sarah.jones@gmail.com vs sarah.jones@gmil.com)

    # Case 4: Non-Match (Control)
    # Michael Brown vs David Wilson

    digital_rows = [
        # ID, First, Last, Email, Phone
        ("D1", "Robert", "Smith", "robert.smith@gmail.com", "555-1000"),  # Clean
        ("D2", "James", "Johnson", "james.j@gmail.com", "555-2000"),  # Fuzzy Candidate A
        ("D3", "Sarah", "Jones", "sarah.jones@gmail.com", "555-3000"),  # Fuzzy Email Base
        ("D4", "Michael", "Brown", "mike.brown@gmail.com", "555-4000"),  # Unique
    ]

    pos_rows = [
        ("P1", "Robert", "Smith", "robert.smith@gmail.com", "555-1000"),  # Exact Match to D1
        (
            "P2",
            "Jim",
            "Johnson",
            "jim.johnson@yahoo.com",
            "555-2001",
        ),  # Fuzzy Match to D2 (Name var, diff email/phone)
        ("P3", "Sarah", "Jones", "sarah.jones@gmil.com", "555-3001"),  # Fuzzy Email Match to D3
        ("P4", "David", "Wilson", "david.wilson@gmail.com", "555-5000"),  # Unique
    ]

    # Convert to PyArrow Tables
    cols = ["customer_id", "first_name", "last_name", "email", "phone"]

    # Digital
    d_data = {
        "customer_id": [r[0] for r in digital_rows],
        "first_name": [r[1] for r in digital_rows],
        "last_name": [r[2] for r in digital_rows],
        "email": [r[3] for r in digital_rows],
        "phone": [r[4] for r in digital_rows],
        "created_at": [datetime.now().isoformat()] * len(digital_rows),
    }
    tbl_d = pa.Table.from_pydict(d_data)
    pq.write_table(tbl_d, f"{out_dir}/digital_customer_account/data.parquet")

    # POS
    p_data = {
        "customer_id": [r[0] for r in pos_rows],
        "first_name": [r[1] for r in pos_rows],
        "last_name": [r[2] for r in pos_rows],
        "email": [r[3] for r in pos_rows],
        "phone": [r[4] for r in pos_rows],
        "created_at": [datetime.now().isoformat()] * len(pos_rows),
    }
    tbl_p = pa.Table.from_pydict(p_data)
    pq.write_table(tbl_p, f"{out_dir}/pos_customer/data.parquet")

    print(f"Manual test data generated in {out_dir}")


if __name__ == "__main__":
    generate_manual_data()
