"""
Quickstart module for SQL Identity Resolution.

Provides a one-command experience to:
1. Create a DuckDB database with demo retail data
2. Initialize IDR schemas and metadata
3. Run the identity resolution pipeline
4. Print a summary of results

Usage:
    idr quickstart
    idr quickstart --rows=5000 --output=my_demo.duckdb
"""

import hashlib
import os
import random
from datetime import datetime, timedelta


def generate_demo_data(conn, rows: int = 10000, seed: int = 42):
    """Generate realistic retail customer data with proper clustering patterns."""
    rng = random.Random(seed)

    first_names = [
        "Emma",
        "Liam",
        "Olivia",
        "Noah",
        "Ava",
        "Ethan",
        "Sophia",
        "Mason",
        "Isabella",
        "William",
        "Mia",
        "James",
        "Charlotte",
        "Benjamin",
        "Amelia",
        "Alexander",
        "Harper",
        "Daniel",
        "Evelyn",
        "Henry",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Wilson",
        "Anderson",
        "Taylor",
        "Thomas",
        "Jackson",
        "White",
        "Harris",
        "Clark",
    ]
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com"]
    sources = ["web", "store", "mobile", "call_center"]

    # Create demo source table
    conn.execute("""
        CREATE OR REPLACE TABLE demo_customers (
            customer_id VARCHAR,
            source_system VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            loyalty_id VARCHAR,
            created_at TIMESTAMP
        )
    """)

    # Generate clusters with SHARED identifiers
    entities = []
    entity_idx = 0
    generated = 0

    # Distribution: 35% singletons, 25% pairs, 25% small (3-5), 15% medium (6-10)
    while generated < rows:
        r = rng.random()
        if r < 0.35:
            cluster_size = 1
        elif r < 0.60:
            cluster_size = 2
        elif r < 0.85:
            cluster_size = rng.randint(3, 5)
        else:
            cluster_size = rng.randint(6, 10)

        cluster_size = min(cluster_size, rows - generated)

        # Anchor identity
        anchor_first = rng.choice(first_names)
        anchor_last = rng.choice(last_names)
        anchor_email = (
            f"{anchor_first.lower()}.{anchor_last.lower()}{entity_idx}@{rng.choice(domains)}"
        )
        anchor_phone = f"{rng.randint(200, 999)}{rng.randint(200, 999)}{rng.randint(1000, 9999)}"
        anchor_loyalty = f"LYL{rng.randint(100000, 999999)}" if rng.random() < 0.7 else None

        for i in range(cluster_size):
            entity_idx += 1
            entity_id = hashlib.md5(f"{seed}:{entity_idx}".encode()).hexdigest()[:16]
            source = rng.choice(sources)

            if i == 0:
                e_first, e_last = anchor_first, anchor_last
                e_email, e_phone, e_loyalty = anchor_email, anchor_phone, anchor_loyalty
            else:
                share_email = rng.random() < 0.6
                share_phone = rng.random() < 0.4
                share_loyalty = rng.random() < 0.3 and anchor_loyalty

                if not share_email and not share_phone and not share_loyalty:
                    share_email = True

                e_first = anchor_first if rng.random() < 0.7 else rng.choice(first_names)
                e_last = anchor_last
                e_email = (
                    anchor_email
                    if share_email
                    else f"{e_first.lower()}{rng.randint(1, 999)}@{rng.choice(domains)}"
                )
                e_phone = (
                    anchor_phone
                    if share_phone
                    else f"{rng.randint(200, 999)}{rng.randint(200, 999)}{rng.randint(1000, 9999)}"
                )
                e_loyalty = (
                    anchor_loyalty
                    if share_loyalty
                    else (f"LYL{rng.randint(100000, 999999)}" if rng.random() < 0.5 else None)
                )

            entities.append(
                (
                    entity_id,
                    source,
                    e_first,
                    e_last,
                    e_email,
                    e_phone,
                    e_loyalty,
                    datetime.now() - timedelta(days=rng.randint(1, 1000)),
                )
            )
            generated += 1

    conn.executemany("INSERT INTO demo_customers VALUES (?, ?, ?, ?, ?, ?, ?, ?)", entities)
    return len(entities)


def configure_metadata(conn):
    """Set up IDR metadata tables for the demo data."""
    # Clean existing demo config
    for table in ["source_table", "identifier_mapping", "run_state"]:
        try:
            conn.execute(f"DELETE FROM idr_meta.{table} WHERE table_id = 'demo'")
        except Exception:
            pass

    # Register source
    # Columns: table_id, table_fqn, entity_type, entity_key_expr, watermark_column,
    #          watermark_lookback_minutes, is_active
    conn.execute("""
        INSERT INTO idr_meta.source_table
        (table_id, table_fqn, entity_type, entity_key_expr, watermark_column,
         watermark_lookback_minutes, is_active)
        VALUES
        ('demo', 'demo_customers', 'PERSON', 'customer_id', 'created_at', 0, TRUE)
    """)

    # Set up rules
    # Columns: rule_id, identifier_type, canonicalize, max_group_size, priority, is_active
    conn.execute("DELETE FROM idr_meta.rule")
    conn.execute("""
        INSERT INTO idr_meta.rule
        (rule_id, identifier_type, canonicalize, max_group_size, priority, is_active)
        VALUES
        ('email_rule', 'EMAIL', 'LOWERCASE', 10000, 1, TRUE),
        ('phone_rule', 'PHONE', 'NONE', 5000, 2, TRUE),
        ('loyalty_rule', 'LOYALTY', 'NONE', 100, 3, TRUE)
    """)

    # Map identifiers
    # Columns: table_id, identifier_type, identifier_value_expr, is_hashed
    conn.execute("DELETE FROM idr_meta.identifier_mapping WHERE table_id = 'demo'")
    conn.execute("""
        INSERT INTO idr_meta.identifier_mapping
        (table_id, identifier_type, identifier_value_expr, is_hashed)
        VALUES
        ('demo', 'EMAIL', 'email', FALSE),
        ('demo', 'PHONE', 'phone', FALSE),
        ('demo', 'LOYALTY', 'loyalty_id', FALSE)
    """)


def print_results(conn, result, duration):
    """Print a beautiful summary of quickstart results."""
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║           🔗 Identity Resolution Complete!              ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # Run stats
    print(f"  Run ID:       {result.run_id}")
    print(f"  Status:       {result.status}")
    print(f"  Duration:     {duration:.1f}s")
    print()

    # Pipeline stats
    print("  ┌─────────────────────────────────────────────┐")
    print(f"  │  Entities processed:  {result.entities_processed:>10,}          │")
    print(f"  │  Edges created:       {result.edges_created:>10,}          │")
    print(f"  │  Clusters formed:     {result.clusters_impacted:>10,}          │")
    print("  └─────────────────────────────────────────────┘")
    print()

    # Cluster size distribution
    try:
        dist = conn.execute("""
            SELECT
                CASE
                    WHEN cluster_size = 1 THEN '1 (singletons)'
                    WHEN cluster_size = 2 THEN '2 (pairs)'
                    WHEN cluster_size <= 5 THEN '3-5 (small)'
                    WHEN cluster_size <= 10 THEN '6-10 (medium)'
                    ELSE '11+ (large)'
                END as bucket,
                COUNT(*) as clusters,
                SUM(cluster_size) as entities
            FROM idr_out.identity_clusters_current
            GROUP BY 1
            ORDER BY MIN(cluster_size)
        """).fetchall()

        print("  Cluster Size Distribution:")
        print("  ─────────────────────────────────────────────")
        print(f"  {'Bucket':<20} {'Clusters':>10} {'Entities':>10}")
        print("  ─────────────────────────────────────────────")
        for row in dist:
            print(f"  {row[0]:<20} {row[1]:>10,} {row[2]:>10,}")
        print()
    except Exception:
        pass

    # Identifier type breakdown
    try:
        edge_types = conn.execute("""
            SELECT identifier_type, COUNT(*) as edges
            FROM idr_out.identity_edges_current
            GROUP BY 1
            ORDER BY 2 DESC
        """).fetchall()

        print("  Edges by Identifier Type:")
        print("  ─────────────────────────────────────────────")
        for row in edge_types:
            print(f"    {row[0]:<15} {row[1]:>8,} edges")
        print()
    except Exception:
        pass


def run_quickstart(
    output: str = "quickstart_demo.duckdb",
    rows: int = 10000,
    seed: int = 42,
    verbose: bool = False,
):
    """Run the complete quickstart flow."""
    import time

    try:
        import duckdb
    except ImportError:
        print("❌ DuckDB not installed. Run: pip install 'sql-identity-resolution[duckdb]'")
        return 1

    from idr_core.adapters.duckdb import DuckDBAdapter
    from idr_core.runner import IDRRunner, RunConfig
    from idr_core.schema_manager import SchemaManager

    print()
    print("🔗 SQL Identity Resolution — Quickstart")
    print("=" * 55)
    print()

    # Step 1: Create database
    # Remove existing to start fresh
    if os.path.exists(output):
        os.remove(output)
    if os.path.exists(f"{output}.wal"):
        os.remove(f"{output}.wal")

    conn = duckdb.connect(output)
    adapter = DuckDBAdapter(conn)
    print(f"  📁 Database: {os.path.abspath(output)}")
    print()

    # Step 2: Initialize schemas
    print("  Step 1/4: Initializing schemas...")
    schema_mgr = SchemaManager(adapter)
    schema_mgr.initialize(reset=True)
    print("    ✓ Schemas created (idr_meta, idr_work, idr_out)")
    print()

    # Step 3: Generate demo data
    print(f"  Step 2/4: Generating {rows:,} demo customer records...")
    count = generate_demo_data(conn, rows=rows, seed=seed)
    print(f"    ✓ {count:,} records across 4 source systems (web, store, mobile, call_center)")
    print()

    # Step 4: Configure metadata
    print("  Step 3/4: Configuring matching rules...")
    configure_metadata(conn)
    print("    ✓ 3 rules configured: EMAIL, PHONE, LOYALTY")
    print()

    # Step 5: Run IDR
    print("  Step 4/4: Running identity resolution...")
    start = time.time()
    runner = IDRRunner(adapter)
    config = RunConfig(run_mode="FULL", strict=False)
    result = runner.run(config)
    duration = time.time() - start

    if "SUCCESS" in result.status:
        print(f"    ✓ Completed in {duration:.1f}s")
        print_results(conn, result, duration)
    else:
        print(f"    ❌ Run failed: {result.status}")
        if result.error:
            print(f"    Error: {result.error}")
        conn.close()
        return 1

    # Next steps
    print("  🚀 What's Next?")
    print("  ─────────────────────────────────────────────")
    print(f"    • Explore results:   idr run --platform=duckdb --db={output} --mode=FULL --dry-run")
    print("    • Start the UI:      idr serve  (then open http://localhost:8000)")
    print(f"    • API + Dashboard:   Connect to '{output}' in the Setup Wizard")
    print(f"    • Query directly:    duckdb {output}")
    print(
        f"    • MCP for AI agents: IDR_PLATFORM=duckdb IDR_DATABASE={os.path.abspath(output)} idr mcp"
    )
    print()

    conn.close()
    return 0
