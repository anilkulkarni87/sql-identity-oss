#!/usr/bin/env python3
"""
Generate demo data for SQL Identity Resolution.

Creates a small (10K rows) sample dataset that demonstrates
identity resolution with realistic retail patterns and PROPER CLUSTERS.
"""

import argparse
import hashlib
import random
from datetime import datetime, timedelta


def generate_demo_data(db_path: str, rows: int = 10000, seed: int = 42):
    """Generate demo data directly into DuckDB with proper clustering."""
    import duckdb

    rng = random.Random(seed)

    # Data pools
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
    ]
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]
    sources = ["web", "store", "mobile", "call_center"]

    conn = duckdb.connect(db_path)

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
        # Determine cluster size
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

        # Generate anchor identity (first entity in cluster)
        anchor_first = rng.choice(first_names)
        anchor_last = rng.choice(last_names)
        anchor_email = (
            f"{anchor_first.lower()}.{anchor_last.lower()}{entity_idx}@{rng.choice(domains)}"
        )
        anchor_phone = f"{rng.randint(200,999)}{rng.randint(200,999)}{rng.randint(1000,9999)}"
        anchor_loyalty = f"LYL{rng.randint(100000, 999999)}" if rng.random() < 0.7 else None

        for i in range(cluster_size):
            entity_idx += 1
            entity_id = hashlib.md5(f"{seed}:{entity_idx}".encode()).hexdigest()[:16]
            source = rng.choice(sources)

            if i == 0:
                # Anchor entity - has all identifiers
                e_first = anchor_first
                e_last = anchor_last
                e_email = anchor_email
                e_phone = anchor_phone
                e_loyalty = anchor_loyalty
            else:
                # Connected entities - SHARE at least one identifier
                # Decide which identifier(s) to share
                share_email = rng.random() < 0.6  # 60% share email
                share_phone = rng.random() < 0.4  # 40% share phone
                share_loyalty = rng.random() < 0.3 and anchor_loyalty  # 30% share loyalty

                # Must share at least one
                if not share_email and not share_phone and not share_loyalty:
                    share_email = True  # Force email match

                # Generate name (can be different - representing typos or variations)
                if rng.random() < 0.7:
                    e_first = anchor_first
                    e_last = anchor_last
                else:
                    e_first = rng.choice(first_names)
                    e_last = anchor_last  # Usually same last name

                # Apply sharing rules
                if share_email:
                    e_email = anchor_email  # SAME email - will match!
                else:
                    e_email = f"{e_first.lower()}{rng.randint(1,999)}@{rng.choice(domains)}"

                if share_phone:
                    e_phone = anchor_phone  # SAME phone - will match!
                else:
                    e_phone = (
                        f"{rng.randint(200,999)}{rng.randint(200,999)}{rng.randint(1000,9999)}"
                    )

                if share_loyalty and anchor_loyalty:
                    e_loyalty = anchor_loyalty  # SAME loyalty - will match!
                else:
                    e_loyalty = f"LYL{rng.randint(100000, 999999)}" if rng.random() < 0.5 else None

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

    # Insert data
    conn.executemany(
        """
        INSERT INTO demo_customers VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        entities,
    )

    # Count expected clusters (entities with cluster_size as generated)
    print(f"  ✓ Generated {len(entities):,} customer records")

    # Configure IDR metadata
    conn.execute("DELETE FROM idr_meta.source_table WHERE table_id = 'demo'")
    conn.execute("DELETE FROM idr_meta.identifier_mapping WHERE table_id = 'demo'")
    conn.execute("DELETE FROM idr_meta.run_state WHERE table_id = 'demo'")

    # Register source
    conn.execute("""
        INSERT INTO idr_meta.source_table VALUES
        ('demo', 'demo_customers', 'PERSON', 'customer_id', 'created_at', 0, TRUE)
    """)

    # Ensure rules exist - 9 columns: rule_id, rule_name, is_active, priority, identifier_type, canonicalize, allow_hashed, require_non_null, max_group_size
    conn.execute("DELETE FROM idr_meta.rule")
    conn.execute("""
        INSERT INTO idr_meta.rule
        (rule_id, rule_name, is_active, priority, identifier_type, canonicalize, allow_hashed, require_non_null, max_group_size)
        VALUES
        ('email_rule', 'Email Match', TRUE, 1, 'EMAIL', 'LOWERCASE', TRUE, TRUE, 10000),
        ('phone_rule', 'Phone Match', TRUE, 2, 'PHONE', 'NONE', TRUE, TRUE, 5000),
        ('loyalty_rule', 'Loyalty Match', TRUE, 3, 'LOYALTY', 'NONE', TRUE, TRUE, 100)
    """)

    # Add mappings
    conn.execute("DELETE FROM idr_meta.identifier_mapping WHERE table_id = 'demo'")
    conn.execute("""
        INSERT INTO idr_meta.identifier_mapping VALUES
        ('demo', 'EMAIL', 'email', TRUE),
        ('demo', 'PHONE', 'phone', TRUE),
        ('demo', 'LOYALTY', 'loyalty_id', TRUE)
    """)

    print("  ✓ Configured IDR metadata")

    # Show expected cluster stats
    email_groups = conn.execute("""
        SELECT COUNT(*) as groups, SUM(cnt) as entities
        FROM (SELECT email, COUNT(*) as cnt FROM demo_customers WHERE email IS NOT NULL GROUP BY email HAVING COUNT(*) > 1)
    """).fetchone()
    phone_groups = conn.execute("""
        SELECT COUNT(*) as groups, SUM(cnt) as entities
        FROM (SELECT phone, COUNT(*) as cnt FROM demo_customers WHERE phone IS NOT NULL GROUP BY phone HAVING COUNT(*) > 1)
    """).fetchone()

    print(
        f"  ℹ️  Email groups with >1 entity: {email_groups[0] or 0} ({email_groups[1] or 0} entities)"
    )
    print(
        f"  ℹ️  Phone groups with >1 entity: {phone_groups[0] or 0} ({phone_groups[1] or 0} entities)"
    )

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Generate demo data")
    parser.add_argument("--db", required=True, help="DuckDB database path")
    parser.add_argument("--rows", type=int, default=10000, help="Number of rows")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    generate_demo_data(args.db, args.rows, args.seed)


if __name__ == "__main__":
    main()
