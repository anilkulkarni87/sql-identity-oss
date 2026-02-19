import duckdb


def verify():
    con = duckdb.connect("fuzzy_test.db")

    # 1. Check total resolved entities
    print("--- Stats ---")
    total = con.execute(
        "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
    ).fetchone()[0]
    print(f"Total Resolved Entities: {total}")

    # 2. Check for clusters > 1 (Matches)
    clusters = con.execute("""
        SELECT cluster_size, COUNT(*)
        FROM (
            SELECT resolved_id, COUNT(*) as cluster_size
            FROM idr_out.identity_resolved_membership_current
            GROUP BY resolved_id
        )
        GROUP BY cluster_size
        ORDER BY cluster_size
    """).fetchall()
    print("\n--- Cluster Size Distribution ---")
    for size, count in clusters:
        print(f"Size {size}: {count} clusters")

    # 3. Inspect some fuzzy matches
    print("\n--- Sample Fuzzy Matches ---")
    # Join resolved_entities with itself to find pairs in same cluster but different source IDs
    # and print their names/emails to see if they were typos.

    # We need to join back to source tables to get PII.
    # identity_resolved_membership_current(entity_key, resolved_id, ...)

    # Create a view of all source data
    con.execute("""
        CREATE OR REPLACE VIEW all_source AS
        SELECT CAST(customer_id AS VARCHAR) as id, first_name, last_name, email, 'digital' as table_name FROM retail.digital_customer_account
        UNION ALL
        SELECT CAST(customer_id AS VARCHAR) as id, first_name, last_name, email, 'pos' as table_name FROM retail.pos_customer
    """)

    sample = con.execute("""
        WITH clusters AS (
            SELECT resolved_id
            FROM idr_out.identity_resolved_membership_current
            GROUP BY resolved_id
            HAVING COUNT(*) > 1
            LIMIT 10
        )
        SELECT
            r.resolved_id,
            s.id,
            s.first_name,
            s.last_name,
            s.email
        FROM idr_out.identity_resolved_membership_current r
        JOIN clusters c ON r.resolved_id = c.resolved_id
        JOIN all_source s ON split_part(r.entity_key, ':', 2) = s.id AND split_part(r.entity_key, ':', 1) = s.table_name
        ORDER BY r.resolved_id
    """).fetchall()

    for row in sample:
        print(row)


if __name__ == "__main__":
    verify()
