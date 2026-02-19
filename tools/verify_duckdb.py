import duckdb

con = duckdb.connect("retail_test.duckdb")

print("--- Fuzzy Rules ---")
print(con.sql("SELECT * FROM idr_meta.fuzzy_rule").df())

print("\n--- Fuzzy Edges ---")
print(con.sql("SELECT * FROM idr_work.fuzzy_edges LIMIT 10").df())

print("\n--- Super Cluster Assignments ---")
print(
    con.sql("""
    SELECT resolved_id, super_cluster_id, count(*) as count
    FROM idr_out.identity_resolved_membership_current
    WHERE super_cluster_id IS NOT NULL
      AND super_cluster_id != resolved_id
    GROUP BY 1,2
    ORDER BY 3 DESC
    LIMIT 10
""").df()
)

print("\n--- Any Super Clusters? ---")
print(
    con.sql("""
    SELECT count(*)
    FROM idr_out.identity_resolved_membership_current
    WHERE super_cluster_id IS NOT NULL
    AND super_cluster_id != resolved_id
""").df()
)
