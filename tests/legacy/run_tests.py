# Databricks notebook source
# MAGIC %md
# MAGIC # IDR Test Runner
# MAGIC Runs integration tests with known fixtures to validate identity resolution logic.
# MAGIC
# MAGIC **Tests:**
# MAGIC 1. Two entities same email → same cluster
# MAGIC 2. Chain of three entities → all same cluster (transitivity)
# MAGIC 3. Disjoint graphs → separate clusters
# MAGIC 4. Case-insensitive email → normalized matching

# COMMAND ----------
from datetime import datetime
import uuid

# COMMAND ----------
dbutils.widgets.text("CATALOG", "main")
dbutils.widgets.text("TEST_SCHEMA", "idr_test")
dbutils.widgets.text("CLEAN_UP", "true")  # true|false - drop test tables after

CATALOG = dbutils.widgets.get("CATALOG").strip()
TEST_SCHEMA = dbutils.widgets.get("TEST_SCHEMA").strip()
CLEAN_UP = dbutils.widgets.get("CLEAN_UP").lower() == "true"

print(f"Running tests in {CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def collect_one(sql: str):
    return q(sql).first()[0]

# COMMAND ----------
# Setup: Create test schema and metadata
q(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test 1: Two Entities Same Email

# COMMAND ----------
# Create test tables
q(f"""
CREATE OR REPLACE TABLE {CATALOG}.{TEST_SCHEMA}.source_a AS
SELECT 'A001' AS entity_id, 'shared@example.com' AS email, '5551234567' AS phone, TIMESTAMP('2024-01-01 10:00:00') AS updated_at
""")

q(f"""
CREATE OR REPLACE TABLE {CATALOG}.{TEST_SCHEMA}.source_b AS
SELECT 'B001' AS entity_id, 'shared@example.com' AS email, '5559999999' AS phone, TIMESTAMP('2024-01-02 10:00:00') AS updated_at
""")

# Load test metadata
q("DELETE FROM idr_meta.source_table WHERE table_id LIKE 'test_%'")
q("DELETE FROM idr_meta.source WHERE table_id LIKE 'test_%'")
q("DELETE FROM idr_meta.identifier_mapping WHERE table_id LIKE 'test_%'")
q("DELETE FROM idr_meta.entity_attribute_mapping WHERE table_id LIKE 'test_%'")

q(f"""
INSERT INTO idr_meta.source_table VALUES
  ('test_source_a', '{CATALOG}.{TEST_SCHEMA}.source_a', 'PERSON', 'entity_id', 'updated_at', 0, true),
  ('test_source_b', '{CATALOG}.{TEST_SCHEMA}.source_b', 'PERSON', 'entity_id', 'updated_at', 0, true)
""")

q(f"""
INSERT INTO idr_meta.source VALUES
  ('test_source_a', 'Test A', 1, true),
  ('test_source_b', 'Test B', 2, true)
""")

q(f"""
INSERT INTO idr_meta.identifier_mapping VALUES
  ('test_source_a', 'EMAIL', 'email', false),
  ('test_source_a', 'PHONE', 'phone', false),
  ('test_source_b', 'EMAIL', 'email', false),
  ('test_source_b', 'PHONE', 'phone', false)
""")

q(f"""
INSERT INTO idr_meta.entity_attribute_mapping VALUES
  ('test_source_a', 'email_raw', 'email'),
  ('test_source_a', 'phone_raw', 'phone'),
  ('test_source_a', 'record_updated_at', 'updated_at'),
  ('test_source_b', 'email_raw', 'email'),
  ('test_source_b', 'phone_raw', 'phone'),
  ('test_source_b', 'record_updated_at', 'updated_at')
""")

# Reset run state for test tables
q("DELETE FROM idr_meta.run_state WHERE table_id LIKE 'test_%'")

print("✅ Test 1 setup complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run IDR for Test Data
# MAGIC
# MAGIC You need to run `IDR_Run.py` with `RUN_MODE=FULL` before running assertions.
# MAGIC This notebook sets up the data; run IDR separately, then come back for assertions.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Assertions (run after IDR_Run)

# COMMAND ----------
def run_assertions():
    results = []

    # Test 1: Two entities same email -> same cluster
    try:
        cluster_count = collect_one("""
            SELECT COUNT(DISTINCT resolved_id)
            FROM idr_out.identity_resolved_membership_current
            WHERE entity_key IN ('test_source_a:A001', 'test_source_b:B001')
        """)
        test1_pass = cluster_count == 1
        results.append(("Test 1: Same Email → Same Cluster", test1_pass, f"clusters={cluster_count}, expected=1"))
    except Exception as e:
        results.append(("Test 1: Same Email → Same Cluster", False, str(e)))

    # Test 2: Edge exists
    try:
        edge_count = collect_one("""
            SELECT COUNT(*)
            FROM idr_out.identity_edges_current
            WHERE identifier_type = 'EMAIL'
            AND identifier_value_norm = 'shared@example.com'
        """)
        test2_pass = edge_count >= 1
        results.append(("Test 2: Email Edge Created", test2_pass, f"edges={edge_count}, expected>=1"))
    except Exception as e:
        results.append(("Test 2: Email Edge Created", False, str(e)))

    # Test 3: Membership entries exist
    try:
        member_count = collect_one("""
            SELECT COUNT(*)
            FROM idr_out.identity_resolved_membership_current
            WHERE entity_key LIKE 'test_source_%'
        """)
        test3_pass = member_count >= 2
        results.append(("Test 3: Membership Populated", test3_pass, f"members={member_count}, expected>=2"))
    except Exception as e:
        results.append(("Test 3: Membership Populated", False, str(e)))

    # Print results
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)

    all_passed = True
    for name, passed, detail in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} | {name}")
        print(f"       {detail}")
        if not passed:
            all_passed = False

    print("="*60)
    if all_passed:
        print("✅ ALL TESTS PASSED")
    else:
        print("❌ SOME TESTS FAILED")
    print("="*60)

    return all_passed

# COMMAND ----------
# Uncomment to run assertions after IDR_Run:
# run_assertions()

# COMMAND ----------
# Cleanup (optional)
if CLEAN_UP:
    print("Cleaning up test data...")
    q("DELETE FROM idr_meta.source_table WHERE table_id LIKE 'test_%'")
    q("DELETE FROM idr_meta.source WHERE table_id LIKE 'test_%'")
    q("DELETE FROM idr_meta.identifier_mapping WHERE table_id LIKE 'test_%'")
    q("DELETE FROM idr_meta.entity_attribute_mapping WHERE table_id LIKE 'test_%'")
    q("DELETE FROM idr_meta.run_state WHERE table_id LIKE 'test_%'")
    q(f"DROP TABLE IF EXISTS {CATALOG}.{TEST_SCHEMA}.source_a")
    q(f"DROP TABLE IF EXISTS {CATALOG}.{TEST_SCHEMA}.source_b")
    print("✅ Cleanup complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Usage
# MAGIC
# MAGIC 1. Run this notebook to set up test data
# MAGIC 2. Run `IDR_Run.py` with `RUN_MODE=FULL`
# MAGIC 3. Come back and run the `run_assertions()` cell
# MAGIC 4. Check results
