# Databricks notebook source
# MAGIC %md
# MAGIC # IDR Metadata Validator
# MAGIC
# MAGIC Validates metadata configuration before running IDR.
# MAGIC
# MAGIC **Checks:**
# MAGIC - Required tables exist and have data
# MAGIC - Foreign key relationships are valid
# MAGIC - Required fields are not NULL
# MAGIC - Source tables exist and are accessible
# MAGIC - Identifier types have matching rules
# MAGIC
# MAGIC **Output:** Clear error messages with fix suggestions

# COMMAND ----------

# COMMAND ----------
dbutils.widgets.text("FAIL_ON_WARNING", "false")  # true|false

FAIL_ON_WARNING = dbutils.widgets.get("FAIL_ON_WARNING").lower() == "true"


# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)


def collect(sql: str):
    return q(sql).collect()


def collect_one(sql: str):
    row = q(sql).first()
    return row[0] if row else None


def table_exists(fqn: str) -> bool:
    parts = fqn.split(".")
    if len(parts) != 3:
        return False
    catalog, schema, table = parts
    try:
        rows = collect(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'")
        return len(rows) > 0
    except Exception:
        return False


def table_in_schema(schema: str, table: str) -> bool:
    try:
        rows = collect(f"SHOW TABLES IN {schema} LIKE '{table}'")
        return len(rows) > 0
    except Exception:
        return False


# COMMAND ----------
class ValidationResult:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.info = []

    def error(self, check: str, message: str, fix: str = None):
        self.errors.append({"check": check, "message": message, "fix": fix})

    def warning(self, check: str, message: str, fix: str = None):
        self.warnings.append({"check": check, "message": message, "fix": fix})

    def ok(self, check: str, message: str):
        self.info.append({"check": check, "message": message})

    def print_results(self):
        print("=" * 70)
        print("METADATA VALIDATION RESULTS")
        print("=" * 70)

        if self.errors:
            print(f"\n❌ ERRORS ({len(self.errors)}):")
            for e in self.errors:
                print(f"   [{e['check']}] {e['message']}")
                if e["fix"]:
                    print(f"      💡 Fix: {e['fix']}")

        if self.warnings:
            print(f"\n⚠️ WARNINGS ({len(self.warnings)}):")
            for w in self.warnings:
                print(f"   [{w['check']}] {w['message']}")
                if w["fix"]:
                    print(f"      💡 Fix: {w['fix']}")

        print(f"\n✅ PASSED ({len(self.info)}):")
        for i in self.info:
            print(f"   [{i['check']}] {i['message']}")

        print("\n" + "=" * 70)
        if self.errors:
            print("❌ VALIDATION FAILED - Fix errors before running IDR")
        elif self.warnings:
            print("⚠️ VALIDATION PASSED WITH WARNINGS")
        else:
            print("✅ VALIDATION PASSED - Ready to run IDR")
        print("=" * 70)

    def has_errors(self):
        return len(self.errors) > 0

    def has_warnings(self):
        return len(self.warnings) > 0


result = ValidationResult()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check 1: Required Metadata Tables Exist

# COMMAND ----------
required_tables = {
    "source_table": ["table_id", "table_fqn", "entity_key_expr", "watermark_column", "is_active"],
    "source": ["table_id", "source_name", "trust_rank", "is_active"],
    "rule": ["rule_id", "identifier_type", "canonicalize", "is_active"],
    "identifier_mapping": ["table_id", "identifier_type", "identifier_value_expr"],
    "entity_attribute_mapping": ["table_id", "attribute_name", "attribute_expr"],
    "survivorship_rule": ["attribute_name", "strategy"],
}

for table, columns in required_tables.items():
    if table_in_schema("idr_meta", table):
        count = collect_one(f"SELECT COUNT(*) FROM idr_meta.{table}")
        if count == 0:
            result.error(
                f"table_{table}",
                f"idr_meta.{table} exists but is empty",
                f"Load data into idr_meta.{table} using IDR_LoadMetadata_Simple.py",
            )
        else:
            result.ok(f"table_{table}", f"idr_meta.{table} has {count} rows")
    else:
        result.error(
            f"table_{table}",
            f"idr_meta.{table} does not exist",
            "Run sql/common/00_ddl_meta.sql to create metadata tables",
        )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check 2: Source Tables Exist and Are Accessible

# COMMAND ----------
if table_in_schema("idr_meta", "source_table"):
    source_rows = collect("""
        SELECT table_id, table_fqn, is_active
        FROM idr_meta.source_table
    """)

    for row in source_rows:
        if row["is_active"]:
            if table_exists(row["table_fqn"]):
                result.ok(f"source_{row['table_id']}", f"Source table {row['table_fqn']} exists")
            else:
                result.error(
                    f"source_{row['table_id']}",
                    f"Source table {row['table_fqn']} does not exist",
                    "Create the table or update table_fqn in idr_meta.source_table",
                )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check 3: Identifier Mappings Reference Valid Tables

# COMMAND ----------
if table_in_schema("idr_meta", "source_table") and table_in_schema(
    "idr_meta", "identifier_mapping"
):
    active_tables = {
        r["table_id"]
        for r in collect("""
        SELECT table_id FROM idr_meta.source_table WHERE is_active = true
    """)
    }

    mappings = collect("SELECT table_id, identifier_type FROM idr_meta.identifier_mapping")

    for m in mappings:
        if m["table_id"] not in active_tables:
            result.error(
                "mapping_table_ref",
                f"identifier_mapping references table_id '{m['table_id']}' which is not an active source_table",
                f"Add '{m['table_id']}' to source_table with is_active=true, or remove from identifier_mapping",
            )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check 4: Identifier Types Have Matching Rules

# COMMAND ----------
if table_in_schema("idr_meta", "rule") and table_in_schema("idr_meta", "identifier_mapping"):
    active_rules = {
        r["identifier_type"]
        for r in collect("""
        SELECT identifier_type FROM idr_meta.rule WHERE is_active = true
    """)
    }

    mapping_types = {
        m["identifier_type"]
        for m in collect("""
        SELECT DISTINCT identifier_type FROM idr_meta.identifier_mapping
    """)
    }

    for t in mapping_types:
        if t not in active_rules:
            result.error(
                "mapping_rule_ref",
                f"identifier_mapping uses type '{t}' but no active rule exists for it",
                f"Add a rule for identifier_type='{t}' in idr_meta.rule with is_active=true",
            )
        else:
            result.ok(f"rule_{t}", f"Rule exists for identifier_type '{t}'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check 5: Required Fields Are Not NULL

# COMMAND ----------
null_checks = [
    ("source_table", "table_id", "is_active = true"),
    ("source_table", "table_fqn", "is_active = true"),
    ("source_table", "entity_key_expr", "is_active = true"),
    ("source_table", "watermark_column", "is_active = true"),
    ("rule", "rule_id", "is_active = true"),
    ("rule", "identifier_type", "is_active = true"),
    ("identifier_mapping", "table_id", "1=1"),
    ("identifier_mapping", "identifier_type", "1=1"),
    ("identifier_mapping", "identifier_value_expr", "1=1"),
]

for table, column, where_clause in null_checks:
    if table_in_schema("idr_meta", table):
        null_count = collect_one(f"""
            SELECT COUNT(*) FROM idr_meta.{table}
            WHERE {column} IS NULL AND {where_clause}
        """)
        if null_count > 0:
            result.error(
                f"null_{table}_{column}",
                f"idr_meta.{table}.{column} has {null_count} NULL values",
                f"Update NULL values in idr_meta.{table}.{column}",
            )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check 6: Source Trust Rankings

# COMMAND ----------
if table_in_schema("idr_meta", "source_table") and table_in_schema("idr_meta", "source"):
    active_tables = {
        r["table_id"]
        for r in collect("""
        SELECT table_id FROM idr_meta.source_table WHERE is_active = true
    """)
    }

    ranked_tables = {
        r["table_id"]
        for r in collect("""
        SELECT table_id FROM idr_meta.source WHERE is_active = true
    """)
    }

    missing_ranks = active_tables - ranked_tables
    if missing_ranks:
        result.warning(
            "source_ranks",
            f"These active tables have no trust ranking: {', '.join(missing_ranks)}",
            "Add entries to idr_meta.source with trust_rank values (lower = more trusted)",
        )
    else:
        result.ok("source_ranks", "All active tables have trust rankings")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check 7: Entity Attribute Mappings

# COMMAND ----------
if table_in_schema("idr_meta", "entity_attribute_mapping"):
    # Check required attributes exist for at least one table
    required_attrs = ["record_updated_at", "email_raw", "phone_raw"]
    attr_counts = {}

    for r in collect(
        "SELECT attribute_name, COUNT(*) as cnt FROM idr_meta.entity_attribute_mapping GROUP BY 1"
    ):
        attr_counts[r["attribute_name"]] = r["cnt"]

    for attr in required_attrs:
        if attr not in attr_counts:
            result.warning(
                f"attr_{attr}",
                f"No tables have mapping for attribute '{attr}'",
                f"Add {attr} mappings to idr_meta.entity_attribute_mapping for golden profile",
            )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Results

# COMMAND ----------
result.print_results()

# COMMAND ----------
if result.has_errors():
    raise RuntimeError("Metadata validation failed. See errors above.")

if FAIL_ON_WARNING and result.has_warnings():
    raise RuntimeError("Metadata validation has warnings and FAIL_ON_WARNING=true")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Validation Complete
# MAGIC
# MAGIC If you see this, your metadata is valid. Run `IDR_Run.py` to process identities.
