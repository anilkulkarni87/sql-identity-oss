# Databricks notebook source
# MAGIC %md
# MAGIC # Scale Test Data Generator for SQL Identity Resolution
# MAGIC
# MAGIC Generates 50M+ realistic retail customer records across 7 sources with overlapping identities.
# MAGIC
# MAGIC **Sources Generated:**
# MAGIC - CRM Master (8M) - Primary customer database
# MAGIC - E-commerce Transactions (15M) - Online purchases
# MAGIC - Loyalty Program (6M) - Rewards members
# MAGIC - Mobile App Users (10M) - App registrations
# MAGIC - In-Store POS (8M) - Physical store transactions
# MAGIC - Survey Responses (2M) - Post-purchase surveys
# MAGIC - Product Reviews (1M) - Website reviews
# MAGIC - Products Dimension (50K) - Product catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.dropdown("scale", "10M", ["5M", "10M", "50M", "100M"], "Data Scale")
dbutils.widgets.text("catalog", "idr_scale_test", "Target Catalog")
dbutils.widgets.text("schema", "raw_data", "Target Schema")
dbutils.widgets.text("seed", "42", "Random Seed")

# Get widget values
SCALE = dbutils.widgets.get("scale")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SEED = int(dbutils.widgets.get("seed"))

print(f"Scale: {SCALE}")
print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Seed: {SEED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scale Configurations

# COMMAND ----------

SCALE_CONFIG = {
    "5M": {
        "unique_customers": 1_000_000,
        "products": 10_000,
        "crm": 800_000,
        "ecom": 1_500_000,
        "loyalty": 600_000,
        "mobile": 1_000_000,
        "pos": 800_000,
        "surveys": 200_000,
        "reviews": 100_000,
    },
    "10M": {
        "unique_customers": 2_000_000,
        "products": 20_000,
        "crm": 1_600_000,
        "ecom": 3_000_000,
        "loyalty": 1_200_000,
        "mobile": 2_000_000,
        "pos": 1_600_000,
        "surveys": 400_000,
        "reviews": 200_000,
    },
    "50M": {
        "unique_customers": 10_000_000,
        "products": 50_000,
        "crm": 8_000_000,
        "ecom": 15_000_000,
        "loyalty": 6_000_000,
        "mobile": 10_000_000,
        "pos": 8_000_000,
        "surveys": 2_000_000,
        "reviews": 1_000_000,
    },
    "100M": {
        "unique_customers": 20_000_000,
        "products": 100_000,
        "crm": 16_000_000,
        "ecom": 30_000_000,
        "loyalty": 12_000_000,
        "mobile": 20_000_000,
        "pos": 16_000_000,
        "surveys": 4_000_000,
        "reviews": 2_000_000,
    },
}

config = SCALE_CONFIG[SCALE]
print(f"Configuration for {SCALE}:")
for k, v in config.items():
    print(f"  {k}: {v:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Catalog and Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✓ Using {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

# Reference data
CATEGORIES = ["Electronics", "Apparel", "Home", "Beauty", "Sports", "Grocery", "Toys"]
BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF", "StoreLabel"]
COUNTRIES = ["US", "UK", "DE", "FR", "IN", "JP", "AU", "CA", "MX", "BR"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
CUSTOMER_SEGMENTS = ["VIP", "Regular", "New", "At-Risk", "Churned"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["credit", "debit", "cash", "mobile_wallet", "gift_card"]
OS_TYPES = ["iOS", "Android"]
SURVEY_TYPES = ["post_purchase", "nps", "product_feedback", "support_followup"]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]


def create_array_literal(items):
    """Create a Spark array literal from Python list."""
    return F.array(*[F.lit(x) for x in items])


def random_element(items, seed_offset=0):
    """Select random element from array based on rand()."""
    arr = create_array_literal(items)
    idx = (F.rand(SEED + seed_offset) * len(items)).cast("int")
    return F.element_at(arr, idx + 1)


def apply_email_noise(df, email_col="email"):
    """Apply realistic noise to email addresses."""
    return df.withColumn(
        email_col,
        F.when(F.rand() < 0.02, F.upper(F.col(email_col)))
        .when(F.rand() < 0.01, F.regexp_replace(F.col(email_col), "@", "+shop@"))
        .when(
            (F.rand() < 0.01) & F.col(email_col).contains("gmail"),
            F.regexp_replace(F.col(email_col), r"\.(?=.*@gmail)", ""),
        )
        .otherwise(F.col(email_col)),
    )


def apply_phone_noise(df, phone_col="phone"):
    """Apply realistic noise to phone numbers."""
    return df.withColumn(
        phone_col,
        F.when(F.rand() < 0.05, F.concat(F.lit("+1"), F.col(phone_col)))
        .when(
            F.rand() < 0.03,
            F.concat(
                F.substring(phone_col, 1, 3),
                F.lit("-"),
                F.substring(phone_col, 4, 3),
                F.lit("-"),
                F.substring(phone_col, 7, 4),
            ),
        )
        .otherwise(F.col(phone_col)),
    )


def generate_timestamp(days_back=365, seed_offset=0):
    """Generate random timestamp within the last N days."""
    return F.expr(
        f"current_timestamp() - make_interval(0, 0, 0, cast(rand({SEED + seed_offset}) * {days_back} as int), 0, 0, 0)"
    )


print("✓ Helper functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products Dimension

# COMMAND ----------

products = (
    spark.range(0, config["products"])
    .select(
        F.concat(F.lit("PROD-"), F.format_string("%06d", F.col("id"))).alias("product_id"),
        F.concat(F.lit("Product "), F.col("id")).alias("product_name"),
        random_element(CATEGORIES, 1).alias("category"),
        random_element(BRANDS, 2).alias("brand"),
        (F.rand(SEED + 3) * 490 + 10).cast("decimal(10,2)").alias("price"),
        (F.rand(SEED + 4) * 40 + 10).cast("decimal(5,2)").alias("margin_pct"),
        (F.rand(SEED + 5) < 0.15).alias("is_private_label"),
        F.date_sub(F.current_date(), (F.rand(SEED + 6) * 1000).cast("int")).alias("launch_date"),
    )
    .cache()
)

print(f"✓ Products: {products.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Master Identity Pool

# COMMAND ----------

master_pool = (
    spark.range(0, config["unique_customers"])
    .select(
        F.col("id").alias("m_id"),
        # Core identifiers
        F.concat(F.lit("user"), F.col("id"), F.lit("@"), random_element(EMAIL_DOMAINS, 10)).alias(
            "true_email"
        ),
        # 10-digit phone number
        F.concat(
            F.lit("5"), F.format_string("%09d", (F.rand(SEED + 11) * 1000000000).cast("long"))
        ).alias("true_phone"),
        # Loyalty ID
        F.concat(F.lit("L-"), F.format_string("%010d", F.col("id"))).alias("loyalty_id"),
        # Demographics
        F.concat(F.lit("First"), F.col("id") % 10000).alias("first_name"),
        F.concat(F.lit("Last"), F.col("id") % 10000).alias("last_name"),
        # Address components
        F.format_string("%05d", (F.rand(SEED + 12) * 99999).cast("int")).alias("zip"),
        random_element(COUNTRIES, 13).alias("country_code"),
        # Device ID for mobile
        F.concat(F.lit("DEV-"), F.sha2(F.col("id").cast("string"), 256).substr(1, 16)).alias(
            "device_id"
        ),
        # Payment token
        F.concat(F.lit("TKN-"), F.sha2(F.col("id").cast("string"), 256).substr(17, 16)).alias(
            "payment_token"
        ),
    )
    .cache()
)

print(f"✓ Master Pool: {master_pool.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate CRM Master

# COMMAND ----------

crm_sample_rate = config["crm"] / config["unique_customers"]
crm = master_pool.sample(False, crm_sample_rate, SEED + 100).select(
    F.concat(F.lit("CRM-"), F.col("m_id")).alias("customer_id"),
    F.col("true_email").alias("email"),
    F.col("true_phone").alias("phone"),
    "first_name",
    "last_name",
    "loyalty_id",
    F.concat(F.col("zip"), F.lit(" Main Street")).alias("address_line1"),
    F.lit("Anytown").alias("city"),
    "country_code",
    generate_timestamp(730, 20).alias("created_at"),
    generate_timestamp(90, 21).alias("updated_at"),
    (F.rand(SEED + 22) > 0.3).alias("opt_in_email"),
    (F.rand(SEED + 23) > 0.5).alias("opt_in_sms"),
    random_element(CUSTOMER_SEGMENTS, 24).alias("customer_segment"),
)
crm = apply_email_noise(crm)
crm = apply_phone_noise(crm).limit(config["crm"])

print(f"✓ CRM Customers: {crm.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate E-commerce Transactions

# COMMAND ----------

ecom_sample_rate = config["ecom"] / config["unique_customers"]
ecom = (
    master_pool.sample(True, ecom_sample_rate, SEED + 200)
    .select(
        F.expr("uuid()").alias("transaction_id"),
        F.concat(F.lit("ECOM-"), F.col("m_id")).alias("user_id"),
        F.col("true_email").alias("email"),
        F.col("true_phone").alias("phone"),
        F.concat(
            F.lit("PROD-"),
            F.format_string("%06d", (F.rand(SEED + 201) * config["products"]).cast("int")),
        ).alias("product_id"),
        (F.rand(SEED + 202) * 4 + 1).cast("int").alias("quantity"),
        (F.rand(SEED + 203) * 200 + 10).cast("decimal(10,2)").alias("unit_price"),
        random_element(["USD", "EUR", "GBP", "CAD", "AUD"], 204).alias("currency"),
        F.sha2(F.concat(F.col("zip"), F.col("country_code")), 256).alias("shipping_address_hash"),
        "payment_token",
        random_element(DEVICE_TYPES, 205).alias("device_type"),
        generate_timestamp(365, 206).alias("order_date"),
        F.when(F.rand(SEED + 207) < 0.2, F.concat(F.lit("PROMO"), (F.rand() * 100).cast("int")))
        .otherwise(F.lit(None))
        .alias("promo_code"),
    )
    .withColumn("order_total", F.col("quantity") * F.col("unit_price"))
)

ecom = apply_email_noise(ecom)
ecom = apply_phone_noise(ecom).limit(config["ecom"])

print(f"✓ E-commerce Transactions: {ecom.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Loyalty Program

# COMMAND ----------

loyalty_sample_rate = config["loyalty"] / config["unique_customers"]
loyalty = master_pool.sample(False, loyalty_sample_rate, SEED + 300).select(
    F.concat(F.lit("LYL-"), F.expr("uuid()").substr(1, 8)).alias("member_id"),
    "loyalty_id",
    F.col("true_email").alias("email"),
    F.col("true_phone").alias("phone"),
    "first_name",
    "last_name",
    random_element(LOYALTY_TIERS, 301).alias("tier"),
    (F.rand(SEED + 302) * 50000).cast("int").alias("points_balance"),
    generate_timestamp(1095, 303).alias("enrollment_date"),
    generate_timestamp(30, 304).alias("last_activity_date"),
)
loyalty = apply_email_noise(loyalty).limit(config["loyalty"])

print(f"✓ Loyalty Members: {loyalty.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Mobile App Users

# COMMAND ----------

mobile_sample_rate = config["mobile"] / config["unique_customers"]
mobile = master_pool.sample(False, mobile_sample_rate, SEED + 400).select(
    F.concat(F.lit("APP-"), F.expr("uuid()").substr(1, 12)).alias("app_user_id"),
    "device_id",
    F.col("true_email").alias("email"),
    F.col("true_phone").alias("phone"),
    F.concat(F.lit("PUSH-"), F.sha2(F.col("device_id"), 256).substr(1, 32)).alias("push_token"),
    random_element(OS_TYPES, 401).alias("os_type"),
    F.concat(F.lit("3."), (F.rand(SEED + 402) * 20).cast("int")).alias("app_version"),
    generate_timestamp(365, 403).alias("first_open_date"),
    generate_timestamp(14, 404).alias("last_active_date"),
    (F.rand(SEED + 405) * 200 + 1).cast("int").alias("total_sessions"),
)
mobile = apply_email_noise(mobile)
mobile = apply_phone_noise(mobile).limit(config["mobile"])

print(f"✓ Mobile App Users: {mobile.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate In-Store POS Transactions

# COMMAND ----------

pos_sample_rate = config["pos"] / config["unique_customers"]
pos = (
    master_pool.sample(True, pos_sample_rate, SEED + 500)
    .select(
        F.concat(F.lit("POS-"), F.expr("uuid()")).alias("pos_transaction_id"),
        F.concat(
            F.lit("STORE-"), F.format_string("%04d", (F.rand(SEED + 501) * 500).cast("int"))
        ).alias("store_id"),
        # Only 30% have loyalty ID (identified customers)
        F.when(F.rand(SEED + 502) < 0.3, F.col("loyalty_id"))
        .otherwise(F.lit(None))
        .alias("loyalty_id"),
        F.concat(
            F.lit("PROD-"),
            F.format_string("%06d", (F.rand(SEED + 503) * config["products"]).cast("int")),
        ).alias("product_id"),
        (F.rand(SEED + 504) * 5 + 1).cast("int").alias("quantity"),
        (F.rand(SEED + 505) * 150 + 5).cast("decimal(10,2)").alias("unit_price"),
        "payment_token",
        generate_timestamp(365, 506).alias("transaction_date"),
        random_element(PAYMENT_METHODS, 507).alias("payment_method"),
    )
    .withColumn("transaction_total", F.col("quantity") * F.col("unit_price"))
    .limit(config["pos"])
)

print(f"✓ POS Transactions: {pos.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Survey Responses

# COMMAND ----------

survey_sample_rate = config["surveys"] / config["ecom"]
surveys = (
    ecom.sample(False, min(survey_sample_rate, 1.0), SEED + 600)
    .select(
        F.concat(F.lit("SURV-"), F.expr("uuid()")).alias("response_id"),
        random_element(SURVEY_TYPES, 601).alias("survey_type"),
        "email",
        F.col("transaction_id").alias("order_id"),
        "product_id",
        (F.rand(SEED + 602) * 11).cast("int").alias("nps_score"),
        (F.rand(SEED + 603) * 5 + 1).cast("int").alias("satisfaction_rating"),
        F.when(F.rand(SEED + 604) < 0.3, F.lit("Great product, would recommend!"))
        .when(F.rand(SEED + 604) < 0.6, F.lit("Meets expectations"))
        .otherwise(F.lit(None))
        .alias("feedback_text"),
        generate_timestamp(90, 605).alias("submitted_at"),
        (F.rand(SEED + 606) * 300 + 30).cast("int").alias("completion_time_seconds"),
    )
    .limit(config["surveys"])
)

print(f"✓ Survey Responses: {surveys.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Product Reviews

# COMMAND ----------

review_sample_rate = config["reviews"] / config["ecom"]
reviews = (
    ecom.sample(False, min(review_sample_rate, 1.0), SEED + 700)
    .select(
        F.concat(F.lit("REV-"), F.expr("uuid()")).alias("review_id"),
        "product_id",
        F.col("user_id").alias("reviewer_user_id"),
        "email".alias("reviewer_email"),
        F.concat(F.lit("Reviewer"), (F.rand(SEED + 702) * 10000).cast("int")).alias(
            "reviewer_display_name"
        ),
        (F.rand(SEED + 703) * 5 + 1).cast("int").alias("rating"),
        F.concat(F.lit("Review Title "), (F.rand() * 1000).cast("int")).alias("review_title"),
        F.when(F.rand(SEED + 704) < 0.7, F.lit("This product is exactly what I needed."))
        .otherwise(F.lit("Not what I expected."))
        .alias("review_text"),
        (F.rand(SEED + 705) < 0.8).alias("verified_purchase"),
        (F.rand(SEED + 706) * 100).cast("int").alias("helpful_votes"),
        generate_timestamp(180, 707).alias("submitted_at"),
    )
    .limit(config["reviews"])
)

print(f"✓ Product Reviews: {reviews.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Tables

# COMMAND ----------

sources = {
    "products": products,
    "crm_customers": crm,
    "ecom_transactions": ecom,
    "loyalty_members": loyalty,
    "mobile_app_users": mobile,
    "pos_transactions": pos,
    "survey_responses": surveys,
    "product_reviews": reviews,
}

for name, df in sources.items():
    print(f"Writing {name}...")
    df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
    print(f"  ✓ {CATALOG}.{SCHEMA}.{name}")

print("\n✓ All tables written successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Record Counts

# COMMAND ----------

print(f"{'Table':<25} {'Records':>15}")
print("-" * 42)

total = 0
for name in sources.keys():
    count = spark.table(f"{CATALOG}.{SCHEMA}.{name}").count()
    print(f"{name:<25} {count:>15,}")
    total += count

print("-" * 42)
print(f"{'TOTAL':<25} {total:>15,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate IDR Metadata SQL
# MAGIC
# MAGIC Run this in your IDR schema to configure the sources:

# COMMAND ----------

metadata_sql = f"""
-- IDR Metadata Configuration for Scale Test Data
-- Generated: {datetime.now().isoformat()}
-- Scale: {SCALE}
-- Catalog: {CATALOG}.{SCHEMA}

-- Source Tables
INSERT INTO idr_meta.source_table (table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active) VALUES
  ('crm', '{CATALOG}.{SCHEMA}.crm_customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE),
  ('ecom', '{CATALOG}.{SCHEMA}.ecom_transactions', 'PERSON', 'user_id', 'order_date', 0, TRUE),
  ('loyalty', '{CATALOG}.{SCHEMA}.loyalty_members', 'PERSON', 'member_id', 'last_activity_date', 0, TRUE),
  ('mobile', '{CATALOG}.{SCHEMA}.mobile_app_users', 'PERSON', 'app_user_id', 'last_active_date', 0, TRUE),
  ('pos', '{CATALOG}.{SCHEMA}.pos_transactions', 'PERSON', 'pos_transaction_id', 'transaction_date', 0, TRUE),
  ('surveys', '{CATALOG}.{SCHEMA}.survey_responses', 'PERSON', 'response_id', 'submitted_at', 0, TRUE),
  ('reviews', '{CATALOG}.{SCHEMA}.product_reviews', 'PERSON', 'review_id', 'submitted_at', 0, TRUE);

-- Identifier Mappings
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
  ('crm', 'EMAIL', 'LOWER(email)', FALSE),
  ('crm', 'PHONE', 'REGEXP_REPLACE(phone, "[^0-9]", "")', FALSE),
  ('crm', 'LOYALTY_ID', 'loyalty_id', FALSE),
  ('ecom', 'EMAIL', 'LOWER(email)', FALSE),
  ('ecom', 'PHONE', 'REGEXP_REPLACE(phone, "[^0-9]", "")', FALSE),
  ('ecom', 'PAYMENT_TOKEN', 'payment_token', FALSE),
  ('loyalty', 'EMAIL', 'LOWER(email)', FALSE),
  ('loyalty', 'PHONE', 'REGEXP_REPLACE(phone, "[^0-9]", "")', FALSE),
  ('loyalty', 'LOYALTY_ID', 'loyalty_id', FALSE),
  ('mobile', 'EMAIL', 'LOWER(email)', FALSE),
  ('mobile', 'PHONE', 'REGEXP_REPLACE(phone, "[^0-9]", "")', FALSE),
  ('mobile', 'DEVICE_ID', 'device_id', FALSE),
  ('pos', 'LOYALTY_ID', 'loyalty_id', FALSE),
  ('pos', 'PAYMENT_TOKEN', 'payment_token', FALSE),
  ('surveys', 'EMAIL', 'LOWER(email)', FALSE),
  ('reviews', 'EMAIL', 'LOWER(reviewer_email)', FALSE);

-- Source Trust Ranking
INSERT INTO idr_meta.source (table_id, source_name, trust_rank, is_active) VALUES
  ('crm', 'CRM Master', 1, TRUE),
  ('loyalty', 'Loyalty Program', 2, TRUE),
  ('ecom', 'E-commerce', 3, TRUE),
  ('mobile', 'Mobile App', 4, TRUE),
  ('pos', 'POS Transactions', 5, TRUE),
  ('surveys', 'Survey Responses', 6, TRUE),
  ('reviews', 'Product Reviews', 7, TRUE);
"""

print(metadata_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Data generation complete! Next steps:
# MAGIC
# MAGIC 1. **Set up IDR schemas**: Run the DDL scripts for `idr_meta`, `idr_work`, `idr_out`
# MAGIC 2. **Configure sources**: Copy the SQL above into your IDR metadata tables
# MAGIC 3. **Run IDR**: Execute `IDR_Run.py` with dry-run first
# MAGIC
# MAGIC ```python
# MAGIC # Example post-generation queries
# MAGIC
# MAGIC # Check email overlap between CRM and E-commerce
# MAGIC spark.sql(f"""
# MAGIC   SELECT COUNT(DISTINCT c.email) as overlapping_emails
# MAGIC   FROM {CATALOG}.{SCHEMA}.crm_customers c
# MAGIC   JOIN {CATALOG}.{SCHEMA}.ecom_transactions e
# MAGIC     ON LOWER(c.email) = LOWER(e.email)
# MAGIC """).show()
# MAGIC ```
