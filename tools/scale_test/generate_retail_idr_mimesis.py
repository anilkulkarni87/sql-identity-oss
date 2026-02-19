#!/usr/bin/env python3
"""
Generate omni-channel retail IDR datasets using Mimesis.

Outputs Parquet files per table into:
  tools/scale_test/output/retail_idr_<config_name>/
and copies the config file into that folder.
"""

import argparse
import hashlib
import os
import random
import shutil
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from mimesis import Address, Datetime, Finance, Person, Text
from mimesis.enums import Gender

DEFAULT_EVENT_TYPES = ["view_product", "search", "add_to_cart", "checkout", "purchase"]
DEFAULT_SUPPORT_ISSUES = ["late_delivery", "damaged_item", "return_request", "payment", "other"]
DEFAULT_CHANNELS = ["email", "chat", "phone", "social"]
DEFAULT_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "AUD"]
DEFAULT_CATEGORIES = ["shoes", "apparel", "accessories", "equipment"]
DEFAULT_BRANDS = ["Nike", "Adidas", "Lululemon", "Pokemon"]
DEFAULT_URLS = ["/home", "/product", "/category", "/search", "/checkout"]


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def stable_unit_float(seed_text: str) -> float:
    digest = hashlib.md5(seed_text.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / float(0xFFFFFFFF)


def is_member(customer_id: int, rate: float) -> bool:
    return stable_unit_float(f"member:{customer_id}") < rate


def random_timestamp(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta = end - start
    offset = rng.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=offset)


def maybe(val, prob: float, rng: random.Random):
    return val if rng.random() < prob else None


def mutate_email(email: str, noise: dict, rng: random.Random) -> str:
    if not email:
        return email
    if rng.random() < noise.get("email_case_variation", 0.0):
        email = email.upper() if rng.random() < 0.5 else email.lower()
    if rng.random() < noise.get("email_typo_rate", 0.0) and len(email) > 3:
        idx = rng.randint(1, len(email) - 2)
        email = email[:idx] + email[idx + 1] + email[idx] + email[idx + 2 :]
    return email


def mutate_phone(phone: str, noise: dict, rng: random.Random) -> str:
    if not phone:
        return phone
    if rng.random() < noise.get("phone_missing_country_code", 0.0):
        phone = phone.replace("+1", "").strip()
    if rng.random() < noise.get("phone_format_variation", 0.0):
        phone = phone.replace("-", "").replace(" ", "")
    return phone


def normalize_domain_config(domains_cfg):
    domains = []
    total = 0.0
    for item in domains_cfg:
        domain = item.get("domain")
        weight = float(item.get("weight", 0))
        if domain and weight > 0:
            total += weight
            domains.append((domain, total))
    if not domains:
        return [("example.com", 1.0)]
    return [(domain, cutoff / total) for domain, cutoff in domains]


def pick_domain(customer_id: int, domain_weights) -> str:
    val = stable_unit_float(f"email_domain:{customer_id}")
    for domain, cutoff in domain_weights:
        if val <= cutoff:
            return domain
    return domain_weights[-1][0]


def customer_email(customer_id: int, domain_weights) -> str:
    domain = pick_domain(customer_id, domain_weights)
    return f"user{customer_id}@{domain}"


def customer_phone(customer_id: int) -> str:
    return f"+1-555-{customer_id:06d}"[-12:]


def account_id(customer_id: int) -> str:
    return f"A{customer_id:09d}"


def loyalty_id(customer_id: int) -> str:
    return f"L{customer_id:09d}"


def device_id(base_id: str) -> str:
    return f"dev_{base_id}"


def cookie_id(base_id: str) -> str:
    return f"cookie_{base_id}"


def write_parquet(
    table_name: str, rows_iter, schema: pa.schema, out_dir: str, batch_size: int
) -> None:
    ensure_dir(out_dir)
    writer = None
    batch = []
    part = 0
    for row in rows_iter:
        batch.append(row)
        if len(batch) >= batch_size:
            table = pa.Table.from_pylist(batch, schema=schema)
            path = os.path.join(out_dir, f"{table_name}_part_{part:04d}.parquet")
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema)
                writer.write_table(table)
                writer.close()
            else:
                pq.write_table(table, path)
            batch = []
            part += 1
    if batch:
        table = pa.Table.from_pylist(batch, schema=schema)
        path = os.path.join(out_dir, f"{table_name}_part_{part:04d}.parquet")
        pq.write_table(table, path)


def compute_totals(cfg: dict) -> dict:
    totals = cfg["scale"].get("totals", {})
    customers = cfg["scale"]["total_customers"]
    ratios = cfg["ratios"]

    def total_or_ratio(key, ratio_key):
        val = totals.get(key)
        if val is not None:
            return int(val)
        return int(customers * ratios.get(ratio_key, 0))

    return {
        "customers": customers,
        "orders": total_or_ratio("total_orders", "orders_per_customer"),
        "pos_txns": total_or_ratio("total_pos_txns", "pos_txns_per_customer"),
        "web_events": total_or_ratio("total_web_events", "web_events_per_customer"),
        "app_events": total_or_ratio("total_app_events", "app_events_per_customer"),
        "support_tickets": total_or_ratio("total_support_tickets", "support_tickets_per_customer"),
        "products": int(cfg["scale"].get("total_products", 200_000)),
        "stores": int(cfg["scale"].get("total_stores", 500)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate retail IDR datasets (Parquet).")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    cfg = load_config(args.config)
    cfg_name = cfg.get("name") or os.path.splitext(os.path.basename(args.config))[0]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base_dir, "output", f"retail_idr_{cfg_name}")
    ensure_dir(out_dir)
    shutil.copy(args.config, os.path.join(out_dir, os.path.basename(args.config)))

    seed = cfg.get("seed", 42)
    rng = random.Random(seed)

    person = Person(seed=seed)
    address = Address(seed=seed + 1)
    finance = Finance(seed=seed + 2)
    dt = Datetime(seed=seed + 3)
    text = Text(seed=seed + 4)

    start_date = datetime.fromisoformat(cfg["time"]["start_date"])
    end_date = datetime.fromisoformat(cfg["time"]["end_date"])

    totals = compute_totals(cfg)
    noise = cfg.get("noise", {})
    overlap = cfg.get("overlap", {})
    membership = cfg.get("membership", {})
    batch_size = int(cfg.get("batch_size", 100_000))

    email_domain_weights = normalize_domain_config(cfg.get("email_domains", []))

    # Products
    product_schema = pa.schema(
        [
            ("product_id", pa.int64()),
            ("sku", pa.string()),
            ("category", pa.string()),
            ("brand", pa.string()),
            ("price", pa.float64()),
            ("currency", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_products():
        for pid in range(1, totals["products"] + 1):
            yield {
                "product_id": pid,
                "sku": f"SKU{pid:09d}",
                "category": rng.choice(DEFAULT_CATEGORIES),
                "brand": rng.choice(DEFAULT_BRANDS),
                "price": round(rng.uniform(10, 250), 2),
                "currency": rng.choice(DEFAULT_CURRENCIES),
                "updated_at": random_timestamp(rng, start_date, end_date),
            }

    write_parquet("products", gen_products(), product_schema, out_dir, batch_size)

    # Stores
    store_schema = pa.schema(
        [
            ("store_id", pa.int64()),
            ("store_name", pa.string()),
            ("city", pa.string()),
            ("state", pa.string()),
            ("country", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_stores():
        for sid in range(1, totals["stores"] + 1):
            yield {
                "store_id": sid,
                "store_name": f"Store {sid:04d}",
                "city": address.city(),
                "state": address.state(),
                "country": address.country_code(),
                "updated_at": random_timestamp(rng, start_date, end_date),
            }

    write_parquet("stores", gen_stores(), store_schema, out_dir, batch_size)

    # CRM Customers
    customer_schema = pa.schema(
        [
            ("customer_id", pa.int64()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("dob", pa.date32()),
            ("gender", pa.string()),
            ("country", pa.string()),
            ("created_at", pa.timestamp("s")),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_customers():
        for cid in range(1, totals["customers"] + 1):
            gender = rng.choice([Gender.MALE, Gender.FEMALE])
            created = random_timestamp(rng, start_date, end_date)
            email = mutate_email(customer_email(cid, email_domain_weights), noise, rng)
            phone = mutate_phone(customer_phone(cid), noise, rng)
            yield {
                "customer_id": cid,
                "email": email,
                "phone": phone,
                "first_name": person.first_name(gender=gender),
                "last_name": person.last_name(gender=gender),
                "dob": dt.date(start=1955, end=2006),
                "gender": "M" if gender == Gender.MALE else "F",
                "country": address.country_code(),
                "created_at": created,
                "updated_at": random_timestamp(rng, created, end_date),
            }

    write_parquet("crm_customers", gen_customers(), customer_schema, out_dir, batch_size)

    # Loyalty Members
    loyalty_schema = pa.schema(
        [
            ("loyalty_id", pa.string()),
            ("customer_id", pa.int64()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("member_since", pa.timestamp("s")),
            ("tier", pa.string()),
            ("status", pa.string()),
            ("country", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    loyalty_rate = float(membership.get("loyalty_rate", 0.6))
    tiers = ["bronze", "silver", "gold", "platinum"]

    def gen_loyalty():
        for cid in range(1, totals["customers"] + 1):
            if not is_member(cid, loyalty_rate):
                continue
            since = random_timestamp(rng, start_date, end_date)
            yield {
                "loyalty_id": loyalty_id(cid),
                "customer_id": cid,
                "email": mutate_email(customer_email(cid, email_domain_weights), noise, rng),
                "phone": mutate_phone(customer_phone(cid), noise, rng),
                "member_since": since,
                "tier": rng.choice(tiers),
                "status": rng.choice(["active", "inactive"]),
                "country": address.country_code(),
                "updated_at": random_timestamp(rng, since, end_date),
            }

    write_parquet("loyalty_members", gen_loyalty(), loyalty_schema, out_dir, batch_size)

    # Web Accounts
    account_schema = pa.schema(
        [
            ("account_id", pa.string()),
            ("customer_id", pa.int64()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("created_at", pa.timestamp("s")),
            ("last_login_ts", pa.timestamp("s")),
            ("country", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    account_rate = float(membership.get("account_rate", 0.7))

    def gen_accounts():
        for cid in range(1, totals["customers"] + 1):
            if not is_member(cid, account_rate):
                continue
            created = random_timestamp(rng, start_date, end_date)
            yield {
                "account_id": account_id(cid),
                "customer_id": cid,
                "email": mutate_email(customer_email(cid, email_domain_weights), noise, rng),
                "phone": mutate_phone(customer_phone(cid), noise, rng),
                "created_at": created,
                "last_login_ts": random_timestamp(rng, created, end_date),
                "country": address.country_code(),
                "updated_at": random_timestamp(rng, created, end_date),
            }

    write_parquet("web_accounts", gen_accounts(), account_schema, out_dir, batch_size)

    # E-commerce Orders
    orders_schema = pa.schema(
        [
            ("order_id", pa.int64()),
            ("customer_id", pa.int64()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("product_id", pa.int64()),
            ("order_ts", pa.timestamp("s")),
            ("total_amount", pa.float64()),
            ("currency", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_orders():
        for oid in range(1, totals["orders"] + 1):
            base_cid = rng.randint(1, totals["customers"])
            has_customer = rng.random() < overlap["ecom"]["customer_id_present"]
            has_email = rng.random() < overlap["ecom"]["email_present"]
            has_phone = rng.random() < overlap["ecom"]["phone_present"]
            order_ts = random_timestamp(rng, start_date, end_date)
            yield {
                "order_id": oid,
                "customer_id": base_cid if has_customer else None,
                "email": mutate_email(customer_email(base_cid, email_domain_weights), noise, rng)
                if has_email
                else None,
                "phone": mutate_phone(customer_phone(base_cid), noise, rng) if has_phone else None,
                "product_id": rng.randint(1, totals["products"]),
                "order_ts": order_ts,
                "total_amount": round(rng.uniform(10, 500), 2),
                "currency": rng.choice(DEFAULT_CURRENCIES),
                "updated_at": random_timestamp(rng, order_ts, end_date),
            }

    write_parquet("ecom_orders", gen_orders(), orders_schema, out_dir, batch_size)

    # POS Transactions
    pos_schema = pa.schema(
        [
            ("txn_id", pa.int64()),
            ("store_id", pa.int64()),
            ("loyalty_id", pa.string()),
            ("receipt_email", pa.string()),
            ("receipt_phone", pa.string()),
            ("product_id", pa.int64()),
            ("txn_ts", pa.timestamp("s")),
            ("total_amount", pa.float64()),
            ("currency", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_pos():
        for tid in range(1, totals["pos_txns"] + 1):
            base_cid = rng.randint(1, totals["customers"])
            has_loyalty = rng.random() < overlap["pos"]["loyalty_id_present"] and is_member(
                base_cid, loyalty_rate
            )
            has_email = rng.random() < overlap["pos"]["receipt_email_present"]
            has_phone = rng.random() < overlap["pos"]["receipt_phone_present"]
            txn_ts = random_timestamp(rng, start_date, end_date)
            yield {
                "txn_id": tid,
                "store_id": rng.randint(1, totals["stores"]),
                "loyalty_id": loyalty_id(base_cid) if has_loyalty else None,
                "receipt_email": mutate_email(
                    customer_email(base_cid, email_domain_weights), noise, rng
                )
                if has_email
                else None,
                "receipt_phone": mutate_phone(customer_phone(base_cid), noise, rng)
                if has_phone
                else None,
                "product_id": rng.randint(1, totals["products"]),
                "txn_ts": txn_ts,
                "total_amount": round(rng.uniform(5, 400), 2),
                "currency": rng.choice(DEFAULT_CURRENCIES),
                "updated_at": random_timestamp(rng, txn_ts, end_date),
            }

    write_parquet("pos_transactions", gen_pos(), pos_schema, out_dir, batch_size)

    # Support Tickets
    support_schema = pa.schema(
        [
            ("ticket_id", pa.int64()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("order_id", pa.int64()),
            ("issue_type", pa.string()),
            ("created_at", pa.timestamp("s")),
            ("channel", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_support():
        for tid in range(1, totals["support_tickets"] + 1):
            base_cid = rng.randint(1, totals["customers"])
            has_email = rng.random() < overlap["support"]["email_present"]
            has_phone = rng.random() < overlap["support"]["phone_present"]
            has_order = rng.random() < overlap["support"]["order_id_present"]
            created = random_timestamp(rng, start_date, end_date)
            yield {
                "ticket_id": tid,
                "email": mutate_email(customer_email(base_cid, email_domain_weights), noise, rng)
                if has_email
                else None,
                "phone": mutate_phone(customer_phone(base_cid), noise, rng) if has_phone else None,
                "order_id": rng.randint(1, totals["orders"]) if has_order else None,
                "issue_type": rng.choice(DEFAULT_SUPPORT_ISSUES),
                "created_at": created,
                "channel": rng.choice(DEFAULT_CHANNELS),
                "updated_at": random_timestamp(rng, created, end_date),
            }

    write_parquet("customer_support_tickets", gen_support(), support_schema, out_dir, batch_size)

    # Web Events
    web_schema = pa.schema(
        [
            ("event_id", pa.int64()),
            ("account_id", pa.string()),
            ("device_id", pa.string()),
            ("cookie_id", pa.string()),
            ("email", pa.string()),
            ("event_ts", pa.timestamp("s")),
            ("event_type", pa.string()),
            ("url", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_web_events():
        for eid in range(1, totals["web_events"] + 1):
            base_cid = rng.randint(1, totals["customers"])
            has_account = rng.random() < overlap["web"]["account_id_present"] and is_member(
                base_cid, account_rate
            )
            has_email = rng.random() < overlap["web"]["email_present"]
            acct = account_id(base_cid) if has_account else None
            event_ts = random_timestamp(rng, start_date, end_date)
            base_id = acct if acct else str(base_cid)
            yield {
                "event_id": eid,
                "account_id": acct,
                "device_id": device_id(base_id),
                "cookie_id": cookie_id(base_id),
                "email": mutate_email(customer_email(base_cid, email_domain_weights), noise, rng)
                if has_email
                else None,
                "event_ts": event_ts,
                "event_type": rng.choice(DEFAULT_EVENT_TYPES),
                "url": rng.choice(DEFAULT_URLS),
                "updated_at": random_timestamp(rng, event_ts, end_date),
            }

    write_parquet("web_events", gen_web_events(), web_schema, out_dir, batch_size)

    # App Events
    app_schema = pa.schema(
        [
            ("event_id", pa.int64()),
            ("account_id", pa.string()),
            ("device_id", pa.string()),
            ("ad_id", pa.string()),
            ("event_ts", pa.timestamp("s")),
            ("event_type", pa.string()),
            ("app_version", pa.string()),
            ("updated_at", pa.timestamp("s")),
        ]
    )

    def gen_app_events():
        for eid in range(1, totals["app_events"] + 1):
            base_cid = rng.randint(1, totals["customers"])
            has_account = rng.random() < overlap["app"]["account_id_present"] and is_member(
                base_cid, account_rate
            )
            acct = account_id(base_cid) if has_account else None
            event_ts = random_timestamp(rng, start_date, end_date)
            base_id = acct if acct else str(base_cid)
            yield {
                "event_id": eid,
                "account_id": acct,
                "device_id": device_id(base_id),
                "ad_id": f"ad_{rng.randint(1, 99999999)}",
                "event_ts": event_ts,
                "event_type": rng.choice(DEFAULT_EVENT_TYPES),
                "app_version": f"{rng.randint(1, 5)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}",
                "updated_at": random_timestamp(rng, event_ts, end_date),
            }

    write_parquet("app_events", gen_app_events(), app_schema, out_dir, batch_size)

    print(f"✅ Data generated at: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
