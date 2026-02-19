#!/usr/bin/env python3
"""
Generate a realistic-ish global retail dataset for deterministic Identity Resolution testing.
Outputs partitioned Parquet + truth_links with canonical person_id.

Tables:
- dim_address
- dim_product
- digital_customer_account
- pos_customer
- ecom_order
- store_order
- sdk_event
- bazaarvoice_survey
- product_review
- truth_links

Config-driven scale: n_persons, rows per table, date range, noise knobs.
python generate_global_retail_idr.py --config config_10m.json

"""

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
from faker import Faker

# -------------------------
# Utilities
# -------------------------

COUNTRIES = ["US", "UK", "HK", "KR", "AU", "IN"]
CURRENCY_BY_COUNTRY = {
    "US": "USD",
    "UK": "GBP",
    "HK": "HKD",
    "KR": "KRW",
    "AU": "AUD",
    "IN": "INR",
}

EVENT_TYPES = np.array(
    ["page_view", "view_item", "add_to_cart", "begin_checkout", "login", "purchase"],
    dtype=object,
)

ORDER_STATUS_ECOM = np.array(["created", "paid", "shipped", "delivered", "cancelled"], dtype=object)
ORDER_STATUS_STORE = np.array(["completed", "voided", "returned"], dtype=object)

DOMAINS = np.array(["gmail.com", "outlook.com", "yahoo.com", "icloud.com"], dtype=object)

ADJECTIVES = np.array(
    ["Aero", "Flex", "Swift", "Cloud", "Core", "Urban", "Trail", "Zen", "Pulse", "Nova"],
    dtype=object,
)
NOUNS = np.array(
    ["Runner", "Tee", "Hoodie", "Tight", "Short", "Jacket", "Shoe", "Tank", "Cap", "Sock"],
    dtype=object,
)
CATEGORIES = np.array(["Footwear", "Apparel", "Accessories"], dtype=object)
SUBCATEGORIES = {
    "Footwear": np.array(["Running", "Training", "Lifestyle"], dtype=object),
    "Apparel": np.array(["Tops", "Bottoms", "Outerwear"], dtype=object),
    "Accessories": np.array(["Bags", "Socks", "Hats"], dtype=object),
}

STREET_SUFFIX = np.array(["St", "Ave", "Rd", "Blvd", "Ln", "Dr", "Way"], dtype=object)

STATE_BY_COUNTRY = {
    "US": np.array(["CA", "NY", "TX", "WA", "IL", "MA", "FL", "NJ", "GA", "CO"], dtype=object),
    "UK": np.array(["ENG", "SCT", "WLS", "NIR"], dtype=object),
    "HK": np.array(["HK"], dtype=object),
    "KR": np.array(["Seoul", "Busan", "Incheon", "Daegu", "Daejeon"], dtype=object),
    "AU": np.array(["NSW", "VIC", "QLD", "WA", "SA"], dtype=object),
    "IN": np.array(["KA", "MH", "DL", "TN", "TS", "GJ", "WB"], dtype=object),
}


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_date(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d")


def sample_from_dist(
    rng: np.random.Generator,
    keys: List[str],
    probs: List[float],
    n: int,
) -> np.ndarray:
    """Fast categorical sampling using inverse CDF."""
    probs_arr = np.array(probs, dtype=np.float64)
    probs_arr = probs_arr / probs_arr.sum()
    cdf = np.cumsum(probs_arr)
    r = rng.random(n)
    idx = np.searchsorted(cdf, r, side="right")
    return np.array(keys, dtype=object)[idx]


def random_datetimes_ms(
    rng: np.random.Generator,
    start: datetime,
    days: int,
    n: int,
) -> np.ndarray:
    """
    Random datetime64[ms] uniformly across [start, start + days).
    """
    # milliseconds offset into window
    max_ms = int(days * 24 * 60 * 60 * 1000)
    off = rng.integers(0, max_ms, size=n, dtype=np.int64)
    base = np.datetime64(start, "ms")
    return base + off.astype("timedelta64[ms]")


def add_updated_at(
    rng: np.random.Generator,
    created_at: np.ndarray,
    max_delay_days: int,
) -> np.ndarray:
    if max_delay_days <= 0:
        return created_at
    max_ms = int(max_delay_days * 24 * 60 * 60 * 1000)
    delay = rng.integers(0, max_ms + 1, size=len(created_at), dtype=np.int64)
    return created_at + delay.astype("timedelta64[ms]")


def make_created_date(created_at: np.ndarray) -> np.ndarray:
    # Store as YYYY-MM-DD string for stable hive partitioning.
    # created_at is datetime64[ms]
    days = created_at.astype("datetime64[D]")
    return days.astype(str)


def format_phone_raw(
    rng: np.random.Generator,
    phone_digits: np.ndarray,
    p_punct: float,
    p_country_prefix: float,
) -> np.ndarray:
    """
    phone_digits: array of strings like '4255551234'
    Produces phone_raw with punctuation and/or +1 prefix, while keeping digits stable.
    NumPy 2.x compatible.
    """
    raw = phone_digits.copy()

    m = rng.random(len(raw)) < p_punct
    if m.any():
        # (425) 555-1234 format
        def format_phone(s):
            if isinstance(s, str) and len(s) >= 10:
                return f"({s[:3]}) {s[3:6]}-{s[6:10]}"
            return s

        raw[m] = np.array([format_phone(s) for s in raw[m]], dtype=object)

    m = rng.random(len(raw)) < p_country_prefix
    if m.any():
        raw[m] = np.array([f"+1{s}" if isinstance(s, str) else s for s in raw[m]], dtype=object)

    return raw


def format_email_raw(
    rng: np.random.Generator,
    email_norm: np.ndarray,
    p_plus_alias: float,
    p_uppercase: float,
    p_spaces: float,
) -> np.ndarray:
    """Format email with noise - NumPy 2.x compatible."""
    raw = email_norm.copy()

    # Use vectorized Python operations for NumPy 2.x compatibility
    m = rng.random(len(raw)) < p_plus_alias
    if m.any():
        # insert +promo before @ (simple, predictable pattern)
        raw[m] = np.array(
            [s.replace("@", "+promo@") if isinstance(s, str) else s for s in raw[m]], dtype=object
        )

    m = rng.random(len(raw)) < p_uppercase
    if m.any():
        raw[m] = np.array([s.upper() if isinstance(s, str) else s for s in raw[m]], dtype=object)

    m = rng.random(len(raw)) < p_spaces
    if m.any():
        raw[m] = np.array([f" {s} " if isinstance(s, str) else s for s in raw[m]], dtype=object)

    return raw


def postal_code_by_country(
    rng: np.random.Generator,
    country_code: np.ndarray,
) -> np.ndarray:
    """
    Lightweight postal code generator per country.
    HK often doesn't use postal codes => nullable-ish; we generate empty string.
    """
    out = np.empty(len(country_code), dtype=object)
    cc = country_code

    # US: 5 digits
    m = cc == "US"
    if m.any():
        out[m] = np.char.zfill(rng.integers(0, 100000, size=m.sum()).astype(str), 5)

    # UK: simplified pattern like "SW1A 1AA"
    m = cc == "UK"
    if m.any():
        n = m.sum()
        letters = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), dtype=object)
        a = letters[rng.integers(0, 26, size=n)]
        b = letters[rng.integers(0, 26, size=n)]
        c = letters[rng.integers(0, 26, size=n)]
        d = letters[rng.integers(0, 26, size=n)]
        digit1 = rng.integers(0, 10, size=n).astype(str)
        digit2 = rng.integers(0, 10, size=n).astype(str)
        out[m] = np.char.add(
            np.char.add(a, b),
            np.char.add(
                digit1,
                np.char.add(
                    " ",
                    np.char.add(digit2, np.char.add(c, d)),
                ),
            ),
        )

    # HK: empty
    m = cc == "HK"
    if m.any():
        out[m] = None

    # KR: 5 digits
    m = cc == "KR"
    if m.any():
        out[m] = np.char.zfill(rng.integers(0, 100000, size=m.sum()).astype(str), 5)

    # AU: 4 digits
    m = cc == "AU"
    if m.any():
        out[m] = np.char.zfill(rng.integers(0, 10000, size=m.sum()).astype(str), 4)

    # IN: 6 digits
    m = cc == "IN"
    if m.any():
        out[m] = np.char.zfill(rng.integers(0, 1000000, size=m.sum()).astype(str), 6)

    return out


def write_partitioned_dataset(table: pa.Table, base_dir: str, partition_cols: List[str]) -> None:
    ensure_dir(base_dir)
    # Hive partitioning uses col=value directory layout.
    part_schema = pa.schema([(c, table.schema.field(c).type) for c in partition_cols])
    ds.write_dataset(
        table,
        base_dir=base_dir,
        format="parquet",
        partitioning=ds.partitioning(part_schema, flavor="hive"),
        existing_data_behavior="overwrite_or_ignore",
        # Compression is set per file by parquet writer defaults; we use zstd via write_options below when needed.
    )


def write_dataset_zstd(table: pa.Table, base_dir: str, partition_cols: List[str]) -> None:
    """
    ds.write_dataset doesn't expose compression directly in all versions the same way.
    We'll set compression at the table level using Parquet file format options where available.
    """
    ensure_dir(base_dir)
    part_schema = pa.schema([(c, table.schema.field(c).type) for c in partition_cols])
    fmt = ds.ParquetFileFormat()
    write_opts = fmt.make_write_options(compression="zstd")
    ds.write_dataset(
        table,
        base_dir=base_dir,
        format=fmt,
        file_options=write_opts,
        partitioning=ds.partitioning(part_schema, flavor="hive"),
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=4096,  # Allow for multi-country × multi-channel × 90-day partitioning
    )


# -------------------------
# Config structures
# -------------------------


@dataclass
class Config:
    seed: int
    out_dir: str
    start_date: datetime
    days: int
    chunk_rows: int

    n_persons: int
    n_households: int
    n_addresses: int
    n_products: int

    rows: Dict[str, int]
    country_mix: Dict[str, float]
    channel_mix: Dict[str, Dict[str, float]]

    identity_noise: Dict[str, float]
    address_rules: Dict[str, float]
    sdk_rules: Dict[str, float]
    bazaarvoice_rules: Dict[str, float]

    write_truth_links: bool


def load_config(path: str) -> Config:
    with open(path, "r") as f:
        raw = json.load(f)

    g = raw["global"]
    s = raw["scale"]

    write_truth_links = bool(raw["global"].get("write_truth_links", True))

    return Config(
        seed=int(g["seed"]),
        out_dir=str(g["out_dir"]),
        start_date=parse_date(g["start_date"]),
        days=int(g["days"]),
        chunk_rows=int(g["chunk_rows"]),
        n_persons=int(s["n_persons"]),
        n_households=int(s["n_households"]),
        n_addresses=int(s["n_addresses"]),
        n_products=int(s["n_products"]),
        rows={k: int(v) for k, v in raw["rows"].items()},
        country_mix={k: float(v) for k, v in raw["country_mix"].items()},
        channel_mix=raw["channel_mix"],
        identity_noise=raw["identity_noise"],
        address_rules=raw["address_rules"],
        sdk_rules=raw["sdk_rules"],
        bazaarvoice_rules=raw["bazaarvoice_rules"],
        write_truth_links=write_truth_links,
    )


# -------------------------
# Pools (Faker only here)
# -------------------------


@dataclass
class Pools:
    first_names: np.ndarray
    last_names: np.ndarray
    first_tok: np.ndarray
    last_tok: np.ndarray
    street_names: np.ndarray
    city_names: np.ndarray


def build_pools(
    seed: int, first_pool=50000, last_pool=50000, street_pool=25000, city_pool=12000
) -> Pools:
    fake = Faker("en_US")
    Faker.seed(seed)

    first = np.array([fake.first_name() for _ in range(first_pool)], dtype=object)
    last = np.array([fake.last_name() for _ in range(last_pool)], dtype=object)

    # tokens for emails (fast + predictable)
    def tok(x: str) -> str:
        x = x.strip().lower()
        out = []
        for ch in x:
            if ("a" <= ch <= "z") or ("0" <= ch <= "9"):
                out.append(ch)
        return "".join(out) or "user"

    first_tok = np.array([tok(x) for x in first], dtype=object)
    last_tok = np.array([tok(x) for x in last], dtype=object)

    street = np.array([fake.street_name() for _ in range(street_pool)], dtype=object)
    city = np.array([fake.city() for _ in range(city_pool)], dtype=object)

    return Pools(first, last, first_tok, last_tok, street, city)


# -------------------------
# Canonical "truth" person state (stored as compact indices)
# -------------------------


@dataclass
class PersonState:
    # per person
    person_country: np.ndarray  # object array of country codes
    household_id: np.ndarray  # int64
    first_idx: np.ndarray  # int32 into pools
    last_idx: np.ndarray  # int32 into pools
    phone_num: np.ndarray  # int64 (10-digit-ish)
    default_address_idx: np.ndarray  # int64 index into address_id sequence
    cookie_num: np.ndarray  # int64
    device_num: np.ndarray  # int64


def build_person_state(cfg: Config, rng: np.random.Generator) -> PersonState:
    country_keys = list(cfg.country_mix.keys())
    country_probs = [cfg.country_mix[k] for k in country_keys]
    person_country = sample_from_dist(rng, country_keys, country_probs, cfg.n_persons)

    household_id = rng.integers(0, cfg.n_households, size=cfg.n_persons, dtype=np.int64)

    # Name indices
    # We’ll fill with actual pool sizes later; keep placeholders here, set in generator functions.
    first_idx = rng.integers(
        0, 50000, size=cfg.n_persons, dtype=np.int32
    )  # overwritten safely if pool differs
    last_idx = rng.integers(0, 50000, size=cfg.n_persons, dtype=np.int32)

    # Phone: create household phones for shared households; else per-person derived
    # Household base phones
    # 10-digit numbers in a realistic range
    hh_phone = rng.integers(2000000000, 9999999999, size=cfg.n_households, dtype=np.int64)

    p_shared = float(cfg.identity_noise.get("p_household_shared_phone", 0.0))
    shared_mask = rng.random(cfg.n_persons) < p_shared
    phone_num = 2000000000 + (np.arange(cfg.n_persons, dtype=np.int64) % 8000000000)
    phone_num[shared_mask] = hh_phone[household_id[shared_mask]]

    # Default address index: reuse within household vs random
    p_reuse = float(cfg.address_rules.get("p_address_reuse_within_household", 0.0))
    hh_addr = rng.integers(0, cfg.n_addresses, size=cfg.n_households, dtype=np.int64)
    default_address_idx = rng.integers(0, cfg.n_addresses, size=cfg.n_persons, dtype=np.int64)
    reuse_mask = rng.random(cfg.n_persons) < p_reuse
    default_address_idx[reuse_mask] = hh_addr[household_id[reuse_mask]]

    # Anonymous identifiers
    cookie_num = rng.integers(10**10, 10**11, size=cfg.n_persons, dtype=np.int64)
    device_num = rng.integers(10**10, 10**11, size=cfg.n_persons, dtype=np.int64)

    return PersonState(
        person_country=person_country,
        household_id=household_id,
        first_idx=first_idx,
        last_idx=last_idx,
        phone_num=phone_num,
        default_address_idx=default_address_idx,
        cookie_num=cookie_num,
        device_num=device_num,
    )


def primary_digital_customer_id(person_id: np.ndarray) -> np.ndarray:
    return np.char.add("D", person_id.astype(str))


def primary_pos_customer_id(person_id: np.ndarray) -> np.ndarray:
    return np.char.add("P", person_id.astype(str))


def address_id_from_idx(idx: np.ndarray) -> np.ndarray:
    return np.char.add("A", idx.astype(str))


def product_id_from_idx(idx: np.ndarray) -> np.ndarray:
    return np.char.add("PRD", idx.astype(str))


def order_id_from_seq(seq: np.ndarray) -> np.ndarray:
    return np.char.add("O", seq.astype(str))


def store_order_id_from_seq(seq: np.ndarray) -> np.ndarray:
    return np.char.add("S", seq.astype(str))


def event_id_from_seq(seq: np.ndarray) -> np.ndarray:
    return np.char.add("E", seq.astype(str))


def survey_id_from_seq(seq: np.ndarray) -> np.ndarray:
    return np.char.add("SV", seq.astype(str))


def review_id_from_seq(seq: np.ndarray) -> np.ndarray:
    return np.char.add("R", seq.astype(str))


# -------------------------
# Table generators (chunked)
# -------------------------


def gen_dim_product(cfg: Config, rng: np.random.Generator) -> pa.Table:
    n = cfg.n_products
    idx = np.arange(n, dtype=np.int64)
    product_id = product_id_from_idx(idx)

    cat = CATEGORIES[rng.integers(0, len(CATEGORIES), size=n)]
    subcat = np.empty(n, dtype=object)
    for c in np.unique(cat):
        m = cat == c
        subcat[m] = SUBCATEGORIES[c][rng.integers(0, len(SUBCATEGORIES[c]), size=m.sum())]

    name = np.char.add(
        np.char.add(ADJECTIVES[rng.integers(0, len(ADJECTIVES), size=n)], " "),
        np.char.add(NOUNS[rng.integers(0, len(NOUNS), size=n)], ""),
    )

    base_price = (rng.normal(loc=80.0, scale=35.0, size=n).clip(10, 350)).round(2)

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    return pa.table(
        {
            "product_id": product_id,
            "product_name": name,
            "brand": np.full(n, "GENERIC", dtype=object),
            "category": cat,
            "sub_category": subcat,
            "gender": rng.choice(
                np.array([None, "M", "F", "U"], dtype=object), size=n, p=[0.20, 0.25, 0.25, 0.30]
            ),
            "sport": rng.choice(
                np.array([None, "Running", "Training", "Yoga", "Lifestyle"], dtype=object),
                size=n,
                p=[0.25, 0.25, 0.20, 0.10, 0.20],
            ),
            "base_price": base_price,
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )


def gen_dim_address(cfg: Config, rng: np.random.Generator, pools: Pools) -> pa.Table:
    n = cfg.n_addresses
    idx = np.arange(n, dtype=np.int64)
    address_id = address_id_from_idx(idx)

    country_keys = list(cfg.country_mix.keys())
    country_probs = [cfg.country_mix[k] for k in country_keys]
    country_code = sample_from_dist(rng, country_keys, country_probs, n)

    street_num = rng.integers(1, 9999, size=n).astype(str)
    street_name = pools.street_names[rng.integers(0, len(pools.street_names), size=n)]
    suffix = STREET_SUFFIX[rng.integers(0, len(STREET_SUFFIX), size=n)]
    address_line1 = np.char.add(
        np.char.add(street_num, " "), np.char.add(street_name, np.char.add(" ", suffix))
    )

    address_line2 = rng.choice(
        np.array([None, "Apt 1", "Apt 2", "Unit 5", "Suite 200"], dtype=object),
        size=n,
        p=[0.85, 0.05, 0.04, 0.04, 0.02],
    )

    city = pools.city_names[rng.integers(0, len(pools.city_names), size=n)]

    state_region = np.empty(n, dtype=object)
    for c in np.unique(country_code):
        m = country_code == c
        state_region[m] = STATE_BY_COUNTRY[c][
            rng.integers(0, len(STATE_BY_COUNTRY[c]), size=m.sum())
        ]

    postal = postal_code_by_country(rng, country_code)

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    return pa.table(
        {
            "address_id": address_id,
            "country_code": country_code,
            "address_line1": address_line1,
            "address_line2": address_line2,
            "city": city,
            "state_region": state_region,
            "postal_code": postal,
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )


def derive_email_norm(
    pools: Pools,
    person_first_idx: np.ndarray,
    person_last_idx: np.ndarray,
    person_id: np.ndarray,
) -> np.ndarray:
    # user.first.last123@domain (stable-ish)
    ft = pools.first_tok[person_first_idx]
    lt = pools.last_tok[person_last_idx]
    suffix = (person_id % 10000).astype(str)
    dom = DOMAINS[person_id % len(DOMAINS)]
    local = np.char.add(np.char.add(ft, "."), np.char.add(lt, suffix))
    return np.char.add(np.char.add(local, "@"), dom)


def generate_truth_links_chunk(
    source_table: str,
    source_record_id: np.ndarray,
    person_id: np.ndarray,
    created_at: np.ndarray,
) -> pa.Table:
    return pa.table(
        {
            "source_table": np.full(len(source_record_id), source_table, dtype=object),
            "source_record_id": source_record_id,
            "person_id": person_id.astype(np.int64),
            "created_at": created_at,
            "created_date": make_created_date(created_at),
        }
    )


def write_table_and_truth(
    cfg: Config,
    table_name: str,
    table: pa.Table,
    truth: pa.Table,
    partition_cols: List[str],
) -> None:
    base = os.path.join(cfg.out_dir, table_name)
    write_dataset_zstd(table, base, partition_cols)

    if cfg.write_truth_links:
        truth_base = os.path.join(cfg.out_dir, "truth_links")
        write_dataset_zstd(truth, truth_base, ["source_table", "created_date"])


def gen_digital_customer_account_chunk(
    cfg: Config,
    rng: np.random.Generator,
    pools: Pools,
    ps: PersonState,
    n: int,
    id_offset: int,
) -> Tuple[pa.Table, pa.Table]:
    person_id = rng.integers(0, cfg.n_persons, size=n, dtype=np.int64)
    digital_id = np.char.add("DACC", (id_offset + np.arange(n, dtype=np.int64)).astype(str))

    # channel mix
    ch_keys = list(cfg.channel_mix["digital_account"].keys())
    ch_probs = [cfg.channel_mix["digital_account"][k] for k in ch_keys]
    channel = sample_from_dist(rng, ch_keys, ch_probs, n)

    country_code = ps.person_country[person_id]

    # names
    fi = ps.first_idx[person_id] % len(pools.first_names)
    li = ps.last_idx[person_id] % len(pools.last_names)
    first_name = pools.first_names[fi]
    last_name = pools.last_names[li]

    email_norm = derive_email_norm(pools, fi, li, person_id)
    email_raw = format_email_raw(
        rng,
        email_norm,
        float(cfg.identity_noise.get("p_email_plus_alias", 0.0)),
        float(cfg.identity_noise.get("p_email_uppercase", 0.0)),
        float(cfg.identity_noise.get("p_email_spaces", 0.0)),
    )

    # phone
    phone_digits = ps.phone_num[person_id].astype(str)
    phone_norm = phone_digits.astype(object)
    phone_raw = format_phone_raw(
        rng,
        phone_digits.astype(object),
        float(cfg.identity_noise.get("p_phone_punct", 0.0)),
        float(cfg.identity_noise.get("p_phone_country_prefix", 0.0)),
    )

    # missing phone
    mphone = rng.random(n) < float(cfg.identity_noise.get("p_missing_phone_digital", 0.0))
    phone_raw[mphone] = None
    phone_norm[mphone] = None

    # default shipping address
    has_addr = rng.random(n) < float(
        cfg.address_rules.get("p_digital_has_default_shipping_address", 0.0)
    )
    default_addr = address_id_from_idx(ps.default_address_idx[person_id]).astype(object)
    default_addr[~has_addr] = None

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    tbl = pa.table(
        {
            "digital_customer_id": digital_id,
            "channel": channel,
            "country_code": country_code,
            "first_name": first_name,
            "last_name": last_name,
            "email_raw": email_raw,
            "email_norm": email_norm,
            "phone_raw": phone_raw,
            "phone_norm": phone_norm,
            "default_shipping_address_id": default_addr,
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )

    truth = generate_truth_links_chunk(
        "digital_customer_account", digital_id, person_id, created_at
    )
    return tbl, truth


def gen_pos_customer_chunk(
    cfg: Config,
    rng: np.random.Generator,
    pools: Pools,
    ps: PersonState,
    n: int,
    id_offset: int,
) -> Tuple[pa.Table, pa.Table]:
    person_id = rng.integers(0, cfg.n_persons, size=n, dtype=np.int64)
    pos_id = np.char.add("PCUST", (id_offset + np.arange(n, dtype=np.int64)).astype(str))
    country_code = ps.person_country[person_id]

    # sparse names
    fi = ps.first_idx[person_id] % len(pools.first_names)
    li = ps.last_idx[person_id] % len(pools.last_names)
    first_name = pools.first_names[fi].astype(object)
    last_name = pools.last_names[li].astype(object)
    # Some POS records lack names
    mname = rng.random(n) < 0.35
    first_name[mname] = None
    last_name[mname] = None

    # Email usually not captured in-store
    email_norm = derive_email_norm(pools, fi, li, person_id).astype(object)
    email_raw = format_email_raw(
        rng,
        email_norm.copy(),
        float(cfg.identity_noise.get("p_email_plus_alias", 0.0)),
        float(cfg.identity_noise.get("p_email_uppercase", 0.0)),
        float(cfg.identity_noise.get("p_email_spaces", 0.0)),
    ).astype(object)
    memail = rng.random(n) < float(cfg.identity_noise.get("p_missing_email_pos", 0.9))
    email_norm[memail] = None
    email_raw[memail] = None

    # phone sometimes captured
    phone_digits = ps.phone_num[person_id].astype(str).astype(object)
    phone_norm = phone_digits.copy()
    phone_raw = format_phone_raw(
        rng,
        phone_digits.copy(),
        float(cfg.identity_noise.get("p_phone_punct", 0.0)),
        float(cfg.identity_noise.get("p_phone_country_prefix", 0.0)),
    ).astype(object)
    mphone = rng.random(n) < float(cfg.identity_noise.get("p_missing_phone_pos", 0.4))
    phone_norm[mphone] = None
    phone_raw[mphone] = None

    store_id = np.char.add("STORE", rng.integers(1, 500, size=n).astype(str)).astype(object)

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    tbl = pa.table(
        {
            "pos_customer_id": pos_id,
            "country_code": country_code,
            "store_id": store_id,
            "first_name": first_name,
            "last_name": last_name,
            "email_raw": email_raw,
            "email_norm": email_norm,
            "phone_raw": phone_raw,
            "phone_norm": phone_norm,
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )

    truth = generate_truth_links_chunk("pos_customer", pos_id, person_id, created_at)
    return tbl, truth


def gen_ecom_order_chunk(
    cfg: Config,
    rng: np.random.Generator,
    pools: Pools,
    ps: PersonState,
    n: int,
    seq_offset: int,
) -> Tuple[pa.Table, pa.Table]:
    person_id = rng.integers(0, cfg.n_persons, size=n, dtype=np.int64)
    order_seq = seq_offset + np.arange(n, dtype=np.int64)
    order_id = order_id_from_seq(order_seq)

    # channel mix
    ch_keys = list(cfg.channel_mix["ecom_order"].keys())
    ch_probs = [cfg.channel_mix["ecom_order"][k] for k in ch_keys]
    channel = sample_from_dist(rng, ch_keys, ch_probs, n)

    country_code = ps.person_country[person_id]
    currency_code = np.array([CURRENCY_BY_COUNTRY[c] for c in country_code], dtype=object)

    # guest checkout
    guest = rng.random(n) < float(cfg.identity_noise.get("p_guest_checkout", 0.0))
    digital_customer_id = primary_digital_customer_id(person_id).astype(object)
    digital_customer_id[guest] = None

    # email captured at checkout almost always; sometimes differs from account email
    fi = ps.first_idx[person_id] % len(pools.first_names)
    li = ps.last_idx[person_id] % len(pools.last_names)
    base_email_norm = derive_email_norm(pools, fi, li, person_id).astype(object)

    diff = rng.random(n) < float(cfg.identity_noise.get("p_order_email_diff_from_account", 0.0))
    alt_email = np.char.replace(base_email_norm.astype(str), "@", "+checkout@").astype(object)
    email_norm = base_email_norm.copy()
    email_norm[diff] = alt_email[diff]

    email_raw = format_email_raw(
        rng,
        email_norm.astype(str),
        float(cfg.identity_noise.get("p_email_plus_alias", 0.0)),
        float(cfg.identity_noise.get("p_email_uppercase", 0.0)),
        float(cfg.identity_noise.get("p_email_spaces", 0.0)),
    ).astype(object)

    # rare missing email
    memail = rng.random(n) < 0.02
    email_norm[memail] = None
    email_raw[memail] = None

    # phone captured less consistently
    phone_digits = ps.phone_num[person_id].astype(str).astype(object)
    phone_norm = phone_digits.copy()
    phone_raw = format_phone_raw(
        rng,
        phone_digits.copy(),
        float(cfg.identity_noise.get("p_phone_punct", 0.0)),
        float(cfg.identity_noise.get("p_phone_country_prefix", 0.0)),
    ).astype(object)
    mphone = rng.random(n) < float(cfg.identity_noise.get("p_missing_phone_digital", 0.0))
    phone_norm[mphone] = None
    phone_raw[mphone] = None

    # addresses
    ship_has = rng.random(n) < float(cfg.address_rules.get("p_order_has_shipping_address", 0.0))
    bill_has = rng.random(n) < float(cfg.address_rules.get("p_order_has_billing_address", 0.0))
    ship_addr = address_id_from_idx(ps.default_address_idx[person_id]).astype(object)
    bill_addr = address_id_from_idx(ps.default_address_idx[person_id]).astype(object)

    ship_addr[~ship_has] = None
    bill_addr[~bill_has] = None

    # product (single reference)
    prod_idx = rng.integers(0, cfg.n_products, size=n, dtype=np.int64)
    product_id = product_id_from_idx(prod_idx).astype(object)

    # amounts (currency differences not modeled deeply; local currency only)
    subtotal = rng.gamma(shape=2.0, scale=45.0, size=n)
    discount = (subtotal * rng.uniform(0, 0.25, size=n)) * (rng.random(n) < 0.55)
    tax = (subtotal - discount) * rng.uniform(0.0, 0.12, size=n)
    shipping = rng.uniform(0, 12, size=n) * (rng.random(n) < 0.65)
    total = subtotal - discount + tax + shipping

    status = ORDER_STATUS_ECOM[rng.integers(0, len(ORDER_STATUS_ECOM), size=n)]

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    tbl = pa.table(
        {
            "order_id": order_id,
            "channel": channel,
            "country_code": country_code,
            "currency_code": currency_code,
            "digital_customer_id": digital_customer_id,
            "email_raw": email_raw,
            "email_norm": email_norm,
            "phone_raw": phone_raw,
            "phone_norm": phone_norm,
            "shipping_address_id": ship_addr,
            "billing_address_id": bill_addr,
            "product_id": product_id,
            "order_status": status,
            "subtotal_amount": np.round(subtotal, 2),
            "discount_amount": np.round(discount, 2),
            "tax_amount": np.round(tax, 2),
            "shipping_amount": np.round(shipping, 2),
            "total_amount": np.round(total, 2),
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )

    truth = generate_truth_links_chunk("ecom_order", order_id, person_id, created_at)
    return tbl, truth


def gen_store_order_chunk(
    cfg: Config,
    rng: np.random.Generator,
    pools: Pools,
    ps: PersonState,
    n: int,
    seq_offset: int,
) -> Tuple[pa.Table, pa.Table]:
    person_id = rng.integers(0, cfg.n_persons, size=n, dtype=np.int64)
    seq = seq_offset + np.arange(n, dtype=np.int64)
    store_order_id = store_order_id_from_seq(seq)

    country_code = ps.person_country[person_id]
    currency_code = np.array([CURRENCY_BY_COUNTRY[c] for c in country_code], dtype=object)

    store_id = np.char.add("STORE", rng.integers(1, 500, size=n).astype(str)).astype(object)

    # POS customer link sometimes missing
    pos_customer_id = primary_pos_customer_id(person_id).astype(object)
    mpos = rng.random(n) < 0.35
    pos_customer_id[mpos] = None

    # Email rare in store, phone more common than email
    fi = ps.first_idx[person_id] % len(pools.first_names)
    li = ps.last_idx[person_id] % len(pools.last_names)
    base_email_norm = derive_email_norm(pools, fi, li, person_id).astype(object)
    email_raw = format_email_raw(
        rng,
        base_email_norm.astype(str),
        float(cfg.identity_noise.get("p_email_plus_alias", 0.0)),
        float(cfg.identity_noise.get("p_email_uppercase", 0.0)),
        float(cfg.identity_noise.get("p_email_spaces", 0.0)),
    ).astype(object)
    email_norm = base_email_norm.copy()

    # mostly missing
    memail = rng.random(n) < 0.92
    email_norm[memail] = None
    email_raw[memail] = None

    phone_digits = ps.phone_num[person_id].astype(str).astype(object)
    phone_norm = phone_digits.copy()
    phone_raw = format_phone_raw(
        rng,
        phone_digits.copy(),
        float(cfg.identity_noise.get("p_phone_punct", 0.0)),
        float(cfg.identity_noise.get("p_phone_country_prefix", 0.0)),
    ).astype(object)
    mphone = rng.random(n) < 0.35
    phone_norm[mphone] = None
    phone_raw[mphone] = None

    prod_idx = rng.integers(0, cfg.n_products, size=n, dtype=np.int64)
    product_id = product_id_from_idx(prod_idx).astype(object)

    subtotal = rng.gamma(shape=2.2, scale=35.0, size=n)
    discount = (subtotal * rng.uniform(0, 0.20, size=n)) * (rng.random(n) < 0.45)
    tax = (subtotal - discount) * rng.uniform(0.0, 0.10, size=n)
    total = subtotal - discount + tax

    status = ORDER_STATUS_STORE[rng.integers(0, len(ORDER_STATUS_STORE), size=n)]

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    tbl = pa.table(
        {
            "store_order_id": store_order_id,
            "channel": np.full(n, "store", dtype=object),
            "country_code": country_code,
            "currency_code": currency_code,
            "store_id": store_id,
            "pos_customer_id": pos_customer_id,
            "email_raw": email_raw,
            "email_norm": email_norm,
            "phone_raw": phone_raw,
            "phone_norm": phone_norm,
            "product_id": product_id,
            "order_status": status,
            "subtotal_amount": np.round(subtotal, 2),
            "discount_amount": np.round(discount, 2),
            "tax_amount": np.round(tax, 2),
            "total_amount": np.round(total, 2),
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )

    truth = generate_truth_links_chunk("store_order", store_order_id, person_id, created_at)
    return tbl, truth


def gen_sdk_event_chunk(
    cfg: Config,
    rng: np.random.Generator,
    ps: PersonState,
    n: int,
    seq_offset: int,
) -> Tuple[pa.Table, pa.Table]:
    person_id = rng.integers(0, cfg.n_persons, size=n, dtype=np.int64)
    seq = seq_offset + np.arange(n, dtype=np.int64)
    event_id = event_id_from_seq(seq)

    # channel mix
    ch_keys = list(cfg.channel_mix["sdk_event"].keys())
    ch_probs = [cfg.channel_mix["sdk_event"][k] for k in ch_keys]
    channel = sample_from_dist(rng, ch_keys, ch_probs, n)

    country_code = ps.person_country[person_id]

    # session + anonymous ids
    # session_id is just a random stable-ish number; not modeling full sessions
    session_id = np.char.add("SESS", rng.integers(10**9, 10**10, size=n).astype(str)).astype(object)
    anon = np.empty(n, dtype=object)
    web = channel == "web"
    if web.any():
        anon[web] = np.char.add("C", ps.cookie_num[person_id[web]].astype(str))
    app = ~web
    if app.any():
        anon[app] = np.char.add("DEV", ps.device_num[person_id[app]].astype(str))

    # event types
    et = EVENT_TYPES[rng.integers(0, len(EVENT_TYPES), size=n)]

    # customer id appears after login; we approximate: populate for:
    # - login events
    # - purchase events
    # - and for an extra fraction of events (post-login)
    post_login_p = float(cfg.sdk_rules.get("p_login_event", 0.0))
    has_customer = (et == "login") | (et == "purchase") | (rng.random(n) < post_login_p)
    digital_customer_id = primary_digital_customer_id(person_id).astype(object)
    digital_customer_id[~has_customer] = None

    # product_id on product-related events
    p_prod = float(cfg.sdk_rules.get("p_events_with_product_id", 0.0))
    has_product = (
        (et == "view_item") | (et == "add_to_cart") | (et == "purchase") | (rng.random(n) < p_prod)
    )
    prod_idx = rng.integers(0, cfg.n_products, size=n, dtype=np.int64)
    product_id = product_id_from_idx(prod_idx).astype(object)
    product_id[~has_product] = None

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = (
        created_at  # events typically immutable; keep identical (or add tiny delay if you want)
    )
    created_date = make_created_date(created_at)

    tbl = pa.table(
        {
            "event_id": event_id,
            "channel": channel,
            "country_code": country_code,
            "event_type": et,
            "session_id": session_id,
            "anonymous_id": anon,
            "digital_customer_id": digital_customer_id,
            "product_id": product_id,
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )

    truth = generate_truth_links_chunk("sdk_event", event_id, person_id, created_at)
    return tbl, truth


def gen_bazaarvoice_survey_chunk(
    cfg: Config,
    rng: np.random.Generator,
    pools: Pools,
    ps: PersonState,
    n: int,
    seq_offset: int,
) -> Tuple[pa.Table, pa.Table]:
    person_id = rng.integers(0, cfg.n_persons, size=n, dtype=np.int64)
    seq = seq_offset + np.arange(n, dtype=np.int64)
    survey_id = survey_id_from_seq(seq)

    # channel distribution: reuse ecom mix + store
    channel = rng.choice(
        np.array(["web", "app", "store"], dtype=object), size=n, p=[0.35, 0.35, 0.30]
    )
    country_code = ps.person_country[person_id]

    # order reference (not FK-perfect; realistic enough for testing)
    has_order = rng.random(n) < float(cfg.bazaarvoice_rules.get("p_survey_has_order_id", 0.0))
    order_source = rng.choice(
        np.array(["ecom", "store"], dtype=object), size=n, p=[0.65, 0.35]
    ).astype(object)
    order_id = np.empty(n, dtype=object)
    # create synthetic references by sampling seq ranges (keeps IDs plausible)
    ecom_max = max(cfg.rows.get("ecom_order", 1), 1)
    store_max = max(cfg.rows.get("store_order", 1), 1)
    ecom_pick = rng.integers(0, ecom_max, size=n)
    store_pick = rng.integers(0, store_max, size=n)
    order_id[order_source == "ecom"] = order_id_from_seq(ecom_pick[order_source == "ecom"])
    order_id[order_source == "store"] = store_order_id_from_seq(store_pick[order_source == "store"])
    order_id[~has_order] = None
    order_source[~has_order] = None

    # identifiers sometimes present
    p_email = float(cfg.bazaarvoice_rules.get("p_survey_has_email", 0.0))
    p_phone = float(cfg.bazaarvoice_rules.get("p_survey_has_phone", 0.0))
    ensure_one = cfg.bazaarvoice_rules.get("ensure_one_identifier", False)

    fi = ps.first_idx[person_id] % len(pools.first_names)
    li = ps.last_idx[person_id] % len(pools.last_names)
    email_norm = derive_email_norm(pools, fi, li, person_id).astype(object)
    email_raw = format_email_raw(
        rng,
        email_norm.astype(str),
        float(cfg.identity_noise.get("p_email_plus_alias", 0.0)),
        float(cfg.identity_noise.get("p_email_uppercase", 0.0)),
        float(cfg.identity_noise.get("p_email_spaces", 0.0)),
    ).astype(object)

    keep_email = rng.random(n) < p_email

    phone_digits = ps.phone_num[person_id].astype(str).astype(object)
    phone_norm = phone_digits.copy()
    phone_raw = format_phone_raw(
        rng,
        phone_digits.copy(),
        float(cfg.identity_noise.get("p_phone_punct", 0.0)),
        float(cfg.identity_noise.get("p_phone_country_prefix", 0.0)),
    ).astype(object)

    keep_phone = rng.random(n) < p_phone

    # Ensure at least one identifier per record to avoid orphans
    if ensure_one:
        orphaned = ~keep_email & ~keep_phone
        # For orphans, randomly pick email or phone to keep (prefer email)
        restore_email = orphaned & (rng.random(n) < 0.7)
        restore_phone = orphaned & ~restore_email
        keep_email = keep_email | restore_email
        keep_phone = keep_phone | restore_phone

    email_norm[~keep_email] = None
    email_raw[~keep_email] = None
    phone_norm[~keep_phone] = None
    phone_raw[~keep_phone] = None

    nps = rng.integers(0, 11, size=n, dtype=np.int16)
    free_text = rng.choice(
        np.array(
            [
                None,
                "Love it",
                "Quality could be better",
                "Great fit",
                "Shipping slow",
                "Will buy again",
            ],
            dtype=object,
        ),
        size=n,
        p=[0.50, 0.12, 0.10, 0.10, 0.08, 0.10],
    )

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    tbl = pa.table(
        {
            "survey_response_id": survey_id,
            "country_code": country_code,
            "channel": channel,
            "order_source": order_source,
            "order_id": order_id,
            "email_raw": email_raw,
            "email_norm": email_norm,
            "phone_raw": phone_raw,
            "phone_norm": phone_norm,
            "nps_score": nps,
            "free_text": free_text,
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )

    truth = generate_truth_links_chunk("bazaarvoice_survey", survey_id, person_id, created_at)
    return tbl, truth


def gen_product_review_chunk(
    cfg: Config,
    rng: np.random.Generator,
    pools: Pools,
    ps: PersonState,
    n: int,
    seq_offset: int,
) -> Tuple[pa.Table, pa.Table]:
    person_id = rng.integers(0, cfg.n_persons, size=n, dtype=np.int64)
    seq = seq_offset + np.arange(n, dtype=np.int64)
    review_id = review_id_from_seq(seq)

    channel = rng.choice(np.array(["web", "app"], dtype=object), size=n, p=[0.55, 0.45])
    country_code = ps.person_country[person_id]

    prod_idx = rng.integers(0, cfg.n_products, size=n, dtype=np.int64)
    product_id = product_id_from_idx(prod_idx).astype(object)

    # sometimes linked to customer id / email
    p_cust = float(cfg.bazaarvoice_rules.get("p_review_has_customer_id", 0.0))
    p_email = float(cfg.bazaarvoice_rules.get("p_review_has_email", 0.0))
    ensure_one = cfg.bazaarvoice_rules.get("ensure_one_identifier", False)

    digital_customer_id = primary_digital_customer_id(person_id).astype(object)
    keep_cust = rng.random(n) < p_cust
    digital_customer_id[~keep_cust] = None

    fi = ps.first_idx[person_id] % len(pools.first_names)
    li = ps.last_idx[person_id] % len(pools.last_names)
    email_norm = derive_email_norm(pools, fi, li, person_id).astype(object)
    email_raw = format_email_raw(
        rng,
        email_norm.astype(str),
        float(cfg.identity_noise.get("p_email_plus_alias", 0.0)),
        float(cfg.identity_noise.get("p_email_uppercase", 0.0)),
        float(cfg.identity_noise.get("p_email_spaces", 0.0)),
    ).astype(object)

    keep_email = rng.random(n) < p_email

    # Ensure at least one identifier per record to avoid orphans
    if ensure_one:
        orphaned = ~keep_cust & ~keep_email
        # For orphans, randomly pick one identifier to keep (prefer customer_id)
        restore_cust = orphaned & (rng.random(n) < 0.7)
        restore_email = orphaned & ~restore_cust
        keep_cust = keep_cust | restore_cust
        keep_email = keep_email | restore_email
        # Restore the values
        digital_customer_id[restore_cust] = primary_digital_customer_id(
            person_id[restore_cust]
        ).astype(object)

    email_norm[~keep_email] = None
    email_raw[~keep_email] = None

    rating = rng.integers(1, 6, size=n, dtype=np.int16)
    title = rng.choice(
        np.array([None, "Great", "Okay", "Not as expected", "Love it"], dtype=object),
        size=n,
        p=[0.45, 0.18, 0.12, 0.10, 0.15],
    )
    text = rng.choice(
        np.array(
            [
                None,
                "Super comfortable.",
                "Nice quality.",
                "Runs small.",
                "Color looks different.",
                "Would recommend.",
            ],
            dtype=object,
        ),
        size=n,
        p=[0.55, 0.12, 0.11, 0.08, 0.07, 0.07],
    )

    created_at = random_datetimes_ms(rng, cfg.start_date, cfg.days, n)
    updated_at = add_updated_at(
        rng, created_at, int(cfg.identity_noise.get("updated_at_delay_days_max", 14))
    )
    created_date = make_created_date(created_at)

    tbl = pa.table(
        {
            "review_id": review_id,
            "country_code": country_code,
            "channel": channel,
            "product_id": product_id,
            "digital_customer_id": digital_customer_id,
            "email_raw": email_raw,
            "email_norm": email_norm,
            "rating": rating,
            "review_title": title,
            "review_text": text,
            "created_at": created_at,
            "updated_at": updated_at,
            "created_date": created_date,
        }
    )

    truth = generate_truth_links_chunk("product_review", review_id, person_id, created_at)
    return tbl, truth


# -------------------------
# Driver
# -------------------------


def generate_table_in_chunks(
    cfg: Config,
    rng: np.random.Generator,
    table_name: str,
    total_rows: int,
    chunk_rows: int,
    gen_fn,
    partition_cols: List[str],
) -> None:
    written = 0
    offset = 0
    while written < total_rows:
        n = min(chunk_rows, total_rows - written)
        tbl, truth = gen_fn(n, offset)
        write_table_and_truth(cfg, table_name, tbl, truth, partition_cols)
        written += n
        offset += n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to JSON config")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ensure_dir(cfg.out_dir)

    rng = np.random.default_rng(cfg.seed)

    # Build pools once
    pools = build_pools(cfg.seed)

    # Build person state (compact)
    ps = build_person_state(cfg, rng)
    # Adjust person name indices to match pool sizes (because build_person_state uses placeholders)
    ps.first_idx = (ps.first_idx % len(pools.first_names)).astype(np.int32)
    ps.last_idx = (ps.last_idx % len(pools.last_names)).astype(np.int32)

    # 1) Dimensions
    print("Writing dim_product...")
    dim_product = gen_dim_product(cfg, rng)
    write_dataset_zstd(dim_product, os.path.join(cfg.out_dir, "dim_product"), ["created_date"])

    print("Writing dim_address...")
    dim_address = gen_dim_address(cfg, rng, pools)
    write_dataset_zstd(
        dim_address, os.path.join(cfg.out_dir, "dim_address"), ["country_code", "created_date"]
    )

    # 2) Facts / sources
    print("Writing digital_customer_account...")

    def gen_digital(n, off):
        return gen_digital_customer_account_chunk(cfg, rng, pools, ps, n, off)

    generate_table_in_chunks(
        cfg,
        rng,
        "digital_customer_account",
        cfg.rows["digital_customer_account"],
        cfg.chunk_rows,
        gen_digital,
        ["country_code", "channel", "created_date"],
    )

    print("Writing pos_customer...")

    def gen_pos(n, off):
        return gen_pos_customer_chunk(cfg, rng, pools, ps, n, off)

    generate_table_in_chunks(
        cfg,
        rng,
        "pos_customer",
        cfg.rows["pos_customer"],
        cfg.chunk_rows,
        gen_pos,
        ["country_code", "created_date"],
    )

    print("Writing ecom_order...")

    def gen_ecom(n, off):
        return gen_ecom_order_chunk(cfg, rng, pools, ps, n, off)

    generate_table_in_chunks(
        cfg,
        rng,
        "ecom_order",
        cfg.rows["ecom_order"],
        cfg.chunk_rows,
        gen_ecom,
        ["country_code", "channel", "created_date"],
    )

    print("Writing store_order...")

    def gen_store(n, off):
        return gen_store_order_chunk(cfg, rng, pools, ps, n, off)

    generate_table_in_chunks(
        cfg,
        rng,
        "store_order",
        cfg.rows["store_order"],
        cfg.chunk_rows,
        gen_store,
        ["country_code", "created_date"],
    )

    print("Writing sdk_event...")

    def gen_sdk(n, off):
        return gen_sdk_event_chunk(cfg, rng, ps, n, off)

    generate_table_in_chunks(
        cfg,
        rng,
        "sdk_event",
        cfg.rows["sdk_event"],
        cfg.chunk_rows,
        gen_sdk,
        ["country_code", "channel", "created_date"],
    )

    print("Writing bazaarvoice_survey...")

    def gen_survey(n, off):
        return gen_bazaarvoice_survey_chunk(cfg, rng, pools, ps, n, off)

    generate_table_in_chunks(
        cfg,
        rng,
        "bazaarvoice_survey",
        cfg.rows["bazaarvoice_survey"],
        cfg.chunk_rows,
        gen_survey,
        ["country_code", "channel", "created_date"],
    )

    print("Writing product_review...")

    def gen_review(n, off):
        return gen_product_review_chunk(cfg, rng, pools, ps, n, off)

    generate_table_in_chunks(
        cfg,
        rng,
        "product_review",
        cfg.rows["product_review"],
        cfg.chunk_rows,
        gen_review,
        ["country_code", "channel", "created_date"],
    )

    print(f"Done. Output at: {cfg.out_dir}")


if __name__ == "__main__":
    main()
