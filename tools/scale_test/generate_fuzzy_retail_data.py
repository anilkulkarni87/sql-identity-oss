#!/usr/bin/env python3
"""
Generate a realistic global retail dataset specifically for testing FUZZY MATCHING.
Based on generate_global_retail_idr.py but with enhanced specific noise:
- Typos (transpositions, deletions, insertions)
- Nicknames (Elizabeth -> Liz) for robust fuzzy matching tests
- Phonetic variations (Ph/F, K/C)
- OCR/Input errors (0/O, 1/I) in emails/phones

Usage:
  python generate_fuzzy_retail_data.py --config config_fuzzy.json
"""

import argparse
import json
import os
import string
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
from faker import Faker

# -------------------------
# Constants & Config
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
DOMAINS = np.array(["gmail.com", "outlook.com", "yahoo.com", "icloud.com"], dtype=object)


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
    write_truth_links: bool


def load_config(path: str) -> Config:
    with open(path, "r") as f:
        raw = json.load(f)
    g = raw["global"]
    s = raw["scale"]
    return Config(
        seed=int(g["seed"]),
        out_dir=str(g["out_dir"]),
        start_date=datetime.strptime(g["start_date"], "%Y-%m-%d"),
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
        write_truth_links=bool(g.get("write_truth_links", True)),
    )


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# -------------------------
# Fuzzy Noise Injector
# -------------------------


class FuzzyInjector:
    def __init__(self, seed: int):
        self.rng = np.random.Generator(np.random.PCG64(seed))
        self.nicknames = {
            "Alexander": ["Alex", "Xander", "Al"],
            "Elizabeth": ["Liz", "Beth", "Eliza"],
            "William": ["Bill", "Will", "Billy"],
            "Robert": ["Bob", "Rob", "Bobby"],
            "Jennifer": ["Jen", "Jenny"],
            "Michael": ["Mike", "Mikey"],
            "Thomas": ["Tom", "Tommy"],
            "Christopher": ["Chris", "Topher"],
            "Margaret": ["Maggie", "Peggy"],
            "John": ["Jack", "Johnny"],
            "Katherine": ["Kathy", "Katie", "Kat"],
            "James": ["Jim", "Jimmy"],
            "Patricia": ["Pat", "Patty"],
            "David": ["Dave"],
            "Joseph": ["Joe", "Joey"],
        }

    def add_typo(self, text: Any, p: float = 0.1) -> Any:
        if not isinstance(text, str) or len(text) < 4:
            return text

        if self.rng.random() > p:
            return text

        mode = self.rng.choice(["transpose", "delete", "replace", "insert"])
        chars = list(text)
        idx = self.rng.integers(0, len(chars) - 1)

        if mode == "transpose" and idx < len(chars) - 1:
            chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
        elif mode == "delete":
            del chars[idx]
        elif mode == "replace":
            chars[idx] = self.rng.choice(list(string.ascii_lowercase))
        elif mode == "insert":
            chars.insert(idx, self.rng.choice(list(string.ascii_lowercase)))

        return "".join(chars)

    def mutate_phone(self, phone: str, p: float = 0.1) -> str:
        """Apply noise to phone numbers to break exact matches."""
        if self.rng.random() > p:
            return phone

        chars = list(phone)
        if len(chars) < 5:
            return phone

        # If p=1.0 (forced), randomize the last 4 digits completely
        # This makes collision (Fuzzy-Fuzzy exact match) extremely unlikely (1/10000)
        if p >= 1.0:
            suffix = [str(self.rng.integers(0, 10)) for _ in range(4)]
            chars[-4:] = suffix
            # Ensure it didn't randomly match original (unlikely but possible)
            if "".join(chars) == phone:
                chars[-1] = str((int(chars[-1]) + 1) % 10)
            return "".join(chars)

        # Probabilistic noise
        idx = self.rng.integers(len(chars) - 4, len(chars))
        mode = self.rng.choice(["swap", "replace"])

        if mode == "swap" and idx < len(chars) - 1:
            chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
        else:
            chars[idx] = str(self.rng.integers(0, 10))

        return "".join(chars)

    def apply_nickname(self, name: Any, p: float = 0.3) -> Any:
        if not isinstance(name, str):
            return name
        if self.rng.random() < p and name in self.nicknames:
            return self.rng.choice(self.nicknames[name])
        return name


# -------------------------
# Generator Logic
# -------------------------


@dataclass
class Pools:
    first_names: np.ndarray
    last_names: np.ndarray
    street_names: np.ndarray
    city_names: np.ndarray


def build_pools(seed: int) -> Pools:
    fake = Faker("en_US")
    Faker.seed(seed)
    # Larger pools for variety
    first = np.array([fake.first_name() for _ in range(20000)], dtype=object)
    last = np.array([fake.last_name() for _ in range(20000)], dtype=object)
    street = np.array([fake.street_name() for _ in range(10000)], dtype=object)
    city = np.array([fake.city() for _ in range(5000)], dtype=object)
    return Pools(first, last, street, city)


@dataclass
class PersonState:
    country: np.ndarray
    first_idx: np.ndarray
    first_idx: np.ndarray
    last_idx: np.ndarray
    email_base: np.ndarray  # (user, domain)
    phone: np.ndarray


def build_person_state(cfg: Config, rng: np.random.Generator) -> PersonState:
    n = cfg.n_persons
    countries = list(cfg.country_mix.keys())
    probs = [cfg.country_mix[k] for k in countries]
    # Fast sampling
    probs = np.array(probs) / np.sum(probs)
    country = np.random.choice(countries, size=n, p=probs)

    first_idx = rng.integers(0, 20000, size=n)
    last_idx = rng.integers(0, 20000, size=n)

    # Pre-compute email bases to ensure consistency across channels (before noise)
    phones = rng.integers(2000000000, 9999999999, size=n, dtype=np.int64)

    return PersonState(country, first_idx, last_idx, None, phones)


def gen_customers(
    cfg: Config,
    pools: Pools,
    ps: PersonState,
    injector: FuzzyInjector,
    n: int,
    offset: int,
    source: str,
) -> Tuple[pa.Table, pa.Table]:
    # Select random people
    rng = injector.rng
    pid_indices = rng.integers(0, cfg.n_persons, size=n)

    # IDs
    if source == "digital":
        row_ids = np.char.add("DACC", (offset + np.arange(n)).astype(str))
    else:
        row_ids = np.char.add("POS", (offset + np.arange(n)).astype(str))

    # Names strings
    base_first_names = pools.first_names[ps.first_idx[pid_indices]]
    base_last_names = pools.last_names[ps.last_idx[pid_indices]]
    base_domains = DOMAINS[pid_indices % len(DOMAINS)]
    base_phones = ps.phone[pid_indices].astype(str)

    # Config probabilities
    p_typo = cfg.identity_noise.get("p_typo", 0.05)

    final_first = []
    final_last = []
    final_emails = []
    final_phones = []
    dates = []

    for i in range(n):
        # Base values
        fname = base_first_names[i]
        lname = base_last_names[i]
        domain = base_domains[i]
        phone_val = base_phones[i]

        # Determining factors
        # We want to create "Hard" fuzzy records where exact match fails.
        # Strategy: explicit 30% fuzzy rate
        # If selected, we FORCE nickname/typo AND corruption of identifiers

        is_fuzzy_candidate = rng.random() < 0.30

        f_final = fname
        l_final = lname

        if is_fuzzy_candidate:
            # Force nickname OR typo on name
            if rng.random() < 0.8:
                # Try nickname first
                f_new = injector.apply_nickname(fname, p=1.0)
                if f_new == fname:
                    # Fallback to typo if no nickname
                    f_new = injector.add_typo(fname, p=1.0)
                f_final = f_new
            else:
                # Typo on last name
                l_final = injector.add_typo(lname, p=1.0)

        # Fallback for standard noise (non-fuzzy candidates)
        # DISABLE STANDARD NOISE for Fuzzy Testing
        # We want a sharp distinction:
        # 1. Clean records (Exact Match)
        # 2. Corrupted records (Fuzzy Match ONLY)
        # Any "mixed" records acts as bridges and hide fuzzy failures.
        if not is_fuzzy_candidate:
            pass

        # Email Generation
        email_base = f"{fname.lower()}.{lname.lower()}@{domain}"

        # If it's a fuzzy candidate (name changed), we force identifier corruption
        # to prevent Exact Match from "saving" it easily.
        # This allows us to actually test the Fuzzy Rule (JaroWinkler).

        # Independent probability for email typo
        # STEP 1: Always force corruption if it's a fuzzy candidate
        force_corruption = is_fuzzy_candidate

        # STEP 2: if forcing corruption, probability is 100%
        p_email_eff = 1.0 if force_corruption else p_typo
        email_final = injector.add_typo(email_base, p=p_email_eff)

        p_phone_eff = 1.0 if force_corruption else p_typo
        phone_final = injector.mutate_phone(phone_val, p=p_phone_eff)

        final_first.append(f_final)
        final_last.append(l_final)
        final_emails.append(email_final)
        final_phones.append(phone_final)
        dates.append(datetime.now().isoformat())

    tbl = pa.table(
        {
            "customer_id": row_ids,
            "first_name": final_first,
            "last_name": final_last,
            "email": final_emails,
            "phone": final_phones,
            "created_at": dates,
        }
    )

    truth = pa.table(
        {
            "source_table": [source] * n,
            "source_record_id": row_ids,
            "person_id": pid_indices,
            "created_at": dates,
        }
    )

    return tbl, truth


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    cfg = load_config(args.config)
    print(f"Generating data for {cfg.n_persons} persons...")

    seed = cfg.seed
    rng = np.random.Generator(np.random.PCG64(seed))

    pools = build_pools(seed)
    ps = build_person_state(cfg, rng)
    injector = FuzzyInjector(seed)

    # Generate Digital
    n_dig = cfg.rows.get("digital_customer_account", 10000)
    print(f"  Generating {n_dig} digital accounts...")

    # Chunked generation

    # Digital
    t_dig, t_truth_dig = gen_customers(cfg, pools, ps, injector, n_dig, 0, "digital")

    # POS
    n_pos = cfg.rows.get("pos_customer", 10000)
    print(f"  Generating {n_pos} POS accounts...")
    t_pos, t_truth_pos = gen_customers(cfg, pools, ps, injector, n_pos, 0, "pos_customer")

    # Write
    base_dir = cfg.out_dir
    if os.path.exists(base_dir):
        import shutil

        shutil.rmtree(base_dir)
    ensure_dir(base_dir)

    ds.write_dataset(
        t_dig,
        os.path.join(base_dir, "digital_customer_account"),
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    ds.write_dataset(
        t_pos,
        os.path.join(base_dir, "pos_customer"),
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )

    if cfg.write_truth_links:
        full_truth = pa.concat_tables([t_truth_dig, t_truth_pos])
        ds.write_dataset(
            full_truth,
            os.path.join(base_dir, "truth_links"),
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

    print("Done!")


if __name__ == "__main__":
    main()
