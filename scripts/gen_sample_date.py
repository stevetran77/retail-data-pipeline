#!/usr/bin/env python3
r"""
generate_retail_data.py
Generate synthetic retail dataset with Faker
and intentionally inject data quality issues
for testing ETL/DBT cleaning logic.

Output folder: C:\Users\cau.tran\retail-data-pipeline\data_samples
"""

import os
import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

# ======= CONFIG =======
OUT_DIR = r"C:\Users\cau.tran\retail-data-pipeline\data_samples"
SEED = 77
N_USERS = 1000
N_PRODUCTS = 200
N_ORDERS = 5000
N_SESSIONS = 8000
START_DATE = "2024-01-01"
END_DATE = "2025-10-01"

# Dirty data injection ratios
INJECT_DIRT = True
PCT_NULLS = 0.01
PCT_DUPLICATES = 0.005
PCT_OUTLIERS = 0.002
PCT_BAD_EMAIL = 0.01
PCT_BAD_PHONE = 0.015
PCT_ORPHAN_ITEMS = 0.003
PCT_NEG_QTY = 0.002
MIXED_DATE_FMT = True
TRAILING_SPACES = True
# =======================


def ensure_dir(d): os.makedirs(d, exist_ok=True)
def random_timestamp_between(start_dt, end_dt):
    delta = end_dt - start_dt
    return start_dt + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


# ---------- Generate tables ----------
def generate_users(n_users, fake):
    rows = []
    for i in range(1, n_users + 1):
        profile = fake.simple_profile()
        rows.append({
            "user_id": f"user_{i}",
            "full_name": profile["name"],
            "username": profile["username"],
            "email": profile["mail"],
            "phone": fake.phone_number(),
            "city": fake.city(),
            "created_at": fake.date_time_between(start_date='-2y', end_date='now').isoformat()
        })
    return pd.DataFrame(rows)


def generate_products(n_products, fake):
    categories = ["Beverages", "Main", "Dessert", "Snack", "Sauce", "Set Meal"]
    rows = []
    for i in range(1, n_products + 1):
        price = round(random.uniform(15000, 350000), -3)
        rows.append({
            "product_id": f"prd_{i}",
            "name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(categories),
            "price": price,
            "created_at": fake.date_time_between(start_date='-3y', end_date='now').isoformat()
        })
    df = pd.DataFrame(rows)
    df["popularity"] = np.random.zipf(a=1.5, size=n_products)
    df = df.sort_values("popularity", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    return df


def generate_orders(n_orders, users_df, products_df, fake, start_dt, end_dt):
    orders, order_items = [], []
    product_ids = products_df["product_id"].tolist()
    weights = products_df["popularity"].astype(float)
    weights = (weights + 1e-6) / weights.sum()

    for oid in tqdm(range(1, n_orders + 1), desc="Generating orders"):
        user = users_df.sample(1).iloc[0]
        created_ts = random_timestamp_between(start_dt, end_dt)
        n_items = np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        chosen = np.random.choice(product_ids, size=n_items, replace=False, p=weights)

        total_amount, items = 0, []
        for pid in chosen:
            prod = products_df.loc[products_df["product_id"] == pid].iloc[0]
            qty = int(np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1]))
            line_total = qty * prod["price"]
            total_amount += line_total
            items.append({
                "order_id": f"ord_{oid}",
                "product_id": pid,
                "quantity": qty,
                "unit_price": prod["price"],
                "line_total": line_total
            })

        status = random.choices(
            ["completed", "cancelled", "returned", "pending"],
            weights=[0.85, 0.05, 0.03, 0.07]
        )[0]
        orders.append({
            "order_id": f"ord_{oid}",
            "user_id": user["user_id"],
            "order_created_at": created_ts.isoformat(),
            "total_amount": total_amount,
            "payment_method": random.choice(["cash", "card", "momo", "vnpay"]),
            "status": status,
            "branch": fake.city()
        })
        order_items.extend(items)
    return pd.DataFrame(orders), pd.DataFrame(order_items)


def generate_sessions(n_sessions, users_df, fake, start_dt, end_dt):
    sessions = []
    for sid in range(1, n_sessions + 1):
        user = users_df.sample(1).iloc[0]
        start_ts = random_timestamp_between(start_dt, end_dt)
        duration = random.randint(30, 60 * 60 * 3)
        end_ts = start_ts + timedelta(seconds=duration)
        sessions.append({
            "session_id": f"sess_{sid}",
            "user_id": user["user_id"],
            "started_at": start_ts.isoformat(),
            "ended_at": end_ts.isoformat(),
            "duration_seconds": duration,
            "device_type": random.choices(["mobile", "desktop", "tablet"], [0.75, 0.2, 0.05])[0],
            "entry_page": fake.uri_path(),
            "campaign": random.choice([None, "fb_ads", "google", "organic", "email"])
        })
    return pd.DataFrame(sessions)


# ---------- Dirty helpers ----------
def _maybe(val, p): return None if random.random() < p else val
def inject_nulls(df, cols, p):
    for c in cols: df[c] = df[c].apply(lambda v: _maybe(v, p))
    return df
def inject_trailing_spaces(df, cols):
    for c in cols: df[c] = df[c].apply(lambda v: (str(v) + " ") if random.random() < 0.2 else v)
    return df
def inject_duplicates(df, pct):
    n = max(1, int(len(df)*pct)); return pd.concat([df, df.sample(n, replace=True)], ignore_index=True)
def inject_outliers_numeric(df, cols, pct, factor=8):
    idx = np.random.choice(df.index, max(1,int(len(df)*pct)), replace=False)
    for c in cols:
        if c in df.columns:
            df.loc[idx, c] = df.loc[idx, c] * factor
    return df
def inject_bad_email(df, col, p):
    def corrupt(e):
        if random.random() >= p: return e
        return random.choice([e.replace("@",""), "not_an_email", e.upper()])
    df[col] = df[col].apply(corrupt); return df
def inject_bad_phone(df, col, p):
    def corrupt(ph):
        if random.random() >= p: return ph
        return random.choice(["123", "abcde", ph.replace(" ", "")[:5]])
    df[col] = df[col].apply(corrupt); return df
def mix_date_formats(series):
    out = []
    for v in series:
        if not v: out.append(v); continue
        dt = pd.to_datetime(v)
        fmt = random.choice(["iso", "dmy", "mdy"])
        if fmt == "iso": out.append(dt.isoformat())
        elif fmt == "dmy": out.append(dt.strftime("%d/%m/%Y %H:%M:%S"))
        else: out.append(dt.strftime("%m-%d-%Y %H:%M:%S"))
    return pd.Series(out, index=series.index)


# ---------- Save ----------
def save(df, name):
    path = os.path.join(OUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False)
    print(f"[OK] Saved {path} ({len(df)} rows)")


# ---------- Main ----------
def main():
    ensure_dir(OUT_DIR)
    random.seed(SEED); np.random.seed(SEED)
    fake = Faker("vi_VN"); Faker.seed(SEED)
    start_dt, end_dt = map(datetime.fromisoformat, [START_DATE, END_DATE])

    print("[>] Generating users...")
    users_df = generate_users(N_USERS, fake)

    print("[>] Generating products...")
    products_df = generate_products(N_PRODUCTS, fake)

    print("[>] Generating orders...")
    orders_df, order_items_df = generate_orders(N_ORDERS, users_df, products_df, fake, start_dt, end_dt)

    print("[>] Generating sessions...")
    sessions_df = generate_sessions(N_SESSIONS, users_df, fake, start_dt, end_dt)

    # ---------- Inject dirty data ----------
    if INJECT_DIRT:
        print("[WARN] Injecting dirty data...")

        users_df = inject_nulls(users_df, ["phone","city"], PCT_NULLS)
        users_df = inject_bad_email(users_df, "email", PCT_BAD_EMAIL)
        users_df = inject_bad_phone(users_df, "phone", PCT_BAD_PHONE)
        if TRAILING_SPACES:
            users_df = inject_trailing_spaces(users_df, ["full_name","city","username"])
        if MIXED_DATE_FMT:
            users_df["created_at"] = mix_date_formats(users_df["created_at"])
        users_df = inject_duplicates(users_df, PCT_DUPLICATES)

        products_df = inject_nulls(products_df, ["category"], PCT_NULLS)
        products_df = inject_outliers_numeric(products_df, ["price"], PCT_OUTLIERS, factor=15)
        if TRAILING_SPACES:
            products_df = inject_trailing_spaces(products_df, ["name","category"])
        if MIXED_DATE_FMT:
            products_df["created_at"] = mix_date_formats(products_df["created_at"])

        orders_df = inject_nulls(orders_df, ["payment_method"], PCT_NULLS)
        if TRAILING_SPACES:
            orders_df = inject_trailing_spaces(orders_df, ["branch"])
        if MIXED_DATE_FMT:
            orders_df["order_created_at"] = mix_date_formats(orders_df["order_created_at"])
        # completed orders with total_amount = 0
        mask = np.random.rand(len(orders_df)) < 0.002
        orders_df.loc[mask, "status"] = "completed"
        orders_df.loc[mask, "total_amount"] = 0
        orders_df = inject_duplicates(orders_df, PCT_DUPLICATES)

        # order_items: negative quantity, missing order id
        idx_neg = np.random.choice(order_items_df.index, max(1,int(len(order_items_df)*PCT_NEG_QTY)), replace=False)
        order_items_df.loc[idx_neg, "quantity"] = -abs(order_items_df.loc[idx_neg, "quantity"])
        order_items_df = inject_outliers_numeric(order_items_df, ["quantity","unit_price","line_total"], PCT_OUTLIERS, factor=20)
        n_orphan = max(1,int(len(order_items_df)*PCT_ORPHAN_ITEMS))
        idx_orphan = np.random.choice(order_items_df.index, n_orphan, replace=False)
        order_items_df.loc[idx_orphan, "order_id"] = "ord_missing_" + order_items_df.loc[idx_orphan].index.astype(str)

    # ---------- Save ----------
    save(users_df, "users")
    save(products_df, "products")
    save(orders_df, "orders")
    save(order_items_df, "order_items")
    save(sessions_df, "sessions")

    print("[DONE] All data saved to", OUT_DIR)


if __name__ == "__main__":
    main()
