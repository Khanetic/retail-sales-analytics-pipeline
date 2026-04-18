import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text


DB_URL = "postgresql://retail:retail123@localhost:5433/retail_db"
DATA_DIR = Path("data/raw")

engine = create_engine(DB_URL)


def now_utc():
    return datetime.now(timezone.utc)

def truncate_and_load(df:pd.DataFrame, table_name:str):
    """Drop existing rows and reload - makes each run idempotent."""
    with engine.begin() as conn:
        if inspect(conn).has_table(table_name):
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            df.to_sql(table_name, con=conn, if_exists="append", index=False)
        else:
            df.to_sql(table_name, con=conn, if_exists="fail", index=False)
    print(f"Loaded {len(df)} records into {table_name} at {now_utc().isoformat()}")


def load_orders():
    orders_df = pd.read_csv(DATA_DIR / "orders.csv")
    orders_df["ingested_at"] = now_utc()
    truncate_and_load(orders_df, "stg_orders")

def load_customers():
    with open(DATA_DIR / "customers.json") as f:
        customers = json.load(f)
        
    # flatten nested address 
    for record in customers:
        addr = record.pop("address")
        record["address_street"] = addr["street"]
        record["address_city"] = addr["city"]
        record["address_state"] = addr["state"]
        record["address_zip"] = addr["zip"]
        record["address_country"] = addr["country"]
        
    customers_df = pd.DataFrame(customers)
    customers_df["ingested_at"] = now_utc()
    truncate_and_load(customers_df, "stg_customers")
    
    
def load_products():
    # Read both sheets, tag each row with its region, combine
    us = pd.read_excel(DATA_DIR / "products.xlsx", sheet_name="US")
    eu = pd.read_excel(DATA_DIR / "products.xlsx", sheet_name="EU")
    us["region"] = "US"
    eu["region"] = "EU"

    df = pd.concat([us, eu], ignore_index=True)
    df["ingested_at"] = now_utc()
    truncate_and_load(df, "stg_products")


if __name__ == "__main__":
    load_orders()
    load_customers()
    load_products()
