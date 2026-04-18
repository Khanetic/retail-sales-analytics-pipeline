# ingestion/generate_data.py
import json
import random
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NUM_CUSTOMERS = 200
NUM_PRODUCTS = 50
NUM_ORDERS = 1000


def generate_customers():
    customers = []
    for _ in range(NUM_CUSTOMERS):
        customers.append({
            "customer_id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip": fake.zipcode(),
                "country": "US"
            },
            "created_at": fake.date_time_between(start_date="-2y").isoformat()
        })

    with open(OUTPUT_DIR / "customers.json", "w") as f:
        json.dump(customers, f, indent=2)
    print(f"Generated {len(customers)} customers")


def generate_products():
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]

    # Two sheets: US catalog and EU catalog (slightly different pricing)
    def make_products(region, price_multiplier):
        return [{
            "product_id": f"P{str(i).zfill(4)}",
            "name": fake.catch_phrase(),
            "category": random.choice(categories),
            "unit_price": round(random.uniform(5, 500) * price_multiplier, 2),
            "stock_quantity": random.randint(0, 500),
            "supplier": fake.company()
        } for i in range(1, NUM_PRODUCTS + 1)]

    with pd.ExcelWriter(OUTPUT_DIR / "products.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(make_products("US", 1.0)).to_excel(writer, sheet_name="US", index=False)
        pd.DataFrame(make_products("EU", 1.15)).to_excel(writer, sheet_name="EU", index=False)
    print(f"Generated {NUM_PRODUCTS} products per region")


def generate_orders():
    # Load customer and product IDs so foreign keys are valid
    with open(OUTPUT_DIR / "customers.json") as f:
        customer_ids = [c["customer_id"] for c in json.load(f)]

    product_ids = [f"P{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]

    orders = []
    for i in range(1, NUM_ORDERS + 1):
        quantity = random.randint(1, 10)
        price = round(random.uniform(5, 500), 2)
        orders.append({
            "order_id": f"ORD{str(i).zfill(5)}",
            "customer_id": random.choice(customer_ids),
            "product_id": random.choice(product_ids),
            "quantity": quantity,
            "unit_price": price,
            "total_price": round(quantity * price, 2),
            "order_date": fake.date_between(start_date="-1y").isoformat(),
            "status": random.choice(["completed", "completed", "completed", "returned", "cancelled"])
        })

    pd.DataFrame(orders).to_csv(OUTPUT_DIR / "orders.csv", index=False)
    print(f"Generated {len(orders)} orders")


if __name__ == "__main__":
    generate_customers()
    generate_products()
    generate_orders()