from pathlib import Path
from sqlalchemy import create_engine, text

DB_URL = "postgresql://retail:retail123@localhost:5433/retail_db"
engine = create_engine(DB_URL)

# order
MODEL_SCRIPTS = [
    "dim_customers.sql",
    "dim_products.sql",
    "fact_orders.sql"
]

def run_models(filename: str):
    sql = (Path(__file__).parent / filename).read_text()
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"Executed {filename}")
    
if __name__ == "__main__":
    for model in MODEL_SCRIPTS:
        run_models(model)
    print("All models executed successfully")