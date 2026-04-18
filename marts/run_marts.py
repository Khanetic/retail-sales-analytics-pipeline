from pathlib import Path
from sqlalchemy import create_engine, text

DB_URL = "postgresql://retail:retail123@localhost:5433/retail_db"
engine = create_engine(DB_URL)

MARTS = [
    "mart_daily_revenue.sql",
    "mart_top_products.sql",
    "mart_customer_ltv.sql"
]

def run_mart(filename: str):
    sql = (Path(__file__).parent / filename).read_text()
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"Executed {filename}")
    

if __name__ == "__main__":
    for mart in MARTS:
        run_mart(mart)
    print("All marts executed successfully")