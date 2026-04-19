from pathlib import Path
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
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