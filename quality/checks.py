from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)


engine = create_engine(DB_URL)


class DataQualityError(Exception):
    pass


def check(description: str, query: str, expect_zero = True):
    """Runs a query that returns a single integer.
    If expect_zero=True, raises if result > 0 (e.g. duplicate count).
    If expect_zero=False, raises if result == 0 (e.g. row count)."""
    with engine.connect() as conn:
        result = conn.execute(text(query)).scalar()
        
    passed = (result == 0) if expect_zero else (result > 0)
    if not passed:
        raise DataQualityError(
            f"Failed check: {description}. Result was {result}."
        )
    print(f"Passed check: {description}. Result was {result}.")
    

def run_staging_checks():
    print("Running staging data quality checks...")
    check("stg_orders has rows", "SELECT COUNT(*) FROM stg_orders", expect_zero=False)
    check("stg_customers has rows", "SELECT COUNT(*) FROM stg_customers", expect_zero=False)
    check("stg_products has rows", "SELECT COUNT(*) FROM stg_products", expect_zero=False)
    check("stg_orders no duplicate PKs",
        "SELECT COUNT(*) FROM (SELECT order_id FROM stg_orders GROUP BY order_id HAVING COUNT(*) > 1) t")
    check("stg_customers no duplicate PKs",
        "SELECT COUNT(*) FROM (SELECT customer_id FROM stg_customers GROUP BY customer_id HAVING COUNT(*) > 1) t")

    

def run_model_checks():
    print("Running model data quality checks...")
    check("dim_customers has rows", "SELECT COUNT(*) FROM dim_customers", expect_zero=False)
    check("dim_products has rows", "SELECT COUNT(*) FROM dim_products", expect_zero=False)
    check("fact_orders has rows", "SELECT COUNT(*) FROM fact_orders", expect_zero=False)
    check("fact_orders no orphan customers",
        """SELECT COUNT(*) FROM fact_orders f
           WHERE NOT EXISTS (SELECT 1 FROM dim_customers c WHERE c.customer_id = f.customer_id)""")
    check("fact_orders no orphan products",
        """SELECT COUNT(*) FROM fact_orders f
           WHERE NOT EXISTS (SELECT 1 FROM dim_products p WHERE p.product_id = f.product_id)""")
    check("fact_orders no null order_dates",
        "SELECT COUNT(*) FROM fact_orders WHERE order_date IS NULL")
    

def run_mart_checks():
    print("Running mart checks...")
    check("mart_daily_revenue has rows",  "SELECT COUNT(*) FROM mart_daily_revenue",   expect_zero=False)
    check("mart_top_products has rows",   "SELECT COUNT(*) FROM mart_top_products",    expect_zero=False)
    check("mart_customer_ltv has rows",   "SELECT COUNT(*) FROM mart_customer_ltv",    expect_zero=False)
    check("mart_daily_revenue no negative revenue",
        "SELECT COUNT(*) FROM mart_daily_revenue WHERE gross_revenue < 0")
    check("mart_customer_ltv no negative LTV",
        "SELECT COUNT(*) FROM mart_customer_ltv WHERE lifetime_value < 0")


if __name__ == "__main__":
    run_staging_checks()
    run_model_checks()
    run_mart_checks()
    print("\nAll quality checks passed.")
    
    
    
    
    
