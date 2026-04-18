-- models/fact_orders.sql
DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders AS
WITH validated AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        total_price,
        order_date::DATE            AS order_date,
        status,
        ingested_at,
        -- flag rows where total_price doesn't match quantity × unit_price
        ABS(total_price - (quantity * unit_price)) < 0.01 AS price_is_valid
    FROM stg_orders
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM validated
)
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_price,
    order_date,
    status,
    price_is_valid,
    ingested_at
FROM deduped
WHERE rn = 1;

ALTER TABLE fact_orders ADD PRIMARY KEY (order_id);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_product  ON fact_orders(product_id);
CREATE INDEX idx_fact_orders_date     ON fact_orders(order_date);

-- Validate foreign keys (logs mismatches, doesn't block load)
DO $$
DECLARE
    orphan_customers INT;
    orphan_products  INT;
BEGIN
    SELECT COUNT(*) INTO orphan_customers
    FROM fact_orders f
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_customers c WHERE c.customer_id = f.customer_id
    );

    SELECT COUNT(*) INTO orphan_products
    FROM fact_orders f
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_products p WHERE p.product_id = f.product_id
    );

    IF orphan_customers > 0 THEN
        RAISE WARNING 'fact_orders has % rows with no matching dim_customers', orphan_customers;
    END IF;

    IF orphan_products > 0 THEN
        RAISE WARNING 'fact_orders has % rows with no matching dim_products', orphan_products;
    END IF;
END $$;