DROP TABLE IF EXISTS mart_customer_ltv;

CREATE TABLE mart_customer_ltv AS
WITH customer_orders AS (
    SELECT
        f.customer_id,
        c.full_name,
        c.email,
        c.address_state,
        c.customer_since,
        COUNT(DISTINCT f.order_id)          AS total_orders,
        SUM(f.total_price)                  AS lifetime_value,
        AVG(f.total_price)                  AS avg_order_value,
        MIN(f.order_date)                   AS first_order_date,
        MAX(f.order_date)                   AS last_order_date,
        -- days between first and last order (customer lifespan)
        MAX(f.order_date) - MIN(f.order_date) AS active_days
    FROM fact_orders f
    JOIN dim_customers c USING (customer_id)
    WHERE f.status = 'completed'
      AND f.price_is_valid = TRUE
    GROUP BY
        f.customer_id, c.full_name,
        c.email, c.address_state, c.customer_since
),
segmented AS (
    SELECT *,
        -- RFM-style segmentation based on LTV percentile
        NTILE(4) OVER (ORDER BY lifetime_value DESC) AS ltv_quartile
    FROM customer_orders
)
SELECT
    customer_id,
    full_name,
    email,
    address_state,
    customer_since,
    total_orders,
    ROUND(lifetime_value::NUMERIC, 2)       AS lifetime_value,
    ROUND(avg_order_value::NUMERIC, 2)      AS avg_order_value,
    first_order_date,
    last_order_date,
    active_days,
    -- human-readable segment label
    CASE ltv_quartile
        WHEN 1 THEN 'VIP'
        WHEN 2 THEN 'Loyal'
        WHEN 3 THEN 'Occasional'
        WHEN 4 THEN 'At Risk'
    END                                     AS customer_segment,
    ltv_quartile
FROM segmented
ORDER BY lifetime_value DESC;

ALTER TABLE mart_customer_ltv ADD PRIMARY KEY (customer_id);
CREATE INDEX idx_mart_ltv_segment  ON mart_customer_ltv(customer_segment);
CREATE INDEX idx_mart_ltv_state    ON mart_customer_ltv(address_state);