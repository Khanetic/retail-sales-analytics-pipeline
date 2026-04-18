DROP TABLE IF EXISTS mart_daily_revenue;

CREATE TABLE mart_daily_revenue AS
WITH completed_orders AS (
    SELECT *
    FROM fact_orders
    WHERE status = 'completed'
      AND price_is_valid = TRUE
),
daily AS (
    SELECT
        order_date,
        COUNT(DISTINCT order_id)        AS total_orders,
        COUNT(DISTINCT customer_id)     AS unique_customers,
        SUM(total_price)                AS gross_revenue,
        AVG(total_price)                AS avg_order_value,
        SUM(quantity)                   AS units_sold
    FROM completed_orders
    GROUP BY order_date
)
SELECT
    order_date,
    total_orders,
    unique_customers,
    ROUND(gross_revenue::NUMERIC, 2)        AS gross_revenue,
    ROUND(avg_order_value::NUMERIC, 2)      AS avg_order_value,
    units_sold,
    -- 7-day rolling revenue (useful for dashboards — smooths daily noise)
    ROUND(SUM(gross_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::NUMERIC, 2)                          AS revenue_7d_rolling,
    -- day-over-day growth
    ROUND((
        (gross_revenue - LAG(gross_revenue) OVER (ORDER BY order_date))
        / NULLIF(LAG(gross_revenue) OVER (ORDER BY order_date), 0) * 100
    )::NUMERIC, 2)                          AS revenue_dod_pct
FROM daily
ORDER BY order_date;

ALTER TABLE mart_daily_revenue ADD PRIMARY KEY (order_date);
CREATE INDEX idx_mart_daily_rev_date ON mart_daily_revenue(order_date);