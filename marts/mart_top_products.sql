DROP TABLE IF EXISTS mart_top_products;

CREATE TABLE mart_top_products AS
WITH product_sales AS (
    SELECT
        f.product_id,
        p.product_name,
        p.category,
        p.supplier,
        COUNT(DISTINCT f.order_id)              AS total_orders,
        SUM(f.quantity)                         AS units_sold,
        SUM(f.total_price)                      AS gross_revenue,
        AVG(f.unit_price)                       AS avg_selling_price,
        COUNT(DISTINCT f.customer_id)           AS unique_buyers
    FROM fact_orders f
    JOIN dim_products p USING (product_id)
    WHERE f.status = 'completed'
      AND f.price_is_valid = TRUE
    GROUP BY
        f.product_id, p.product_name,
        p.category, p.supplier
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY gross_revenue DESC)   AS revenue_rank,
        RANK() OVER (ORDER BY units_sold DESC)      AS volume_rank,
        -- rank within category
        RANK() OVER (
            PARTITION BY category
            ORDER BY gross_revenue DESC
        )                                           AS category_rank
    FROM product_sales
)
SELECT
    product_id,
    product_name,
    category,
    supplier,
    total_orders,
    units_sold,
    ROUND(gross_revenue::NUMERIC, 2)        AS gross_revenue,
    ROUND(avg_selling_price::NUMERIC, 2)    AS avg_selling_price,
    unique_buyers,
    revenue_rank,
    volume_rank,
    category_rank
FROM ranked
ORDER BY revenue_rank;

ALTER TABLE mart_top_products ADD PRIMARY KEY (product_id);
CREATE INDEX idx_mart_top_products_category ON mart_top_products(category);
CREATE INDEX idx_mart_top_products_rev_rank ON mart_top_products(revenue_rank);