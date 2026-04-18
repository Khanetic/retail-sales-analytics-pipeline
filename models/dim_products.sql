-- models/dim_products.sql
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products AS
WITH us_products AS (
    SELECT *
    FROM stg_products
    WHERE region = 'US'
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM us_products
)
SELECT
    product_id,
    TRIM(name)                      AS product_name,
    TRIM(category)                  AS category,
    unit_price,
    stock_quantity,
    TRIM(supplier)                  AS supplier,
    region,
    ingested_at
FROM deduped
WHERE rn = 1;

ALTER TABLE dim_products ADD PRIMARY KEY (product_id);
CREATE INDEX idx_dim_products_category ON dim_products(category);