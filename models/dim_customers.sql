DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM stg_customers
)
SELECT
    customer_id,
    name                            AS full_name,
    LOWER(TRIM(email))              AS email,
    phone,
    address_street,
    address_city,
    address_state,
    address_zip,
    address_country,
    created_at::TIMESTAMP           AS customer_since,
    ingested_at
FROM deduped
WHERE rn = 1;

ALTER TABLE dim_customers ADD PRIMARY KEY (customer_id);
CREATE INDEX idx_dim_customers_email ON dim_customers(email);