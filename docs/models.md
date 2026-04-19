# docs/models.md

## What this layer does
Transforms staging tables into a clean star schema: two dimension tables
and one fact table. Handles deduplication, type casting, and validation.

## Schema
dim_customers ──┐
├── fact_orders
dim_products  ──┘

## Tables produced
| Table          | Grain              | PK            |
|----------------|--------------------|---------------|
| dim_customers  | One row/customer   | customer_id   |
| dim_products   | One row/product    | product_id    |
| fact_orders    | One row/order      | order_id      |

## Key decisions
- **ROW_NUMBER() for deduplication** — handles cases where the same
  entity appears twice with different ingested_at values. Keeps the
  most recently ingested version.
- **price_is_valid flag** — bad rows are flagged, not deleted.
  Deleting silently hides data quality problems from downstream users.
- **EU products excluded from dim_products** — fact_orders uses US
  pricing. EU rows are preserved in staging for future regional analysis.
