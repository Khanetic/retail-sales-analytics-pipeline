# docs/ingestion.md

## What this layer does
Reads raw source files and loads them as-is into Postgres staging tables.
No business logic — just raw data plus an `ingested_at` timestamp.

## Tables produced
| Table           | Source            | Rows (approx) |
|-----------------|-------------------|---------------|
| stg_orders      | orders.csv        | ~1,000        |
| stg_customers   | customers.json    | ~200          |
| stg_products    | products.xlsx     | ~100          |

## Key decisions
- **Truncate before load** — makes every run idempotent. Re-running
  the loader won't duplicate rows.
- **No transformations here** — the staging layer is an audit trail.
  If a model produces wrong output, you can always re-run from staging
  without re-ingesting the source files.
- **Nested JSON flattened at load time** — customer address fields
  are flattened into columns before hitting Postgres. Plain columns
  are easier to query and index than JSON blobs.