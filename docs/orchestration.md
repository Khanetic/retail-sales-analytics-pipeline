# docs/orchestration.md

## What this layer does
Airflow DAG runs the full pipeline daily at 6am UTC and runs data
quality checks between each layer.

## DAG structure
generate_data → ingest_to_staging → check_staging
→ build_models → check_models
→ build_marts → check_marts
→ export_mart_csvs


## Quality checks
| Check                          | Layer    | Failure means              |
|--------------------------------|----------|----------------------------|
| Row count > 0                  | Staging  | Source file was empty      |
| No duplicate PKs               | Staging  | File was loaded twice      |
| No orphan foreign keys         | Models   | Dim/fact join will break   |
| No null order_dates            | Models   | Date filtering broken      |
| No negative revenue            | Marts    | Aggregation logic error    |

## Retry policy
Tasks retry once after a 2-minute delay. This handles transient DB
connection failures without masking real bugs.



