# docs/marts.md

## What this layer does
Pre-aggregates model layer tables into analyst-ready views. Analysts
query marts directly — no joining or filtering required.

## Tables produced
| Table                 | Grain           | Primary use case          |
|-----------------------|-----------------|---------------------------|
| mart_daily_revenue    | One row/day     | Revenue trend dashboard   |
| mart_top_products     | One row/product | Product performance        |
| mart_customer_ltv     | One row/customer| Customer segmentation      |

## Key decisions
- **Completed orders only** — cancelled and returned orders are
  excluded here, not in the dashboard. Business rules live in the
  pipeline, not in BI tools.
- **NTILE(4) segmentation** — customers split into VIP / Loyal /
  Occasional / At Risk quartiles by lifetime value. Quartile boundaries
  shift as new customers are added, which is intentional.
- **Rolling 7-day revenue** — computed in SQL using a window function
  rather than in Tableau, so the logic is consistent across any tool
  that queries the mart.