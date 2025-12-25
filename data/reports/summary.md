# Week 2 Summary — ETL + EDA

## Key findings

- **Revenue by country**:
  - Saudi Arabia (SA) generates the highest revenue, suggesting more users and higher engagement.
  - United Arab Emirates (AE) generates the lowest revenue, likely due to fewer orders, lower average order value, or smaller sample size.
- **Revenue trend over time**: Peaks and dips observed across the dataset, indicating seasonal or periodic fluctuations in user activity.
- **Amount distribution**: Most orders fall within the $10–20 range (31 orders), with outliers representing unusually high or low order amounts.
- **Paid vs refunded orders**: 80 paid orders vs 20 refunded orders. Paid orders dominate, showing strong user engagement and satisfaction.
- **Refund rate by country**: Bootstrap comparison between SA and AE shows AE has a slightly higher refund rate, but the difference is not statistically significant.

## Definitions

- Revenue = sum(amount) per country or period.
- Refund rate = number of refunded orders / total orders, where refund = status_clean == "refund".
- Time window = period covered by dataset (earliest to latest `created_at`).
- Outliers = orders with amounts outside 1.5× IQR or winsorized for analysis.
- Bootstrap comparison = resampling 2000 times to estimate mean differences with 95% confidence intervals.

## Data quality caveats

- Missingness: Some columns have missing values:
  - `quantity`: 8 missing (8%)
  - `amount`: 5 missing (5%)
  - `created_at`: 3 missing (3%)
  - `order_id` and `user_id` have no missing values.
- Duplicates: Users are unique by `user_id`; orders assumed unique by `order_id`.
- Join coverage: Only orders with valid `user_id` joined to users; row count consistent.
- Outliers: Winsorized amounts affect distribution metrics; extreme values may impact interpretation.
- Sample size: AE data may be small, affecting representativeness.
