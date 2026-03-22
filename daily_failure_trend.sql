-- ============================================================
-- daily_failure_trend.sql
-- Purpose : Track daily failure counts with a 7-day rolling
--           average to distinguish genuine spikes from noise.
-- Context : Used for ops dashboard reporting. A spike of >2x
--           the rolling average was defined as a P1 event.
-- ============================================================

WITH daily_counts AS (
    SELECT
        DATE(transaction_date)           AS txn_date,
        failure_category,
        COUNT(*)                         AS daily_failures,
        COUNT(CASE WHEN is_resolved = TRUE
                   THEN 1 END)           AS resolved_same_day,
        COUNT(CASE WHEN is_resolved = FALSE
                    OR resolution_date > transaction_date
                   THEN 1 END)           AS unresolved_or_delayed
    FROM payment_transactions
    WHERE
        transaction_status = 'FAILED'
        AND transaction_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY
        DATE(transaction_date),
        failure_category
),
with_rolling AS (
    SELECT
        *,
        ROUND(
            AVG(daily_failures) OVER (
                PARTITION BY failure_category
                ORDER BY txn_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 1
        )                                AS rolling_7day_avg,
        LAG(daily_failures, 7) OVER (
            PARTITION BY failure_category
            ORDER BY txn_date
        )                                AS failures_same_day_last_week
    FROM daily_counts
)
SELECT
    txn_date,
    failure_category,
    daily_failures,
    rolling_7day_avg,
    failures_same_day_last_week,
    ROUND(
        (daily_failures - rolling_7day_avg)
        / NULLIF(rolling_7day_avg, 0) * 100, 1
    )                                    AS pct_deviation_from_avg,
    resolved_same_day,
    unresolved_or_delayed
FROM with_rolling
ORDER BY txn_date DESC, daily_failures DESC;

-- Business note:
-- pct_deviation_from_avg > 100 means double the average that day.
-- Use failures_same_day_last_week to check if a spike is a
-- recurring weekly batch pattern rather than a real incident.
