-- ============================================================
-- failure_frequency_by_gateway.sql
-- Purpose : Identify the most frequent failure types per payment
--           gateway to prioritise triage and engineering effort.
-- Context : Used during root-cause analysis of the payment failure
--           triage system. Helps ops teams focus on high-volume
--           failure categories rather than investigating uniformly.
-- ============================================================

WITH failure_summary AS (
    SELECT
        gateway_id,
        gateway_name,
        failure_category,
        failure_code,
        COUNT(*)                                      AS failure_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
            PARTITION BY gateway_id
        )                                             AS pct_of_gateway_failures
    FROM payment_transactions
    WHERE
        transaction_status = 'FAILED'
        AND transaction_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY
        gateway_id,
        gateway_name,
        failure_category,
        failure_code
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY gateway_id
            ORDER BY failure_count DESC
        ) AS rank_within_gateway
    FROM failure_summary
)
SELECT
    gateway_name,
    failure_category,
    failure_code,
    failure_count,
    ROUND(pct_of_gateway_failures, 2)  AS pct_of_gateway_failures,
    rank_within_gateway
FROM ranked
WHERE rank_within_gateway <= 5
ORDER BY gateway_name, rank_within_gateway;

-- Business note:
-- Rows with rank_within_gateway = 1 are the primary failure driver
-- per gateway. High pct_of_gateway_failures (>40%) suggests a
-- systemic issue worth escalating to engineering.
