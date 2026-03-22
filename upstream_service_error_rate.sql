-- ============================================================
-- upstream_service_error_rate.sql
-- Purpose : Calculate error rates per upstream service to
--           isolate root cause from downstream symptoms.
-- Context : A high error rate on a specific upstream service
--           narrows investigation scope and reduces resolution
--           time significantly.
-- ============================================================

WITH service_volumes AS (
    SELECT
        upstream_service_name,
        upstream_service_id,
        error_code,
        error_description,
        COUNT(*)                          AS total_calls,
        COUNT(CASE WHEN call_status = 'ERROR'
                   THEN 1 END)            AS error_count,
        COUNT(CASE WHEN call_status = 'TIMEOUT'
                   THEN 1 END)            AS timeout_count,
        AVG(response_time_ms)             AS avg_response_ms,
        MAX(response_time_ms)             AS max_response_ms
    FROM payment_transaction_logs
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY
        upstream_service_name,
        upstream_service_id,
        error_code,
        error_description
),
with_rates AS (
    SELECT
        *,
        ROUND(
            error_count * 100.0 / NULLIF(total_calls, 0), 2
        )                                 AS error_rate_pct,
        ROUND(
            timeout_count * 100.0 / NULLIF(total_calls, 0), 2
        )                                 AS timeout_rate_pct,
        CASE
            WHEN error_count * 100.0 / NULLIF(total_calls, 0) > 5
            THEN 'INVESTIGATE'
            WHEN error_count * 100.0 / NULLIF(total_calls, 0) > 2
            THEN 'MONITOR'
            ELSE 'NORMAL'
        END                               AS ops_status
    FROM service_volumes
)
SELECT
    upstream_service_name,
    error_code,
    error_description,
    total_calls,
    error_count,
    error_rate_pct,
    timeout_rate_pct,
    ROUND(avg_response_ms, 0)             AS avg_response_ms,
    max_response_ms,
    ops_status
FROM with_rates
WHERE error_count > 0
ORDER BY error_rate_pct DESC, error_count DESC;

-- Business note:
-- ops_status = INVESTIGATE requires immediate escalation.
-- Thresholds (5% / 2%) were defined in business requirements
-- and validated with ops stakeholders before go-live.
