-- ============================================================
-- reconciliation_gap_detection.sql
-- Purpose : Find transactions that failed in the source system
--           but are missing from downstream reconciliation,
--           creating financial exposure.
-- Context : Reconciliation gaps were the highest-impact failure
--           type found during BA-led analysis. These are
--           prioritised above raw failure volume in triage logic.
-- ============================================================

WITH failed_transactions AS (
    SELECT
        transaction_id,
        gateway_name,
        merchant_id,
        transaction_amount,
        transaction_currency,
        failure_category,
        failure_code,
        transaction_date
    FROM payment_transactions
    WHERE
        transaction_status = 'FAILED'
        AND transaction_date >= CURRENT_DATE - INTERVAL '7 days'
),
reconciled AS (
    SELECT
        transaction_id,
        reconciliation_status,
        reconciled_at,
        reconciliation_notes
    FROM reconciliation_records
    WHERE created_date >= CURRENT_DATE - INTERVAL '7 days'
),
gaps AS (
    SELECT
        ft.*,
        CASE
            WHEN r.transaction_id IS NULL
            THEN 'MISSING_FROM_RECONCILIATION'
            WHEN r.reconciliation_status = 'PENDING'
            THEN 'PENDING_RECONCILIATION'
            ELSE 'RECONCILED'
        END                               AS reconciliation_state,
        r.reconciliation_notes
    FROM failed_transactions ft
    LEFT JOIN reconciled r
        ON ft.transaction_id = r.transaction_id
)
SELECT
    transaction_date,
    gateway_name,
    failure_category,
    reconciliation_state,
    COUNT(*)                              AS transaction_count,
    SUM(transaction_amount)               AS total_amount_at_risk,
    transaction_currency
FROM gaps
WHERE reconciliation_state != 'RECONCILED'
GROUP BY
    transaction_date,
    gateway_name,
    failure_category,
    reconciliation_state,
    transaction_currency
ORDER BY total_amount_at_risk DESC, transaction_date DESC;

-- Business note:
-- MISSING_FROM_RECONCILIATION = financial exposure with no record.
-- total_amount_at_risk is reviewed daily by finance ops.
-- This query became the basis for the P0 alert in the dashboard.
