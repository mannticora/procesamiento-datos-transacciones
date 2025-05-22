
CREATE OR REPLACE VIEW daily_company_transactions AS
SELECT
    c.company_id,
    c.company_name,
    DATE(ch.created_at) AS transaction_date,
    SUM(ch.amount) AS total_amount,
    COUNT(ch.id) AS transaction_count
FROM
    charges ch
JOIN
    companies c ON ch.company_id = c.company_id
GROUP BY
    c.company_id, c.company_name, DATE(ch.created_at)
ORDER BY
    transaction_date DESC, total_amount DESC;