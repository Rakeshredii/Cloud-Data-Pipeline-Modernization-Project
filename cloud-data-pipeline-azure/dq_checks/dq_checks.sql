
-- dq_checks.sql
-- Example data quality checks

-- Row count threshold example
SELECT CASE WHEN COUNT(*) < 100 THEN 'FAIL' ELSE 'PASS' END AS rowcount_check
FROM source_a;

-- Duplicate detection
SELECT user_id, COUNT(*) AS cnt
FROM source_a
GROUP BY user_id
HAVING COUNT(*) > 1;

-- Referential integrity (users must exist in reference table)
SELECT a.user_id
FROM source_a a
LEFT JOIN source_c c ON a.user_id = c.user_id
WHERE c.user_id IS NULL;
