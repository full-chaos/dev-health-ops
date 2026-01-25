WITH identities AS (
    SELECT
        identity_id AS identity,
        max(day) AS last_seen
    FROM user_metrics_daily
    WHERE identity_id != ''
    GROUP BY identity_id

    UNION ALL

    SELECT
        user_identity AS identity,
        max(day) AS last_seen
    FROM work_item_user_metrics_daily
    WHERE user_identity != ''
    GROUP BY user_identity
)
SELECT
    identity AS identity_id,
    max(last_seen) AS last_seen
FROM identities
WHERE identity != ''
  AND lower(identity) LIKE %(query)s
GROUP BY identity
ORDER BY last_seen DESC
LIMIT %(limit)s
