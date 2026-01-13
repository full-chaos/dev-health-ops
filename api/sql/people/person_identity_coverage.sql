SELECT countDistinct(source) AS sources
FROM (
    SELECT 'code' AS source
    FROM user_metrics_daily
    WHERE identity_id IN %(identities)s
    LIMIT 1

    UNION ALL

    SELECT 'work' AS source
    FROM work_item_user_metrics_daily
    WHERE user_identity IN %(identities)s
    LIMIT 1
)
