SELECT
    team_id
FROM (
    SELECT team_id, day
    FROM user_metrics_daily
    WHERE identity_id IN %(identities)s
      AND team_id IS NOT NULL
      AND team_id != ''

    UNION ALL

    SELECT team_id, day
    FROM work_item_user_metrics_daily
    WHERE user_identity IN %(identities)s
      AND team_id IS NOT NULL
      AND team_id != ''
)
ORDER BY day DESC
LIMIT 1
