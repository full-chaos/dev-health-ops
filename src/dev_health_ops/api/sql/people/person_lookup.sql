WITH identities AS (
    SELECT identity_id AS identity
    FROM user_metrics_daily
    WHERE identity_id != ''
      AND org_id = %(org_id)s

    UNION DISTINCT

    SELECT user_identity AS identity
    FROM work_item_user_metrics_daily
    WHERE user_identity != ''
      AND org_id = %(org_id)s
)
SELECT
    identity AS identity_id
FROM identities
WHERE lower(hex(MD5(identity))) = %(person_id)s
LIMIT 1
