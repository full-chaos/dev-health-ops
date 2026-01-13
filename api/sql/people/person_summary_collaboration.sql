SELECT
    'review_load' AS section,
    'Reviews given' AS label,
    sum(reviews_given) AS value
FROM user_metrics_daily
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND identity_id IN %(identities)s

UNION ALL

SELECT
    'review_load' AS section,
    'Reviews received' AS label,
    sum(reviews_received) AS value
FROM user_metrics_daily
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND identity_id IN %(identities)s

UNION ALL

SELECT
    'review_load' AS section,
    'PRs authored' AS label,
    sum(prs_authored) AS value
FROM user_metrics_daily
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND identity_id IN %(identities)s

UNION ALL

SELECT
    'review_load' AS section,
    'PRs merged' AS label,
    sum(prs_merged) AS value
FROM user_metrics_daily
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND identity_id IN %(identities)s

UNION ALL

SELECT
    'handoff_points' AS section,
    'Items started' AS label,
    sum(items_started) AS value
FROM work_item_user_metrics_daily
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND user_identity IN %(identities)s

UNION ALL

SELECT
    'handoff_points' AS section,
    'Items completed' AS label,
    sum(items_completed) AS value
FROM work_item_user_metrics_daily
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND user_identity IN %(identities)s
