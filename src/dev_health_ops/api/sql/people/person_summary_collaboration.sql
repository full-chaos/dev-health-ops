WITH latest_user_metrics AS (
    SELECT
        day,
        repo_id,
        author_email,
        argMax(reviews_given, computed_at) AS reviews_given,
        argMax(reviews_received, computed_at) AS reviews_received,
        argMax(prs_authored, computed_at) AS prs_authored,
        argMax(prs_merged, computed_at) AS prs_merged
    FROM user_metrics_daily
    WHERE day >= %(start_day)s AND day < %(end_day)s
      AND identity_id IN %(identities)s
      AND org_id = %(org_id)s
    GROUP BY day, repo_id, author_email
)

SELECT
    'review_load' AS section,
    'Reviews given' AS label,
    sum(reviews_given) AS value
FROM latest_user_metrics

UNION ALL

SELECT
    'review_load' AS section,
    'Reviews received' AS label,
    sum(reviews_received) AS value
FROM latest_user_metrics

UNION ALL

SELECT
    'review_load' AS section,
    'PRs authored' AS label,
    sum(prs_authored) AS value
FROM latest_user_metrics

UNION ALL

SELECT
    'review_load' AS section,
    'PRs merged' AS label,
    sum(prs_merged) AS value
FROM latest_user_metrics

UNION ALL

SELECT
    'handoff_points' AS section,
    'Items started' AS label,
    sum(items_started) AS value
FROM work_item_user_metrics_daily FINAL
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND user_identity IN %(identities)s
  AND org_id = %(org_id)s

UNION ALL

SELECT
    'handoff_points' AS section,
    'Items completed' AS label,
    sum(items_completed) AS value
FROM work_item_user_metrics_daily FINAL
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND user_identity IN %(identities)s
  AND org_id = %(org_id)s
