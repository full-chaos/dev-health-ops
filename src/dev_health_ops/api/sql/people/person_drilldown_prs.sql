SELECT
    repo_id,
    number,
    title,
    author_name,
    author_email AS author,
    created_at,
    merged_at,
    first_review_at,
    if(first_review_at IS NULL, NULL,
       dateDiff('hour', created_at, first_review_at)) AS review_latency_hours
FROM git_pull_requests
WHERE created_at >= %(start_ts)s AND created_at < %(end_ts)s
  AND (author_email IN %(identities)s OR author_name IN %(identities)s)
  {cursor_filter}
ORDER BY created_at DESC
LIMIT %(limit)s
