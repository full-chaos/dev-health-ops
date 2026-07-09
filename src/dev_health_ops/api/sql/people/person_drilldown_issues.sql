SELECT
    wct.work_item_id,
    wct.provider,
    wct.status,
    nullIf(t.team_id, '') AS team_id,
    wct.cycle_time_hours,
    wct.lead_time_hours,
    wct.started_at,
    wct.completed_at
FROM work_item_cycle_times AS wct FINAL
LEFT JOIN {primary_team_attribution_source} AS t
  ON t.work_item_id = wct.work_item_id
WHERE wct.day >= %(start_day)s AND wct.day < %(end_day)s
  AND wct.assignee IN %(identities)s
  AND wct.org_id = %(org_id)s
  {cursor_filter}
ORDER BY wct.completed_at DESC
LIMIT %(limit)s
