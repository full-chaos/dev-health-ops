---
page_id: use-scope
summary: Select and preserve the repository or team scope and time window for a product question.
content_type: task-guide
owner: product-analytics
applicability: current
lifecycle: active
---

# Set scope and time window

1. Select the workspace that owns the source data.
2. Choose the repository, team, or supported aggregate scope required by the question.
3. Choose a time window that overlaps collected data and is long enough for the signal you are reading.
4. Record the context before opening evidence or another view.
5. Preserve it when comparing results.

A wider period can test coverage, but it changes the analytical question. A team and a repository can contain different work, attribution, and coverage even when their names appear related.

Use [Unexpected scope or filters](../troubleshooting/scope-and-filters.md) when the result does not match the visible context.

## Ask Dev scope and comparisons

When Ask Dev is enabled, its V1 direct scope is limited to the signed-in
organization, up to 20 repositories, a project or WorkUnit, an issue/work item,
or a pull request. A team narrows one of those scopes as a filter; it is not a
standalone top-level Ask Dev scope. Incidents, deployments, commits, reviews,
CI/test runs, AI workflow runs, and files may appear as supporting evidence but
cannot be selected as the conversation scope.

Choose a 7-, 30-, or 90-day window, or an explicit inclusive date range. Ask
Dev resolves the visible local dates to explicit UTC boundaries. The comparison
window is the immediately preceding window with the same elapsed duration, so a
daylight-saving transition can produce a 23- or 25-hour local day without
silently changing the comparison duration.

Context inherited from a product page or earlier conversation is proposed, not
trusted. The server resolves its controlled entity IDs again for each run. An
explicit reference that is missing or no longer authorized produces a not-found
or disambiguation state; it never widens the question to the whole organization.
