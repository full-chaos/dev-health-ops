// GET /api/v1/people/{person_id}/drilldown/issues -- ports
// people_drilldown_issues (api/main.py:1152-1178) and
// build_person_drilldown_issues_response (services/people.py:793-844),
// backed by fetch_person_issues (queries/people.py:262-289) and
// sql/people/person_drilldown_issues.sql.
//
// Identity resolution reuses resolveIdentityContext/identityVariants --
// see drilldownprs.go's own doc comment for the shared-function rationale.
//
// DEDUP: Python's own query already reads work_item_cycle_times WITH FINAL
// (person_drilldown_issues.sql:10) -- no Python-plane dedup defect on that
// table for this route, matching internal/drilldown/issues.go's own
// fetchIssuesQuery doc comment for the identical table. The team-attribution
// join (work_item_team_attributions) is FINAL-deduped inside
// primaryWorkItemTeamAttributionSourceForPerson below, the SAME shape
// internal/drilldown/issues.go's own copy already establishes (this is the
// fourth package-local copy in the tree -- quadrant, investmentexplain,
// drilldown -- each "repeat, don't couple" per those packages' own doc
// comments).
//
// ORG SCOPE: wct.org_id sits directly in this query's own WHERE, on the
// SAME top-level read FINAL dedups -- identical placement and identical
// migration-024 DEFAULT 'default' backfill caveat internal/drilldown/
// issues.go's own fetchIssuesQuery doc comment already documents for this
// exact table; this port carries no alternative org source to route
// through instead, for the same reason that one does.
package people

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// primaryWorkItemTeamAttributionSourceForPerson inlines api/queries/
// investment.py's PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE verbatim
// (bound-parameter form) -- see internal/drilldown/issues.go's own
// primaryWorkItemTeamAttributionSource doc comment for the full org-scope
// citation; identical shape, package-local copy.
const primaryWorkItemTeamAttributionSourceForPerson = `(
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String}
          GROUP BY work_item_id
      )
)`

// DrilldownIssuesParams is the already-resolved request shape GET
// /api/v1/people/{person_id}/drilldown/issues's route file reduces to
// before calling BuildDrilldownIssuesResponse.
type DrilldownIssuesParams struct {
	PersonID  string
	RangeDays int
	Limit     int
	// Cursor ports the optional `cursor: datetime | None` query param
	// (main.py:1162): nil means no cursor filter, matching
	// fetch_person_issues' own `if cursor is not None` branch
	// (queries/people.py:274-275).
	Cursor *time.Time
	Now    time.Time
}

// IssueRow ports one IssueRow the response wraps (api/models/schemas.py:
// 181-190), in the same field order.
//
// DATETIME WIRE FORM (started_at/completed_at): RFC 3339 (Go's default
// time.Time JSON marshaling), not Python's naive isoformat -- the same
// class-level direction internal/drilldown/issues.go's own IssueItem
// already carries for this exact field pair on the sibling scope-based
// route. Python's naive-isoformat wire form is the declared baseline
// defect; Go's RFC 3339 form is canonical.
type IssueRow struct {
	WorkItemID     string     `json:"work_item_id"`
	Provider       string     `json:"provider"`
	Status         string     `json:"status"`
	TeamID         *string    `json:"team_id"`
	CycleTimeHours *float64   `json:"cycle_time_hours"`
	LeadTimeHours  *float64   `json:"lead_time_hours"`
	StartedAt      *time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at"`
	// Link is always null -- build_person_drilldown_issues_response never
	// sets it (services/people.py:839's `link=None`).
	Link *string `json:"link"`
}

// DrilldownIssuesResponse ports PersonDrilldownResponse(items=...,
// next_cursor=...) for this route (api/models/schemas.py:459-461).
type DrilldownIssuesResponse struct {
	Items      []IssueRow `json:"items"`
	NextCursor *time.Time `json:"next_cursor"`
}

// fetchPersonIssuesQuery is the Go port of fetch_person_issues' SELECT
// (queries/people.py:262-289, sql/people/person_drilldown_issues.sql).
const fetchPersonIssuesQuery = `
SELECT
    wct.work_item_id AS work_item_id,
    wct.provider AS provider,
    wct.status AS status,
    nullIf(t.team_id, '') AS team_id,
    wct.cycle_time_hours AS cycle_time_hours,
    wct.lead_time_hours AS lead_time_hours,
    wct.started_at AS started_at,
    wct.completed_at AS completed_at
FROM work_item_cycle_times AS wct FINAL
LEFT JOIN %s AS t
  ON t.work_item_id = wct.work_item_id
WHERE wct.day >= {start_day:Date} AND wct.day < {end_day:Date}
  AND wct.assignee IN {identities:Array(String)}
  AND wct.org_id = {org_id:String}
  %s
ORDER BY wct.completed_at DESC
LIMIT {limit:UInt64}
%s
`

// fetchPersonIssues runs fetchPersonIssuesQuery and scans every row, in
// SELECT order.
func fetchPersonIssues(ctx context.Context, client QueryClient, identities []string, startDay, endDay time.Time, limit int, cursor *time.Time, orgID string) ([]IssueRow, error) {
	cursorFilter := ""
	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}
	if cursor != nil {
		cursorFilter = "AND wct.completed_at < {cursor:DateTime64(3, 'UTC')}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "cursor", Value: *cursor})
	}

	query := fmt.Sprintf(fetchPersonIssuesQuery, primaryWorkItemTeamAttributionSourceForPerson, cursorFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("people: fetch_person_issues query: %w", err)
	}
	defer rows.Close()

	items := make([]IssueRow, 0)
	for rows.Next() {
		var item IssueRow
		if err := rows.Scan(
			&item.WorkItemID, &item.Provider, &item.Status, &item.TeamID,
			&item.CycleTimeHours, &item.LeadTimeHours,
			&item.StartedAt, &item.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("people: scan person issue row: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: iterate person issue rows: %w", err)
	}
	return items, nil
}

// BuildDrilldownIssuesResponse is the Go port of
// build_person_drilldown_issues_response (services/people.py:793-844): see
// BuildDrilldownPRsResponse's own doc comment for the identity-resolution/
// 404 mapping this function shares.
func BuildDrilldownIssuesResponse(ctx context.Context, reader *Reader, orgID string, params DrilldownIssuesParams) (*DrilldownIssuesResponse, error) {
	if reader == nil {
		return nil, ErrUnavailable
	}

	startDay, endDay, _, _ := timeWindow(params.Now, params.RangeDays, params.RangeDays)
	limit := boundedDrilldownLimit(params.Limit)

	canonical, aliasList, err := resolveIdentityContext(ctx, reader.client, params.PersonID, orgID)
	if err != nil {
		return nil, err
	}
	if canonical == "" {
		return nil, notFound("Person not found")
	}
	identities := identityVariants(canonical, aliasList)

	rows, err := fetchPersonIssues(ctx, reader.client, identities, startDay, endDay, limit, params.Cursor, orgID)
	if err != nil {
		return nil, err
	}

	var nextCursor *time.Time
	if len(rows) > 0 {
		nextCursor = rows[len(rows)-1].CompletedAt
	}

	return &DrilldownIssuesResponse{Items: rows, NextCursor: nextCursor}, nil
}
