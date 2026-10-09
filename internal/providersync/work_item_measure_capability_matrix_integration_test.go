//go:build integration

package providersync

// CHAOS-8895 on a real ClickHouse at the migration head: whether a provider
// tracks story points and bugs, for github, gitlab, jira and linear.
//
// Each case starts from the provider's own answer (the JSON its API returns),
// runs it through that provider's production normalizer -- where the native
// estimate field becomes story_points and the provider's type or labels
// become "bug" -- stores the rows through the production raw work-item sink,
// and runs the daily families by their production constructors: work_item
// over the partition, then the finalize family work_item_measure_capability.
// The assertions read what they wrote: the capability rows, and the daily row
// whose 0 the capability explains.

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

var capabilityMatrixDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

// The three item sets of every provider:
//
//	tracked    a bug without an estimate, created on the day and open, and
//	           an item with the provider's estimate completed 10 days before
//	           the day: nothing with points completes on the day, so the
//	           daily row's story_points_completed is a real 0.
//	untracked  one item created on the day and open, no estimate, not a bug.
//	outside    one bug with an estimate, created and completed long before
//	           the 90-day window.
const (
	capabilityTracked   = "tracked"
	capabilityUntracked = "untracked"
	capabilityOutside   = "outside"
)

// capabilityMatrixItem is one item of a case in provider-neutral terms; each
// provider renders it as its own API answer.
type capabilityMatrixItem struct {
	number    int
	bug       bool
	points    *float64
	createdAt time.Time
	// completedAt is nil for an open item.
	completedAt *time.Time
}

func capabilityMatrixItems(variant string) []capabilityMatrixItem {
	at := func(days int, hours int) time.Time {
		return capabilityMatrixDay.AddDate(0, 0, days).Add(time.Duration(hours) * time.Hour)
	}
	points := 3.0
	switch variant {
	case capabilityTracked:
		estimatedDone := at(-10, 0)
		return []capabilityMatrixItem{
			{number: 1, bug: true, createdAt: at(0, 8)},
			{number: 2, points: &points, createdAt: at(-20, 0), completedAt: &estimatedDone},
		}
	case capabilityUntracked:
		return []capabilityMatrixItem{{number: 1, createdAt: at(0, 8)}}
	default:
		done := at(-150, 0)
		return []capabilityMatrixItem{{number: 1, bug: true, points: &points, createdAt: at(-200, 0), completedAt: &done}}
	}
}

func capabilityRFC3339(value time.Time) string { return value.UTC().Format(time.RFC3339) }

func capabilityJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func capabilityClaim(t *testing.T, provider, orgID string) Claim {
	t.Helper()
	claim := nativeTestClaim(provider, "work-items")
	claim.OrgID = orgID
	since, before := capabilityMatrixDay, capabilityMatrixDay.AddDate(0, 0, 1)
	claim.SinceAt, claim.BeforeAt = &since, &before
	if provider == "jira" {
		claim.DatasetOptions = map[string]any{"story_points_field": "customfield_10016"}
	}
	if provider == "linear" {
		claim.SourceExternalID = "OPS"
	}
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
	return claim
}

// capabilityGitHubRows renders an item without an estimate as a REST issue and
// an item with an estimate as a Projects v2 item: a GitHub issue has no
// estimate field, the number field of a project board is where points live.
func capabilityGitHubRows(
	t *testing.T, claim Claim, repoID uuid.UUID, items []capabilityMatrixItem, normalizedAt time.Time,
) []githubWorkItemRow {
	t.Helper()
	var rows []githubWorkItemRow
	for _, item := range items {
		labels := []map[string]string{{"name": "enhancement"}}
		if item.bug {
			labels = []map[string]string{{"name": "bug"}}
		}
		state := "OPEN"
		var closedAt *string
		if item.completedAt != nil {
			state = "CLOSED"
			value := capabilityRFC3339(*item.completedAt)
			closedAt = &value
		}
		if item.points == nil {
			issue := map[string]any{
				"number": item.number, "title": fmt.Sprintf("Item %d", item.number),
				"state":      map[string]string{"OPEN": "open", "CLOSED": "closed"}[state],
				"created_at": capabilityRFC3339(item.createdAt), "updated_at": capabilityRFC3339(item.createdAt),
				"closed_at": closedAt, "labels": labels,
			}
			row, err := normalizeGitHubIssueWorkItem(claim, "acme/api", repoID,
				capabilityJSON(t, issue), nil, normalizedAt)
			if err != nil {
				t.Fatalf("github issue %d: %v", item.number, err)
			}
			rows = append(rows, row)
			continue
		}
		var payload gitHubProjectV2ItemPayload
		board := map[string]any{
			"id":        fmt.Sprintf("PVTI_%d", item.number),
			"createdAt": capabilityRFC3339(item.createdAt),
			"content": map[string]any{
				"__typename": "Issue", "id": fmt.Sprintf("I_%d", item.number), "number": 100 + item.number,
				"title": fmt.Sprintf("Board item %d", item.number), "state": state,
				"createdAt": capabilityRFC3339(item.createdAt), "updatedAt": capabilityRFC3339(item.createdAt),
				"closedAt": closedAt, "repository": map[string]string{"nameWithOwner": "acme/api"},
				"labels": map[string]any{"nodes": labels}, "assignees": map[string]any{"nodes": []any{}},
			},
			"fieldValues": map[string]any{"nodes": []map[string]any{{
				"__typename": "ProjectV2ItemFieldNumberValue", "number": *item.points,
				"field": map[string]string{"name": "Estimate"},
			}}},
			"changes": map[string]any{"nodes": []any{}},
		}
		if err := json.Unmarshal(capabilityJSON(t, board), &payload); err != nil {
			t.Fatal(err)
		}
		row, _, kept, err := normalizeGitHubProjectV2Item(claim, payload, "ghprojv2:acme/1", nil, normalizedAt)
		if err != nil || !kept {
			t.Fatalf("github board item %d: kept=%v err=%v", item.number, kept, err)
		}
		rows = append(rows, row)
	}
	return rows
}

func capabilityGitLabRows(
	t *testing.T, claim Claim, repoID uuid.UUID, items []capabilityMatrixItem, normalizedAt time.Time,
) []githubWorkItemRow {
	t.Helper()
	mapping := loadRealStatusMapping(t)
	var rows []githubWorkItemRow
	for _, item := range items {
		labels := []string{"feature"}
		if item.bug {
			labels = []string{"bug"}
		}
		issue := map[string]any{
			"iid": item.number, "title": fmt.Sprintf("Item %d", item.number), "state": "opened",
			"created_at": capabilityRFC3339(item.createdAt), "updated_at": capabilityRFC3339(item.createdAt),
			"labels": labels, "weight": item.points,
		}
		if item.completedAt != nil {
			issue["state"] = "closed"
			issue["closed_at"] = capabilityRFC3339(*item.completedAt)
		}
		var payload gitlabIssueWorkItemPayload
		if err := json.Unmarshal(capabilityJSON(t, issue), &payload); err != nil {
			t.Fatal(err)
		}
		row, _, err := normalizeGitLabIssueWorkItem(claim, "acme/api", repoID, payload, nil, mapping, nil, normalizedAt)
		if err != nil {
			t.Fatalf("gitlab issue %d: %v", item.number, err)
		}
		rows = append(rows, row)
	}
	return rows
}

func capabilityJiraRows(
	t *testing.T, claim Claim, items []capabilityMatrixItem, normalizedAt time.Time,
) []githubWorkItemRow {
	t.Helper()
	mapping := loadRealStatusMapping(t)
	var rows []githubWorkItemRow
	for _, item := range items {
		issueType := "Story"
		if item.bug {
			issueType = "Bug"
		}
		jiraTime := func(value time.Time) string { return value.UTC().Format("2006-01-02T15:04:05.000+0000") }
		fields := map[string]any{
			"summary": fmt.Sprintf("Item %d", item.number), "issuetype": map[string]string{"name": issueType},
			"status":  map[string]any{"name": "To Do", "statusCategory": map[string]string{"key": "new"}},
			"project": map[string]string{"id": "10001", "key": "OPS", "name": "Ops Project"},
			"created": jiraTime(item.createdAt), "updated": jiraTime(item.createdAt),
		}
		if item.points != nil {
			fields["customfield_10016"] = *item.points
		}
		if item.completedAt != nil {
			fields["status"] = map[string]any{"name": "Done", "statusCategory": map[string]string{"key": "done"}}
			fields["resolutiondate"] = jiraTime(*item.completedAt)
			fields["updated"] = jiraTime(*item.completedAt)
		}
		var raw map[string]any
		if err := json.Unmarshal(capabilityJSON(t, map[string]any{
			"key": fmt.Sprintf("OPS-%d", item.number), "fields": fields,
		}), &raw); err != nil {
			t.Fatal(err)
		}
		row, _, err := normalizeJiraWorkItem(claim, jiraWorkItemFixtureInput{Raw: raw}, mapping,
			func(string, string, string) string { return "" }, normalizedAt)
		if err != nil {
			t.Fatalf("jira issue %d: %v", item.number, err)
		}
		rows = append(rows, row)
	}
	return rows
}

func capabilityLinearRows(
	t *testing.T, claim Claim, items []capabilityMatrixItem, normalizedAt time.Time,
) []linearWorkItemRow {
	t.Helper()
	var rows []linearWorkItemRow
	for _, item := range items {
		label := "Feature"
		if item.bug {
			label = "Bug"
		}
		issue := map[string]any{
			"id": fmt.Sprintf("issue-%d", item.number), "identifier": fmt.Sprintf("OPS-%d", item.number),
			"title":     fmt.Sprintf("Item %d", item.number),
			"createdAt": capabilityRFC3339(item.createdAt), "updatedAt": capabilityRFC3339(item.createdAt),
			"estimate": item.points, "state": map[string]string{"name": "Todo", "type": "unstarted"},
			"labels": map[string]any{"nodes": []map[string]string{{"id": "label-1", "name": label}}},
			"team":   map[string]string{"id": "team-ops", "key": "OPS", "name": "Ops"},
		}
		if item.completedAt != nil {
			issue["state"] = map[string]string{"name": "Done", "type": "completed"}
			issue["completedAt"] = capabilityRFC3339(*item.completedAt)
		}
		var payload linearWorkItemPayload
		if err := json.Unmarshal(capabilityJSON(t, issue), &payload); err != nil {
			t.Fatal(err)
		}
		row, _, err := normalizeLinearWorkItem(claim, payload, normalizedAt)
		if err != nil {
			t.Fatalf("linear issue %d: %v", item.number, err)
		}
		rows = append(rows, row)
	}
	return rows
}

// capabilityAssertProducerOutput checks the producer gave each item the
// measure the case is about. Without it a normalizer that dropped the estimate
// would make the "tracked" case read "untracked" and the test would still name
// a provider it never exercised.
func capabilityAssertProducerOutput(
	t *testing.T, provider string, items []capabilityMatrixItem, projected []workitemmetrics.Item,
) {
	t.Helper()
	wantPoints, wantBugs := 0, 0
	for _, item := range items {
		if item.points != nil {
			wantPoints++
		}
		if item.bug {
			wantBugs++
		}
	}
	gotPoints, gotBugs := 0, 0
	for _, item := range projected {
		if item.StoryPoints != nil {
			gotPoints++
		}
		if item.Type == "bug" {
			gotBugs++
		}
	}
	if gotPoints != wantPoints || gotBugs != wantBugs {
		t.Fatalf("%s producer: %d item(s) with points and %d bug(s), want %d and %d: %+v",
			provider, gotPoints, gotBugs, wantPoints, wantBugs, projected)
	}
}

func capabilityProject(rows []githubWorkItemRow) []workitemmetrics.Item {
	items := make([]workitemmetrics.Item, 0, len(rows))
	for _, row := range rows {
		items = append(items, workitemmetrics.Item{
			Provider: row.Provider, Type: row.Type, Status: row.Status,
			CreatedAt: row.CreatedAt, CompletedAt: row.CompletedAt, StoryPoints: row.StoryPoints,
		})
	}
	return items
}

func capabilityProjectLinear(rows []linearWorkItemRow) []workitemmetrics.Item {
	items := make([]workitemmetrics.Item, 0, len(rows))
	for _, row := range rows {
		items = append(items, workitemmetrics.Item{
			Provider: row.Provider, Type: row.Type, Status: row.Status,
			CreatedAt: row.CreatedAt, CompletedAt: row.CompletedAt, StoryPoints: row.StoryPoints,
		})
	}
	return items
}

// capabilitySeed stores one case's rows through the production raw sink of
// its provider and returns the partition the daily job computes them in.
func capabilitySeed(
	ctx context.Context, t *testing.T, conn driver.Conn, provider, variant, orgID string, normalizedAt time.Time,
) []daily.RepositoryID {
	t.Helper()
	claim := capabilityClaim(t, provider, orgID)
	items := capabilityMatrixItems(variant)
	repoID := uuid.New()
	var (
		rows      []githubWorkItemRow
		partition []daily.RepositoryID
	)
	switch provider {
	case "github":
		rows = capabilityGitHubRows(t, claim, repoID, items, normalizedAt)
		partition = []daily.RepositoryID{daily.RepositoryID(repoID.String()), daily.RepositoryID(uuid.Nil.String())}
	case "gitlab":
		rows = capabilityGitLabRows(t, claim, repoID, items, normalizedAt)
		partition = []daily.RepositoryID{daily.RepositoryID(repoID.String())}
	case "jira":
		rows = capabilityJiraRows(t, claim, items, normalizedAt)
		partition = []daily.RepositoryID{daily.RepositoryID(uuid.Nil.String())}
	case "linear":
		linearRows := capabilityLinearRows(t, claim, items, normalizedAt)
		capabilityAssertProducerOutput(t, provider, items, capabilityProjectLinear(linearRows))
		effect, err := BuildEffectBatch("work_items", EffectReadbackRequired, marshalDailyFamilyMatrixRows(t, linearRows))
		if err != nil {
			t.Fatal(err)
		}
		identity, err := newLinearWorkItemEffectIdentity(claim, effect)
		if err != nil {
			t.Fatal(err)
		}
		if err := (LinearWorkItemsClickHouseAdapter{Conn: conn}).WriteLinearWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatalf("linear: write work_items: %v", err)
		}
		return []daily.RepositoryID{daily.RepositoryID(uuid.Nil.String())}
	default:
		t.Fatalf("unknown provider %q", provider)
	}
	capabilityAssertProducerOutput(t, provider, items, capabilityProject(rows))
	sink, err := NewGitHubWorkItemClickHouseEffects(conn, githubDerivedIntegrationLease(), nil)
	if err != nil {
		t.Fatal(err)
	}
	writeDailyFamilyMatrixEffect(ctx, t, claim, "work_items", marshalDailyFamilyMatrixRows(t, rows), sink.WorkItems)
	return partition
}

// capabilityRunDaily runs what a daily run of the day runs for these tables:
// the partition family work_item over the partition, then the finalize family
// work_item_measure_capability once for the run, each by its production
// constructor.
func capabilityRunDaily(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, day time.Time, repoIDs []daily.RepositoryID,
) {
	t.Helper()
	run := daily.Run{ID: "00000000-0000-4000-8000-0000000088a0", OrganizationID: orgID, TargetDay: day}
	workItem, err := daily.NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workItem.ComputeFamily(ctx, run,
		daily.Partition{ID: "00000000-0000-4000-8000-0000000088a1", RunID: run.ID, RepoIDs: repoIDs}); err != nil {
		t.Fatalf("work_item family: %v", err)
	}
	capability, err := daily.NewWorkItemMeasureCapabilityExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.ComputeFinalizeFamily(ctx, run); err != nil {
		t.Fatalf("%s family: %v", daily.WorkItemMeasureCapabilityFamilyName, err)
	}
}

type capabilityStoredRow struct {
	Provider      string
	Measure       string
	Tracked       uint8
	EvidenceCount uint32
	ItemCount     uint32
	WindowStart   time.Time
	WindowEnd     time.Time
}

// capabilityRead is the reader contract of the table (exact day): for each
// key, the row with window_end = asOf, newest computed_at first. No row for
// that day is unknown, never an earlier window's answer.
func capabilityRead(ctx context.Context, t *testing.T, conn driver.Conn, orgID string, asOf time.Time) []capabilityStoredRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT provider, measure,
       argMax(tracked, computed_at),
       argMax(evidence_count, computed_at),
       argMax(item_count, computed_at),
       argMax(window_start, computed_at),
       any(window_end)
FROM work_item_measure_capability
WHERE org_id = ? AND window_end = ?
GROUP BY provider, measure
ORDER BY provider, measure`, orgID, asOf)
	if err != nil {
		t.Fatalf("read work_item_measure_capability: %v", err)
	}
	defer rows.Close()
	result := []capabilityStoredRow{}
	for rows.Next() {
		var row capabilityStoredRow
		if err := rows.Scan(&row.Provider, &row.Measure, &row.Tracked, &row.EvidenceCount,
			&row.ItemCount, &row.WindowStart, &row.WindowEnd); err != nil {
			t.Fatal(err)
		}
		row.WindowStart, row.WindowEnd = row.WindowStart.UTC(), row.WindowEnd.UTC()
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

// capabilityDailyValues returns the newest bug_completed_ratio and
// story_points_completed of every work_item_metrics_daily key of the day.
func capabilityDailyValues(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) [][2]float64 {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT argMax(bug_completed_ratio, computed_at), argMax(story_points_completed, computed_at)
FROM work_item_metrics_daily
WHERE org_id = ? AND day = ?
GROUP BY provider, work_scope_id, team_id`, orgID, capabilityMatrixDay)
	if err != nil {
		t.Fatalf("read work_item_metrics_daily: %v", err)
	}
	defer rows.Close()
	var values [][2]float64
	for rows.Next() {
		var value [2]float64
		if err := rows.Scan(&value[0], &value[1]); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func TestWorkItemMeasureCapabilityProviderMatrix(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	normalizedAt := capabilityMatrixDay.Add(20 * time.Hour)
	first, last := workitemmetrics.CapabilityWindow(capabilityMatrixDay)
	expected := func(provider string, tracked uint8, evidence, count uint32) []capabilityStoredRow {
		return []capabilityStoredRow{
			{provider, workitemmetrics.MeasureBugCompletedRatio, tracked, evidence, count, first, last},
			{provider, workitemmetrics.MeasureStoryPointsCompleted, tracked, evidence, count, first, last},
		}
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		for _, variant := range []string{capabilityTracked, capabilityUntracked, capabilityOutside} {
			t.Run(provider+"/"+variant, func(t *testing.T) {
				orgID := uuid.NewString()
				repoIDs := capabilitySeed(ctx, t, conn, provider, variant, orgID, normalizedAt)
				capabilityRunDaily(ctx, t, conn, orgID, capabilityMatrixDay, repoIDs)

				got := capabilityRead(ctx, t, conn, orgID, capabilityMatrixDay)
				var want []capabilityStoredRow
				switch variant {
				case capabilityTracked:
					want = expected(provider, 1, 1, 2)
				case capabilityUntracked:
					want = expected(provider, 0, 0, 1)
				default:
					want = []capabilityStoredRow{}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("capability rows:\n got %+v\nwant %+v", got, want)
				}

				// The daily row reads 0 in both stored cases: the capability
				// is what tells a real 0 (tracked) from "not tracked".
				if variant == capabilityOutside {
					return
				}
				values := capabilityDailyValues(ctx, t, conn, orgID)
				if len(values) == 0 {
					t.Fatal("the work_item family wrote no daily row for the day")
				}
				for _, value := range values {
					if value != [2]float64{0, 0} {
						t.Fatalf("daily (bug_completed_ratio, story_points_completed) = %v, want (0, 0)", value)
					}
				}
			})
		}
	}
}

// A run for an old day adds the answer of its own window and leaves the
// answer of the newer window in place, also after a merge.
func TestWorkItemMeasureCapabilityBackfillKeepsTheNewerWindow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	normalizedAt := capabilityMatrixDay.Add(20 * time.Hour)
	orgID := uuid.NewString()
	repoIDs := capabilitySeed(ctx, t, conn, "jira", capabilityTracked, orgID, normalizedAt)
	// An item open since long before both windows, without points and not a
	// bug: the old window sees only this item, so its answer is "not tracked"
	// for both measures, and the current window's answer is "tracked".
	claim := capabilityClaim(t, "jira", orgID)
	old := capabilityJiraRows(t, claim, []capabilityMatrixItem{
		{number: 9, createdAt: capabilityMatrixDay.AddDate(-1, 0, 0)},
	}, normalizedAt)
	sink, err := NewGitHubWorkItemClickHouseEffects(conn, githubDerivedIntegrationLease(), nil)
	if err != nil {
		t.Fatal(err)
	}
	writeDailyFamilyMatrixEffect(ctx, t, claim, "work_items", marshalDailyFamilyMatrixRows(t, old), sink.WorkItems)

	capabilityRunDaily(ctx, t, conn, orgID, capabilityMatrixDay, repoIDs)
	oldDay := capabilityMatrixDay.AddDate(0, 0, -120)
	capabilityRunDaily(ctx, t, conn, orgID, oldDay, repoIDs)
	if err := conn.Exec(ctx, "OPTIMIZE TABLE work_item_measure_capability FINAL"); err != nil {
		t.Fatal(err)
	}
	first, last := workitemmetrics.CapabilityWindow(capabilityMatrixDay)
	want := []capabilityStoredRow{
		{"jira", workitemmetrics.MeasureBugCompletedRatio, 1, 1, 3, first, last},
		{"jira", workitemmetrics.MeasureStoryPointsCompleted, 1, 1, 3, first, last},
	}
	if got := capabilityRead(ctx, t, conn, orgID, capabilityMatrixDay); !reflect.DeepEqual(got, want) {
		t.Fatalf("after a backfill of %s, the current answer:\n got %+v\nwant %+v",
			oldDay.Format(time.DateOnly), got, want)
	}
	oldFirst, oldLast := workitemmetrics.CapabilityWindow(oldDay)
	wantOld := []capabilityStoredRow{
		{"jira", workitemmetrics.MeasureBugCompletedRatio, 0, 0, 1, oldFirst, oldLast},
		{"jira", workitemmetrics.MeasureStoryPointsCompleted, 0, 0, 1, oldFirst, oldLast},
	}
	if got := capabilityRead(ctx, t, conn, orgID, oldDay); !reflect.DeepEqual(got, wantOld) {
		t.Fatalf("the answer of the old window:\n got %+v\nwant %+v", got, wantOld)
	}
}

func capabilityStoreGitLab(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, repoID uuid.UUID,
	items []capabilityMatrixItem, normalizedAt time.Time,
) {
	t.Helper()
	claim := capabilityClaim(t, "gitlab", orgID)
	rows := capabilityGitLabRows(t, claim, repoID, items, normalizedAt)
	sink, err := NewGitHubWorkItemClickHouseEffects(conn, githubDerivedIntegrationLease(), nil)
	if err != nil {
		t.Fatal(err)
	}
	writeDailyFamilyMatrixEffect(ctx, t, claim, "work_items", marshalDailyFamilyMatrixRows(t, rows), sink.WorkItems)
}

// The capability is the organization's answer: a run whose partition holds a
// repository without items still writes it, from the items of the other
// repositories.
func TestWorkItemMeasureCapabilityReadsEveryRepositoryOfTheOrganization(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	normalizedAt := capabilityMatrixDay.Add(20 * time.Hour)
	orgID := uuid.NewString()
	points := 3.0
	capabilityStoreGitLab(ctx, t, conn, orgID, uuid.New(), []capabilityMatrixItem{
		{number: 1, points: &points, createdAt: capabilityMatrixDay.AddDate(0, 0, -30)},
	}, normalizedAt)
	emptyRepository := daily.RepositoryID(uuid.NewString())
	capabilityRunDaily(ctx, t, conn, orgID, capabilityMatrixDay, []daily.RepositoryID{emptyRepository})

	first, last := workitemmetrics.CapabilityWindow(capabilityMatrixDay)
	want := []capabilityStoredRow{
		{"gitlab", workitemmetrics.MeasureBugCompletedRatio, 0, 0, 1, first, last},
		{"gitlab", workitemmetrics.MeasureStoryPointsCompleted, 1, 1, 1, first, last},
	}
	if got := capabilityRead(ctx, t, conn, orgID, capabilityMatrixDay); !reflect.DeepEqual(got, want) {
		t.Fatalf("capability rows:\n got %+v\nwant %+v", got, want)
	}
}

// One work item stored under two repository ids is one item.
func TestWorkItemMeasureCapabilityCountsAnItemStoredUnderTwoRepositoriesOnce(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	normalizedAt := capabilityMatrixDay.Add(20 * time.Hour)
	orgID := uuid.NewString()
	points := 3.0
	item := []capabilityMatrixItem{{number: 1, bug: true, points: &points, createdAt: capabilityMatrixDay.Add(8 * time.Hour)}}
	first, second := uuid.New(), uuid.New()
	capabilityStoreGitLab(ctx, t, conn, orgID, first, item, normalizedAt)
	capabilityStoreGitLab(ctx, t, conn, orgID, second, item, normalizedAt)
	capabilityRunDaily(ctx, t, conn, orgID, capabilityMatrixDay, []daily.RepositoryID{
		daily.RepositoryID(first.String()), daily.RepositoryID(second.String()),
	})

	windowFirst, windowLast := workitemmetrics.CapabilityWindow(capabilityMatrixDay)
	want := []capabilityStoredRow{
		{"gitlab", workitemmetrics.MeasureBugCompletedRatio, 1, 1, 1, windowFirst, windowLast},
		{"gitlab", workitemmetrics.MeasureStoryPointsCompleted, 1, 1, 1, windowFirst, windowLast},
	}
	if got := capabilityRead(ctx, t, conn, orgID, capabilityMatrixDay); !reflect.DeepEqual(got, want) {
		t.Fatalf("capability rows:\n got %+v\nwant %+v", got, want)
	}
}

// Two runs of one day can write the same key within one second. The newer
// write is the answer, before and after a merge, whichever order the two
// inserts land in.
func TestWorkItemMeasureCapabilityNewestWriteOfOneSecondWins(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	first, last := workitemmetrics.CapabilityWindow(capabilityMatrixDay)
	row := func(tracked bool) []workitemmetrics.CapabilityRow {
		evidence := 0
		if tracked {
			evidence = 1
		}
		return []workitemmetrics.CapabilityRow{{
			Provider: "jira", Measure: workitemmetrics.MeasureStoryPointsCompleted, Tracked: tracked,
			EvidenceCount: evidence, ItemCount: 1, WindowStart: first, WindowEnd: last,
		}}
	}
	second := capabilityMatrixDay.Add(21 * time.Hour)
	older, newer := second.Add(100*time.Millisecond), second.Add(900*time.Millisecond)
	// The newer observation lands first: an equal version would let the
	// later insert, the older observation, win the merge.
	if _, err := daily.WriteWorkItemMeasureCapability(ctx, conn, orgID, row(true), newer); err != nil {
		t.Fatal(err)
	}
	if _, err := daily.WriteWorkItemMeasureCapability(ctx, conn, orgID, row(false), older); err != nil {
		t.Fatal(err)
	}
	want := []capabilityStoredRow{{"jira", workitemmetrics.MeasureStoryPointsCompleted, 1, 1, 1, first, last}}
	if got := capabilityRead(ctx, t, conn, orgID, capabilityMatrixDay); !reflect.DeepEqual(got, want) {
		t.Fatalf("before a merge:\n got %+v\nwant %+v", got, want)
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE work_item_measure_capability FINAL"); err != nil {
		t.Fatal(err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM work_item_measure_capability WHERE org_id = ?", orgID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if got := capabilityRead(ctx, t, conn, orgID, capabilityMatrixDay); rows != 1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("after a merge (%d row(s) left):\n got %+v\nwant %+v", rows, got, want)
	}
}

// The answer for a day is the row of that day's window. A day whose window
// holds no item has no row, and reads as unknown: never as the answer of an
// earlier window.
func TestWorkItemMeasureCapabilityDayWithoutItemsIsUnknown(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	normalizedAt := capabilityMatrixDay.Add(20 * time.Hour)
	orgID := uuid.NewString()
	points := 3.0
	done := capabilityMatrixDay.AddDate(0, 0, -1)
	claim := capabilityClaim(t, "jira", orgID)
	rows := capabilityJiraRows(t, claim, []capabilityMatrixItem{
		{number: 1, bug: true, points: &points, createdAt: capabilityMatrixDay.AddDate(0, 0, -10), completedAt: &done},
	}, normalizedAt)
	sink, err := NewGitHubWorkItemClickHouseEffects(conn, githubDerivedIntegrationLease(), nil)
	if err != nil {
		t.Fatal(err)
	}
	writeDailyFamilyMatrixEffect(ctx, t, claim, "work_items", marshalDailyFamilyMatrixRows(t, rows), sink.WorkItems)
	repoIDs := []daily.RepositoryID{daily.RepositoryID(uuid.Nil.String())}

	// The item was completed the day before capabilityMatrixDay, so it is in
	// that day's window and in no window of a day 120 days later.
	laterDay := capabilityMatrixDay.AddDate(0, 0, 120)
	capabilityRunDaily(ctx, t, conn, orgID, capabilityMatrixDay, repoIDs)
	capabilityRunDaily(ctx, t, conn, orgID, laterDay, repoIDs)

	if got := capabilityRead(ctx, t, conn, orgID, laterDay); len(got) != 0 {
		t.Fatalf("as of %s, a day whose window has no item: got %+v, want no row (unknown)",
			laterDay.Format(time.DateOnly), got)
	}
	first, last := workitemmetrics.CapabilityWindow(capabilityMatrixDay)
	want := []capabilityStoredRow{
		{"jira", workitemmetrics.MeasureBugCompletedRatio, 1, 1, 1, first, last},
		{"jira", workitemmetrics.MeasureStoryPointsCompleted, 1, 1, 1, first, last},
	}
	if got := capabilityRead(ctx, t, conn, orgID, capabilityMatrixDay); !reflect.DeepEqual(got, want) {
		t.Fatalf("as of %s:\n got %+v\nwant %+v", capabilityMatrixDay.Format(time.DateOnly), got, want)
	}
}
