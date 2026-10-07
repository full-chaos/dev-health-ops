//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-8810, acceptance (a): the end-to-end proof through the production
// GitHub route. The harness (the stateful provider double, the real route, the
// real deriver, the real effect committer and sinks, ClickHouse from the
// migration chain, Postgres) is the work of the lane that found the defect
// (CHAOS-8808); this file holds the part of it that this change makes green.
// The two other properties of that harness -- an hourly unit by itself leaves
// the store equal to a full sync, and thirteen windows equal one unit -- need
// the sync unit to stop writing derived rows and the post-sync fan-out to
// cover the touched days; their tests come with those changes.
//
// The window-equivalence fixture: one repository whose issues have their
// creation, their state changes and their last update spread over a 90-day
// first-sync depth. The provider answer is the same for both arms; only the
// unit windows differ.
var (
	windowEquivalenceNow   = time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	windowEquivalenceStart = windowEquivalenceNow.AddDate(0, 0, -90)
)

const windowEquivalenceOrgID = "6f1c2d3e-4a5b-4c6d-8e9f-0a1b2c3d4e5f"

type windowEquivalenceEvent struct {
	day   float64
	event string
	actor string
}

type windowEquivalenceComment struct {
	day    float64
	author string
}

type windowEquivalenceIssue struct {
	number     int
	title      string
	createdDay float64
	labels     []string
	assignees  []string
	events     []windowEquivalenceEvent
	comments   []windowEquivalenceComment
}

func windowEquivalenceAt(day float64) time.Time {
	return windowEquivalenceStart.Add(time.Duration(day * 24 * float64(time.Hour))).Truncate(time.Second)
}

// updatedDay is the provider's updated_at: the latest of the creation, every
// event and every comment.
func (issue windowEquivalenceIssue) updatedDay() float64 {
	latest := issue.createdDay
	for _, event := range issue.events {
		latest = max(latest, event.day)
	}
	for _, comment := range issue.comments {
		latest = max(latest, comment.day)
	}
	return latest
}

func (issue windowEquivalenceIssue) state() string {
	state := "open"
	for _, event := range issue.events {
		switch event.event {
		case "closed":
			state = "closed"
		case "reopened":
			state = "open"
		}
	}
	return state
}

func windowEquivalenceIssues() []windowEquivalenceIssue {
	return []windowEquivalenceIssue{
		// Whole life inside the first window.
		{number: 1, title: "Fix login crash", createdDay: 1.5, labels: []string{"bug"}, assignees: []string{"ada"},
			events: []windowEquivalenceEvent{{2.25, "closed", "ada"}}},
		// Created in the first window, completed in the sixth.
		{number: 2, title: "Add export", createdDay: 4.5, labels: []string{"feature"}, assignees: []string{"grace"},
			events: []windowEquivalenceEvent{{39.5, "closed", "grace"}}},
		// Changes in two windows: closed in the second, reopened in the
		// fourth, closed again in the ninth.
		{number: 3, title: "Flaky upload", createdDay: 8.25, labels: []string{"bug"}, assignees: []string{"ada"},
			events: []windowEquivalenceEvent{{10.5, "closed", "ada"}, {24.5, "reopened", "linus"}, {59.75, "closed", "ada"}}},
		// Closed early, commented on much later: its last update is in the
		// twelfth window, its completion in the second.
		{number: 4, title: "Rotate keys", createdDay: 9.5, labels: []string{"security"}, assignees: []string{"grace"},
			events:   []windowEquivalenceEvent{{11.5, "closed", "grace"}},
			comments: []windowEquivalenceComment{{79.5, "linus"}}},
		// Older than the depth, still open, touched inside it.
		{number: 5, title: "Tracking: migration", createdDay: -30, labels: []string{"chore"}, assignees: []string{"linus"},
			comments: []windowEquivalenceComment{{20.5, "ada"}, {84.5, "grace"}}},
		// Open, created late, never closed.
		{number: 6, title: "Dark mode", createdDay: 69.5, labels: []string{"feature"}, assignees: []string{"ada"}},
		// Several issues completed on the same day by different people.
		{number: 7, title: "Bump deps", createdDay: 30.25, labels: []string{"chore"}, assignees: []string{"grace"},
			events: []windowEquivalenceEvent{{33.5, "closed", "grace"}}},
		{number: 8, title: "Crash on save", createdDay: 31.5, labels: []string{"bug"}, assignees: []string{"ada"},
			events: []windowEquivalenceEvent{{33.75, "closed", "ada"}}},
		// Unassigned, commented in three different windows.
		{number: 9, title: "Docs outdated", createdDay: 15.5, labels: []string{"documentation"},
			comments: []windowEquivalenceComment{{16.5, "ada"}, {45.5, "grace"}, {72.5, "linus"}},
			events:   []windowEquivalenceEvent{{73.5, "closed", "linus"}}},
		// Updated in the last, shorter window.
		{number: 10, title: "Late regression", createdDay: 86.25, labels: []string{"bug"}, assignees: []string{"grace"},
			events: []windowEquivalenceEvent{{88.5, "closed", "grace"}}},
		// Created before the depth and closed inside it.
		{number: 11, title: "Old cleanup", createdDay: -10, labels: []string{"chore"}, assignees: []string{"linus"},
			events: []windowEquivalenceEvent{{50.5, "closed", "linus"}}},
		// Last update exactly on a window boundary (day 14 = end of window 2).
		{number: 12, title: "Boundary item", createdDay: 12.5, labels: []string{"feature"}, assignees: []string{"ada"},
			events: []windowEquivalenceEvent{{14, "closed", "ada"}}},
	}
}

// windowEquivalenceDoer answers the GitHub REST calls of the work-items route
// from the fixture. It honours the `since` parameter of the issue list exactly
// as the provider does (updated_at >= since) and applies no upper bound: the
// route itself leaves out what was updated after the window end.
type windowEquivalenceDoer struct {
	t        *testing.T
	issues   []windowEquivalenceIssue
	requests int
}

var windowEquivalenceIssuePath = regexp.MustCompile(`^/repos/acme/api/issues/(\d+)/(events|comments)$`)

func (doer *windowEquivalenceDoer) Do(request *http.Request) (*http.Response, error) {
	doer.requests++
	reply := func(status int, body any) (*http.Response, error) {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(string(encoded))), Request: request,
		}, nil
	}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return reply(http.StatusOK, map[string]any{"id": 4567, "full_name": "acme/api"})
	case path == "/repos/acme/api/issues":
		var since time.Time
		if raw := request.URL.Query().Get("since"); raw != "" {
			parsed, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				doer.t.Errorf("issue list since=%q: %v", raw, err)
			}
			since = parsed
		}
		listed := []map[string]any{}
		for _, issue := range doer.issues {
			updated := windowEquivalenceAt(issue.updatedDay())
			if updated.Before(since) {
				continue
			}
			labels := []map[string]any{}
			for _, label := range issue.labels {
				labels = append(labels, map[string]any{"name": label})
			}
			assignees := []map[string]any{}
			for _, assignee := range issue.assignees {
				assignees = append(assignees, map[string]any{"login": assignee})
			}
			entry := map[string]any{
				"number": issue.number, "title": issue.title, "body": "", "state": issue.state(),
				"created_at": windowEquivalenceAt(issue.createdDay).Format(time.RFC3339),
				"updated_at": updated.Format(time.RFC3339),
				"user":       map[string]any{"login": "reporter"},
				"labels":     labels, "assignees": assignees,
			}
			if issue.state() == "closed" {
				for index := len(issue.events) - 1; index >= 0; index-- {
					if issue.events[index].event == "closed" {
						entry["closed_at"] = windowEquivalenceAt(issue.events[index].day).Format(time.RFC3339)
						break
					}
				}
			}
			listed = append(listed, entry)
		}
		return reply(http.StatusOK, listed)
	}
	if match := windowEquivalenceIssuePath.FindStringSubmatch(path); match != nil {
		number, _ := strconv.Atoi(match[1])
		for _, issue := range doer.issues {
			if issue.number != number {
				continue
			}
			if match[2] == "events" {
				events := []map[string]any{}
				for _, event := range issue.events {
					events = append(events, map[string]any{
						"event": event.event, "created_at": windowEquivalenceAt(event.day).Format(time.RFC3339),
						"actor": map[string]any{"login": event.actor},
					})
				}
				return reply(http.StatusOK, events)
			}
			comments := []map[string]any{}
			for index, comment := range issue.comments {
				comments = append(comments, map[string]any{
					"id": issue.number*1000 + index, "body": "note",
					"created_at": windowEquivalenceAt(comment.day).Format(time.RFC3339),
					"user":       map[string]any{"login": comment.author},
				})
			}
			return reply(http.StatusOK, comments)
		}
	}
	doer.t.Errorf("unexpected provider request %s", request.URL.String())
	return reply(http.StatusNotFound, map[string]any{"message": "Not Found"})
}

type windowEquivalenceWindow struct {
	since, before time.Time
	// issues is the provider's state when this window's unit runs; nil means
	// the fixture as windowEquivalenceIssues returns it.
	issues []windowEquivalenceIssue
}

// runWindowEquivalenceArm syncs the fixture through the production route --
// the real collector, the real effect committer and sink --
// once per window, into its own ClickHouse, and returns every row of every
// destination table.
func runWindowEquivalenceArm(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository,
	conn driver.Conn, arm string, windows []windowEquivalenceWindow,
) map[string][]string {
	t.Helper()
	doer := &windowEquivalenceDoer{t: t}
	for index, window := range windows {
		doer.issues = window.issues
		if doer.issues == nil {
			doer.issues = windowEquivalenceIssues()
		}
		unitID := fmt.Sprintf("%s-0000-4000-8000-%012d", arm, index+1)
		runAt := window.before.Add(time.Duration(index+1) * time.Minute)
		if runAt.Before(windowEquivalenceNow) {
			runAt = windowEquivalenceNow.Add(time.Duration(index+1) * time.Minute)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, $2, $3, $4, $5, 'github', 'work-items', 'medium',
    'incremental', $6, $7, 'dispatching',
    '{"family_dataset_work_items":true,
      "family_dataset_work_item_labels":true,
      "family_dataset_work_item_projects":true,
      "family_dataset_work_item_history":true,
      "family_dataset_work_item_comments":true}'::jsonb,
    $7
)`, unitID, windowEquivalenceOrgID, firstRunID, firstIntegrationID, firstSourceID,
			window.since, window.before); err != nil {
			t.Fatal(err)
		}
		claim, err := repository.Claim(ctx, ClaimRequest{
			UnitID: unitID, OrgID: windowEquivalenceOrgID, Owner: unitID,
			Now: runAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
		})
		if err != nil {
			t.Fatalf("%s window %d claim: %v", arm, index+1, err)
		}
		guard := leaseGuardAt(repository, claim, runAt)
		metrics := providerfoundation.NewMetrics()
		sink, err := NewGitHubWorkItemClickHouseEffects(conn, guard, metrics)
		if err != nil {
			t.Fatal(err)
		}
		executor := CompleteRouteExecutor{
			Credentials: providerfoundation.CredentialResolver{
				Repository: projectsV2DurableCredentialRepository{},
				Decryptor:  projectsV2DurableCredentialDecryptor{},
			},
			Doer: fakehttp.Client(doer),
			Retry: providerfoundation.RetryPolicy{
				MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
			},
			Budget:       executorBudgetStore{},
			BudgetLimits: map[CostClass]int{CostMedium: 1},
			BudgetTTL:    time.Minute,
			Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
				return executorBackoffGate{}
			},
			Metrics: metrics,
			Handler: GitHubWorkItemsRouteHandler{
				Projects: GitHubProjectV2Fetcher{},
				ProjectMembershipSnapshotDiff: GitHubProjectV2SnapshotDiffClickHouseReader{
					Conn: conn,
				},
			},
			Comparator: ProductionContractComparator{},
			Committer: EffectCommitter{
				Ledger: repository, Sink: sink, Readback: sink,
				Now: func() time.Time { return runAt },
			},
			HeartbeatInterval: 30 * time.Second,
			Now:               func() time.Time { return runAt },
		}
		descriptor, ok := Descriptor("github", "work-items")
		if !ok || !descriptor.RouteReady || !descriptor.Plannable {
			t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
		}
		session := &LeaseSession{
			Repository: repository, Claim: claim, LeaseDuration: time.Minute,
			Deadline: claim.LeaseExpiresAt, Now: func() time.Time { return runAt },
		}
		result, err := executor.Execute(ctx, session, descriptor)
		if err != nil {
			t.Fatalf("%s window %d [%s, %s] execute: %v", arm, index+1, window.since, window.before, err)
		}
		if result.Watermark == nil || !result.Watermark.Equal(window.before) {
			t.Fatalf("%s window %d watermark = %v, want the window end %s", arm, index+1, result.Watermark, window.before)
		}
		if err := repository.Complete(
			ctx, claim, result.Result, result.Watermark, runAt, runAt.Add(time.Second),
		); err != nil {
			t.Fatalf("%s window %d complete: %v", arm, index+1, err)
		}
	}
	tables := readWindowEquivalenceTables(t, ctx, conn)
	t.Logf("arm %s: %d units, %d provider requests", arm, len(windows), doer.requests)
	return tables
}

// windowEquivalenceAppendOnlyKeys names, for each destination that is a plain
// (append-only) MergeTree and is written again by a later unit or by the daily
// job, the key its readers take the newest computed_at of. The keys are the
// GROUP BY of the production readers: investment_metrics_daily in
// internal/queryapi/analytics/timeseries.go (investmentMetricsDailyDedupSource),
// internal/queryapi/home/queries_freshness.go (fetchReworkThemeAllocation) and
// internal/queryapi/operatingreview/operatingreview.go (fetchInvestment);
// issue_type_metrics_daily has one reader, a DISTINCT of issue_type_norm, for
// which the table's own row key is the key. investment_classifications_daily
// has no reader: its key here is the item and the day.
var windowEquivalenceAppendOnlyKeys = map[string]string{
	"issue_type_metrics_daily":         "org_id, day, repo_id, provider, team_id, issue_type_norm",
	"investment_metrics_daily":         "org_id, day, repo_id, team_id, investment_area, project_stream",
	"investment_classifications_daily": "org_id, day, repo_id, artifact_type, artifact_id",
}

// readWindowEquivalenceTables returns every row of every destination table as
// a production reader sees it: the newest version of each sorting key (FINAL)
// where the table replaces versions; the newest computed_at of each reader key
// where the table is append only and a reader dedupes it
// (windowEquivalenceAppendOnlyKeys); every row otherwise.
//
// What a reader with NO dedupe would see in an append-only table is every
// version at once: the full sync's row, the hourly unit's partial row and the
// daily job's row for one key, three rows where one is true. No ops reader of
// the three tables reads them that way (see the reader tests of
// internal/queryapi), which is why this read does not either.
func readWindowEquivalenceTables(t *testing.T, ctx context.Context, conn driver.Conn) map[string][]string {
	t.Helper()
	tables := map[string][]string{}
	deduped := map[string]bool{}
	for _, destination := range githubWorkItemRouteDestinations() {
		var engine string
		if err := conn.QueryRow(ctx,
			"SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = ?",
			destination).Scan(&engine); err != nil {
			t.Fatalf("engine of %s: %v", destination, err)
		}
		query := "SELECT formatRowNoNewline('JSONEachRow', *) FROM " + destination
		key, appendOnly := windowEquivalenceAppendOnlyKeys[destination]
		switch {
		case strings.Contains(engine, "Replacing"):
			if appendOnly {
				t.Fatalf("%s is %s: it replaces versions, remove it from windowEquivalenceAppendOnlyKeys", destination, engine)
			}
			query += " FINAL WHERE org_id = ?"
		case appendOnly:
			deduped[destination] = true
			query += " WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1 BY " + key
		default:
			query += " WHERE org_id = ?"
		}
		rows, err := conn.Query(ctx, query, windowEquivalenceOrgID)
		if err != nil {
			t.Fatalf("read %s: %v", destination, err)
		}
		table := []string{}
		for rows.Next() {
			var encoded string
			if err := rows.Scan(&encoded); err != nil {
				t.Fatal(err)
			}
			table = append(table, encoded)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		tables[destination] = table
	}
	for destination := range windowEquivalenceAppendOnlyKeys {
		if !deduped[destination] {
			t.Fatalf("%s is in windowEquivalenceAppendOnlyKeys and is not a destination of the route", destination)
		}
	}
	return tables
}

// windowEquivalenceRunStampColumns are the columns a unit stamps with its own
// run clock. Two arms run at different instants by construction, so these
// columns differ for every row and are left out of the comparison; every other
// column of every destination is compared. A name that matches no column of
// any destination fails the test.
var windowEquivalenceRunStampColumns = []string{
	"last_synced", "computed_at",
	// work_items: the wall clock of the insert.
	"ingested_at",
	// work_item_team_attributions: the id of the unit that wrote the row.
	"run_id",
}

func normalizeWindowEquivalenceRows(t *testing.T, destination string, rows []string, stamped map[string]bool) []string {
	t.Helper()
	normalized := make([]string, 0, len(rows))
	for _, encoded := range rows {
		var row map[string]json.RawMessage
		if err := json.Unmarshal([]byte(encoded), &row); err != nil {
			t.Fatalf("%s row %s: %v", destination, encoded, err)
		}
		for _, column := range windowEquivalenceRunStampColumns {
			if _, exists := row[column]; exists {
				stamped[column] = true
				delete(row, column)
			}
		}
		canonical, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		normalized = append(normalized, string(canonical))
	}
	sort.Strings(normalized)
	return normalized
}

// windowEquivalenceDifference is the multiset difference of two sorted lists.
func windowEquivalenceDifference(left, right []string) (onlyLeft, onlyRight []string) {
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i] == right[j]:
			i++
			j++
		case left[i] < right[j]:
			onlyLeft = append(onlyLeft, left[i])
			i++
		default:
			onlyRight = append(onlyRight, right[j])
			j++
		}
	}
	return append(onlyLeft, left[i:]...), append(onlyRight, right[j:]...)
}

// newWindowEquivalencePostgres starts the semantic store with the provider-sync
// fixture: one GitHub integration and source, with the work-items dataset
// options of an issues-only repository (comments fetched, no pull requests,
// no milestones).
func newWindowEquivalencePostgres(t *testing.T, ctx context.Context) (*pgxpool.Pool, *PostgresRepository) {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := postgres.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	for _, statement := range []string{
		"UPDATE public.integrations SET org_id = $1",
		"UPDATE public.integration_sources SET org_id = $1",
		"UPDATE public.integration_datasets SET org_id = $1",
		"UPDATE public.sync_runs SET org_id = $1",
		"UPDATE public.sync_run_units SET org_id = $1",
	} {
		if _, err := pool.Exec(ctx, statement, windowEquivalenceOrgID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.integrations SET config = '{"api_url":"https://api.github.com"}'::jsonb WHERE id = $1`,
		firstIntegrationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, options)
VALUES ($1, $3, $2, 'work-items',
        '{"include_issues":true,"include_pull_requests":false,"fetch_comments":true,"fetch_milestones":false}'::jsonb)`,
		firstCredentialID, firstIntegrationID, windowEquivalenceOrgID); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}

	return pool, repository
}

// windowEquivalenceSteadyIssues is the fixture plus two issues completed on
// the day of `now`, before `now`, so that day's rows have something to lose.
func windowEquivalenceSteadyIssues() []windowEquivalenceIssue {
	return append(windowEquivalenceIssues(),
		windowEquivalenceIssue{number: 13, title: "Morning fix one", createdDay: 85.25, labels: []string{"bug"}, assignees: []string{"ada"},
			events: []windowEquivalenceEvent{{89.6, "closed", "ada"}}},
		windowEquivalenceIssue{number: 14, title: "Morning fix two", createdDay: 86.5, labels: []string{"feature"}, assignees: []string{"grace"},
			events: []windowEquivalenceEvent{{89.8, "closed", "grace"}}},
	)
}

func logWindowEquivalenceDifference(t *testing.T, label string, onlyLeft, onlyRight []string, leftName, rightName string) {
	t.Helper()
	for index, row := range onlyLeft {
		if index == 6 {
			t.Logf("  %s: ... %d more only in %s", label, len(onlyLeft)-index, leftName)
			break
		}
		t.Logf("  %s only in %s: %s", label, leftName, row)
	}
	for index, row := range onlyRight {
		if index == 6 {
			t.Logf("  %s: ... %d more only in %s", label, len(onlyRight)-index, rightName)
			break
		}
		t.Logf("  %s only in %s: %s", label, rightName, row)
	}
}

// runWindowEquivalenceDailyFamilies runs the six work-item daily families for
// one target day over every repository the store holds work items for, each
// built by its production constructor and in the families.json order
// (runDailyWorkItemFamilies).
func runWindowEquivalenceDailyFamilies(t *testing.T, ctx context.Context, conn driver.Conn, day time.Time) map[string]int {
	t.Helper()
	rows, err := conn.Query(ctx,
		"SELECT DISTINCT toString(repo_id) FROM work_items WHERE org_id = ?", windowEquivalenceOrgID)
	if err != nil {
		t.Fatal(err)
	}
	repoIDs := []daily.RepositoryID{}
	for rows.Next() {
		var repoID string
		if err := rows.Scan(&repoID); err != nil {
			t.Fatal(err)
		}
		repoIDs = append(repoIDs, daily.RepositoryID(repoID))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if len(repoIDs) == 0 {
		t.Fatal("the store holds no work item repository")
	}
	return runDailyWorkItemFamilies(ctx, t, conn, windowEquivalenceOrgID, day, repoIDs)
}

// TestGitHubWorkItemsDailyFamiliesRestoreTheDayAfterAnHourlyUnit runs the
// daily metric families for the day of an hourly unit, on the store that the
// hourly unit wrote to and on a store that holds one full sync of the same
// provider state. After the families ran, the rows of that day in every
// destination table must be the same in both stores, read as the production
// readers read them (readWindowEquivalenceTables).
func TestGitHubWorkItemsDailyFamiliesRestoreTheDayAfterAnHourlyUnit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	_, steadyConn := newWorkItemEffectsConn(t)
	_, truthConn := newWorkItemEffectsConn(t)
	pool, repository := newWindowEquivalencePostgres(t, ctx)

	base := windowEquivalenceSteadyIssues()
	changed := windowEquivalenceSteadyIssues()
	for index := range changed {
		if changed[index].number == 6 {
			changed[index].comments = append(changed[index].comments,
				windowEquivalenceComment{day: 90 + 0.5/24, author: "linus"})
			changed[index].events = append(changed[index].events,
				windowEquivalenceEvent{day: 90 + (40.0/60)/24, event: "closed", actor: "ada"})
		}
	}
	hourOne := windowEquivalenceNow.Add(time.Hour)
	runWindowEquivalenceArm(t, ctx, pool, repository, steadyConn, "51111111",
		[]windowEquivalenceWindow{{since: windowEquivalenceStart, before: windowEquivalenceNow, issues: base}})
	runWindowEquivalenceArm(t, ctx, pool, repository, steadyConn, "52111111",
		[]windowEquivalenceWindow{{since: windowEquivalenceNow, before: hourOne, issues: changed}})
	runWindowEquivalenceArm(t, ctx, pool, repository, truthConn, "61111111",
		[]windowEquivalenceWindow{{since: windowEquivalenceStart, before: hourOne, issues: changed}})
	storedBefore := readWindowEquivalenceTables(t, ctx, steadyConn)
	truthBefore := readWindowEquivalenceTables(t, ctx, truthConn)

	// What the sync stored for the transitions: every row has the nil repository
	// id, so a family that selects transitions by the repository id of the
	// work items finds none.
	var transitions, withItemRepo, withNilRepo uint64
	if err := steadyConn.QueryRow(ctx, `
SELECT count(),
       countIf(repo_id IN (SELECT repo_id FROM work_items WHERE org_id = ?)),
       countIf(toString(repo_id) = '00000000-0000-0000-0000-000000000000')
FROM work_item_transitions WHERE org_id = ?`, windowEquivalenceOrgID, windowEquivalenceOrgID,
	).Scan(&transitions, &withItemRepo, &withNilRepo); err != nil {
		t.Fatal(err)
	}
	t.Logf("work_item_transitions stored by the sync units: %d rows, %d with the repository id of their work items, %d with the nil repository id",
		transitions, withItemRepo, withNilRepo)

	day := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	t.Logf("daily families on the hourly store wrote %v", runWindowEquivalenceDailyFamilies(t, ctx, steadyConn, day))
	t.Logf("daily families on the full-sync store wrote %v", runWindowEquivalenceDailyFamilies(t, ctx, truthConn, day))
	storedAfter := readWindowEquivalenceTables(t, ctx, steadyConn)
	truthAfter := readWindowEquivalenceTables(t, ctx, truthConn)

	// Rows are split by their `day` column: the rows OF THE DAY the families
	// ran for (and every row of a table with no day) must be equal in both
	// stores. Rows of OTHER days are compared too and reported, because the
	// families of one day do not write them.
	const targetDay = `"day":"2026-06-17"`
	ofTheDay := func(rows []string) (dayRows, otherDays []string) {
		for _, row := range rows {
			if strings.Contains(row, `"day":"`) && !strings.Contains(row, targetDay) {
				otherDays = append(otherDays, row)
				continue
			}
			dayRows = append(dayRows, row)
		}
		return dayRows, otherDays
	}
	stamped := map[string]bool{}
	wrongBeforeJob, stillWrong, otherDaysWrong := []string{}, []string{}, []string{}
	for _, destination := range githubWorkItemRouteDestinations() {
		beforeStored, _ := ofTheDay(normalizeWindowEquivalenceRows(t, destination, storedBefore[destination], stamped))
		beforeTruth, _ := ofTheDay(normalizeWindowEquivalenceRows(t, destination, truthBefore[destination], stamped))
		afterStored, otherStored := ofTheDay(normalizeWindowEquivalenceRows(t, destination, storedAfter[destination], stamped))
		afterTruth, otherTruth := ofTheDay(normalizeWindowEquivalenceRows(t, destination, truthAfter[destination], stamped))
		wrongBeforeStored, wrongBeforeTruth := windowEquivalenceDifference(beforeStored, beforeTruth)
		onlyStored, onlyTruth := windowEquivalenceDifference(afterStored, afterTruth)
		jobGone, jobNew := windowEquivalenceDifference(beforeStored, afterStored)
		otherOnlyStored, otherOnlyTruth := windowEquivalenceDifference(otherStored, otherTruth)
		t.Logf("TABLE %-34s the day, before_job: only_stored=%-3d only_truth=%-3d | job_on_hourly_store: replaced=%-3d new=%-3d | the day, after_job: only_stored=%-3d only_truth=%-3d | other days, after_job: only_stored=%-3d only_truth=%d",
			destination, len(wrongBeforeStored), len(wrongBeforeTruth), len(jobGone), len(jobNew),
			len(onlyStored), len(onlyTruth), len(otherOnlyStored), len(otherOnlyTruth))
		if len(wrongBeforeStored)+len(wrongBeforeTruth) > 0 {
			wrongBeforeJob = append(wrongBeforeJob, destination)
		}
		if len(onlyStored)+len(onlyTruth) > 0 {
			stillWrong = append(stillWrong, destination)
			logWindowEquivalenceDifference(t, destination+" after the daily families", onlyStored, onlyTruth, "the hourly store", "the full-sync store")
		}
		if len(otherOnlyStored)+len(otherOnlyTruth) > 0 {
			otherDaysWrong = append(otherDaysWrong, destination)
		}
	}
	for _, column := range windowEquivalenceRunStampColumns {
		if !stamped[column] {
			t.Errorf("run-stamp column %q matched no column of any destination", column)
		}
	}
	// The precondition: before the families ran, the hourly unit had left the
	// day wrong in the tables this change gives a daily family (or a working
	// one) for. A harness that no longer produces the partial rows would make
	// the equality below true for no reason.
	for _, destination := range []string{
		"issue_type_metrics_daily", "investment_metrics_daily", "work_item_state_durations_daily",
	} {
		if !slices.Contains(wrongBeforeJob, destination) {
			t.Errorf("before the daily families, %s of the day was already equal to a full sync: the hourly unit no longer leaves the partial rows this test is about (wrong before the job: %v)",
				destination, wrongBeforeJob)
		}
	}
	if len(stillWrong) > 0 {
		t.Fatalf("after the daily families %d of %d destination tables still differ from a full sync for the day: %v",
			len(stillWrong), len(githubWorkItemRouteDestinations()), stillWrong)
	}
	// NOT repaired by the families of one day, and measured here so that it is
	// not read as covered: issue 6 got its first status change in the hourly
	// unit. An item with no status change has no state-duration rows; with one
	// it has rows on every day since its creation. Those earlier days are
	// recomputed only when the daily job runs for them, which is the job of
	// the post-sync fan-out over the days the new data touches. No other table
	// may differ on another day.
	if len(otherDaysWrong) > 0 {
		t.Logf("OTHER DAYS still differ from a full sync (not written by the families of one day): %v", otherDaysWrong)
	}
	for _, destination := range otherDaysWrong {
		if destination != "work_item_state_durations_daily" {
			t.Errorf("%s differs from a full sync on a day other than the target day", destination)
		}
	}
}
