//go:build integration

package workerservice

// The acceptance test of the rule "a work-items sync unit writes raw rows
// only; the daily job computes every derived table from the stored rows", for
// github, gitlab, jira and linear.
//
// The defect it closes: a sync unit used to compute the daily work-item rows
// from only the items it held, so a later unit that held fewer items of a work
// scope replaced the day's number with a smaller one.
//
// The seam is the production one on both sides. The unit's rows go through the
// provider's production effect sink (the constructor
// buildProviderSyncHandlerWithWorkItemsRuntimeConfig calls). The recompute is
// the real post-sync fan-out with the real touched-day store, and the daily
// runs it starts go through the real dispatcher and partition handler with the
// registered families (the rig of the touched-day tests).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// rawOnlyDerivedTables are the nine tables the daily job owns. A work-items
// sync unit writes none of them.
var rawOnlyDerivedTables = []string{
	"estimate_coverage_metrics_daily",
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
	"work_item_cycle_times",
	"work_item_metrics_daily",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_user_metrics_daily",
}

var rawOnlyProviders = []string{"github", "gitlab", "jira", "linear"}

// rawOnlyItem is one work item of a unit. The transitions of the item are its
// start and its completion, when it has them.
type rawOnlyItem struct {
	id        string
	repo      *uuid.UUID
	scope     string
	created   time.Time
	started   *time.Time
	completed *time.Time
}

func rawOnlyClaim(t *testing.T, provider, orgID, runID string, since, before time.Time) providersync.Claim {
	t.Helper()
	capability, ok := providersync.Capability(provider, "work-items")
	if !ok {
		t.Fatalf("%s: no work-items capability", provider)
	}
	externalID := "acme/api"
	if provider == "gitlab" {
		externalID = "123"
	}
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: runID, OrgID: orgID,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: externalID, SourceName: "acme/api",
			Provider: provider, Dataset: "work-items", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: time.Now().Add(time.Hour),
	}
	if err := claim.Validate(); err != nil {
		t.Fatalf("%s claim: %v", provider, err)
	}
	return claim
}

// rawOnlySink is the provider's work-items effect sink, built by the
// constructor the worker calls for a work-items claim.
func rawOnlySink(t *testing.T, conn driver.Conn, provider string) providersync.EffectSink {
	t.Helper()
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	var sink providersync.EffectSink
	var err error
	switch provider {
	case "github":
		sink, err = providersync.NewGitHubWorkItemClickHouseEffects(conn, lease, nil)
	case "gitlab":
		sink, err = providersync.NewGitLabWorkItemFamilyClickHouseEffects(conn, lease, nil)
	case "jira":
		sink, err = providersync.NewJiraWorkItemCompositeClickHouseEffects(conn, lease, nil)
	case "linear":
		sink, err = providersync.NewLinearWorkItemFamilyClickHouseEffects(conn, lease, nil)
	default:
		t.Fatalf("provider %q", provider)
	}
	if err != nil {
		t.Fatalf("%s sink: %v", provider, err)
	}
	return sink
}

func rawOnlyEffect(t *testing.T, destination string, rows []map[string]any) providersync.EffectBatch {
	t.Helper()
	encoded := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, raw)
	}
	effect, err := providersync.BuildEffectBatch(destination, providersync.EffectReadbackRequired, encoded)
	if err != nil {
		t.Fatalf("effect %s: %v", destination, err)
	}
	return effect
}

// writeRawOnlyUnit stores the raw rows of one unit through the provider's
// sink, with normalizedAt as the stamp of the rows. Every key of a row is a
// field of the provider's row type: the sink decodes with unknown fields
// refused.
func writeRawOnlyUnit(
	ctx context.Context, t *testing.T, sink providersync.EffectSink, claim providersync.Claim,
	items []rawOnlyItem, normalizedAt time.Time,
) {
	t.Helper()
	var itemRows, transitionRows []map[string]any
	for _, item := range items {
		status, statusRaw, updated := "todo", "Todo", item.created
		if item.started != nil {
			status, statusRaw, updated = "in_progress", "In Progress", *item.started
		}
		if item.completed != nil {
			status, statusRaw, updated = "done", "Done", *item.completed
		}
		var repo any
		if item.repo != nil {
			repo = item.repo.String()
		}
		itemRows = append(itemRows, map[string]any{
			"work_item_id": item.id, "provider": claim.Provider, "title": item.id, "type": "task",
			"status": status, "status_raw": statusRaw, "repo_id": repo,
			"project_key": item.scope, "project_id": item.scope, "project_name": item.scope,
			"assignees": []string{"dev@example.com"}, "labels": []string{},
			"created_at": item.created, "updated_at": updated,
			"started_at": item.started, "completed_at": item.completed,
			"org_id": claim.OrgID, "last_synced": normalizedAt,
		})
		transition := func(at time.Time, fromRaw, toRaw, from, to string) {
			transitionRows = append(transitionRows, map[string]any{
				"work_item_id": item.id, "provider": claim.Provider, "occurred_at": at,
				"from_status_raw": fromRaw, "to_status_raw": toRaw, "from_status": from, "to_status": to,
				"org_id": claim.OrgID, "last_synced": normalizedAt,
			})
		}
		if item.started != nil {
			transition(*item.started, "Todo", "In Progress", "todo", "in_progress")
		}
		if item.completed != nil {
			transition(*item.completed, "In Progress", "Done", "in_progress", "done")
		}
	}
	if err := sink.WriteEffect(ctx, claim, rawOnlyEffect(t, "work_items", itemRows)); err != nil {
		t.Fatalf("%s: write work_items: %v", claim.Provider, err)
	}
	if len(transitionRows) > 0 {
		if err := sink.WriteEffect(ctx, claim, rawOnlyEffect(t, "work_item_transitions", transitionRows)); err != nil {
			t.Fatalf("%s: write work_item_transitions: %v", claim.Provider, err)
		}
	}
}

// assertRawOnlySinkRefusesDerivedTables offers the provider's sink one effect
// for each of the nine tables and requires a refusal.
func assertRawOnlySinkRefusesDerivedTables(
	ctx context.Context, t *testing.T, sink providersync.EffectSink, claim providersync.Claim,
) {
	t.Helper()
	for _, table := range rawOnlyDerivedTables {
		effect := rawOnlyEffect(t, table, []map[string]any{{"org_id": claim.OrgID}})
		if err := sink.WriteEffect(ctx, claim, effect); !errors.Is(err, providersync.ErrInvalidConfiguration) {
			t.Fatalf("%s: the sync sink took an effect for %s (error %v); the daily job is its one writer",
				claim.Provider, table, err)
		}
	}
}

// rawOnlyStoredVersions counts every stored row version of the organization in
// each of the nine tables.
func rawOnlyStoredVersions(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) map[string]uint64 {
	t.Helper()
	counts := map[string]uint64{}
	for _, table := range rawOnlyDerivedTables {
		var count uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM `+table+` WHERE org_id = ?`, orgID).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		counts[table] = count
	}
	return counts
}

func rawOnlyTotal(counts map[string]uint64) uint64 {
	var total uint64
	for _, count := range counts {
		total += count
	}
	return total
}

// rawOnlyDayValues is what a reader of the day sees in the four daily
// work-item tables: the newest row of each key (argMax by computed_at). The
// numbers are the completed items of the day in the two metric tables, the
// items touched in a status in the state table, and the items with a row in
// the cycle-time table.
func rawOnlyDayValues(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string, day time.Time,
) map[string]uint64 {
	t.Helper()
	statements := map[string]string{
		"work_item_metrics_daily": `
SELECT toUInt64(sum(v)) FROM (SELECT argMax(items_completed, computed_at) AS v FROM work_item_metrics_daily
WHERE org_id = ? AND provider = ? AND day = toDate(?) GROUP BY work_scope_id, team_id)`,
		"work_item_user_metrics_daily": `
SELECT toUInt64(sum(v)) FROM (SELECT argMax(items_completed, computed_at) AS v FROM work_item_user_metrics_daily
WHERE org_id = ? AND provider = ? AND day = toDate(?) GROUP BY work_scope_id, user_identity)`,
		"work_item_state_durations_daily": `
SELECT toUInt64(max(v)) FROM (SELECT argMax(items_touched, computed_at) AS v FROM work_item_state_durations_daily
WHERE org_id = ? AND provider = ? AND day = toDate(?) GROUP BY work_scope_id, team_id, status)`,
		"work_item_cycle_times": `
SELECT toUInt64(count()) FROM (SELECT work_item_id FROM work_item_cycle_times
WHERE org_id = ? AND provider = ? AND day = toDate(?) GROUP BY work_item_id)`,
	}
	values := map[string]uint64{}
	for table, statement := range statements {
		var value uint64
		if err := conn.QueryRow(ctx, statement, orgID, provider, day.Format("2006-01-02")).Scan(&value); err != nil {
			t.Fatalf("read %s: %v", table, err)
		}
		values[table] = value
	}
	return values
}

func rawOnlyValuesLine(values map[string]uint64) string {
	return fmt.Sprintf("metrics.items_completed=%d user.items_completed=%d state.items_touched=%d cycle_times.items=%d",
		values["work_item_metrics_daily"], values["work_item_user_metrics_daily"],
		values["work_item_state_durations_daily"], values["work_item_cycle_times"])
}

// rawOnlyTouchedKeys are the (day, repository) keys the organization has a
// 'touched' event for.
func rawOnlyTouchedKeys(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) map[string]bool {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT toString(day), toString(repo_id) FROM daily_metrics_touched_days
WHERE org_id = ? AND kind = 'touched' GROUP BY day, repo_id`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	keys := map[string]bool{}
	for rows.Next() {
		var day, repo string
		if err := rows.Scan(&day, &repo); err != nil {
			t.Fatal(err)
		}
		keys[day+"/"+repo] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

func rawOnlyKey(day time.Time, repo uuid.UUID) string {
	return day.Format("2006-01-02") + "/" + repo.String()
}

// seedRawOnlySyncRun stores the finished sync run of one successful work-items
// unit of the provider and returns the arguments of its post_sync job.
func (rig *touchedRig) seedRawOnlySyncRun(
	t *testing.T, ctx context.Context, claim providersync.Claim, startedAt time.Time,
) syncdispatchruntime.PostSyncArgs {
	t.Helper()
	outboxID, integrationID := uuid.NewString(), uuid.NewString()
	pgseed.EnsureSyncRun(ctx, t, rig.pool, pgseed.SyncRun{ID: claim.SyncRunID, OrgID: claim.OrgID, IntegrationID: integrationID})
	pgseed.SyncDispatchOutbox(ctx, t, rig.pool, outboxID, claim.SyncRunID, claim.OrgID, "post_sync", "dispatched", "river", touchedRigRouteGeneration)
	pgseed.InsertSyncRunUnit(ctx, t, rig.pool, pgseed.SyncRunUnit{
		ID: claim.ID, RunID: claim.SyncRunID, OrgID: claim.OrgID, IntegrationID: integrationID, SourceID: uuid.NewString(),
		Provider: claim.Provider, DatasetKey: claim.Dataset, Status: "success",
		SinceAt: claim.SinceAt, BeforeAt: claim.BeforeAt,
	})
	args := syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: syncdispatchruntime.ContractVersionV1, OrgID: claim.OrgID, RunID: claim.SyncRunID,
		DispatchOutbox: outboxID, RouteGeneration: touchedRigRouteGeneration,
	}}
	rig.setRunStart(t, ctx, args, startedAt)
	return args
}

// fanoutAndRun runs the post-sync fan-out of one sync run and then every daily
// run it started, and returns those runs by day.
func (rig *touchedRig) fanoutAndRun(
	t *testing.T, ctx context.Context, service *syncdispatchruntime.NativePostSyncService,
	orgID string, args syncdispatchruntime.PostSyncArgs,
) map[string]touchedRun {
	t.Helper()
	if err := service.Fanout(ctx, args); err != nil {
		t.Fatalf("fan-out: %v", err)
	}
	runs := rig.runsOf(t, ctx, orgID, args)
	days := touchedRunDays(runs)
	for _, day := range days {
		rig.dispatchAndRun(t, ctx, daily.Run{ID: runs[day].id, OrganizationID: orgID})
	}
	return runs
}

func (rig *touchedRig) assertNoFamilyFailed(t *testing.T) {
	t.Helper()
	rig.recorder.mu.Lock()
	defer rig.recorder.mu.Unlock()
	if len(rig.recorder.other) != 0 {
		t.Fatalf("daily families with an outcome other than computed: %v", rig.recorder.other)
	}
}

// Two items of one work scope were started and completed on one past day. For
// github and gitlab they are in two repositories; jira and linear items carry
// no repository.
//
//	unit 1 holds both items (a first sync of the day).
//	unit 2 runs twenty days later and holds only the first item (an
//	       incremental unit in which only that item changed).
//
// After each unit the derived tables hold no row of the sync. The fan-out of
// each sync run marks the day as touched and the daily run of the day gives
// the whole numbers: two items, never one.
func TestWorkItemsSyncWritesRawRowsOnlyAndTheDailyJobGivesTheWholeDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	service := rig.service(t, rig.touched, nil)
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	nextDay := day.AddDate(0, 0, 1)
	laterDay := day.AddDate(0, 0, 20)
	whole := map[string]uint64{
		"work_item_metrics_daily": 2, "work_item_user_metrics_daily": 2,
		"work_item_state_durations_daily": 2, "work_item_cycle_times": 2,
	}
	absent := map[string]uint64{
		"work_item_metrics_daily": 0, "work_item_user_metrics_daily": 0,
		"work_item_state_durations_daily": 0, "work_item_cycle_times": 0,
	}
	var failures []string
	check := func(provider, step string, got, want map[string]uint64) {
		t.Logf("%-6s %-58s %s", provider, step, rawOnlyValuesLine(got))
		tables := make([]string, 0, len(want))
		for table := range want {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		for _, table := range tables {
			if got[table] != want[table] {
				failures = append(failures, fmt.Sprintf("%s %s %s: %d, want %d", provider, step, table, got[table], want[table]))
			}
		}
	}

	for _, provider := range rawOnlyProviders {
		orgID := uuid.NewString()
		repoA, repoB := uuid.New(), uuid.New()
		keyRepoA, keyRepoB := repoA, repoB
		itemRepoA, itemRepoB := &repoA, &repoB
		if provider == "jira" || provider == "linear" {
			// These providers store no repository on an item: the nil
			// repository is the key of their items.
			itemRepoA, itemRepoB = nil, nil
			keyRepoA, keyRepoB = uuid.Nil, uuid.Nil
		} else {
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", provider)
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", provider)
		}
		started, completed := day.Add(time.Hour), day.Add(9*time.Hour)
		item := func(id string, repo *uuid.UUID) rawOnlyItem {
			return rawOnlyItem{
				id: provider + ":" + id, repo: repo, scope: "PLAT",
				created: day.AddDate(0, 0, -3), started: &started, completed: &completed,
			}
		}
		inA, inB := item("A-1", itemRepoA), item("B-1", itemRepoB)
		sink := rawOnlySink(t, rig.conn, provider)

		// Unit 1: both items, the window is the day.
		firstClaim := rawOnlyClaim(t, provider, orgID, uuid.NewString(), day, nextDay)
		writeRawOnlyUnit(ctx, t, sink, firstClaim, []rawOnlyItem{inA, inB}, nextDay.Add(2*time.Hour))
		assertRawOnlySinkRefusesDerivedTables(ctx, t, sink, firstClaim)
		if counts := rawOnlyStoredVersions(ctx, t, rig.conn, orgID); rawOnlyTotal(counts) != 0 {
			t.Fatalf("%s: derived rows after unit 1 and before any daily run = %v, want none", provider, counts)
		}
		check(provider, "after unit 1 (both items), before the daily run", rawOnlyDayValues(ctx, t, rig.conn, orgID, provider, day), absent)
		firstArgs := rig.seedRawOnlySyncRun(t, ctx, firstClaim, nextDay.Add(119*time.Minute))
		firstRuns := rig.fanoutAndRun(t, ctx, service, orgID, firstArgs)
		touched := rawOnlyTouchedKeys(ctx, t, rig.conn, orgID)
		if !touched[rawOnlyKey(day, keyRepoA)] || !touched[rawOnlyKey(day, keyRepoB)] {
			t.Fatalf("%s: touched keys after the fan-out of unit 1 = %v, want the day for each repository of the items", provider, touched)
		}
		if _, ran := firstRuns[day.Format("2006-01-02")]; !ran {
			t.Fatalf("%s: the fan-out of unit 1 started runs for %v, want one for the day", provider, touchedRunDays(firstRuns))
		}
		check(provider, "after unit 1 and the daily run of the day", rawOnlyDayValues(ctx, t, rig.conn, orgID, provider, day), whole)

		// Unit 2: only the item of repository A, twenty days later.
		before := rawOnlyStoredVersions(ctx, t, rig.conn, orgID)
		secondClaim := rawOnlyClaim(t, provider, orgID, uuid.NewString(), laterDay.Add(5*time.Hour), laterDay.Add(6*time.Hour))
		writeRawOnlyUnit(ctx, t, sink, secondClaim, []rawOnlyItem{inA}, laterDay.Add(6*time.Hour+time.Minute))
		assertRawOnlySinkRefusesDerivedTables(ctx, t, sink, secondClaim)
		if after := rawOnlyStoredVersions(ctx, t, rig.conn, orgID); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Fatalf("%s: stored derived row versions changed with unit 2 and no daily run\n before %v\n after  %v", provider, before, after)
		}
		check(provider, "after unit 2 (only the item of repository A), before the daily run",
			rawOnlyDayValues(ctx, t, rig.conn, orgID, provider, day), whole)
		if touched := rawOnlyTouchedKeys(ctx, t, rig.conn, orgID); touched[rawOnlyKey(laterDay, keyRepoA)] {
			t.Fatalf("%s: the window day of unit 2 is touched before its fan-out: %v", provider, touched)
		}
		secondArgs := rig.seedRawOnlySyncRun(t, ctx, secondClaim, laterDay.Add(6*time.Hour))
		secondRuns := rig.fanoutAndRun(t, ctx, service, orgID, secondArgs)
		touched = rawOnlyTouchedKeys(ctx, t, rig.conn, orgID)
		if !touched[rawOnlyKey(laterDay, keyRepoA)] {
			t.Fatalf("%s: touched keys after the fan-out of unit 2 = %v, want the window day of the unit", provider, touched)
		}
		dayRun, ran := secondRuns[day.Format("2006-01-02")]
		if !ran || dayRun.fullOrg || dayRun.repos != keyRepoA.String() {
			t.Fatalf("%s: the fan-out of unit 2 started %+v (days %v), want a run of the past day for the repository of the item the unit stored (%s)",
				provider, dayRun, touchedRunDays(secondRuns), keyRepoA)
		}
		check(provider, "after unit 2 and the daily run of the touched day", rawOnlyDayValues(ctx, t, rig.conn, orgID, provider, day), whole)
	}
	rig.assertNoFamilyFailed(t)
	if len(failures) > 0 {
		t.Fatalf("the day's rows are not the whole day:\n  %s", strings.Join(failures, "\n  "))
	}
}

// The reason the fan-out marks every day of a unit window. The first sync of
// an organization is a unit with a window of 21 days. It stores:
//
//   - one OPEN item, started before the window, with no event inside it. The
//     unit used to write the state and WIP rows of every day of its window;
//     it writes none now, and no raw row has an event on a quiet day.
//   - one old DONE item, completed sixty days before the window, with no
//     later event.
//
// After the fan-out and the daily runs it started: a quiet day of the window
// that is behind the days of the full-organization runs is marked, has a run
// of its own, and holds its state row and its WIP number; and the old item
// has its team attribution row.
func TestFirstSyncMarksEveryWindowDayAndTheDailyJobFillsAQuietDayAndAnOldItem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	service := rig.service(t, rig.touched, nil)
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	since, before := target.AddDate(0, 0, -20), target.Add(12*time.Hour)
	quiet := since.AddDate(0, 0, 2)
	old := since.AddDate(0, 0, -60)
	var failures []string
	for _, provider := range rawOnlyProviders {
		orgID := uuid.NewString()
		repo := uuid.New()
		keyRepo, itemRepo := repo, &repo
		if provider == "jira" || provider == "linear" {
			keyRepo, itemRepo = uuid.Nil, nil
		} else {
			insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/api", provider)
		}
		openStarted := since.AddDate(0, 0, -8).Add(3 * time.Hour)
		oldStarted, oldCompleted := old.Add(time.Hour), old.Add(9*time.Hour)
		open := rawOnlyItem{id: provider + ":OPEN-1", repo: itemRepo, scope: "PLAT", created: since.AddDate(0, 0, -10), started: &openStarted}
		done := rawOnlyItem{id: provider + ":DONE-1", repo: itemRepo, scope: "PLAT", created: old, started: &oldStarted, completed: &oldCompleted}

		claim := rawOnlyClaim(t, provider, orgID, uuid.NewString(), since, before)
		sink := rawOnlySink(t, rig.conn, provider)
		writeRawOnlyUnit(ctx, t, sink, claim, []rawOnlyItem{open, done}, before.Add(time.Hour))
		if counts := rawOnlyStoredVersions(ctx, t, rig.conn, orgID); rawOnlyTotal(counts) != 0 {
			t.Fatalf("%s: derived rows after the unit and before any daily run = %v, want none", provider, counts)
		}
		args := rig.seedRawOnlySyncRun(t, ctx, claim, before.Add(30*time.Minute))
		runs := rig.fanoutAndRun(t, ctx, service, orgID, args)

		touched := rawOnlyTouchedKeys(ctx, t, rig.conn, orgID)
		for day := since; !day.After(target); day = day.AddDate(0, 0, 1) {
			if !touched[rawOnlyKey(day, keyRepo)] {
				t.Fatalf("%s: window day %s is not marked as touched: %v", provider, day.Format("2006-01-02"), touched)
			}
		}
		quietRun, ran := runs[quiet.Format("2006-01-02")]
		if !ran || quietRun.fullOrg || quietRun.repos != keyRepo.String() {
			t.Fatalf("%s: run of the quiet day = %+v (days %v), want a run of its own for the repository of the unit's items",
				provider, quietRun, touchedRunDays(runs))
		}

		var stateRows uint64
		var stateHours float64
		if err := rig.conn.QueryRow(ctx, `
SELECT toUInt64(count()), sum(hours) FROM (SELECT argMax(duration_hours, computed_at) AS hours
FROM work_item_state_durations_daily
WHERE org_id = ? AND provider = ? AND day = toDate(?) AND status = 'in_progress' GROUP BY work_scope_id, team_id, status)`,
			orgID, provider, quiet.Format("2006-01-02")).Scan(&stateRows, &stateHours); err != nil {
			t.Fatal(err)
		}
		var wip uint64
		if err := rig.conn.QueryRow(ctx, `
SELECT toUInt64(sum(v)) FROM (SELECT argMax(wip_count_end_of_day, computed_at) AS v FROM work_item_metrics_daily
WHERE org_id = ? AND provider = ? AND day = toDate(?) GROUP BY work_scope_id, team_id)`,
			orgID, provider, quiet.Format("2006-01-02")).Scan(&wip); err != nil {
			t.Fatal(err)
		}
		var attributions uint64
		var attributionSources string
		if err := rig.conn.QueryRow(ctx, `
SELECT toUInt64(count()), arrayStringConcat(arraySort(groupUniqArray(toString(source))), ',')
FROM work_item_team_attributions WHERE org_id = ? AND work_item_id = ?`, orgID, done.id).Scan(&attributions, &attributionSources); err != nil {
			t.Fatal(err)
		}
		t.Logf("%-6s quiet day %s: in_progress state rows=%d hours=%.1f wip_end_of_day=%d; old done item (day %s): attribution rows=%d sources=[%s]",
			provider, quiet.Format("2006-01-02"), stateRows, stateHours, wip, old.Format("2006-01-02"), attributions, attributionSources)
		if stateRows != 1 || stateHours != 24 {
			failures = append(failures, fmt.Sprintf("%s: the quiet day has %d in_progress state row(s) of %.1f hours, want one row of 24 hours", provider, stateRows, stateHours))
		}
		if wip != 1 {
			failures = append(failures, fmt.Sprintf("%s: WIP at the end of the quiet day = %d, want 1 (the open item)", provider, wip))
		}
		if attributions == 0 {
			failures = append(failures, fmt.Sprintf("%s: the old done item has no work_item_team_attributions row after the run of its day", provider))
		}
	}
	rig.assertNoFamilyFailed(t)
	if len(failures) > 0 {
		t.Fatalf("after a first sync and the runs its fan-out started:\n  %s", strings.Join(failures, "\n  "))
	}
}

// A worker whose issue-type and investment engines do not load (the two config
// artifacts are not readable) serves neither of the two families. The sync
// unit used to write their three tables; it writes none now. So for a worker
// in that state the three tables get no row at all, from the sync or from the
// daily job: a gap that is loud, not a number from a part of the items.
//
// For github, gitlab, jira and linear, after a unit and the daily runs its
// fan-out started: the three tables hold no row, the other daily work-item
// tables hold the whole day, and no served family failed. The refusal is one
// ERROR line for each family and one count of the refused outcome for each
// family.
func TestARefusedEngineFamilyLeavesItsTablesEmptyAfterASyncAndIsLoggedAndCounted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	t.Chdir("../..")
	// The same call buildDailyWorker makes, on a configuration with no engine
	// paths.
	engines := dailyWorkItemEnginesFrom(config.Config{})
	if engines.err == nil {
		t.Fatal("the engines loaded from a configuration with no engine paths")
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, nil))
	base, refusals := newNilPartitionRigWithEngines(t, ctx, engines, collector, logger)
	if failFast := reportDailyFamilyRefusals(logger, refusals); len(failFast) != 0 {
		t.Fatalf("refusals that fail the worker: %v, want none", failFast)
	}
	engineFamilies := []string{daily.WorkItemIssueTypeFamilyName, daily.WorkItemInvestmentFamilyName}
	servedFamilies := []string{"work_item", "work_item_attribution", "work_item_estimate", "work_item_state"}
	for _, family := range engineFamilies {
		if _, registered := base.registers[family]; registered {
			t.Fatalf("family %q is registered with no engine", family)
		}
	}
	for _, family := range servedFamilies {
		if _, registered := base.registers[family]; !registered {
			t.Fatalf("family %q is not registered: a missing engine took it down", family)
		}
	}
	rig := newTouchedRigOn(t, ctx, base)
	service := rig.service(t, rig.touched, nil)

	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	nextDay := day.AddDate(0, 0, 1)
	engineTables := []string{"issue_type_metrics_daily", "investment_classifications_daily", "investment_metrics_daily"}
	var failures []string
	for _, provider := range rawOnlyProviders {
		orgID := uuid.NewString()
		repo := uuid.New()
		itemRepo := &repo
		if provider == "jira" || provider == "linear" {
			itemRepo = nil
		} else {
			insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/api", provider)
		}
		started, completed := day.Add(time.Hour), day.Add(9*time.Hour)
		items := []rawOnlyItem{
			{id: provider + ":A-1", repo: itemRepo, scope: "PLAT", created: day.AddDate(0, 0, -3), started: &started, completed: &completed},
			{id: provider + ":A-2", repo: itemRepo, scope: "PLAT", created: day.AddDate(0, 0, -3), started: &started, completed: &completed},
		}
		claim := rawOnlyClaim(t, provider, orgID, uuid.NewString(), day, nextDay)
		sink := rawOnlySink(t, rig.conn, provider)
		writeRawOnlyUnit(ctx, t, sink, claim, items, nextDay.Add(2*time.Hour))
		assertRawOnlySinkRefusesDerivedTables(ctx, t, sink, claim)
		rig.fanoutAndRun(t, ctx, service, orgID, rig.seedRawOnlySyncRun(t, ctx, claim, nextDay.Add(119*time.Minute)))

		counts := rawOnlyStoredVersions(ctx, t, rig.conn, orgID)
		values := rawOnlyDayValues(ctx, t, rig.conn, orgID, provider, day)
		t.Logf("%-6s engine tables: issue_type=%d investment_classifications=%d investment_metrics=%d; served: %s",
			provider, counts["issue_type_metrics_daily"], counts["investment_classifications_daily"],
			counts["investment_metrics_daily"], rawOnlyValuesLine(values))
		for _, table := range engineTables {
			if counts[table] != 0 {
				failures = append(failures, fmt.Sprintf("%s: %s holds %d row(s) with its family refused", provider, table, counts[table]))
			}
		}
		for _, table := range []string{
			"work_item_metrics_daily", "work_item_user_metrics_daily",
			"work_item_state_durations_daily", "work_item_cycle_times",
		} {
			if values[table] != 2 {
				failures = append(failures, fmt.Sprintf("%s: %s = %d with the engine families refused, want the whole day (2)", provider, table, values[table]))
			}
		}
	}
	rig.assertNoFamilyFailed(t)

	exposition := collector.PrometheusText()
	refusedSample := func(family string, count int) string {
		return fmt.Sprintf(`worker_daily_metrics_native_family_outcome_total{family=%q,outcome="refused"} %d`, family, count)
	}
	logged := map[string]int{}
	scanner := bufio.NewScanner(&logOutput)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var line struct {
			Level  string `json:"level"`
			Msg    string `json:"msg"`
			Family string `json:"family"`
			Error  string `json:"error"`
			Remedy string `json:"remedy"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		if line.Msg == dailyFamilyScopedRefusalLogMessage && line.Level == "ERROR" && line.Error != "" && line.Remedy != "" {
			logged[line.Family]++
		}
	}
	for _, family := range engineFamilies {
		if want := refusedSample(family, 1); !strings.Contains(exposition, want) {
			failures = append(failures, fmt.Sprintf("family %q: want the sample %s; its samples:\n%s", family, want, familySamples(exposition, family)))
		}
		if logged[family] != 1 {
			failures = append(failures, fmt.Sprintf("family %q: %d ERROR line(s) of the scoped refusal with the cause and the remedy, want 1", family, logged[family]))
		}
	}
	for _, family := range servedFamilies {
		if want := refusedSample(family, 0); !strings.Contains(exposition, want) {
			failures = append(failures, fmt.Sprintf("served family %q: want the sample %s; its samples:\n%s", family, want, familySamples(exposition, family)))
		}
	}
	if len(failures) > 0 {
		t.Fatalf("a worker with the two engine families refused:\n  %s", strings.Join(failures, "\n  "))
	}
}
