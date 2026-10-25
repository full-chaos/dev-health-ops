//go:build integration

package providersync

// CHAOS-8493 on a real ClickHouse, at the migration head (this file authors
// no DDL: newWorkItemEffectsConn applies the production migration chain).
//
// work_item_state_durations_daily has ONE writer and `status` is part of its
// row key: the work_item_state daily family (internal/jobs/metrics/daily).
// The work-items sync unit (this package) stores raw rows only and its sink
// refuses the table. The rule that says which hours are "blocked" is one
// function, and the daily family has its own read of the facts the rule
// needs; no unit test of the rule can see a wrong read. The first test here
// stores a unit's raw rows through the sync adapters, shows that the day has
// no row, then runs the daily family through its real read and holds its
// stored rows to the expected hours.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

// stateDurationFields is the compared field set: every column the daily family writes.
func stateDurationFields(t *testing.T) []string {
	t.Helper()
	return []string{
		"day", "provider", "work_scope_id", "team_id", "team_name", "status",
		"duration_hours", "items_touched", "computed_at", "avg_wip", "org_id",
	}
}

// stateDurationExclusions names each field that is NOT compared, with its
// reason. Every other field of the row type is.
var stateDurationExclusions = map[string]string{
	"computed_at": "each run stamps its own time",
}

// readStateDurations reads the stored rows of one organization and day, each
// as field -> the column's own text. The rows of every run are read from the
// same table by this one statement, so equal text is an equal stored value.
// The rows are keyed by the table's row key after the day.
func readStateDurations(ctx context.Context, t *testing.T, conn driver.Conn, org string, day time.Time) map[string]map[string]any {
	t.Helper()
	fields := stateDurationFields(t)
	selected := make([]string, 0, len(fields))
	for _, field := range fields {
		selected = append(selected, "toString(`"+field+"`)")
	}
	rows, err := conn.Query(ctx, `
SELECT `+strings.Join(selected, ", ")+`
FROM work_item_state_durations_daily
WHERE org_id = ? AND day = toDate(?) AND row_view IN ('', 'org')
ORDER BY provider, work_scope_id, team_id, status`, org, day.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]map[string]any{}
	for rows.Next() {
		values := make([]string, len(fields))
		targets := make([]any, len(fields))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		row := map[string]any{}
		for index, field := range fields {
			row[field] = map[string]any{"t": "str", "v": values[index]}
		}
		text := func(field string) string { return row[field].(map[string]any)["v"].(string) }
		key := strings.Join([]string{text("provider"), text("work_scope_id"), text("team_id"), text("status")}, "|")
		if _, twice := result[key]; twice {
			t.Fatalf("two stored rows for the key %s", key)
		}
		result[key] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

// diffStateDurations compares the rows of two runs of the daily family,
// field by field (oraclecompare.DiffRows). In its messages "python" is the
// expected run and "go" is the run under test.
func diffStateDurations(want, got map[string]map[string]any) []string {
	keys := map[string]struct{}{}
	for key := range want {
		keys[key] = struct{}{}
	}
	for key := range got {
		keys[key] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	var messages []string
	for _, key := range sorted {
		left, inWant := want[key]
		right, inGot := got[key]
		switch {
		case !inWant:
			messages = append(messages, fmt.Sprintf("row %s: the run under test wrote it, the expected run did not", key))
		case !inGot:
			messages = append(messages, fmt.Sprintf("row %s: the expected run wrote it, the run under test did not", key))
		default:
			messages = append(messages, oraclecompare.DiffRows(key, left, right, nil, stateDurationExclusions)...)
		}
	}
	return messages
}

// plantedReadConn is the real connection with ONE statement changed: a
// planted defect in one read of the daily family.
type plantedReadConn struct {
	driver.Conn
	rewrite func(string) string
}

func (conn plantedReadConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return conn.Conn.Query(ctx, conn.rewrite(query), args...)
}

func writeLinearRawRows[Row any](
	ctx context.Context, t *testing.T, claim Claim, destination string, rows []Row,
	write func(context.Context, LinearWorkItemEffectIdentity, EffectBatch) error,
) {
	t.Helper()
	encoded := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, raw)
	}
	effect, err := BuildEffectBatch(destination, EffectReadbackRequired, encoded)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newLinearWorkItemEffectIdentity(claim, effect)
	if err != nil {
		t.Fatal(err)
	}
	if err := write(ctx, identity, effect); err != nil {
		t.Fatalf("write %s: %v", destination, err)
	}
}

// TestDailyFamilyIsTheOneWriterOfBlockedStateDurations seeds one organization
// and one day, stores a sync unit's raw rows, and holds
// work_item_state_durations_daily to its one writer: no row after the unit,
// the expected rows after the daily family.
//
// The day is 2026-08-24. The sync unit is linear and holds six items, each
// created at 00:00 and todo -> in_progress at 06:00 (with no blocker: todo 6h
// + in_progress 18h). It runs at 02:00 on the next day.
//
//	OPS-1  OPEN BLOCKER. OPS-11 blocks it; OPS-11 is open. Blocked 24h.
//	OPS-2  CLOSED BLOCKER. OPS-12 blocks it; OPS-12 was completed at 12:00.
//	       Blocked 00:00-12:00 (12h), in_progress 12h.
//	OPS-3  REMOVED RELATION. OPS-13 blocked it; the link was last written at
//	       09:00, and neither item reported it at its later sync. Blocked
//	       00:00-09:00 (9h), in_progress 15h.
//	OPS-4  TERMINAL ITEM. OPS-11 blocks it, and OPS-4 itself was completed at
//	       10:00. Blocked 00:00-10:00 (10h); its done hours stay done.
//	OPS-5  UNKNOWN BLOCKER. OPS-99 blocks it; no stored item is OPS-99. Not
//	       blocked: todo 6h, in_progress 18h.
//	OPS-6  NAMED BY ITS KEY. A github issue's text says "blocks OPS-6"; the
//	       stored relation names it as extkey:OPS-6, not by its id. Blocked
//	       24h.
//
// The sync sink refuses a state duration effect and the table has no row of
// the day after the unit. The daily families (team attribution, then state
// durations) then run for the day through their real reads. Last, a defect is
// planted in ONE read of the daily family (the stored relations read a wrong
// time) and the comparison with the expected rows must report it.
func TestDailyFamilyIsTheOneWriterOfBlockedStateDurations(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := githubDerivedIntegrationLease()

	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	nextDay := day.AddDate(0, 0, 1)
	earlier := day.AddDate(0, 0, -1)           // an earlier sync: the relations are first seen here
	lastWritten := day.Add(9 * time.Hour)      // the last sync that wrote the removed relation
	evening := day.Add(20 * time.Hour)         // a later sync of two blockers
	normalizedAt := nextDay.Add(2 * time.Hour) // the sync of the unit under test

	claim := nativeTestClaim("linear", "work-items")
	// The linear deriver needs a tenant id that is a UUID (its AI attribution
	// rows are keyed by one).
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	claim.SinceAt, claim.BeforeAt = &day, &nextDay
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
	org := claim.OrgID

	linearItem := func(key, status string, createdAt time.Time, completedAt *time.Time, synced time.Time) linearWorkItemRow {
		return linearWorkItemRow{
			WorkItemID: "linear:" + key, Provider: "linear", Title: key, Type: "task",
			Status: status, StatusRaw: stringPtr(status), NativeTeamKey: stringPtr("OPS"),
			ProjectID: stringPtr("project-platform"), ProjectName: stringPtr("Platform"),
			Assignees: []string{}, Labels: []string{}, CreatedAt: createdAt, UpdatedAt: synced,
			CompletedAt: completedAt, OrgID: org, LastSynced: synced,
		}
	}
	relation := func(blocker, blocked, raw string, synced time.Time) linearWorkItemDependencyRow {
		return linearWorkItemDependencyRow{
			SourceWorkItemID: blocker, TargetWorkItemID: blocked, RelationshipType: "blocks",
			RelationshipTypeRaw: raw, RelationshipSemanticsVersion: workitemmetrics.CanonicalBlocksSemantics,
			LastSynced: synced, OrgID: org,
		}
	}
	const native = "linear_relation:blocks"
	writeItems := func(rows []linearWorkItemRow) {
		t.Helper()
		writeLinearRawRows(ctx, t, claim, "work_items", rows, LinearWorkItemsClickHouseAdapter{Conn: conn}.WriteLinearWorkItemEffect)
	}
	writeTransitions := func(rows []linearWorkItemTransitionRow) {
		t.Helper()
		writeLinearRawRows(ctx, t, claim, "work_item_transitions", rows, LinearWorkItemTransitionsClickHouseAdapter{Conn: conn}.WriteLinearWorkItemEffect)
	}
	writeRelations := func(rows []linearWorkItemDependencyRow) {
		t.Helper()
		adapter := LinearWorkItemDependenciesClickHouseAdapter{Delegate: GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn}}
		writeLinearRawRows(ctx, t, claim, "work_item_dependencies", rows, adapter.WriteLinearWorkItemEffect)
	}

	// --- what EARLIER syncs stored -------------------------------------
	longAgo := day.AddDate(0, 0, -20)
	noon := day.Add(12 * time.Hour)
	writeItems([]linearWorkItemRow{linearItem("OPS-11", "in_progress", longAgo, nil, earlier)})
	writeItems([]linearWorkItemRow{
		linearItem("OPS-12", "done", longAgo, &noon, evening),
		linearItem("OPS-13", "in_progress", longAgo, nil, evening),
	})
	writeRelations([]linearWorkItemDependencyRow{
		relation("linear:OPS-11", "linear:OPS-1", native, earlier),
		relation("linear:OPS-12", "linear:OPS-2", native, earlier),
		relation("linear:OPS-13", "linear:OPS-3", native, earlier),
		relation("linear:OPS-11", "linear:OPS-4", native, earlier),
	})
	// The removed relation was written once more at 09:00. OPS-13's sync of
	// the evening did not write it, and OPS-12's did write its own.
	writeRelations([]linearWorkItemDependencyRow{relation("linear:OPS-13", "linear:OPS-3", native, lastWritten)})
	writeRelations([]linearWorkItemDependencyRow{relation("linear:OPS-12", "linear:OPS-2", native, evening)})
	// The github issue whose text names OPS-6 by its key, and its relation.
	githubItem := workItemTestRow(org, earlier)
	githubItem.WorkItemID, githubItem.Status = "gh:acme/api#7", "in_progress"
	githubItem.CreatedAt, githubItem.UpdatedAt = longAgo, earlier
	writeGitHubRow := func(destination string, row any, write func(context.Context, GitHubWorkItemEffectIdentity, EffectBatch) error) {
		t.Helper()
		identity, effect := workItemEffect(t, destination, row)
		identity.OrgID = org
		if err := write(ctx, identity, effect); err != nil {
			t.Fatalf("write github %s: %v", destination, err)
		}
	}
	writeGitHubRow("work_items", githubItem, GitHubWorkItemsClickHouseAdapter{Conn: conn}.WriteGitHubWorkItemEffect)
	writeGitHubRow("work_item_dependencies", githubWorkItemDependencyRow{
		SourceWorkItemID: "gh:acme/api#7", TargetWorkItemID: "extkey:OPS-6", RelationshipType: "blocks",
		RelationshipTypeRaw: "external_issue_key", RelationshipSemanticsVersion: workitemmetrics.CanonicalBlocksSemantics,
		LastSynced: earlier, OrgID: org,
	}, GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn}.WriteGitHubWorkItemEffect)

	// --- the unit under test: what THIS sync normalized ----------------
	tenOClock := day.Add(10 * time.Hour)
	unit := linearWorkItemRows{}
	for _, key := range []string{"OPS-1", "OPS-2", "OPS-3", "OPS-5", "OPS-6"} {
		unit.WorkItems = append(unit.WorkItems, linearItem(key, "in_progress", day, nil, normalizedAt))
	}
	unit.WorkItems = append(unit.WorkItems, linearItem("OPS-4", "done", day, &tenOClock, normalizedAt))
	for _, item := range unit.WorkItems {
		unit.StatusTransitions = append(unit.StatusTransitions, linearWorkItemTransitionRow{
			WorkItemID: item.WorkItemID, Provider: "linear", OccurredAt: day.Add(6 * time.Hour),
			FromStatusRaw: stringPtr("Todo"), ToStatusRaw: stringPtr("In Progress"),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: org, LastSynced: normalizedAt,
		})
	}
	unit.StatusTransitions = append(unit.StatusTransitions, linearWorkItemTransitionRow{
		WorkItemID: "linear:OPS-4", Provider: "linear", OccurredAt: tenOClock,
		FromStatusRaw: stringPtr("In Progress"), ToStatusRaw: stringPtr("Done"),
		FromStatus: "in_progress", ToStatus: "done", OrgID: org, LastSynced: normalizedAt,
	})
	// The links the unit's items still report. OPS-3 reports none.
	unit.Dependencies = []linearWorkItemDependencyRow{
		relation("linear:OPS-11", "linear:OPS-1", native, normalizedAt),
		relation("linear:OPS-12", "linear:OPS-2", native, normalizedAt),
		relation("linear:OPS-11", "linear:OPS-4", native, normalizedAt),
		relation("linear:OPS-99", "linear:OPS-5", native, normalizedAt),
	}

	derivedSink, err := NewLinearWorkItemDerivedClickHouseEffects(conn, lease, nil)
	if err != nil {
		t.Fatal(err)
	}
	truncate := func() {
		t.Helper()
		if err := conn.Exec(ctx, "TRUNCATE TABLE work_item_state_durations_daily"); err != nil {
			t.Fatal(err)
		}
	}

	// --- THE SYNC UNIT: raw rows only ------------------------------------
	writeItems(unit.WorkItems)
	writeTransitions(unit.StatusTransitions)
	writeRelations(unit.Dependencies)
	// A state duration effect of the unit's day, with a row: the sync sink
	// refuses it and the table stays empty for the tenant.
	refusedRaw, err := effectRowsFromValues([]map[string]any{{
		"day": day.Format("2006-01-02"), "provider": "linear", "work_scope_id": "project-platform",
		"team_id": "OPS", "team_name": "OPS", "status": "blocked", "duration_hours": 79, "items_touched": 5,
		"computed_at": normalizedAt, "avg_wip": 1, "org_id": org,
	}})
	if err != nil {
		t.Fatal(err)
	}
	refusedEffect, err := BuildEffectBatch("work_item_state_durations_daily", EffectReadbackRequired, refusedRaw)
	if err != nil {
		t.Fatal(err)
	}
	assertSyncSinkRefusesDailyJobTable(t, ctx, conn, derivedSink, claim, refusedEffect)
	if afterUnit := readStateDurations(ctx, t, conn, org, day); len(afterUnit) != 0 {
		t.Fatalf("after the sync unit the day has %d state duration rows; the daily family is the one writer", len(afterUnit))
	}
	for _, table := range []string{"work_item_state_durations_daily", "work_item_team_attributions"} {
		var stored uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE org_id = ?", org).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if stored != 0 {
			t.Fatalf("after the sync unit %s holds %d rows of the tenant", table, stored)
		}
	}

	// --- THE DAILY FAMILIES ----------------------------------------------
	// Linear items have no repository: their rows are stored under the nil
	// repository id, which is the partition the daily family computes them in.
	run := daily.Run{ID: "00000000-0000-4000-8000-0000000000c0", OrganizationID: org, TargetDay: day}
	partition := daily.Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []daily.RepositoryID{"00000000-0000-0000-0000-000000000000"},
	}
	// The team attribution rows the state family reads are written by the
	// daily job too, before it.
	attribution, err := daily.NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	attributed, err := attribution.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatalf("daily team attribution family: %v", err)
	}
	if attributed == 0 {
		t.Fatal("the daily team attribution family wrote no row; the state family reads them")
	}
	dailyRows := func(queries driver.Conn) map[string]map[string]any {
		t.Helper()
		executor, err := daily.NewWorkItemStateExecutor(queries)
		if err != nil {
			t.Fatal(err)
		}
		written, err := executor.ComputeFamily(ctx, run, partition)
		if err != nil {
			t.Fatalf("daily family: %v", err)
		}
		if written == 0 {
			t.Fatal("the daily family wrote no row")
		}
		return readStateDurations(ctx, t, conn, org, day)
	}
	dailyFamily := dailyRows(conn)

	// The state the rule exists to reach, as stored by the daily family.
	hours := map[string][2]string{}
	for _, row := range dailyFamily {
		text := func(field string) string { return row[field].(map[string]any)["v"].(string) }
		if text("provider") != "linear" {
			t.Fatalf("a stored row of provider %q", text("provider"))
		}
		if _, twice := hours[text("status")]; twice {
			t.Fatalf("two stored rows of status %s: %v", text("status"), dailyFamily)
		}
		hours[text("status")] = [2]string{text("duration_hours"), text("items_touched")}
	}
	wantHours := map[string][2]string{
		"blocked":     {"79", "5"}, // 24 + 12 + 9 + 10 + 24
		"in_progress": {"45", "3"}, // 12 + 15 + 18
		"todo":        {"6", "1"},  // OPS-5
	}
	for status, want := range wantHours {
		if hours[status] != want {
			t.Fatalf("daily family, status %s: hours and items = %v, want %v (all: %v)", status, hours[status], want, hours)
		}
	}
	if len(dailyFamily) < 3 {
		t.Fatalf("too few rows: %d from the daily family", len(dailyFamily))
	}
	t.Logf("daily family stored %d rows; hours and items by status: %v", len(dailyFamily), hours)

	// A second run of the daily family on the same stored facts stores the
	// same rows, on every field of the row type.
	truncate()
	if messages := diffStateDurations(dailyFamily, dailyRows(conn)); len(messages) != 0 {
		t.Fatalf("two runs of the daily family on the same facts stored different rows:\n  %s", strings.Join(messages, "\n  "))
	}

	// --- THE PLANT: the same comparison must see a wrong read ------------
	// The daily family reads its stored relations one day too old.
	const column = "relationship_semantics_version, last_synced, relation_started_at"
	rewrites := 0
	planted := plantedReadConn{Conn: conn, rewrite: func(query string) string {
		if !strings.Contains(query, column) {
			return query
		}
		rewrites++
		return strings.Replace(query, column, "relationship_semantics_version, last_synced - INTERVAL 1 DAY, relation_started_at", 1)
	}}
	truncate()
	plantedRows := dailyRows(planted)
	if rewrites == 0 {
		t.Fatal("the plant changed no statement: the relation read no longer has the text it replaces")
	}
	messages := diffStateDurations(dailyFamily, plantedRows)
	blockedKey := ""
	for key := range dailyFamily {
		if strings.HasSuffix(key, "|blocked") {
			blockedKey = key
		}
	}
	found := false
	for _, message := range messages {
		if strings.Contains(message, `case "`+blockedKey+`", field "duration_hours"`) {
			found = true
		}
	}
	if blockedKey == "" || !found {
		t.Fatalf("a daily family that reads its relations a day too old is not reported on the blocked hours; the comparison said: %q", messages)
	}
}
