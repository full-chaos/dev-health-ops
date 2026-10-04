//go:build integration

package providersync

// CHAOS-8493 on a real ClickHouse, at the migration head (this file authors
// no DDL: newWorkItemEffectsConn applies the production migration chain).
//
// work_item_state_durations_daily has TWO writers and `status` is part of its
// row key: the sync-time deriver (this package) and the work_item_state daily
// family (internal/jobs/metrics/daily). The rule that says which hours are
// "blocked" is one function, but each writer has its OWN read of the facts
// the rule needs. A read that one writer does differently makes the two write
// different rows for one item and one day, and no unit test of the rule can
// see that. The first test here runs both writers, each through its real
// read, on one set of stored facts and compares their rows.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemblockers"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

// stateDurationFields is the compared field set: every field of the row type
// the sync-time deriver writes. It is derived from the TYPE, so a column
// added to the row is compared with no edit here.
func stateDurationFields(t *testing.T) []string {
	t.Helper()
	fields := oraclecompare.EncodedFieldNames(reflect.TypeOf(githubWorkItemStateDurationDailyRow{}))
	if len(fields) < 10 {
		t.Fatalf("the state duration row type gives %d fields: %v", len(fields), fields)
	}
	return fields
}

// stateDurationExclusions names each field that is NOT compared, with its
// reason. Every other field of the row type is.
var stateDurationExclusions = map[string]string{
	"computed_at": "each writer stamps the time of its own run",
}

// readStateDurations reads the stored rows of one organization and day, each
// as field -> the column's own text. Both writers' rows are read from the
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
WHERE org_id = ? AND day = toDate(?)
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

// diffStateDurations compares the rows of the sync-time deriver with the rows
// of the daily family, field by field (oraclecompare.DiffRows). In its
// messages "python" is the sync-time deriver and "go" is the daily family.
func diffStateDurations(syncTime, dailyFamily map[string]map[string]any) []string {
	keys := map[string]struct{}{}
	for key := range syncTime {
		keys[key] = struct{}{}
	}
	for key := range dailyFamily {
		keys[key] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	var messages []string
	for _, key := range sorted {
		left, inSync := syncTime[key]
		right, inDaily := dailyFamily[key]
		switch {
		case !inSync:
			messages = append(messages, fmt.Sprintf("row %s: the daily family wrote it, the sync-time deriver did not", key))
		case !inDaily:
			messages = append(messages, fmt.Sprintf("row %s: the sync-time deriver wrote it, the daily family did not", key))
		default:
			messages = append(messages, oraclecompare.DiffRows(key, left, right, nil, stateDurationExclusions)...)
		}
	}
	return messages
}

// plantedReadConn is the real connection with ONE statement changed: a
// planted defect in one read of one writer.
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

// TestBothWritersOfStateDurationsWriteTheSameBlockedRows seeds one
// organization and one day, runs BOTH writers of
// work_item_state_durations_daily through their real reads, and compares
// their stored rows field by field.
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
// Both writers' rows are then compared. Last, a defect is planted in ONE read
// of the sync-time deriver (the stored relations read a wrong time) and the
// same comparison must report it.
func TestBothWritersOfStateDurationsWriteTheSameBlockedRows(t *testing.T) {
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
	// syncTimeRows runs the sync-time deriver over the unit, with its stored
	// facts read through queries, and returns what it derived by destination.
	// The deriver is built by its production constructor
	// (NewLinearWorkItemDeriver, as the worker builds it), with the real
	// status mapping and investment configuration: its engine derives the
	// destinations the deriver requires of every run.
	syncTimeRows := func(queries driver.Conn) map[string][]json.RawMessage {
		t.Helper()
		deriver, err := NewLinearWorkItemDeriver(
			queries, lease, resolveStatusMappingConfig(t, "real"), investmentConfigPath(t, "real"),
		)
		if err != nil {
			t.Fatalf("sync-time deriver: %v", err)
		}
		derived, _, err := deriver.Derive(ctx, claim, unit, normalizedAt)
		if err != nil {
			t.Fatalf("sync-time deriver: %v", err)
		}
		if len(derived[githubStateDurationsDestination]) == 0 {
			t.Fatal("the sync-time deriver derived no state duration row")
		}
		return derived
	}
	storeDerived := func(derived map[string][]json.RawMessage, destination string) {
		t.Helper()
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, derived[destination])
		if err != nil {
			t.Fatal(err)
		}
		if err := derivedSink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write %s: %v", destination, err)
		}
	}
	truncate := func() {
		t.Helper()
		if err := conn.Exec(ctx, "TRUNCATE TABLE work_item_state_durations_daily"); err != nil {
			t.Fatal(err)
		}
	}

	// --- WRITER 1: the sync-time deriver --------------------------------
	// It derives BEFORE the unit's rows are stored, as in production: the
	// unit's own rows reach the rule from memory, the rest from the store.
	derived := syncTimeRows(conn)
	writeItems(unit.WorkItems)
	writeTransitions(unit.StatusTransitions)
	writeRelations(unit.Dependencies)
	if len(derived[githubTeamAttributionsDestination]) == 0 {
		t.Fatal("the sync-time deriver derived no team attribution row; the daily family reads them")
	}
	storeDerived(derived, githubTeamAttributionsDestination)
	storeDerived(derived, githubStateDurationsDestination)
	syncTime := readStateDurations(ctx, t, conn, org, day)

	// The state the rule exists to reach, as stored by the sync-time deriver.
	hours := map[string][2]string{}
	for _, row := range syncTime {
		text := func(field string) string { return row[field].(map[string]any)["v"].(string) }
		if text("provider") != "linear" {
			t.Fatalf("a stored row of provider %q", text("provider"))
		}
		hours[text("status")] = [2]string{text("duration_hours"), text("items_touched")}
	}
	for status, want := range map[string][2]string{
		"blocked":     {"79", "5"}, // 24 + 12 + 9 + 10 + 24
		"in_progress": {"45", "3"}, // 12 + 15 + 18
		"todo":        {"6", "1"},  // OPS-5
	} {
		if hours[status] != want {
			t.Fatalf("sync-time deriver, status %s: hours and items = %v, want %v (all: %v)", status, hours[status], want, hours)
		}
	}

	// --- WRITER 2: the daily family -------------------------------------
	truncate()
	executor, err := daily.NewWorkItemStateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	// Linear items have no repository: their rows are stored under the nil
	// repository id, which is the partition the daily family computes them in.
	written, err := executor.ComputeFamily(ctx, daily.Run{OrganizationID: org, TargetDay: day}, daily.Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []daily.RepositoryID{"00000000-0000-0000-0000-000000000000"},
	})
	if err != nil {
		t.Fatalf("daily family: %v", err)
	}
	if written == 0 {
		t.Fatal("the daily family wrote no row")
	}
	dailyFamily := readStateDurations(ctx, t, conn, org, day)

	// --- THE COMPARISON --------------------------------------------------
	if len(syncTime) < 3 || len(dailyFamily) < 3 {
		t.Fatalf("too few rows to compare: %d from the sync-time deriver, %d from the daily family", len(syncTime), len(dailyFamily))
	}
	if messages := diffStateDurations(syncTime, dailyFamily); len(messages) != 0 {
		t.Fatalf("the two writers of work_item_state_durations_daily wrote different rows (python = the sync-time deriver, go = the daily family):\n  %s",
			strings.Join(messages, "\n  "))
	}
	t.Logf("compared %d rows of each writer on %d fields (not compared: %v)", len(syncTime), len(stateDurationFields(t))-len(stateDurationExclusions), stateDurationExclusions)

	// --- THE PLANT: the same comparison must see a wrong read ------------
	// The sync-time deriver reads its stored relations one day too old. The
	// relations the unit reports again are not changed by that (the unit's
	// own row is newer); the two it does not report are: OPS-3's and OPS-6's
	// relation end before the day, so their 9 and 24 blocked hours go.
	const column = "relationship_semantics_version, last_synced, relation_started_at"
	rewrites := 0
	planted := plantedReadConn{Conn: conn, rewrite: func(query string) string {
		if !strings.Contains(query, column) {
			return query
		}
		rewrites++
		return strings.Replace(query, column, "relationship_semantics_version, last_synced - INTERVAL 1 DAY, relation_started_at", 1)
	}}
	plantedDerived := syncTimeRows(planted)
	if rewrites == 0 {
		t.Fatal("the plant changed no statement: the relation read no longer has the text it replaces")
	}
	truncate()
	storeDerived(plantedDerived, githubStateDurationsDestination)
	messages := diffStateDurations(readStateDurations(ctx, t, conn, org, day), dailyFamily)
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
		t.Fatalf("a sync-time deriver that reads its relations a day too old is not reported on the blocked hours; the comparison said: %q", messages)
	}
}

// TestBlockingReadsOfALargeOrganizationStayUnderTheStatementLimit is the
// size proof of both reads on a real server.
//
// The organization has 101,000 blocking relations and 201,000 relation ends.
// The daily family's read sent the ends as one list; clickhouse-go writes a
// list into the statement text, and the server refuses a text above
// max_query_size with code 62. That refusal is shown here first, on this
// server, with the list the old read would send.
func TestBlockingReadsOfALargeOrganizationStayUnderTheStatementLimit(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const (
		org       = "org-large"
		relations = 100000
		keyed     = 1000 // text relations to a linear issue key
		synced    = "toDateTime64('2026-08-20 00:00:00', 3)"
	)
	for _, statement := range []string{
		`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version)
SELECT concat('gh:big/api#', toString(number)), concat('gh:big/web#', toString(number)), 'blocks', concat('blocks #', toString(number)), ` + synced + `, '` + org + `', 'canonical-blocks.v2'
FROM numbers(` + fmt.Sprint(relations) + `)`,
		`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version)
SELECT concat('gh:big/api#', toString(number)), concat('extkey:BIG-', toString(number)), 'blocked_by', 'external_issue_key', ` + synced + `, '` + org + `', 'canonical-blocks.v2'
FROM numbers(` + fmt.Sprint(keyed) + `)`,
		`INSERT INTO work_items (repo_id, work_item_id, provider, status, created_at, org_id, last_synced)
SELECT toUUID('00000000-0000-4000-8000-0000000000a1'), concat('gh:big/', kind, '#', toString(number)), 'github', 'in_progress', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'), '` + org + `', toDateTime64('2026-08-20 00:00:00', 3, 'UTC')
FROM numbers(` + fmt.Sprint(relations) + `) ARRAY JOIN ['api', 'web'] AS kind`,
		`INSERT INTO work_items (repo_id, work_item_id, provider, status, created_at, org_id, last_synced)
SELECT toUUID('00000000-0000-0000-0000-000000000000'), concat('linear:BIG-', toString(number)), 'linear', 'in_progress', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'), '` + org + `', toDateTime64('2026-08-20 00:00:00', 3, 'UTC')
FROM numbers(` + fmt.Sprint(keyed) + `)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	// THE OLD READ, on this server: every relation end as one list.
	everyEnd := make([]string, 0, 2*relations)
	for index := 0; index < relations; index++ {
		everyEnd = append(everyEnd, fmt.Sprintf("gh:big/api#%d", index), fmt.Sprintf("gh:big/web#%d", index))
	}
	var refused *clickhouse.Exception
	rows, err := conn.Query(ctx, `SELECT count() FROM work_items FINAL WHERE org_id = ? AND work_item_id IN ?`, org, everyEnd)
	if err == nil {
		read := 0
		for rows.Next() {
			read++
		}
		err = rows.Err()
		rows.Close()
		if err == nil {
			t.Fatalf("the list of every relation end in one statement was accepted (%d rows read)", read)
		}
	}
	if !errors.As(err, &refused) || refused.Code != 62 {
		t.Fatalf("the list of every relation end in one statement: %v, want the server's code 62 (max query size)", err)
	}

	// THE ORGANIZATION-WIDE READ (the daily family).
	stored, err := workitemblockers.LoadRelations(ctx, conn, org)
	if err != nil {
		t.Fatalf("organization-wide relations: %v", err)
	}
	ends, err := workitemblockers.LoadEnds(ctx, conn, org)
	if err != nil {
		t.Fatalf("organization-wide ends: %v", err)
	}
	if len(stored) != relations+keyed || len(ends) != 2*relations+keyed {
		t.Fatalf("organization-wide: %d relations and %d ends, want %d and %d", len(stored), len(ends), relations+keyed, 2*relations+keyed)
	}
	for _, relation := range stored {
		if relation.FirstSeenAt == nil {
			t.Fatalf("relation %s -> %s has no first-seen time; the migration's view fills it", relation.SourceID, relation.TargetID)
		}
	}
	blocked, ended, err := workitemblockers.LoadBlockedIntervals(ctx, conn, org)
	if err != nil {
		t.Fatalf("organization-wide blocked intervals: %v", err)
	}
	// Every relation is one its writer wrote at its latest sync: none ended.
	if ended.Relations != relations+keyed || len(ended.Ended) != 0 || ended.GitHubBoardCandidates != 0 {
		t.Fatalf("stats = %+v, want %d relations and none ended", ended, relations+keyed)
	}
	// Each web item is blocked by its api item; each of the first 1,000 api
	// items is blocked by the linear issue its text names.
	if len(blocked) != relations+keyed {
		t.Fatalf("%d blocked items, want %d", len(blocked), relations+keyed)
	}

	// THE KEYED READ (the sync-time deriver), for a unit of 20,000 items:
	// their ids are above the server's limit as one list, and go in chunks.
	naming := make([]string, 0, 20000)
	for index := 0; index < 10000; index++ {
		naming = append(naming, fmt.Sprintf("gh:big/api#%d", index), fmt.Sprintf("gh:big/web#%d", index))
	}
	sort.Strings(naming)
	listBytes := 2
	for _, id := range naming {
		listBytes += len(id) + 4
	}
	if listBytes <= 262144 {
		t.Fatalf("the unit's ids are %d bytes as one list: the case does not reach the server's limit", listBytes)
	}
	claim := nativeTestClaim("github", "work-items")
	claim.OrgID = org
	source := githubWorkItemClickHouseDerivationContextSource{Conn: conn, Lease: githubDerivedIntegrationLease()}
	keyedRelations, keyedEnds, err := source.LoadStoredBlockingFacts(ctx, claim, naming, nil)
	if err != nil {
		t.Fatalf("keyed read: %v", err)
	}

	// It reads the same rows as the organization-wide read does for those items.
	named := make(map[string]bool, len(naming))
	for _, id := range naming {
		named[id] = true
	}
	var wantRelations []workitemmetrics.BlockingRelation
	wantEndIDs := map[string]bool{}
	for _, relation := range stored {
		if !named[relation.SourceID] && !named[relation.TargetID] {
			continue
		}
		wantRelations = append(wantRelations, relation)
		for _, end := range []string{relation.SourceID, relation.TargetID} {
			if key, external := workitemmetrics.ExternalKey(end); external {
				end = "linear:" + key
			}
			wantEndIDs[end] = true
		}
	}
	var wantEnds []workitemmetrics.RelationEnd
	for _, end := range ends {
		if wantEndIDs[end.WorkItemID] {
			wantEnds = append(wantEnds, end)
		}
	}
	if len(wantRelations) != 10000+keyed || len(wantEnds) != 20000+keyed {
		t.Fatalf("the organization-wide read gives %d relations and %d ends for the unit, want %d and %d", len(wantRelations), len(wantEnds), 10000+keyed, 20000+keyed)
	}
	if !reflect.DeepEqual(keyedRelations, wantRelations) {
		t.Fatalf("the keyed read gives %d relations, the organization-wide read %d for the same items, and they differ", len(keyedRelations), len(wantRelations))
	}
	if !reflect.DeepEqual(keyedEnds, wantEnds) {
		t.Fatalf("the keyed read gives %d ends, the organization-wide read %d for the same items, and they differ", len(keyedEnds), len(wantEnds))
	}
	// And so the one rule gives both writers the same intervals for the unit.
	if got, want := workitemmetrics.BlockedIntervalsByItem(keyedRelations, keyedEnds), 10000+keyed; len(got) != want {
		t.Fatalf("the keyed read blocks %d items, want %d", len(got), want)
	}
	for id, intervals := range workitemmetrics.BlockedIntervalsByItem(keyedRelations, keyedEnds) {
		if !reflect.DeepEqual(intervals, blocked[id]) {
			t.Fatalf("item %s: the keyed read gives %v, the organization-wide read %v", id, intervals, blocked[id])
		}
	}
}
