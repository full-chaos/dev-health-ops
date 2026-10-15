//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemblockers"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// CHAOS-9706 real-store tests at the migration head. The fixture holds one row
// of each kind the drain must tell apart; every assertion names the row.

const (
	drainOrg      = "00000000-0000-4000-8000-0000000000d1"
	drainOtherOrg = "00000000-0000-4000-8000-0000000000d2"
	drainRepo     = "toUUID('00000000-0000-4000-8000-0000000000a1')"
)

var drainNow = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func drainTS(value string) string { return "toDateTime64('" + value + "', 3, 'UTC')" }

type drainFixture struct {
	t    *testing.T
	ctx  context.Context
	conn driver.Conn
}

func (f drainFixture) exec(statement string, args ...any) {
	f.t.Helper()
	if err := f.conn.Exec(f.ctx, statement, args...); err != nil {
		f.t.Fatalf("%v\n%s", err, statement)
	}
}

func (f drainFixture) item(org, id, provider, synced string) {
	f.t.Helper()
	f.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, status, created_at, org_id, last_synced) VALUES (` +
		drainRepo + `, '` + id + `', '` + provider + `', 'in_progress', ` + drainTS("2026-01-01 00:00:00") + `, '` + org + `', ` + drainTS(synced) + `)`)
}

func (f drainFixture) relation(org, source, target, relationship, raw, version, synced string) {
	f.t.Helper()
	f.exec(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version, relation_started_at) VALUES ('` +
		source + `', '` + target + `', '` + relationship + `', '` + raw + `', ` + drainTS(synced) + `, '` + org + `', '` + version + `', ` + drainTS("2026-02-01 00:00:00") + `)`)
}

func (f drainFixture) firstSeen(org, source, target, relationship string) {
	f.t.Helper()
	f.exec(`INSERT INTO work_item_dependency_first_seen (org_id, source_work_item_id, target_work_item_id, relationship_type, first_seen_at) VALUES ('` +
		org + `', '` + source + `', '` + target + `', '` + relationship + `', ` + drainTS("2026-02-01 00:00:00") + `)`)
}

// rows reads the stored keys of an organization as "source>target|type|last_synced".
func (f drainFixture) rows(org string) []string {
	f.t.Helper()
	rows, err := f.conn.Query(f.ctx, `SELECT source_work_item_id, target_work_item_id, relationship_type, toString(last_synced), toString(relation_started_at)
FROM work_item_dependencies FINAL WHERE org_id = ? ORDER BY source_work_item_id, target_work_item_id, relationship_type`, org)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var source, target, relationship, synced, started string
		if err := rows.Scan(&source, &target, &relationship, &synced, &started); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, source+">"+target+"|"+relationship+"|"+synced+"|"+started)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

func (f drainFixture) count(query string, args ...any) uint64 {
	f.t.Helper()
	var n uint64
	if err := f.conn.QueryRow(f.ctx, query, args...).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

const canonical = "canonical-blocks.v2"

// newDrainFixture stores, for drainOrg (now = 2026-10-15, retention 90 days, cutoff 2026-07-17):
//
//	old-removed     linear:OPS-1 blocks gh:a/r#1  last seen 2026-05-01, #1 synced 2026-09-01  -> drained
//	text-removed    gh:a/r#5 blocked_by extkey:OPS-7  last seen 2026-04-01, #5 synced later   -> drained (one writer)
//	young-removed   linear:OPS-1 blocks gh:a/r#2  last seen 2026-09-01, #2 synced 2026-09-20  -> kept (inside retention)
//	current         linear:OPS-1 blocks gh:a/r#3  last seen 2026-09-20, both synced then       -> kept
//	old-current     linear:OPS-2 blocks gh:a/r#6  last seen 2026-04-01, both synced 2026-03-01 -> kept (old, but the provider still reports it)
//	undecided       linear:OPS-1 blocks gh:a/missing#9 (no stored item) last seen 2026-04-01   -> kept
//	legacy          legacy semantics, old                                                      -> kept (not read)
//	other-type      relates_to, old, an item synced later                                      -> kept (not blocking)
//
// and the same old-removed key in drainOtherOrg, which must stay.
func newDrainFixture(t *testing.T) drainFixture {
	t.Helper()
	ctx, conn := newWorkItemEffectsConn(t)
	f := drainFixture{t: t, ctx: ctx, conn: conn}
	for _, org := range []string{drainOrg, drainOtherOrg} {
		f.item(org, "linear:OPS-1", "linear", "2026-09-20 00:00:00")
		f.item(org, "linear:OPS-7", "linear", "2026-09-20 00:00:00")
		f.item(org, "gh:a/r#1", "github", "2026-09-01 00:00:00")
		f.item(org, "gh:a/r#2", "github", "2026-09-20 00:00:00")
		f.item(org, "gh:a/r#3", "github", "2026-09-20 00:00:00")
		f.item(org, "gh:a/r#4", "github", "2026-09-20 00:00:00")
		f.item(org, "gh:a/r#5", "github", "2026-09-01 00:00:00")
		f.item(org, "linear:OPS-2", "linear", "2026-03-01 00:00:00")
		f.item(org, "gh:a/r#6", "github", "2026-03-01 00:00:00")
		f.relation(org, "linear:OPS-1", "gh:a/r#1", "blocks", "linear_relation:blocks", canonical, "2026-05-01 00:00:00")
		f.firstSeen(org, "linear:OPS-1", "gh:a/r#1", "blocks")
	}
	f.relation(drainOrg, "gh:a/r#5", "extkey:OPS-7", "blocked_by", "external_issue_key", canonical, "2026-04-01 00:00:00")
	f.relation(drainOrg, "linear:OPS-1", "gh:a/r#2", "blocks", "linear_relation:blocks", canonical, "2026-09-01 00:00:00")
	f.relation(drainOrg, "linear:OPS-1", "gh:a/r#3", "blocks", "linear_relation:blocks", canonical, "2026-09-20 00:00:00")
	f.relation(drainOrg, "linear:OPS-2", "gh:a/r#6", "blocks", "linear_relation:blocks", canonical, "2026-04-01 00:00:00")
	f.relation(drainOrg, "linear:OPS-1", "gh:a/missing#9", "blocks", "linear_relation:blocks", canonical, "2026-04-01 00:00:00")
	f.relation(drainOrg, "linear:OPS-1", "gh:a/r#4", "blocks", "blocks", "legacy.v1", "2026-04-01 00:00:00")
	f.relation(drainOrg, "gh:a/r#4", "gh:a/r#3", "relates_to", "relates", canonical, "2026-04-01 00:00:00")
	return f
}

func drainOptions() WorkItemDependencyDrainOptions {
	return WorkItemDependencyDrainOptions{Orgs: []string{drainOrg}, RetainDays: 90, Now: func() time.Time { return drainNow }}
}

func drainApply() WorkItemDependencyDrainOptions {
	options := drainOptions()
	options.Apply, options.WorkersPaused = true, true
	return options
}

const (
	keyOldRemoved   = "linear:OPS-1>gh:a/r#1|blocks"
	keyTextRemoved  = "gh:a/r#5>extkey:OPS-7|blocked_by"
	keyYoungRemoved = "linear:OPS-1>gh:a/r#2|blocks"
	keyCurrent      = "linear:OPS-1>gh:a/r#3|blocks"
	keyOldCurrent   = "linear:OPS-2>gh:a/r#6|blocks"
	keyUndecided    = "linear:OPS-1>gh:a/missing#9|blocks"
	keyLegacy       = "linear:OPS-1>gh:a/r#4|blocks"
	keyOtherType    = "gh:a/r#4>gh:a/r#3|relates_to"
)

func keysOf(rows []string) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row[:len(row)-len(row[indexNth(row, '|', 2):])])
	}
	sort.Strings(keys)
	return keys
}

// indexNth is the index of the nth occurrence of the byte in the text.
func indexNth(text string, b byte, n int) int {
	for index := 0; index < len(text); index++ {
		if text[index] == b {
			n--
			if n == 0 {
				return index
			}
		}
	}
	return len(text)
}

func TestDrainDryRunReportsAndWritesNothing(t *testing.T) {
	f := newDrainFixture(t)
	before := f.rows(drainOrg)
	outcome, err := DrainWorkItemDependencies(f.ctx, f.conn, drainOptions())
	if err != nil {
		t.Fatal(err)
	}
	totals := outcome.Totals
	if totals.Relations != 6 || totals.Current != 2 || totals.Undecided != 1 || totals.Removed != 3 || totals.Drainable != 2 || totals.KeptInRetention != 1 {
		t.Fatalf("counts: %+v", totals)
	}
	if totals.Deleted != 0 || outcome.RunID != "" {
		t.Fatalf("a dry run reports a write: %+v", outcome)
	}
	if after := f.rows(drainOrg); !reflect.DeepEqual(before, after) {
		t.Fatalf("a dry run changed the table:\n%v\n%v", before, after)
	}
	if n := f.count("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'work_item_dependencies_drain_bak'"); n != 0 {
		t.Fatal("a dry run created the backup table")
	}
}

func TestDrainApplyDeletesOnlyRemovedRowsOlderThanRetention(t *testing.T) {
	f := newDrainFixture(t)
	other := f.rows(drainOtherOrg)
	firstSeenBefore := f.count("SELECT count() FROM work_item_dependency_first_seen WHERE org_id = ?", drainOrg)
	outcome, err := DrainWorkItemDependencies(f.ctx, f.conn, drainApply())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Totals.Deleted != 2 || outcome.Totals.BackupRows != 2 || outcome.Totals.SurvivedTheDelete != 0 {
		t.Fatalf("outcome: %+v", outcome.Totals)
	}
	want := []string{keyOtherType, keyLegacy, keyUndecided, keyCurrent, keyOldCurrent, keyYoungRemoved}
	sort.Strings(want)
	if got := keysOf(f.rows(drainOrg)); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after the drain:\n got %v\nwant %v", got, want)
	}
	// Backward references: the row of another organization with the same key, and the first-seen row of the
	// drained key, are untouched; the survivors keep every column (start time included).
	if got := f.rows(drainOtherOrg); !reflect.DeepEqual(got, other) {
		t.Fatalf("another organization changed:\n%v\n%v", got, other)
	}
	if n := f.count("SELECT count() FROM work_item_dependency_first_seen WHERE org_id = ?", drainOrg); n != firstSeenBefore {
		t.Fatalf("first-seen rows of the organization: %d, want the %d stored before", n, firstSeenBefore)
	}
	for _, row := range f.rows(drainOrg) {
		if len(row) < 10 || row[len(row)-23:] != "2026-02-01 00:00:00.000" {
			t.Fatalf("a kept row lost its link time: %s", row)
		}
	}
	// What a reader still sees: the removed set is the one row inside retention, the current one is whole.
	relations, err := workitemblockers.LoadRelations(f.ctx, f.conn, drainOrg)
	if err != nil || len(relations) != 4 {
		t.Fatalf("blocking relations after the drain: %d, %v", len(relations), err)
	}
	second, err := DrainWorkItemDependencies(f.ctx, f.conn, drainApply())
	if err != nil || second.Totals.Drainable != 0 || second.Totals.Deleted != 0 {
		t.Fatalf("a second run is not a no-op: %+v, %v", second.Totals, err)
	}
}

func TestDrainReadersSeeTheSameIntervalsInsideRetention(t *testing.T) {
	f := newDrainFixture(t)
	cutoff := drainNow.AddDate(0, 0, -90)
	before, _, err := workitemblockers.LoadBlockedIntervals(f.ctx, f.conn, drainOrg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DrainWorkItemDependencies(f.ctx, f.conn, drainApply()); err != nil {
		t.Fatal(err)
	}
	after, _, err := workitemblockers.LoadBlockedIntervals(f.ctx, f.conn, drainOrg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(clipIntervals(before, cutoff), clipIntervals(after, cutoff)) {
		t.Fatalf("intervals inside retention changed:\n%v\n%v", clipIntervals(before, cutoff), clipIntervals(after, cutoff))
	}
	if len(before) == 0 {
		t.Fatal("the fixture gives no interval at all: the comparison proves nothing")
	}
}

func TestDrainWriteNeedsThePauseStatementAndTheRetentionFloor(t *testing.T) {
	f := newDrainFixture(t)
	options := drainOptions()
	options.Apply = true
	if _, err := DrainWorkItemDependencies(f.ctx, f.conn, options); !errors.Is(err, ErrWorkItemDependencyDrainNotPaused) {
		t.Fatalf("apply without the pause statement: %v", err)
	}
	options = drainOptions()
	options.RetainDays = WorkItemDependencyDrainMinRetainDays - 1
	if _, err := DrainWorkItemDependencies(f.ctx, f.conn, options); !errors.Is(err, ErrWorkItemDependencyDrainRetention) {
		t.Fatalf("retention below the floor: %v", err)
	}
	options = drainApply()
	options.QuietSeconds = 3600
	if _, err := DrainWorkItemDependencies(f.ctx, f.conn, options); !errors.Is(err, ErrWorkItemDependencyDrainWritesActive) {
		t.Fatalf("apply right after a write: %v", err)
	}
	if n := len(f.rows(drainOrg)); n != 8 {
		t.Fatalf("a refused run deleted rows: %d left", n)
	}
}

// A sync that writes a drained key again between the read and the delete has a newer last_synced: it survives.
func TestDrainSparesARowAResyncWroteAfterTheRead(t *testing.T) {
	f := newDrainFixture(t)
	options := drainApply()
	options.afterBackup = func(string) {
		f.relation(drainOrg, "linear:OPS-1", "gh:a/r#1", "blocks", "linear_relation:blocks", canonical, "2026-10-15 11:00:00")
	}
	outcome, err := DrainWorkItemDependencies(f.ctx, f.conn, options)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Totals.Deleted != 2 {
		t.Fatalf("outcome: %+v", outcome.Totals)
	}
	found := false
	for _, row := range f.rows(drainOrg) {
		if len(row) > len(keyOldRemoved) && row[:len(keyOldRemoved)] == keyOldRemoved {
			found = true
			if row[len(keyOldRemoved)+1:len(keyOldRemoved)+11] != "2026-10-15" {
				t.Fatalf("the re-synced row is not the new one: %s", row)
			}
		}
	}
	if !found {
		t.Fatal("the row a sync wrote after the read was deleted")
	}
}

func TestDrainRestoreBringsBackTheRowsAndLeavesNewerOnes(t *testing.T) {
	f := newDrainFixture(t)
	before := f.rows(drainOrg)
	outcome, err := DrainWorkItemDependencies(f.ctx, f.conn, drainApply())
	if err != nil {
		t.Fatal(err)
	}
	restoreOptions := WorkItemDependencyDrainOptions{Orgs: []string{drainOrg}}
	dry, err := RestoreWorkItemDependencyDrain(f.ctx, f.conn, outcome.RunID, restoreOptions)
	if err != nil || dry.Totals.Restored != 2 || len(f.rows(drainOrg)) != 6 {
		t.Fatalf("restore dry run: %+v, %v", dry.Totals, err)
	}
	// A sync wrote one drained key again with a newer stamp: restore leaves it.
	f.relation(drainOrg, "linear:OPS-1", "gh:a/r#1", "blocks", "linear_relation:blocks", canonical, "2026-10-15 11:00:00")
	restoreOptions.Apply, restoreOptions.WorkersPaused = true, true
	applied, err := RestoreWorkItemDependencyDrain(f.ctx, f.conn, outcome.RunID, restoreOptions)
	if err != nil || applied.Totals.Restored != 1 {
		t.Fatalf("restore: %+v, %v", applied.Totals, err)
	}
	got := keysOf(f.rows(drainOrg))
	if want := keysOf(before); !reflect.DeepEqual(got, want) {
		t.Fatalf("keys after restore:\n got %v\nwant %v", got, want)
	}
	if _, err := RestoreWorkItemDependencyDrain(f.ctx, f.conn, "drain-unknown", restoreOptions); !errors.Is(err, ErrWorkItemDependencyDrainBackup) {
		t.Fatalf("restore of an unknown run: %v", err)
	}
}

// Above the per-mutation batch and above the 1,000-row default result cap of the query client.
func TestDrainBeyondOneMutationBatch(t *testing.T) {
	f := newDrainFixture(t)
	const old = 4500
	f.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, status, created_at, org_id, last_synced)
SELECT ` + drainRepo + `, concat('gh:big/api#', toString(number)), 'github', 'in_progress', ` + drainTS("2026-01-01 00:00:00") + `, '` + drainOrg + `', ` + drainTS("2026-09-01 00:00:00") + `
FROM numbers(` + fmt.Sprint(old) + `)`)
	f.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, status, created_at, org_id, last_synced)
SELECT ` + drainRepo + `, concat('linear:BIG-', toString(number)), 'linear', 'in_progress', ` + drainTS("2026-01-01 00:00:00") + `, '` + drainOrg + `', ` + drainTS("2026-09-01 00:00:00") + `
FROM numbers(` + fmt.Sprint(old) + `)`)
	f.exec(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version, relation_started_at)
SELECT concat('linear:BIG-', toString(number)), concat('gh:big/api#', toString(number)), 'blocks', 'linear_relation:blocks', ` + drainTS("2026-03-01 00:00:00") + `, '` + drainOrg + `', '` + canonical + `', ` + drainTS("2026-02-01 00:00:00") + `
FROM numbers(` + fmt.Sprint(old) + `)`)
	outcome, err := DrainWorkItemDependencies(f.ctx, f.conn, drainApply())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Totals.Deleted != old+2 || outcome.Totals.SurvivedTheDelete != 0 {
		t.Fatalf("outcome: %+v", outcome.Totals)
	}
	if n := f.count("SELECT count() FROM work_item_dependencies FINAL WHERE org_id = ?", drainOrg); n != 6 {
		t.Fatalf("rows left: %d, want the 6 kept", n)
	}
}

// An older version of a key that no merge has folded yet is deleted with the newest: left behind it would show under
// FINAL as a removed row again.
func TestDrainDeletesUnmergedOlderVersionsOfADrainedKey(t *testing.T) {
	f := newDrainFixture(t)
	f.exec("SYSTEM STOP MERGES work_item_dependencies")
	f.relation(drainOrg, "linear:OPS-1", "gh:a/r#1", "blocks", "linear_relation:blocks", canonical, "2026-04-01 00:00:00")
	if n := f.count("SELECT count() FROM work_item_dependencies WHERE org_id = ? AND source_work_item_id = 'linear:OPS-1' AND target_work_item_id = 'gh:a/r#1'", drainOrg); n < 2 {
		t.Skipf("the older version was merged away at once (%d physical rows): nothing to prove", n)
	}
	f.exec("SYSTEM START MERGES work_item_dependencies")
	outcome, err := DrainWorkItemDependencies(f.ctx, f.conn, drainApply())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Totals.SurvivedTheDelete != 0 {
		t.Fatalf("outcome: %+v", outcome.Totals)
	}
	if n := f.count("SELECT count() FROM work_item_dependencies WHERE org_id = ? AND source_work_item_id = 'linear:OPS-1' AND target_work_item_id = 'gh:a/r#1'", drainOrg); n != 0 {
		t.Fatalf("%d physical versions of a drained key are left", n)
	}
}

// Two organizations in one apply: the second is not refused because of the first one's own delete.
func TestDrainAllOrgsApplyDrainsEveryOrganization(t *testing.T) {
	f := newDrainFixture(t)
	for _, org := range []string{drainOrg, drainOtherOrg} {
		f.relation(org, "linear:OPS-1", "gh:a/r#1", "blocks", "linear_relation:blocks", canonical, "2026-05-01 00:00:00")
	}
	options := drainApply()
	options.Orgs, options.AllOrgs = nil, true
	options.QuietSeconds = 60
	options.afterBackup = nil
	// The quiet window is judged by the server clock against the fixture inserts above, so wait them out.
	options.QuietSeconds = 1
	time.Sleep(2 * time.Second)
	outcome, err := DrainWorkItemDependencies(f.ctx, f.conn, options)
	if err != nil {
		t.Fatalf("%v: %+v", err, outcome.Totals)
	}
	if len(outcome.Orgs) != 2 || outcome.Orgs[0].Deleted == 0 || outcome.Orgs[1].Deleted == 0 {
		t.Fatalf("both organizations are drained: %+v", outcome.Orgs)
	}
	restore := WorkItemDependencyDrainOptions{AllOrgs: true, Apply: true, WorkersPaused: true, QuietSeconds: 1}
	time.Sleep(2 * time.Second)
	restored, err := RestoreWorkItemDependencyDrain(f.ctx, f.conn, outcome.RunID, restore)
	if err != nil || len(restored.Orgs) != 2 || restored.Orgs[1].Restored == 0 {
		t.Fatalf("restore of both organizations: %+v, %v", restored.Orgs, err)
	}
}

// The differential gate refuses before anything is written, through the production entry point.
func TestDrainRefusesWhenTheDifferentialFails(t *testing.T) {
	f := newDrainFixture(t)
	before := f.rows(drainOrg)
	options := drainApply()
	options.planned = func(orgID string, drain []workitemmetrics.BlockingRelation) []workitemmetrics.BlockingRelation {
		relations, err := workitemblockers.LoadRelations(f.ctx, f.conn, orgID)
		if err != nil {
			t.Fatal(err)
		}
		for _, relation := range relations {
			if relation.TargetID == "gh:a/r#3" {
				return append(drain, relation)
			}
		}
		t.Fatal("fixture relation missing")
		return drain
	}
	if _, err := DrainWorkItemDependencies(f.ctx, f.conn, options); !errors.Is(err, ErrWorkItemDependencyDrainDifferential) {
		t.Fatalf("a live relation in the drain list: %v", err)
	}
	if after := f.rows(drainOrg); !reflect.DeepEqual(before, after) {
		t.Fatal("rows changed although the differential refused")
	}
}

// The edge of a drained relation stays in work_graph_edges until the next workgraph run (the window the PR body
// names); that run's cleanup (the sequence the issue<->issue pre-step performs) retracts it, because no
// dependency row backs it any more.
func TestDrainedRelationEdgeIsRetractedByTheNextWorkgraphCleanup(t *testing.T) {
	f := newDrainFixture(t)
	edgeID := edges.EdgeID(edges.NodeTypeIssue, "linear:OPS-1", edges.EdgeTypeBlocks, edges.NodeTypeIssue, "gh:a/r#1")
	seeded := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	f.exec(`INSERT INTO work_graph_edges (edge_id, source_type, source_id, target_type, target_id, edge_type, provenance,
 confidence, evidence, discovered_at, last_synced, event_ts, day, org_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		edgeID, "issue", "linear:OPS-1", "issue", "gh:a/r#1", "blocks", "native", float32(0.9), "linear_relation:blocks",
		seeded, seeded, seeded, seeded, drainOrg)
	live := func() uint64 {
		return f.count("SELECT count() FROM work_graph_edges FINAL WHERE edge_id = ? AND is_deleted = 0", edgeID)
	}
	if _, err := DrainWorkItemDependencies(f.ctx, f.conn, drainApply()); err != nil {
		t.Fatal(err)
	}
	if live() != 1 {
		t.Fatal("the edge is gone before any workgraph run: the window the PR body names does not exist")
	}
	dependencies, err := edges.ReadDependencies(f.ctx, f.conn, drainOrg)
	if err != nil {
		t.Fatal(err)
	}
	derived, err := edges.DeriveIssueIssueEdges(dependencies, drainNow)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := edges.ReadExistingDependencyEdgeIDs(f.ctx, f.conn, drainOrg)
	if err != nil {
		t.Fatal(err)
	}
	plan := edges.BuildCleanupPlan(dependencies, existing)
	versions, err := edges.ReadIssueIssueEdgeVersions(f.ctx, f.conn, drainOrg)
	if err != nil {
		t.Fatal(err)
	}
	if err := edges.DeleteEdgesByID(f.ctx, f.conn, drainOrg, plan, versions, derived.Edges, drainNow); err != nil {
		t.Fatal(err)
	}
	if live() != 0 {
		t.Fatal("the next workgraph cleanup did not retract the edge of a drained relation")
	}
}
