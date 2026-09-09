//go:build integration

// CHAOS-5448's durable, CI-enrolled proof that the WORK_TYPE flow-matrix
// templates MUST read work_item_cycle_times with FINAL -- the two
// "CHAOS-4516 FIX SITE" deviations from Python that flowmatrix.go's own
// doc comments carried as UNMEASURED until this file measured them.
//
// WHY THIS FILE EXISTS. The 2026-09-07 Go/Python enablement run recorded
// flowMatrix as a parity divergence (CHAOS-5448): edges[0].value 596 vs
// 594, edges[4].value 18 vs 16, nodes[0].value 1804 vs 1803. In every one
// of those pairs the LEFT number is PYTHON and the RIGHT is GO -- the
// enablement probe called compare_responses(baseline=python,
// candidate=go) and go_api_comparator.py renders "baseline != candidate".
// So Go returned the LOWER count on all three, which is exactly what
// adding FINAL predicts, and the whole divergence reduces to ONE token:
// `FINAL` on work_item_cycle_times, present in
// flowMatrixWorkTypeEnrichedSelect / flowMatrixWorkTypeNodesTemplate and
// absent from Python's sql/templates.py:304 / :397. Every other clause in
// both templates is character-identical.
//
// WHAT THE MEASUREMENT SHOWED. work_item_cycle_times is
// ReplacingMergeTree(computed_at) ORDER BY (provider, work_item_id) --
// `day` is NOT in the dedup key (001_metrics_v2.sql:137-155). A work item
// whose superseded row still carries a `day` inside the query window is
// counted by a read without FINAL and not by a read with it. That makes
// Python's number a TRANSIENT: it depends on which parts ClickHouse has
// merged at the instant of the read, and it silently converges onto Go's
// once a background merge runs, with no code change on either side. Go's
// FINAL read is the converged steady state. TestFlowMatrixWorkType_
// PythonUnfinalReadConvergesOntoTheFinalRead below demonstrates that
// convergence directly, which is what makes "Go is correct here" a
// measured claim rather than an argument.
//
// THIS IS THE ANTI-MUTANT GUARD FOR A DELIBERATE DIVERGENCE. Deleting
// `FINAL` from either template to make the Go/Python comparator "match"
// turns both tests below RED (verified by doing exactly that before this
// file was committed -- see the PR body's mutant proof). A match produced
// by removing FINAL is not parity, it is Go adopting Python's defect.
package analytics

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	// The seeded repo every work item belongs to. The WORK_TYPE edges
	// template bridges pairs of work_item_types through (repo_id, day),
	// so one shared repo plus one shared day is what makes the seeded
	// items form edges at all.
	flowMatrixSeedRepoID = "11111111-1111-1111-1111-111111111111"

	// Window: 2026-09-01..2026-09-07 inclusive.
	flowMatrixSeedWindowStart = "2026-09-01"
	flowMatrixSeedWindowEnd   = "2026-09-07"

	// The day every LIVE row sits on -- inside the window.
	flowMatrixSeedLiveDay = "2026-09-03"

	// The day the SUPERSEDING version of wi-stale moves to -- outside the
	// window, and deliberately in the SAME 202609 partition as the live
	// day, because ReplacingMergeTree deduplicates per partition. A day in
	// a different month would never collapse and the fixture would prove
	// nothing.
	flowMatrixSeedSupersededDay = "2026-09-20"

	// queryNodes/queryEdges prefix every id with its dimension
	// (flowmatrix.go:987 and :1019: `dimension + ":" + nodeID`), so the
	// ids a caller sees are "WORK_TYPE:Bug", never a bare "Bug".
	nodeBug   = "WORK_TYPE:Bug"
	nodeStory = "WORK_TYPE:Story"

	// The REPO-dimension node id for the one seeded repo, same
	// dimension-prefixed shape (flowmatrix.go:987).
	nodeSeedRepo = "REPO:" + flowMatrixSeedRepoID
)

// seededCycleTimeRow is one work_item_cycle_times row. Only the columns
// the WORK_TYPE templates read carry meaningful values; the rest get
// valid filler.
type seededCycleTimeRow struct {
	workItemID string
	day        string
	computedAt string
	workType   string
}

// seedWorkItems writes the work_items side. Every work item shares one
// repo, so the edges template's (repo_id, day) bridge is satisfied.
func seedWorkItems(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, items map[string]string) {
	t.Helper()
	values := ""
	i := 0
	for workItemID, workType := range items {
		if i > 0 {
			values += ", "
		}
		i++
		values += fmt.Sprintf(
			`('%s','%s','github','title-%s','%s','done','done','P','p','reporter',toDateTime64('2026-09-01 00:00:00',3),toDateTime64('2026-09-01 00:00:00',3),toDateTime64('2026-09-01 00:00:00',3),'%s')`,
			flowMatrixSeedRepoID, workItemID, workItemID, workType, orgID,
		)
	}
	insert := fmt.Sprintf(
		"INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, reporter, created_at, updated_at, last_synced, org_id) VALUES %s",
		values,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_items: %v", err)
	}
}

// seedCycleTimeRowsEachInItsOwnPart writes ONE INSERT per row on purpose.
// A single multi-row INSERT lands in a single part, where ClickHouse may
// already have collapsed the two versions of wi-stale before the query
// runs -- which would make the un-FINAL read agree with the FINAL read by
// accident and the whole fixture vacuous. Separate INSERTs guarantee
// separate parts, i.e. the genuinely pre-merge state this test is about.
func seedCycleTimeRowsEachInItsOwnPart(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, rows []seededCycleTimeRow) {
	t.Helper()
	for _, r := range rows {
		insert := fmt.Sprintf(
			"INSERT INTO work_item_cycle_times (work_item_id, provider, day, work_scope_id, team_id, type, status, created_at, computed_at, org_id) VALUES "+
				`('%s','github',toDate('%s'),'scope-a','team-a','%s','done',toDateTime('2026-09-01 00:00:00'),toDateTime('%s'),'%s')`,
			r.workItemID, r.day, r.workType, r.computedAt, orgID,
		)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed work_item_cycle_times (%s @ %s): %v", r.workItemID, r.day, err)
		}
	}
}

// startSeededFlowMatrixClickHouse brings up an isolated ClickHouse, applies
// the DDL, and seeds the shared fixture both tests in this file use:
//
//	wi-stale : TWO versions. v1 day=2026-09-03 (INSIDE the window),
//	           v2 day=2026-09-20 (OUTSIDE it) with a LATER computed_at, so
//	           v2 is the winning version. Type "Bug".
//	wi-bug   : one version, day inside the window. Type "Bug".
//	wi-story : one version, day inside the window. Type "Story".
//
// A read WITH FINAL sees wi-stale only at its winning day 2026-09-20,
// outside the window, so it counts ONE "Bug". A read WITHOUT FINAL still
// sees the superseded 2026-09-03 version and counts TWO.
func startSeededFlowMatrixClickHouse(t *testing.T, ctx context.Context, orgID string) (*containers.Instance, stdclickhouse.Conn) {
	t.Helper()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// The REAL migration chain, never hand-typed DDL. This test's entire
	// claim rests on work_item_cycle_times being
	// ReplacingMergeTree(computed_at) sorted by (provider, work_item_id) --
	// a hand-written CREATE TABLE would keep the test green while a
	// migration changed the production engine or sorting key underneath it,
	// which is exactly the failure mode internal/testsupport/chschema's
	// package doc was written to remove.
	chschema.Apply(ctx, t, inst)
	assertCycleTimesDedupContract(t, ctx, conn)

	seedWorkItems(t, ctx, conn, orgID, map[string]string{
		"wi-stale": "Bug",
		"wi-bug":   "Bug",
		"wi-story": "Story",
	})
	seedCycleTimeRowsEachInItsOwnPart(t, ctx, conn, orgID, []seededCycleTimeRow{
		{workItemID: "wi-stale", day: flowMatrixSeedLiveDay, computedAt: "2026-09-07 01:00:00", workType: "Bug"},
		{workItemID: "wi-stale", day: flowMatrixSeedSupersededDay, computedAt: "2026-09-07 02:00:00", workType: "Bug"},
		{workItemID: "wi-bug", day: flowMatrixSeedLiveDay, computedAt: "2026-09-07 01:00:00", workType: "Bug"},
		{workItemID: "wi-story", day: flowMatrixSeedLiveDay, computedAt: "2026-09-07 01:00:00", workType: "Story"},
	})

	assertUnmergedParts(t, ctx, conn)
	return inst, conn
}

// assertCycleTimesDedupContract pins the two schema properties this file
// depends on, read back from the migrated table rather than assumed:
// the engine must collapse duplicate versions by computed_at, and `day`
// must NOT be part of the sorting key. If a future migration adds `day`
// to the key, the two seeded versions of wi-stale stop being duplicates,
// FINAL stops changing the answer, and every assertion below would go
// green for the wrong reason. Failing loudly here is the difference
// between a regression test and a decorative one.
func assertCycleTimesDedupContract(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) {
	t.Helper()
	var engineFull, sortingKey string
	row := conn.QueryRow(ctx,
		"SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'work_item_cycle_times'")
	if err := row.Scan(&engineFull, &sortingKey); err != nil {
		t.Fatalf("read work_item_cycle_times schema from the migrated database: %v", err)
	}
	if !strings.Contains(engineFull, "ReplacingMergeTree(computed_at)") {
		t.Fatalf("work_item_cycle_times engine is %q; this test only means something while the table "+
			"collapses duplicate versions by computed_at", engineFull)
	}
	if strings.Contains(sortingKey, "day") {
		t.Fatalf("work_item_cycle_times sorting key is %q -- `day` has entered the dedup key, so the two "+
			"seeded versions of wi-stale are no longer duplicates and FINAL can no longer change the "+
			"answer; this fixture must be redesigned rather than left passing vacuously", sortingKey)
	}
}

// assertUnmergedParts fails the test if ClickHouse merged the seeded parts
// before the query ran. Without this the fixture could pass vacuously: a
// collapsed table makes FINAL a no-op, so both templates would agree and
// the test would report success while proving nothing. This is the
// "prove the test reaches the thing it claims to test" discipline.
func assertUnmergedParts(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) {
	t.Helper()
	var parts uint64
	row := conn.QueryRow(ctx,
		"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'work_item_cycle_times' AND active")
	if err := row.Scan(&parts); err != nil {
		t.Fatalf("count active parts: %v", err)
	}
	if parts < 2 {
		t.Fatalf("fixture is vacuous: work_item_cycle_times has %d active part(s), so the two versions of wi-stale "+
			"were already merged and FINAL cannot change the answer; the seed must land in separate parts", parts)
	}
}

func workTypeFlowMatrixRequest() FlowMatrixRequest {
	return FlowMatrixRequest{
		Dimension: DimensionWorkType,
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate(flowMatrixSeedWindowStart),
		EndDate:   mustGraphQLDate(flowMatrixSeedWindowEnd),
		MaxNodes:  50,
		MaxEdges:  200,
	}
}

// nodeValueByID / edgeValueBySourceTarget report found=false both when no
// such node/edge exists AND when one exists with a nil Value -- Value is
// *float64 on both models (CHAOS-4701), and a nil there is a distinct,
// separately-wrong outcome that must never be silently read as 0.
func nodeValueByID(nodes []model.SankeyNode, id string) (float64, bool) {
	for _, n := range nodes {
		if n.ID == id {
			if n.Value == nil {
				return 0, false
			}
			return *n.Value, true
		}
	}
	return 0, false
}

func edgeValueBySourceTarget(edges []model.SankeyEdge, source, target string) (float64, bool) {
	for _, e := range edges {
		if e.Source == source && e.Target == target {
			if e.Value == nil {
				return 0, false
			}
			return *e.Value, true
		}
	}
	return 0, false
}

// TestFlowMatrixWorkType_FinalExcludesSupersededCycleTimeVersions is the
// primary anti-mutant guard: it pins the numbers the FINAL read produces
// against a real engine holding a genuinely pre-merge table.
//
// Hand-computed from the seed. Under FINAL, wi-stale resolves to its
// winning version (day 2026-09-20, outside the window) and drops out
// entirely, leaving one "Bug" (wi-bug) and one "Story" (wi-story) inside
// the window, both on day 2026-09-03 in the same repo:
//
//	nodes: Bug = 1, Story = 1
//	edges: Bug->Story = 1, Story->Bug = 1
//
// Delete `FINAL` from flowMatrixWorkTypeNodesTemplate and the Bug node
// becomes 2; delete it from flowMatrixWorkTypeEnrichedSelect and the
// Bug->Story edge becomes 2. Each deletion fails this test on its own.
func TestFlowMatrixWorkType_FinalExcludesSupersededCycleTimeVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const orgID = "chaos-5448-seeded-flowmatrix"
	inst, _ := startSeededFlowMatrixClickHouse(t, ctx, orgID)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	nodesQuery, edgesQuery, err := CompileFlowMatrix(workTypeFlowMatrixRequest(), orgID, 30, nil)
	if err != nil {
		t.Fatalf("CompileFlowMatrix: %v", err)
	}
	nodes, edges, err := ExecuteFlowMatrix(ctx, client, nodesQuery, edgesQuery)
	if err != nil {
		t.Fatalf("ExecuteFlowMatrix: %v", err)
	}

	bug, ok := nodeValueByID(nodes, nodeBug)
	if !ok {
		t.Fatalf("no %s node with a non-nil value in %+v", nodeBug, nodes)
	}
	if bug != 1 {
		t.Errorf("nodes[Bug].value = %v, want 1 -- the superseded 2026-09-03 version of wi-stale must not be "+
			"counted; getting 2 means FINAL is missing from flowMatrixWorkTypeNodesTemplate", bug)
	}
	if story, ok := nodeValueByID(nodes, nodeStory); !ok {
		t.Errorf("no %s node with a non-nil value in %+v", nodeStory, nodes)
	} else if story != 1 {
		t.Errorf("nodes[Story].value = %v, want 1", story)
	}

	bugToStory, ok := edgeValueBySourceTarget(edges, nodeBug, nodeStory)
	if !ok {
		t.Fatalf("no %s->%s edge with a non-nil value in %+v", nodeBug, nodeStory, edges)
	}
	if bugToStory != 1 {
		t.Errorf("edges[Bug->Story].value = %v, want 1 -- the superseded version of wi-stale must not be counted; "+
			"getting 2 means FINAL is missing from flowMatrixWorkTypeEnrichedSelect", bugToStory)
	}
	if storyToBug, ok := edgeValueBySourceTarget(edges, nodeStory, nodeBug); !ok {
		t.Errorf("no %s->%s edge with a non-nil value in %+v", nodeStory, nodeBug, edges)
	} else if storyToBug != 1 {
		t.Errorf("edges[Story->Bug].value = %v, want 1", storyToBug)
	}
}

// TestFlowMatrixWorkType_PythonUnfinalReadConvergesOntoTheFinalRead is the
// evidence behind CHAOS-5448's verdict that Python -- not Go -- is the
// wrong side, and the reason this divergence closes without a resolver
// change.
//
// It runs Python's exact nodes SQL (byte-for-byte this package's own
// template with the single token `FINAL` removed, so the two texts cannot
// drift apart in this file) three times against one engine:
//
//	pre-merge   un-FINAL read  -> Bug = 2   (counts the superseded version)
//	pre-merge   FINAL read     -> Bug = 1
//	post-merge  un-FINAL read  -> Bug = 1   (CONVERGES onto the FINAL read)
//
// The third result is the point: OPTIMIZE ... FINAL is what a background
// merge does on its own schedule, so Python's answer changes from 2 to 1
// with no code change on either side. A number that moves when an
// unrelated merge runs is not a parity baseline -- it is the same class of
// defect as a hardcoded fixture date under a rolling window. Go's FINAL
// read is the value both sides converge to.
func TestFlowMatrixWorkType_PythonUnfinalReadConvergesOntoTheFinalRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const orgID = "chaos-5448-seeded-convergence"
	_, conn := startSeededFlowMatrixClickHouse(t, ctx, orgID)

	// Derived from the shipped template by removing exactly one token, so
	// this test can never compare against a stale hand-copy of Python's
	// SQL: whatever the Go template says, the "Python" variant is it
	// minus FINAL.
	goNodesSQL := renderWorkTypeNodesSQLForTest(orgID)
	pythonNodesSQL := withoutCycleTimesFinal(t, "flowMatrixWorkTypeNodesTemplate", goNodesSQL)

	preMergeUnfinal := scanNodeValue(t, ctx, conn, pythonNodesSQL, "Bug")
	if preMergeUnfinal != 2 {
		t.Fatalf("pre-merge un-FINAL read returned Bug = %v, want 2; the fixture is not exercising the "+
			"superseded-version condition this test exists to demonstrate", preMergeUnfinal)
	}

	preMergeFinal := scanNodeValue(t, ctx, conn, goNodesSQL, "Bug")
	if preMergeFinal != 1 {
		t.Fatalf("pre-merge FINAL read returned Bug = %v, want 1", preMergeFinal)
	}

	if err := conn.Exec(ctx, "OPTIMIZE TABLE work_item_cycle_times FINAL"); err != nil {
		t.Fatalf("OPTIMIZE TABLE work_item_cycle_times FINAL: %v", err)
	}

	postMergeUnfinal := scanNodeValue(t, ctx, conn, pythonNodesSQL, "Bug")
	if postMergeUnfinal != preMergeFinal {
		t.Errorf("after the merge the un-FINAL read returned Bug = %v, but the FINAL read returned %v; "+
			"the two are expected to CONVERGE, which is what makes the FINAL read the correct steady state",
			postMergeUnfinal, preMergeFinal)
	}
	if postMergeUnfinal == preMergeUnfinal {
		t.Errorf("the un-FINAL read returned %v both before and after the merge; this test only proves "+
			"anything if the un-FINAL answer MOVES, which is the defect being demonstrated", postMergeUnfinal)
	}
}

// renderWorkTypeNodesSQLForTest materializes flowMatrixWorkTypeNodesTemplate
// with its bindings inlined, so both variants can be run through the raw
// driver connection (which has no named-parameter support here) rather
// than through the query client.
func renderWorkTypeNodesSQLForTest(orgID string) string {
	sql := fmt.Sprintf(flowMatrixWorkTypeNodesTemplate, "")
	replacements := [][2]string{
		{"{start_date:Date}", "toDate('" + flowMatrixSeedWindowStart + "')"},
		{"{end_date:Date}", "toDate('" + flowMatrixSeedWindowEnd + "')"},
		{"{org_id:String}", "'" + orgID + "'"},
		{"{limit_per_dim:UInt32}", "50"},
	}
	for _, r := range replacements {
		sql = strings.ReplaceAll(sql, r[0], r[1])
	}
	return sql
}

// withoutCycleTimesFinal removes the ONE token under test, and fails the
// test if it is not there -- so a future edit that drops FINAL from the
// template cannot silently turn this test into a comparison of two
// identical queries.
func withoutCycleTimesFinal(t *testing.T, templateName, sql string) string {
	t.Helper()
	const withFinal = "FROM work_item_cycle_times AS wct FINAL"
	const withoutFinal = "FROM work_item_cycle_times AS wct"
	if !strings.Contains(sql, withFinal) {
		t.Fatalf("%s no longer contains %q -- FINAL was removed from the template, which is the exact "+
			"regression CHAOS-5448 pinned; see this file's package doc comment", templateName, withFinal)
	}
	return strings.ReplaceAll(sql, withFinal, withoutFinal)
}

// scanNodeValue runs a rendered nodes query on the raw driver connection
// and returns the value of the node whose id is wantNodeID, failing if no
// such node comes back. Takes the id rather than hardcoding one so the
// WORK_TYPE and REPO variants share one scanner.
func scanNodeValue(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, sql, wantNodeID string) float64 {
	t.Helper()
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query: %v\nSQL:\n%s", err, sql)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var dimension, nodeID string
		var value float64
		if err := rows.Scan(&dimension, &nodeID, &value); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if nodeID == wantNodeID {
			return value
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	t.Fatalf("no %q node returned by:\n%s", wantNodeID, sql)
	return 0
}

// TestFlowMatrixRepoNodes_FinalExcludesSupersededCycleTimeVersions covers
// CHAOS-4516 FIX SITE 2 of 3, flowMatrixRepoNodesTemplate, which the
// WORK_TYPE tests above do not reach.
//
// Site 2 carried "argued-by-analogy rather than measured" until this
// test: the mechanism is identical to sites 1 and 3 -- Python omits
// `FINAL` on `wct` at sql/templates.py:325 where this template has it --
// but an identical MECHANISM is not an executed result, and CHAOS-4516's
// fix-shape ruling asks for measurement per site, not per family.
//
// It reuses the same seeded fixture deliberately. wi-stale's winning
// version sits on 2026-09-20, outside the window, so the one seeded repo
// holds TWO work items in the window under FINAL (wi-bug, wi-story) and
// THREE without it. Reusing the fixture is what makes the comparison
// across the three sites exact rather than approximate: same rows, same
// window, only the template differs.
//
// SCOPE, stated so nobody reads more into a green than it carries: this
// covers the REPO NODES template only. The REPO EDGES template reads
// through flowMatrixRepoEnrichedSelect, which already carried `wct FINAL`
// before CHAOS-4516 and is explicitly not one of the three exposed sites;
// it also joins work_item_team_attributions, which this fixture does not
// seed, so it would return no rows here regardless.
func TestFlowMatrixRepoNodes_FinalExcludesSupersededCycleTimeVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const orgID = "chaos-5448-seeded-repo-nodes"
	inst, conn := startSeededFlowMatrixClickHouse(t, ctx, orgID)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// UseInvestment left nil: resolveUseInvestment (investment.go:447-458)
	// returns false for REPO, so this takes the fixed-template path --
	// flowMatrixRepoNodesTemplate, the site under test -- and not
	// compileFlowMatrixInvestmentTeamRepoDimension.
	req := workTypeFlowMatrixRequest()
	req.Dimension = DimensionRepo

	nodesQuery, _, err := CompileFlowMatrix(req, orgID, 30, nil)
	if err != nil {
		t.Fatalf("CompileFlowMatrix(REPO): %v", err)
	}
	// t.Errorf, deliberately NOT t.Fatalf: a text check is a linter, not a
	// regression test, and stopping here would leave the BEHAVIOURAL
	// assertion below unrun on exactly the mutation this test exists to
	// catch. Reporting both -- the missing token and the wrong count -- is
	// what makes a failure diagnosable.
	if !strings.Contains(nodesQuery.sql, "FROM work_item_cycle_times AS wct FINAL") {
		t.Errorf("flowMatrixRepoNodesTemplate no longer reads work_item_cycle_times with FINAL -- that is the "+
			"CHAOS-4516 fix site 2 this test pins:\n%s", nodesQuery.sql)
	}

	nodes, err := queryNodes(ctx, client, nodesQuery)
	if err != nil {
		t.Fatalf("queryNodes(REPO): %v", err)
	}

	repoCount, ok := nodeValueByID(nodes, nodeSeedRepo)
	if !ok {
		t.Fatalf("no %s node with a non-nil value in %+v", nodeSeedRepo, nodes)
	}
	if repoCount != 2 {
		t.Errorf("nodes[%s].value = %v, want 2 (wi-bug and wi-story); 3 means the superseded 2026-09-03 "+
			"version of wi-stale was counted, i.e. FINAL is missing from flowMatrixRepoNodesTemplate",
			nodeSeedRepo, repoCount)
	}

	// Same control as the WORK_TYPE convergence test, applied to this
	// template: Python's variant is this exact SQL minus one token, so the
	// two texts cannot drift apart, and it must DISAGREE here or the
	// fixture is not exercising site 2 at all.
	pythonSQL := withoutCycleTimesFinal(t, "flowMatrixRepoNodesTemplate", renderRepoNodesSQLForTest(orgID))
	unfinal := scanNodeValue(t, ctx, conn, pythonSQL, flowMatrixSeedRepoID)
	if unfinal != 3 {
		t.Fatalf("the un-FINAL REPO read returned %v, want 3; the superseded version is not reaching it, "+
			"so this test proves nothing about site 2", unfinal)
	}
}

// renderRepoNodesSQLForTest is renderWorkTypeNodesSQLForTest's REPO twin
// -- same binding inlining, different template.
func renderRepoNodesSQLForTest(orgID string) string {
	sql := fmt.Sprintf(flowMatrixRepoNodesTemplate, "")
	replacements := [][2]string{
		{"{start_date:Date}", "toDate('" + flowMatrixSeedWindowStart + "')"},
		{"{end_date:Date}", "toDate('" + flowMatrixSeedWindowEnd + "')"},
		{"{org_id:String}", "'" + orgID + "'"},
		{"{limit_per_dim:UInt32}", "50"},
	}
	for _, r := range replacements {
		sql = strings.ReplaceAll(sql, r[0], r[1])
	}
	return sql
}
