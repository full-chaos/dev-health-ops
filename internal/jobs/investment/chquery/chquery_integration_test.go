//go:build integration

package chquery

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The corpus below is built from GATE-CHOSEN values, not from captured data.
//
// That distinction is the whole point of this file. A golden taken from one
// real capture can only test the dimensions that capture happened to vary —
// PR1's component golden came from a single scoped run on one org, so it
// cannot say anything about an empty org, a zero UUID, a NULL column or a
// tz-naive timestamp. Those are exactly the values that sit ON the gates, and
// exactly where a port diverges. So they are seeded deliberately here.
//
// Everything asserted here is invisible to a mocked conn by construction:
// type-exact scans, ReplacingMergeTree collapse semantics, and the
// HAVING-on-the-alias provenance rule are properties of the engine, not of the
// Go code's shape.

const (
	orgAlpha = "11111111-1111-4111-8111-111111111111"
	orgBeta  = "22222222-2222-4222-8222-222222222222"
)

func newTestReader(t *testing.T) (*Reader, driver.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	reader, err := NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	return reader, conn, ctx
}

func mustExec(t *testing.T, ctx context.Context, conn driver.Conn, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("seed failed: %v\nquery: %s", err, query)
	}
}

// TestFetchWorkGraphEdgesRespectsDedupThenFilterOrder proves the property that
// makes this query correct and that a mock cannot express: the heuristic
// exclusion is judged on the LATEST provenance after the argMax collapse, not
// on whatever version of the row the storage engine happens to return.
//
// work_graph_edges is a ReplacingMergeTree whose sorting key does NOT include
// provenance, so an edge can exist twice — once heuristic, once native — and
// which one a raw read returns depends on merge timing. Filtering before
// collapsing would let a stale heuristic row exclude an edge that is now
// native, or the reverse, non-deterministically.
func TestFetchWorkGraphEdgesRespectsDedupThenFilterOrder(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	// One edge identity, two generations: heuristic first, native later.
	// The LATEST provenance is native, so the edge must be KEPT.
	seedEdge(t, ctx, conn, orgAlpha, "promoted", "heuristic", 0.3, "2026-01-01 00:00:00.000")
	seedEdge(t, ctx, conn, orgAlpha, "promoted", "native", 1.0, "2026-01-02 00:00:00.000")

	// The mirror case: native first, heuristic later. Latest is heuristic, so
	// the edge must be EXCLUDED. Without dedup-before-filter this is the row
	// that leaks in.
	seedEdge(t, ctx, conn, orgAlpha, "demoted", "native", 1.0, "2026-01-01 00:00:00.000")
	seedEdge(t, ctx, conn, orgAlpha, "demoted", "heuristic", 0.3, "2026-01-02 00:00:00.000")

	rows, err := reader.FetchWorkGraphEdges(ctx, DefaultEdgeQueryOptions(orgAlpha))
	if err != nil {
		t.Fatal(err)
	}

	outcome := map[string]string{}
	for _, row := range rows {
		outcome[row.Edge.SourceID] = row.Provenance
	}

	if provenance, kept := outcome["promoted-src"]; !kept {
		t.Error("an edge whose LATEST provenance is native was excluded; " +
			"the heuristic filter is being applied before the argMax collapse")
	} else if provenance != "native" {
		t.Errorf("promoted edge resolved provenance %q, want native", provenance)
	}

	if _, kept := outcome["demoted-src"]; kept {
		t.Error("an edge whose LATEST provenance is heuristic was kept; " +
			"a stale native row is resurrecting an edge that must be excluded")
	}
}

// TestFetchWorkGraphEdgesEmptyOrgReadsEveryTenant pins CHAOS-4804's behaviour
// against a real engine.
//
// This asserts the WRONG-BUT-PYTHON-IDENTICAL behaviour on purpose. The port's
// contract is to match Python, and the fix belongs on both planes at once; a
// test that asserted the safe behaviour here would be asserting a divergence.
// When CHAOS-4804 lands, THIS TEST is what should fail, loudly, on both planes.
func TestFetchWorkGraphEdgesEmptyOrgReadsEveryTenant(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	seedEdge(t, ctx, conn, orgAlpha, "alpha", "native", 1.0, "2026-01-01 00:00:00.000")
	seedEdge(t, ctx, conn, orgBeta, "beta", "native", 1.0, "2026-01-01 00:00:00.000")

	scoped, err := reader.FetchWorkGraphEdges(ctx, DefaultEdgeQueryOptions(orgAlpha))
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped) != 1 {
		t.Fatalf("scoped read returned %d edges, want exactly org alpha's 1", len(scoped))
	}

	unscoped, err := reader.FetchWorkGraphEdges(ctx, EdgeQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unscoped) != 2 {
		t.Fatalf(
			"unscoped read returned %d edges, want 2 (BOTH tenants). "+
				"If this now returns 1, CHAOS-4804 has been fixed on the Go side "+
				"only -- check that components.py moved in the same change set, "+
				"or the two planes will group differently",
			len(unscoped),
		)
	}
}

// TestScansTolerateNullAndBoundaryValues is the type-exactness proof: every
// Nullable column carrying an actual NULL, and every integer column carrying
// its boundary value, scanned through the real driver.
//
// A fake conn cannot fail this test, because a fake returns whatever Go values
// the test hands it. Only the real driver rejects a NULL scanned into a
// non-pointer.
func TestScansTolerateNullAndBoundaryValues(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	repoID := uuid.New().String()

	// work_items: NULL description, NULL completed_at, empty parent/epic.
	mustExec(t, ctx, conn, `
        INSERT INTO work_items (
            org_id, work_item_id, provider, repo_id, title, description, type,
            labels, parent_id, epic_id, created_at, updated_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, [], '', '', ?, ?, NULL)
    `, orgAlpha, "wi-nulls", "linear", repoID, "title", "task",
		"2026-03-01 12:34:56.789", "2026-03-02 01:02:03.004")

	// git_pull_requests: NULL title/body/merged_at/closed_at, and additions
	// and deletions at the top of UInt32 -- the out-of-range-int boundary.
	mustExec(t, ctx, conn, `
        INSERT INTO git_pull_requests (
            org_id, repo_id, number, title, body, created_at, merged_at,
            closed_at, additions, deletions
        ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, NULL, ?, ?)
    `, orgAlpha, repoID, uint32(0), /* PR #0 -- kept by Python's `is None` check */
		"2026-03-01 00:00:00.000", uint32(4294967295), uint32(4294967295))

	// git_commits: NULL message.
	mustExec(t, ctx, conn, `
        INSERT INTO git_commits (org_id, repo_id, hash, message, author_when, committer_when)
        VALUES (?, ?, ?, NULL, ?, ?)
    `, orgAlpha, repoID, "deadbeef", "2026-03-01 00:00:00.000", "2026-03-01 00:00:00.000")

	items, err := reader.FetchWorkItems(ctx, []string{"wi-nulls"}, orgAlpha)
	if err != nil {
		t.Fatalf("NULL description/completed_at broke the scan: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 work item, got %d", len(items))
	}
	if items[0].Description != "" {
		t.Errorf("NULL description should coerce to empty, got %q", items[0].Description)
	}
	if items[0].CompletedAt != nil {
		t.Errorf("NULL completed_at should stay nil, got %v", items[0].CompletedAt)
	}

	prs, err := reader.FetchPullRequests(ctx, map[string][]uint32{repoID: {0}}, orgAlpha)
	if err != nil {
		t.Fatalf("NULLs / UInt32 max broke the PR scan: %v", err)
	}
	if len(prs) != 1 {
		t.Fatalf("PR #0 must be returned -- python rejects on `number is None`, "+
			"not on truthiness, so zero is a real PR number; got %d rows", len(prs))
	}
	if prs[0].Additions != 4294967295 || prs[0].Deletions != 4294967295 {
		t.Errorf("UInt32 boundary lost precision: additions=%v deletions=%v",
			prs[0].Additions, prs[0].Deletions)
	}
	if prs[0].Title != "" || prs[0].Body != "" || prs[0].MergedAt != nil || prs[0].ClosedAt != nil {
		t.Errorf("NULL PR fields did not coerce as expected: %+v", prs[0])
	}

	commits, err := reader.FetchCommits(ctx, map[string][]string{repoID: {"deadbeef"}}, orgAlpha)
	if err != nil {
		t.Fatalf("NULL commit message broke the scan: %v", err)
	}
	if len(commits) != 1 || commits[0].Message != "" {
		t.Errorf("NULL message should coerce to empty, got %+v", commits)
	}
}

// TestTimestampsPreserveWallClockAsUTC is the VERIFICATION OWED by
// normalizeTimestamp's doc comment.
//
// work_items.created_at is DateTime64(3) with NO timezone; git_commits
// .author_when declares 'UTC'. Python reads the naive one as a naive datetime
// and _ensure_utc REINTERPRETS the wall clock as UTC rather than converting
// it. This asserts the Go reader lands on the same instant for both column
// kinds given the same wall-clock literal -- which is what makes a work unit's
// time bounds agree across the two planes.
func TestTimestampsPreserveWallClockAsUTC(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	const wallClock = "2026-03-01 12:34:56.789"
	repoID := uuid.New().String()

	mustExec(t, ctx, conn, `
        INSERT INTO work_items (
            org_id, work_item_id, provider, repo_id, title, description, type,
            labels, parent_id, epic_id, created_at, updated_at, completed_at
        ) VALUES (?, ?, 'linear', ?, 't', NULL, 'task', [], '', '', ?, ?, NULL)
    `, orgAlpha, "wi-tz", repoID, wallClock, wallClock)

	mustExec(t, ctx, conn, `
        INSERT INTO git_commits (org_id, repo_id, hash, message, author_when, committer_when)
        VALUES (?, ?, 'tzhash', NULL, ?, ?)
    `, orgAlpha, repoID, wallClock, wallClock)

	expected := time.Date(2026, 3, 1, 12, 34, 56, 789000000, time.UTC)

	items, err := reader.FetchWorkItems(ctx, []string{"wi-tz"}, orgAlpha)
	if err != nil || len(items) != 1 {
		t.Fatalf("fetch work item: %v (%d rows)", err, len(items))
	}
	if !items[0].CreatedAt.Equal(expected) {
		t.Errorf(
			"tz-NAIVE column: got %s, want %s. Python reinterprets the naive wall "+
				"clock as UTC; a .UTC() conversion here would shift the instant",
			items[0].CreatedAt.Format(time.RFC3339Nano), expected.Format(time.RFC3339Nano),
		)
	}
	if items[0].CreatedAt.Location() != time.UTC {
		t.Errorf("timestamps must be returned in UTC, got %s", items[0].CreatedAt.Location())
	}

	commits, err := reader.FetchCommits(ctx, map[string][]string{repoID: {"tzhash"}}, orgAlpha)
	if err != nil || len(commits) != 1 {
		t.Fatalf("fetch commit: %v (%d rows)", err, len(commits))
	}
	if !commits[0].AuthorWhen.Equal(expected) {
		t.Errorf("tz-AWARE column: got %s, want %s",
			commits[0].AuthorWhen.Format(time.RFC3339Nano), expected.Format(time.RFC3339Nano))
	}

	// The point of the whole test: both column kinds land on ONE instant.
	if !items[0].CreatedAt.Equal(commits[0].AuthorWhen) {
		t.Errorf(
			"the tz-naive and tz-aware columns disagree for the same wall clock "+
				"(%s vs %s) -- a work unit spanning an issue and a commit would get "+
				"different time bounds on the two planes",
			items[0].CreatedAt, commits[0].AuthorWhen,
		)
	}
}

// TestZeroUUIDRepoIsKeptNotRejected is the false-negative proof against a real
// engine: the row Python maps and a uuid.Nil-excluding port would drop.
func TestZeroUUIDRepoIsKeptNotRejected(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	mustExec(t, ctx, conn, `
        INSERT INTO user_metrics_daily (org_id, repo_id, team_id, day, computed_at)
        VALUES (?, ?, ?, ?, ?)
    `, orgAlpha, uuid.Nil.String(), "team-zero", "2026-03-01", "2026-03-01 00:00:00")

	repoIDs, err := reader.ResolveRepoIDsForTeams(ctx, []string{"team-zero"}, orgAlpha)
	if err != nil {
		t.Fatal(err)
	}
	if len(repoIDs) != 1 || repoIDs[0] != uuid.Nil.String() {
		t.Fatalf(
			"the all-zero repo_id must be KEPT (python: `if row.get(\"id\")` on the "+
				"rendered string is truthy). Got %v. A uuid.Nil exclusion here is a "+
				"false negative -- it drops a row python maps, keeps read==mapped+rejected "+
				"balanced, and is invisible to every conservation check",
			repoIDs,
		)
	}
}

// TestWorkItemsUsesFinalDedup proves the FINAL in workItemsDeduped is load
// bearing: two generations of one work item must collapse to the latest.
func TestWorkItemsUsesFinalDedup(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	repoID := uuid.New().String()

	for _, generation := range []struct {
		title   string
		updated string
	}{
		{title: "old title", updated: "2026-01-01 00:00:00.000"},
		{title: "new title", updated: "2026-06-01 00:00:00.000"},
	} {
		mustExec(t, ctx, conn, `
            INSERT INTO work_items (
                org_id, work_item_id, provider, repo_id, title, description, type,
                labels, parent_id, epic_id, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'linear', ?, ?, NULL, 'task', [], '', '', ?, ?, NULL)
        `, orgAlpha, "wi-dup", repoID, generation.title,
			"2026-01-01 00:00:00.000", generation.updated)
	}

	items, err := reader.FetchWorkItems(ctx, []string{"wi-dup"}, orgAlpha)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("FINAL must collapse the two generations to one row, got %d", len(items))
	}
	if items[0].Title != "new title" {
		t.Errorf("FINAL kept the wrong generation: title=%q", items[0].Title)
	}
}

// seedEdge writes one generation of a work_graph_edges row. sourceID is derived
// from name so a caller can assert on it.
func seedEdge(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, name, provenance string, confidence float32, lastSynced string,
) {
	t.Helper()
	mustExec(t, ctx, conn, `
        INSERT INTO work_graph_edges (
            edge_id, source_type, source_id, target_type, target_id, edge_type,
            repo_id, provider, provenance, confidence, evidence,
            discovered_at, last_synced, event_ts, org_id
        ) VALUES (?, 'issue', ?, 'pr', ?, 'implements', NULL, 'linear', ?, ?, 'seed', ?, ?, ?, ?)
    `,
		name+"-edge", name+"-src", name+"-tgt",
		provenance, confidence, lastSynced, lastSynced, lastSynced, orgID,
	)
}

// TestActiveHoursUnscopedCollapsesTenants is the SECOND deliberate pin of
// behaviour that is wrong (CHAOS-4804).
//
// fetch_work_item_active_hours groups by work_item_id ALONE while filtering on
// org conditionally, so an empty scope drops the filter and collapses every
// tenant's row for a shared work-item id into one group. argMax then returns
// whichever tenant wrote last. work_item_id is provider-scoped, not
// tenant-scoped, so ids really are shared across orgs.
//
// Asserted as-is on purpose: the port's contract is to match Python, and the
// fix belongs on BOTH planes at once. When CHAOS-4804 lands, this test and
// TestFetchWorkGraphEdgesEmptyOrgReadsEveryTenant are what should fail, and
// they should be flipped deliberately in the same change set that moves
// queries.py -- never one plane alone.
func TestActiveHoursUnscopedCollapsesTenants(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	// The SAME work-item id in two tenants -- the realistic case, since
	// "linear:CHAOS-4441" is provider-scoped.
	const sharedID = "linear:CHAOS-4441"
	mustExec(t, ctx, conn, `
        INSERT INTO work_item_cycle_times
            (org_id, work_item_id, provider, type, active_time_hours, created_at, computed_at)
        VALUES (?, ?, 'linear', 'task', ?, ?, ?)
    `, orgAlpha, sharedID, 10.0, "2026-03-01 00:00:00", "2026-03-01 00:00:00")
	mustExec(t, ctx, conn, `
        INSERT INTO work_item_cycle_times
            (org_id, work_item_id, provider, type, active_time_hours, created_at, computed_at)
        VALUES (?, ?, 'linear', 'task', ?, ?, ?)
    `, orgBeta, sharedID, 99.0, "2026-03-01 00:00:00", "2026-06-01 00:00:00")

	// Scoped: correct. Org alpha sees its own 10 hours, never beta's 99.
	scoped, err := reader.FetchWorkItemActiveHours(ctx, []string{sharedID}, orgAlpha)
	if err != nil {
		t.Fatal(err)
	}
	if scoped[sharedID] != 10.0 {
		t.Fatalf("scoped read must return org alpha's own value, got %v", scoped[sharedID])
	}

	// Unscoped: CHAOS-4804. The filter disappears, the GROUP BY collapses both
	// tenants, and argMax returns beta's later-written 99.
	unscoped, err := reader.FetchWorkItemActiveHours(ctx, []string{sharedID}, "")
	if err != nil {
		t.Fatal(err)
	}
	if unscoped[sharedID] != 99.0 {
		t.Fatalf(
			"unscoped read returned %v, want 99 (the OTHER tenant's value). "+
				"If this now returns 10 or nothing, CHAOS-4804 has been fixed -- verify "+
				"queries.py moved in the SAME change set, then flip this test and "+
				"TestFetchWorkGraphEdgesEmptyOrgReadsEveryTenant together. Fixing one "+
				"plane alone makes the two group differently, which is the failure this "+
				"port exists to prevent",
			unscoped[sharedID],
		)
	}
}

// TestZeroValueOptionsExcludeHeuristicEdges is the red-proof for codex round
// 1's P2 on this PR, and the reason the option is spelled IncludeHeuristic.
//
// Go's zero value for a bool is false. With the option spelled
// `ExcludeHeuristic`, the obvious construction -- `EdgeQueryOptions{
// OrganizationID: org}` -- silently DISABLED the CHAOS-2775 heuristic
// exclusion, while Python's omitted `exclude_heuristic` argument ENABLES it.
// A heuristic edge would then reach component grouping and percolate unrelated
// nodes into one work unit.
//
// This asserts the ZERO VALUE behaves like Python's default. If someone
// re-inverts the field to make it "read more naturally", this fails.
func TestZeroValueOptionsExcludeHeuristicEdges(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	seedEdge(t, ctx, conn, orgAlpha, "native-edge", "native", 1.0, "2026-01-01 00:00:00.000")
	seedEdge(t, ctx, conn, orgAlpha, "heuristic-edge", "heuristic", 0.3, "2026-01-01 00:00:00.000")

	// The bare struct literal -- no exclusion flag set anywhere.
	rows, err := reader.FetchWorkGraphEdges(ctx, EdgeQueryOptions{OrganizationID: orgAlpha})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Provenance == "heuristic" {
			t.Fatalf(
				"a heuristic edge (%s) survived a ZERO-VALUE EdgeQueryOptions. "+
					"Python's omitted exclude_heuristic argument excludes it; if the "+
					"Go zero value does not, every caller who writes the obvious "+
					"struct literal silently disables CHAOS-2775's exclusion",
				row.Edge.SourceID,
			)
		}
	}
	if len(rows) != 1 {
		t.Fatalf("want exactly the one native edge, got %d rows", len(rows))
	}

	// And the opt-in still works, for a display caller that wants everything.
	all, err := reader.FetchWorkGraphEdges(ctx,
		EdgeQueryOptions{OrganizationID: orgAlpha, IncludeHeuristic: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("IncludeHeuristic must return both edges, got %d", len(all))
	}
}

// TestParentTitlesAndCommitChurnAreExercised closes codex round 1's P3: neither
// fetcher was invoked by any test, so a reader returning an empty map for
// either would have passed the whole suite.
func TestParentTitlesAndCommitChurnAreExercised(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	repoID := uuid.New().String()

	// Two parents: one with a title, one with an EMPTY title. Python drops the
	// empty one (`if row.get("work_item_id") and row.get("title")`), so it must
	// be ABSENT from the map rather than mapped to "".
	for _, parent := range []struct{ id, title string }{
		{id: "parent-with-title", title: "Epic: investment port"},
		{id: "parent-empty-title", title: ""},
	} {
		mustExec(t, ctx, conn, `
            INSERT INTO work_items (
                org_id, work_item_id, provider, repo_id, title, description, type,
                labels, parent_id, epic_id, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'linear', ?, ?, NULL, 'epic', [], '', '', ?, ?, NULL)
        `, orgAlpha, parent.id, repoID, parent.title,
			"2026-01-01 00:00:00.000", "2026-01-01 00:00:00.000")
	}

	titles, err := reader.FetchParentTitles(ctx,
		[]string{"parent-with-title", "parent-empty-title", "parent-absent"}, orgAlpha)
	if err != nil {
		t.Fatal(err)
	}
	if titles["parent-with-title"] != "Epic: investment port" {
		t.Errorf("titled parent missing or wrong: %q", titles["parent-with-title"])
	}
	if _, present := titles["parent-empty-title"]; present {
		t.Error("an EMPTY title must be absent from the map, not mapped to \"\" -- " +
			"python drops it on truthiness and the text bundle asks whether a " +
			"parent title EXISTS")
	}
	if _, present := titles["parent-absent"]; present {
		t.Error("an id with no row must be absent from the map")
	}

	// Churn: two commits, one with stats split across two rows (the sum must
	// aggregate), one with no stats row at all (absent, not zero).
	for _, stat := range []struct {
		hash                 string
		additions, deletions int32
	}{
		{hash: "aaa111", additions: 10, deletions: 5},
		{hash: "aaa111", additions: 3, deletions: 2},
	} {
		mustExec(t, ctx, conn, `
            INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, additions, deletions)
            VALUES (?, ?, ?, ?, ?)
        `, orgAlpha, repoID, stat.hash, stat.additions, stat.deletions)
	}

	churn, err := reader.FetchCommitChurn(ctx,
		map[string][]string{repoID: {"aaa111", "bbb222"}}, orgAlpha)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := repoID + "@aaa111"
	if churn[wantKey] != 20 {
		t.Errorf("churn for %s = %v, want 20 (10+5+3+2 summed across both stat rows)",
			wantKey, churn[wantKey])
	}
	if _, present := churn[repoID+"@bbb222"]; present {
		t.Error("a commit with no stats row must be ABSENT from the map, not zero -- " +
			"the caller's lookup defaults to 0, and a present zero would be " +
			"indistinguishable from a real zero-churn commit")
	}
}

// TestInvalidUTF8StringsAreHexedLikeThePythonDriver is the end-to-end proof for
// the decode policy measured under CHAOS-4441 plan section 5d.
//
// ClickHouse `String` is arbitrary bytes. clickhouse-connect decodes it as
// UTF-8 and, on failure, substitutes the LOWERCASE HEX OF THE WHOLE VALUE
// (driver/buffer.py:135-138). clickhouse-go returns the raw bytes instead, so
// the fetchers apply pythonparity.DecodeClickHouseStringValue to close the gap.
//
// Nothing else in the suite can catch the removal of those calls. A unit test
// cannot, because the divergence only exists once real bytes come off a real
// wire; the frozen goldens cannot, because they were captured from decodable
// data. This test seeds the undecodable bytes deliberately, which is the only
// way the substitution is observable at all.
//
// Why it matters more than an encoding curiosity: source_id and target_id are
// hashed into work_unit_id, which addresses rows in BOTH
// work_unit_investments and work_unit_membership -- two tables written by two
// different jobs. If the planes spell one byte sequence differently they mint
// different work_unit_ids, and the two tables stop agreeing with no error.
func TestInvalidUTF8StringsAreHexedLikeThePythonDriver(t *testing.T) {
	reader, conn, ctx := newTestReader(t)

	// 0xFF is never valid in UTF-8; 0xED 0xA0 0x80 is a lone surrogate encoded
	// WTF-8, which strict UTF-8 also rejects. Both are written as raw bytes via
	// unhex() so the driver's own encoding cannot launder them on the way in.
	const (
		badSourceHex = "6100FF62" // "a" 0x00 0xFF "b" -- mixed valid/invalid
		badTargetHex = "EDA080"   // lone high surrogate
	)
	edgeID := uuid.NewString()

	if err := conn.Exec(ctx, `
        INSERT INTO work_graph_edges
            (org_id, edge_id, source_type, source_id, edge_type, target_type,
             target_id, repo_id, provider, provenance, confidence, evidence, last_synced)
        SELECT ?, ?, 'issue', unhex(?), 'relates', 'pr', unhex(?),
               ?, 'github', 'model', 0.9, '', now()
    `, orgAlpha, edgeID, badSourceHex, badTargetHex, uuid.Nil.String()); err != nil {
		t.Fatalf("seed edge with invalid utf-8: %v", err)
	}

	rows, err := reader.FetchWorkGraphEdges(ctx, EdgeQueryOptions{OrganizationID: orgAlpha})
	if err != nil {
		t.Fatalf("fetch edges: %v", err)
	}

	var found bool
	for _, row := range rows {
		if row.Edge.EdgeID != edgeID {
			continue
		}
		found = true

		// The WHOLE value is hexed, including the valid 'a' and 'b' around the
		// bad byte -- not just the offending byte. A per-byte substitution
		// would produce something like "a<x>b" and diverge on every mixed
		// value.
		if want := "6100ff62"; row.Edge.SourceID != want {
			t.Errorf("source_id = %q, python driver yields %q (whole value hexed, "+
				"lowercase)", row.Edge.SourceID, want)
		}
		if want := "eda080"; row.Edge.TargetID != want {
			t.Errorf("target_id = %q, python driver yields %q", row.Edge.TargetID, want)
		}

		// And the substituted values must be pure ASCII, which is what makes
		// MarshalPythonJSON's invalid-UTF-8 branch unreachable via this path.
		for _, value := range []string{row.Edge.SourceID, row.Edge.TargetID} {
			for index := 0; index < len(value); index++ {
				if value[index] >= 0x80 {
					t.Errorf("substituted value %q has a non-ASCII byte at %d",
						value, index)
				}
			}
		}
	}
	if !found {
		t.Fatal("the seeded edge was not returned; the row may have been filtered " +
			"before the decode could be observed")
	}
}
