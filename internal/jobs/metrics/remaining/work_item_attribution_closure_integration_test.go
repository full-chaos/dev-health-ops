//go:build integration

package remaining

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWorkItemAttributionClosurePromotesToOrgWide is the real-engine proof
// for team-lead's PR-B ruling: a scoped run whose linked_issue closure
// (donors of affected items, and items whose donor is affected, one hop
// each way) exceeds workItemAttributionClosurePromotionBound of the org's
// total item count is promoted to fully org-wide, with the reason recorded
// on the org-wide marker rather than a scoped one -- something no unit
// test against fakes can prove, since evaluateClosurePromotion's reverse
// hop is a live SQL query this file has no narrow interface to fake.
//
// Fixture: org has 4 items. A is owned by repoX (the only ownership
// change, so the FIRST run's scope is exactly {A}). A --relates_to--> C
// (forward: C is A's donor target). B --relates_to--> A (reverse: B's
// donor is the affected item A). D has no dependency edges at all --
// present only so the org total (4) makes the closure {A, B, C} = 3 cross
// the 25% bound (75%) without being the WHOLE org already, which would
// prove nothing about promotion specifically.
func TestWorkItemAttributionClosurePromotesToOrgWide(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)

	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor (real migrated schema must be accepted): %v", err)
	}

	orgID := "org-closure-" + uuid.NewString()
	repoX := uuid.New()
	now := time.Now().UTC()

	seedWorkItemAttributionItem(t, ctx, conn, orgID, "A", repoX, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "B", uuid.Nil, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "C", uuid.Nil, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "D", uuid.Nil, now)
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", now)
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "A", "C", "relates_to", now)
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "B", "A", "relates_to", now)

	// A raw `SELECT count() ... FINAL` sees the freshly-inserted
	// team_repo_ownership row immediately, every time. teamattribution's
	// own LoadRepos -- ClickHouseFactSource's SELECT, the one ComputeOrg's
	// loadFacts actually calls -- does NOT: it aggregates via
	// argMax(...)/GROUP BY with no FINAL (a deliberate choice upstream, not
	// something PR-B can change), and was observed, reproducibly, to return
	// ZERO rows on the FIRST query issued right after the insert, then the
	// correct row on a second query a beat later, with no error either
	// time. Confirmed by hand: the SAME LoadRepos call, called twice in a
	// row, differs only in elapsed wall time. That gap is a real property
	// of the query this file depends on, not a bug this test should paper
	// over with FINAL of its own -- so it polls the EXACT call ComputeOrg
	// will make, not a proxy for it.
	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 1: %v", err)
	}
	if !outcome.OrgWide {
		t.Fatalf("outcome = %+v, want a PROMOTED (org-wide) run: closure {A,B,C} is 3/4 "+
			"of the org's items, over the 25%% bound", outcome)
	}
	if outcome.ItemsSeen != 4 {
		t.Fatalf("outcome.ItemsSeen = %d, want 4 -- a promoted run must cover the WHOLE "+
			"org (A,B,C,D), not just the 3-item closure that triggered the promotion", outcome.ItemsSeen)
	}
	if outcome.RowsWritten < 4 {
		t.Fatalf("outcome.RowsWritten = %d, want at least 4 (one row per item, even an "+
			"unassigned one for B/C/D)", outcome.RowsWritten)
	}

	runs := queryWorkItemAttributionRuns(t, ctx, conn, orgID)
	if len(runs) != 1 {
		t.Fatalf("work_item_attribution_backstop_runs has %d rows, want exactly 1: %v", len(runs), runs)
	}
	if !strings.Contains(runs[0].promotedReason, "linked_issue_closure_exceeded") {
		t.Fatalf("run marker promoted_reason = %q, want it to name the closure-promotion rule", runs[0].promotedReason)
	}

	scopedRuns := queryWorkItemAttributionScopedRuns(t, ctx, conn, orgID)
	if len(scopedRuns) != 0 {
		t.Fatalf("work_item_attribution_backstop_scoped_runs has %d rows, want 0 -- a "+
			"PROMOTED run must never also leave a scoped marker behind", len(scopedRuns))
	}

	attributionRows := queryWorkItemAttributionRows(t, ctx, conn, orgID)
	var sawA bool
	for _, row := range attributionRows {
		if row.workItemID == "A" {
			sawA = true
			if row.teamID == nil || *row.teamID != "team-infra" {
				t.Errorf("A's attribution row team_id = %v, want \"team-infra\" (repo_ownership)", row.teamID)
			}
		}
	}
	if !sawA {
		t.Fatal("no work_item_team_attributions row for A -- the promoted run must still cover the originally affected item")
	}

	// Run 2, immediately, with nothing changed: the org-wide watermark run 1
	// just published now covers everything (including the ownership row
	// that triggered run 1), so this must be a genuine no-op -- not a
	// second promoted run, and not a second write.
	outcome2, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 2: %v", err)
	}
	if !outcome2.SkippedNoop {
		t.Fatalf("run 2 outcome = %+v, want SkippedNoop -- nothing changed since run 1's watermark", outcome2)
	}

	runsAfter := queryWorkItemAttributionRuns(t, ctx, conn, orgID)
	if len(runsAfter) != 1 {
		t.Fatalf("work_item_attribution_backstop_runs has %d rows after the no-op run, want still 1", len(runsAfter))
	}
}

// TestWorkItemAttributionBelowThresholdClosureIsWritten is codex round r1's
// P1 fix, proven live: a scoped run's linked_issue closure is owed a
// rederive even when it stays well UNDER the promotion bound, not just when
// it crosses it. Before the fix, ComputeOrg only used the closure to decide
// promotion and never merged it into what actually got written -- a
// below-bound closure item was silently left stale forever.
//
// Fixture: org has 20 items. A is repo-owned (the only ownership change).
// A --relates_to--> C (C is A's forward donor target -- the closure).
// 18 filler items with no ownership/dependency involvement keep the
// closure {A, C} at 2/20 = 10%, comfortably under the 25% bound, so this
// proves the NON-promoted path specifically.
func TestWorkItemAttributionBelowThresholdClosureIsWritten(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)

	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-belowclosure-" + uuid.NewString()
	repoX := uuid.New()
	now := time.Now().UTC()

	seedWorkItemAttributionItem(t, ctx, conn, orgID, "A", repoX, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "C", uuid.Nil, now)
	for i := 0; i < 18; i++ {
		seedWorkItemAttributionItem(t, ctx, conn, orgID, fmt.Sprintf("filler-%d", i), uuid.Nil, now)
	}
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", now)
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "A", "C", "relates_to", now)

	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome.OrgWide {
		t.Fatalf("outcome.OrgWide = true, want a SCOPED run: closure {A,C} is 2/20 = 10%%, "+
			"well under the 25%% bound (outcome=%+v)", outcome)
	}

	rows := queryWorkItemAttributionRows(t, ctx, conn, orgID)
	written := map[string]bool{}
	for _, row := range rows {
		written[row.workItemID] = true
	}
	if !written["A"] {
		t.Error("no attribution row for A, the originally affected item")
	}
	if !written["C"] {
		t.Error("no attribution row for C, A's below-threshold closure member -- " +
			"this is the exact defect codex round r1 found: a below-bound closure " +
			"was computed but never merged into what actually gets written")
	}
	if written["filler-0"] {
		t.Error("a filler item outside the scope/closure was written -- this should " +
			"still be a SCOPED run, not org-wide")
	}

	scopedRuns := queryWorkItemAttributionScopedRuns(t, ctx, conn, orgID)
	if len(scopedRuns) == 0 {
		t.Error("no scoped run marker published for a below-threshold scoped run")
	}
}

// TestWorkItemAttributionBelowThresholdReverseClosureUsesDonorEdge is codex
// round r2's P1 fix: a reverse-hop closure item (B, found because
// B --relates_to--> A and A is affected) has its OWN outgoing edge (B->A),
// which the FIRST loadDependencyEdges call in ComputeOrg never sees (that
// call is scoped to the ORIGINAL affected set as source, before B is
// known). Without reloading dependencies after the closure merge, B
// resolves unassigned instead of inheriting A's team via linked_issue --
// the below-threshold closure test above never caught this because its
// fixture only exercised the FORWARD direction (A->C), where A's own edge
// was already loaded before the closure decision.
func TestWorkItemAttributionBelowThresholdReverseClosureUsesDonorEdge(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)

	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-reverseclosure-" + uuid.NewString()
	repoX := uuid.New()
	now := time.Now().UTC()

	seedWorkItemAttributionItem(t, ctx, conn, orgID, "A", repoX, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "B", uuid.Nil, now)
	for i := 0; i < 18; i++ {
		seedWorkItemAttributionItem(t, ctx, conn, orgID, fmt.Sprintf("filler-%d", i), uuid.Nil, now)
	}
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", now)
	// B -> A: B's donor is A (the affected item), the REVERSE hop.
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "B", "A", "relates_to", now)

	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome.OrgWide {
		t.Fatalf("outcome.OrgWide = true, want a SCOPED run: closure {A,B} is 2/20 = 10%%, "+
			"well under the 25%% bound (outcome=%+v)", outcome)
	}

	rows := queryWorkItemAttributionRows(t, ctx, conn, orgID)
	var bTeam *string
	var sawB bool
	for _, row := range rows {
		if row.workItemID == "B" {
			sawB = true
			bTeam = row.teamID
		}
	}
	if !sawB {
		t.Fatal("no attribution row for B, the reverse-hop closure member")
	}
	if bTeam == nil || *bTeam != "team-infra" {
		t.Fatalf("B's attribution team_id = %v, want \"team-infra\" via linked_issue inheritance "+
			"from A -- this is codex round r2's P1 finding: B's own B->A edge was never "+
			"reloaded after the closure merge, so B resolved unassigned instead", bTeam)
	}
}

// TestWorkItemAttributionDanglingExtkeyDoesNotFalselyPromote is codex round
// r3's P2 fix: a forward-hop closure candidate that is an UNRESOLVED
// extkey (a cross-provider linked-issue reference with no matching synced
// item -- a dangling reference) used to be counted as "+1" toward the
// closure size before it was ever resolved, so a single dangling extkey
// could push a small org over the 25% promotion bound even though the
// REAL closure (the set of items that actually exist) was empty. Fixed by
// sizing the promotion decision on the RESOLVED closure subject count
// (evaluateClosurePromotion now calls loadDonorSubjects itself, before
// deciding promotion, not after).
//
// Fixture: 4 items total (matching codex's own minimal repro exactly). A
// is repo-owned (the only ownership change). A --relates_to--> extkey:MISSING-1,
// a Linear/Jira-shaped cross-provider reference nothing in the org
// actually has. With the bug, closureSize=1 (the raw key) against
// affected=1/total=4 promotes (ratio 0.50); the real, resolved closure is
// empty, so this must stay SCOPED.
func TestWorkItemAttributionDanglingExtkeyDoesNotFalselyPromote(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)

	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-danglingextkey-" + uuid.NewString()
	repoX := uuid.New()
	now := time.Now().UTC()

	seedWorkItemAttributionItem(t, ctx, conn, orgID, "A", repoX, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "C", uuid.Nil, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "D", uuid.Nil, now)
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", now)
	// A -> a cross-provider issue key nothing in the org actually has.
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "A", "extkey:MISSING-1", "relates_to", now)

	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome.OrgWide {
		t.Fatalf("outcome.OrgWide = true, want a SCOPED run: the ONLY closure candidate is a "+
			"dangling extkey with no matching item, so the REAL (resolved) closure is empty -- "+
			"this is codex round r3's P2 finding, sizing the promotion decision on the raw "+
			"unresolved candidate count instead of the resolved subject count (outcome=%+v)", outcome)
	}

	rows := queryWorkItemAttributionRows(t, ctx, conn, orgID)
	written := map[string]bool{}
	for _, row := range rows {
		written[row.workItemID] = true
	}
	if !written["A"] {
		t.Error("no attribution row for A, the originally affected item")
	}
	if written["C"] || written["D"] {
		t.Error("C or D was written -- this should still be a SCOPED run, not org-wide, " +
			"since the real closure is empty")
	}
}

// TestDetectScopeCatchesFutureTeamMembershipActivation is codex round r2's
// P1 fix: detectScope's org-wide trigger for team_memberships used to
// compare only updated_at, the same future-activation gap the repo/project
// ownership queries were widened to close in round r1 -- left unfixed for
// this table (and manual_attribution_fallbacks, see the sibling test
// below) even though both are bitemporal and their facts are validity-
// filtered downstream the same way ownership facts are.
func TestDetectScopeCatchesFutureTeamMembershipActivation(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-futuremembership-" + uuid.NewString()
	insertedAt := time.Now().UTC()
	activatesAt := insertedAt.Add(3 * time.Second)
	floorTime := insertedAt.Add(1500 * time.Millisecond)

	seedWorkItemAttributionTeamMembershipWithValidity(
		t, ctx, conn, orgID, "team-eng", "user-alice", insertedAt, activatesAt, nil)
	// Simulate a prior completed ORG-WIDE run (membership changes are
	// always org-wide, never scoped) that covered the org AFTER the row was
	// inserted but BEFORE the membership actually took effect.
	if err := writer.WriteAttributionRun(ctx, WorkItemAttributionRunRecord{
		OrgID: orgID, RunID: "seed-run-floor", CompletedAt: floorTime,
	}); err != nil {
		t.Fatalf("seed org-wide run marker: %v", err)
	}

	scope := waitForWorkItemAttributionScope(t, ctx, executor, orgID, activatesAt.Add(1500*time.Millisecond),
		func(s workItemAttributionScopeDecision) bool { return s.orgWide })
	if !scope.orgWide {
		t.Fatalf("scope after team_memberships' valid_from passed = %+v, want org-wide -- "+
			"a provider-membership row's own future valid_from crossing into effect never "+
			"re-triggers a rederive on updated_at alone", scope)
	}
}

// TestDetectScopeCatchesExpiringManualFallback mirrors the membership test
// above for manual_attribution_fallbacks' valid_to (expiry) instead of
// team_memberships' valid_from (activation) -- codex round r2 named both
// directions on both tables as the same failure class.
func TestDetectScopeCatchesExpiringManualFallback(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-expiringfallback-" + uuid.NewString()
	insertedAt := time.Now().UTC()
	expiresAt := insertedAt.Add(3 * time.Second)
	floorTime := insertedAt.Add(1500 * time.Millisecond)

	seedWorkItemAttributionManualFallbackWithValidity(
		t, ctx, conn, orgID, "team-eng", insertedAt, insertedAt, &expiresAt)
	if err := writer.WriteAttributionRun(ctx, WorkItemAttributionRunRecord{
		OrgID: orgID, RunID: "seed-run-floor", CompletedAt: floorTime,
	}); err != nil {
		t.Fatalf("seed org-wide run marker: %v", err)
	}

	scope := waitForWorkItemAttributionScope(t, ctx, executor, orgID, expiresAt.Add(1500*time.Millisecond),
		func(s workItemAttributionScopeDecision) bool { return s.orgWide })
	if !scope.orgWide {
		t.Fatalf("scope after manual_attribution_fallbacks' valid_to passed = %+v, want "+
			"org-wide -- an expired manual fallback's own valid_to crossing into the past "+
			"never re-triggers a rederive on updated_at alone", scope)
	}
}

// TestDetectScopeCatchesProjectIDOnlyOwnership is codex round r1's P1 fix,
// proven live: a team_project_ownership row with a project_id but no
// project_key used to be entirely invisible to detectScope (it only
// tracked project_key), even though Resolve() itself matches project
// ownership by EITHER identifier.
func TestDetectScopeCatchesProjectIDOnlyOwnership(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-projid-" + uuid.NewString()
	now := time.Now().UTC()
	// project_key is deliberately absent (nil) -- only project_id is set.
	seedWorkItemAttributionProjectOwnership(t, ctx, conn, orgID, "jira", "P-42", nil, "team-jira", now)

	scope := waitForWorkItemAttributionScope(t, ctx, executor, orgID, now.Add(time.Second), func(s workItemAttributionScopeDecision) bool {
		return !s.orgWide && containsString(s.projectKeys, "P-42")
	})
	if scope.orgWide {
		t.Fatalf("scope = %+v, want a SCOPED project rederive (project_id-only ownership should never fail open to org-wide)", scope)
	}
}

// TestDetectScopeCatchesFutureOwnershipActivation is codex round r1's P1
// fix, proven live: a team_repo_ownership row inserted TODAY with a FUTURE
// valid_from used to never trigger a rederive on the day it actually
// activates, because nothing writes a NEW updated_at at that moment --
// detectScope only ever compared against updated_at.
func TestDetectScopeCatchesFutureOwnershipActivation(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-futureactivation-" + uuid.NewString()
	repoX := uuid.New()
	insertedAt := time.Now().UTC()
	activatesAt := insertedAt.Add(3 * time.Second)
	floorTime := insertedAt.Add(1500 * time.Millisecond) // between insertedAt and activatesAt

	seedWorkItemAttributionRepoOwnershipWithValidity(t, ctx, conn, orgID, repoX, "team-infra", insertedAt, activatesAt, nil)
	// Simulate a prior completed run that already covered this repo AFTER
	// the row was inserted (so plain updated_at comparison alone would
	// never re-trigger) but BEFORE the ownership actually took effect.
	if err := writer.WriteScopedAttributionRuns(ctx, []WorkItemAttributionScopedRunRecord{
		{OrgID: orgID, ScopeKind: "repo", ScopeID: repoX.String(), RunID: "seed-run-floor", CompletedAt: floorTime},
	}); err != nil {
		t.Fatalf("seed scoped run marker: %v", err)
	}

	scope := waitForWorkItemAttributionScope(t, ctx, executor, orgID, activatesAt.Add(1500*time.Millisecond), func(s workItemAttributionScopeDecision) bool {
		return !s.orgWide && containsString(s.repoIDs, repoX.String())
	})
	if scope.orgWide || !containsString(scope.repoIDs, repoX.String()) {
		t.Fatalf("scope after valid_from passed = %+v, want repoX in a scoped rederive -- "+
			"this is the exact defect codex round r1 found: an ownership row's own future "+
			"valid_from crossing into effect never re-triggers a rederive on updated_at alone", scope)
	}
}

// TestDetectScopeCatchesProviderMembershipChange is codex round r1's P1
// fix, proven live: loadFacts consumes team_memberships (the PROVIDER
// FALLBACK membership layer) and manual_attribution_fallbacks (the manual
// override layer), but detectScope's org-wide trigger used to check only
// identities/teams (the ADMIN layer) -- a changed provider-membership fact
// used to be silently accepted as a no-op even though it can retarget an
// assignee_membership candidate the same way an identities/teams change
// can.
func TestDetectScopeCatchesProviderMembershipChange(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-providermember-" + uuid.NewString()
	now := time.Now().UTC()
	seedWorkItemAttributionTeamMembership(t, ctx, conn, orgID, "team-eng", "user-alice", now)

	scope := waitForWorkItemAttributionScope(t, ctx, executor, orgID, now.Add(time.Second), func(s workItemAttributionScopeDecision) bool {
		return s.orgWide
	})
	if !scope.orgWide {
		t.Fatalf("scope after a team_memberships change = %+v, want org-wide -- "+
			"provider-layer membership changes retarget attribution the same way "+
			"identities/teams changes do, with no single repo/project scope to key on", scope)
	}
}

// TestDetectScopeCatchesManualFallbackChange mirrors the membership test
// above for manual_attribution_fallbacks specifically.
func TestDetectScopeCatchesManualFallbackChange(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-manualfallback-" + uuid.NewString()
	now := time.Now().UTC()
	seedWorkItemAttributionManualFallback(t, ctx, conn, orgID, "team-eng", now)

	scope := waitForWorkItemAttributionScope(t, ctx, executor, orgID, now.Add(time.Second), func(s workItemAttributionScopeDecision) bool {
		return s.orgWide
	})
	if !scope.orgWide {
		t.Fatalf("scope after a manual_attribution_fallbacks change = %+v, want org-wide", scope)
	}
}

// TestWorkItemAttributionObserverIsCalled is codex round r1's P2 fix,
// proven live: SetObserver/CollectorWorkItemAttributionObserver were fully
// wired but ComputeOrg never actually called ObserveWorkItemAttributionRun
// on any return path, so scoped/org-wide/no-op/item/row counters stayed
// zero forever -- the intended alerting signal for this safety-net job
// silently never fired.
func TestWorkItemAttributionObserverIsCalled(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	observer := &fakeWorkItemAttributionObserver{}
	executor.SetObserver(observer)

	orgID := "org-observer-noop-" + uuid.NewString()
	// No ownership/membership changes seeded at all -- detectScope must
	// find nothing and ComputeOrg must return SkippedNoop. The observer is
	// still owed a call: "nothing happened" is itself a signal worth
	// counting, the same way membership_backfill's own no-op path reports.
	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if !outcome.SkippedNoop {
		t.Fatalf("outcome = %+v, want SkippedNoop for a fresh org with nothing seeded", outcome)
	}
	if observer.calls != 1 {
		t.Fatalf("observer called %d time(s), want exactly 1 -- ComputeOrg's no-op return "+
			"path never called ObserveWorkItemAttributionRun before this fix", observer.calls)
	}
	if !observer.lastOutcome.SkippedNoop {
		t.Fatalf("observer's recorded outcome = %+v, want SkippedNoop=true", observer.lastOutcome)
	}
}

// fakeWorkItemAttributionObserver lives in work_item_attribution_review_repro_test.go
// (no build tag) so both the unit tests there and this integration test can
// use it.

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// waitForWorkItemAttributionScope polls detectScope (never FINAL on its own
// ownership-table queries, matching upstream LoadRepos' own precedent --
// see waitForWorkItemAttributionRepoFactVisible's comment) until check
// reports satisfied, or fails the test once the budget is exhausted.
func waitForWorkItemAttributionScope(
	t *testing.T, ctx context.Context, executor *WorkItemAttributionExecutor,
	orgID string, asOf time.Time, check func(workItemAttributionScopeDecision) bool,
) workItemAttributionScopeDecision {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last workItemAttributionScopeDecision
	for {
		scope, err := executor.detectScope(ctx, orgID, asOf)
		if err != nil {
			t.Fatalf("detectScope: %v", err)
		}
		last = scope
		if check(scope) {
			return scope
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func seedWorkItemAttributionRepoOwnershipWithValidity(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, teamID string, updatedAt, validFrom time.Time, validTo *time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_repo_ownership (
		org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", teamID, repoID, "acme/"+teamID, "exact", "native",
		uint8(1), uint16(100), int32(0), validFrom, validTo, updatedAt,
	); err != nil {
		t.Fatalf("append team_repo_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}
}

func seedWorkItemAttributionProjectOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, provider, projectID string, projectKey *string, teamID string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_project_ownership (
		org_id, provider, team_id, project_id, project_key, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_project_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, provider, teamID, projectID, projectKey, "native",
		uint8(1), uint16(100), int32(0), now, nil, now,
	); err != nil {
		t.Fatalf("append team_project_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_project_ownership batch: %v", err)
	}
}

func seedWorkItemAttributionTeamMembership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, teamID, memberID string, now time.Time,
) {
	t.Helper()
	seedWorkItemAttributionTeamMembershipWithValidity(t, ctx, conn, orgID, teamID, memberID, now, now, nil)
}

func seedWorkItemAttributionTeamMembershipWithValidity(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, teamID, memberID string, updatedAt, validFrom time.Time, validTo *time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_memberships (
		org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_memberships batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", teamID, memberID, memberID, nil, "native",
		uint8(1), uint16(100), int32(0), validFrom, validTo, updatedAt,
	); err != nil {
		t.Fatalf("append team_memberships row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_memberships batch: %v", err)
	}
}

func seedWorkItemAttributionManualFallback(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, teamID string, now time.Time,
) {
	t.Helper()
	seedWorkItemAttributionManualFallbackWithValidity(t, ctx, conn, orgID, teamID, now, now, nil)
}

func seedWorkItemAttributionManualFallbackWithValidity(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, teamID string, updatedAt, validFrom time.Time, validTo *time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO manual_attribution_fallbacks (
		org_id, provider, scope_type, scope_id, team_id, team_name, reason,
		priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare manual_attribution_fallbacks batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", "org", orgID, teamID, teamID, "test fallback",
		int32(100), validFrom, validTo, updatedAt,
	); err != nil {
		t.Fatalf("append manual_attribution_fallbacks row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send manual_attribution_fallbacks batch: %v", err)
	}
}

// waitForWorkItemAttributionRowVisible polls query (expected to return one
// count() column) until it reports at least one row, or fails the test once
// the budget below is exhausted. See its call site's comment for why this
// exists.
func waitForWorkItemAttributionRepoFactVisible(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for attempt := 1; ; attempt++ {
		repos, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadRepos(ctx, orgID, time.Now().UTC())
		if err != nil {
			t.Fatalf("poll LoadRepos: %v", err)
		}
		if len(repos) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("LoadRepos never returned a row within the poll budget (%d attempts)", attempt)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func seedWorkItemAttributionItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID string, repoID uuid.UUID, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		`INSERT INTO work_items (repo_id, work_item_id, provider, project_id, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, "github", "", orgID, now); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

func seedWorkItemAttributionRepoOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, teamID string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_repo_ownership (
		org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", teamID, repoID, "acme/"+teamID, "exact", "native",
		uint8(1), uint16(100), int32(0), now, nil, now,
	); err != nil {
		t.Fatalf("append team_repo_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}
}

func seedWorkItemAttributionDependency(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, sourceID, targetID, relationshipType string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_dependencies (
		source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw,
		last_synced, org_id
	)`)
	if err != nil {
		t.Fatalf("prepare work_item_dependencies batch: %v", err)
	}
	if err := batch.Append(sourceID, targetID, relationshipType, relationshipType, now, orgID); err != nil {
		t.Fatalf("append work_item_dependencies row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_item_dependencies batch: %v", err)
	}
}

type workItemAttributionRunRow struct {
	runID          string
	promotedReason string
}

func queryWorkItemAttributionRuns(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []workItemAttributionRunRow {
	t.Helper()
	rows, err := conn.Query(ctx,
		`SELECT run_id, promoted_reason FROM work_item_attribution_backstop_runs FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("query work_item_attribution_backstop_runs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []workItemAttributionRunRow
	for rows.Next() {
		var row workItemAttributionRunRow
		if err := rows.Scan(&row.runID, &row.promotedReason); err != nil {
			t.Fatalf("scan work_item_attribution_backstop_runs row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_attribution_backstop_runs: %v", err)
	}
	return result
}

func queryWorkItemAttributionScopedRuns(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []string {
	t.Helper()
	rows, err := conn.Query(ctx,
		`SELECT run_id FROM work_item_attribution_backstop_scoped_runs FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("query work_item_attribution_backstop_scoped_runs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			t.Fatalf("scan work_item_attribution_backstop_scoped_runs row: %v", err)
		}
		result = append(result, runID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_attribution_backstop_scoped_runs: %v", err)
	}
	return result
}

type workItemAttributionRowResult struct {
	workItemID string
	teamID     *string
}

func queryWorkItemAttributionRows(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []workItemAttributionRowResult {
	t.Helper()
	rows, err := conn.Query(ctx,
		`SELECT work_item_id, team_id FROM work_item_team_attributions FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("query work_item_team_attributions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []workItemAttributionRowResult
	for rows.Next() {
		var row workItemAttributionRowResult
		if err := rows.Scan(&row.workItemID, &row.teamID); err != nil {
			t.Fatalf("scan work_item_team_attributions row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_team_attributions: %v", err)
	}
	return result
}

// workItemAttributionMigratedClickHouse mirrors membershipMigratedClickHouse
// (membership_native_integration_test.go, CHAOS-4282): a fresh container,
// the real migration chain, and a real clickhouse-go connection -- no
// shared-instance caching, since this file's only test does not need it.
func workItemAttributionMigratedClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := instance.Close(ctx); closeErr != nil {
			t.Logf("close clickhouse container: %v", closeErr)
		}
	})
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	password, _ := parsed.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
