//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TestTeamRepoOwnershipDerivationAgainstMigratedSchema is the standing
// obligation's red-first proof, run against the REAL migration chain (via
// newWorkItemEffectsConn, shared with the GitHub direct-effects integration
// tests): a fixture-shaped org with team_project_ownership + work_items +
// work_item_dependencies + work_graph_issue_pr rows -- everything a real
// sync could have already written -- produces ZERO team_repo_ownership rows
// before this producer runs (nothing in this repo derives it), and the
// expected rows after Derive() is called. Exercises all three signal paths:
// own project_id (design check a), the dependency-donor walk gated to
// inheritance-safe relationship types, and PR inheritance via
// work_graph_issue_pr (design check b) using that table's OWN repo_id for a
// genuine cross-repo link.
func TestTeamRepoOwnershipDerivationAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4365-item1b-org"
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	repoA := uuid.New() // owned via design check (a): work item's own project_id
	repoB := uuid.New() // owned via the dependency-donor walk (design check a2)
	repoC := uuid.New() // owned via PR inheritance (design check b), cross-repo
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{
		repoA: "acme/repo-a",
		repoB: "acme/repo-b",
		repoC: "acme/repo-c",
	})
	// Two separate provider-tracked "proj-1" ownership rows -- the donor
	// item below is linear-tracked, the own-project_id item is
	// github-tracked; resolution is keyed by (provider, project_id), never
	// bare project_id (codex adversarial review, 2026-08-28, confirmed
	// finding), so each needs its own matching ownership row even though
	// both happen to share the literal project_id string here.
	seedTeamProjectOwnership(t, ctx, conn, orgID, "linear", "proj-1", "team-platform", true, now)
	seedTeamProjectOwnership(t, ctx, conn, orgID, "github", "proj-1", "team-platform", true, now)
	seedWorkItem(t, ctx, conn, orgID, "linear:PLAT-1", "linear", uuid.Nil, "proj-1", now)
	seedWorkItem(t, ctx, conn, orgID, "gh:acme/repo-a#1", "github", repoA, "proj-1", now)
	seedWorkItem(t, ctx, conn, orgID, "ghpr:acme/repo-b#7", "github", repoB, "", now)
	seedWorkItemDependency(t, ctx, conn, orgID, "ghpr:acme/repo-b#7", "linear:PLAT-1", "relates_to", now)
	seedWorkGraphIssuePR(t, ctx, conn, orgID, repoC, "linear:PLAT-1", 42, now)

	// Red: before Derive() runs, nothing in this repo has ever written
	// team_repo_ownership for this org -- confirmed against the real
	// migrated schema, not a mock.
	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgID, 0)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, retracted, inputsReady, _, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if written != 3 {
		t.Fatalf("expected 3 rows written (repo-a, repo-b, repo-c), got %d", written)
	}
	if retracted != 0 {
		t.Fatalf("expected 0 rows retracted -- this org has no prior inferred rows to retract, got %d", retracted)
	}
	if !inputsReady {
		t.Fatal("expected inputsReady=true -- this org's project ownership and linkage rows are present")
	}

	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	want := map[string]string{
		"acme/repo-a": "team-platform",
		"acme/repo-b": "team-platform",
		"acme/repo-c": "team-platform",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d rows, got %d: %+v", len(want), len(got), got)
	}
	for repoFullName, wantTeam := range want {
		row, ok := got[repoFullName]
		if !ok {
			t.Fatalf("missing row for %s: %+v", repoFullName, got)
		}
		if row.teamID != wantTeam {
			t.Fatalf("%s: expected team %s, got %s", repoFullName, wantTeam, row.teamID)
		}
		if row.source != "inferred" {
			t.Fatalf("%s: expected source=inferred, got %s", repoFullName, row.source)
		}
		if row.provider != "github" {
			t.Fatalf("%s: expected provider=github (from repos.provider, not the work item's tracker provider), got %s", repoFullName, row.provider)
		}
		// codex adversarial review, 2026-08-28 (confirmed high-severity):
		// is_primary must be 0. The read path (providers/teams.py::
		// load_team_repo_ownership_map) orders "is_primary DESC, specificity
		// DESC" -- is_primary=1 would let this row always outrank a real
		// GitHub-direct grant (team_autoimport_github.py writes every row
		// is_primary=0) regardless of this producer's deliberately-low
		// specificity.
		if row.isPrimary {
			t.Fatalf("%s: expected is_primary=0 for an inferred row, got true -- this would override a real direct ownership grant", repoFullName)
		}
	}

	// Idempotent per (org, sync run): calling Derive() again for the same
	// already-derived state does not duplicate rows -- ReplacingMergeTree
	// dedup on (org_id, provider, repo_full_name, team_id, source,
	// valid_from) collapses re-derivation to the same logical row set once
	// merged, and this read path uses FINAL, so it must already read back
	// exactly the same 3 rows.
	written2, _, _, _, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("second Derive: %v", err)
	}
	if written2 != 3 {
		t.Fatalf("expected the second Derive to also write 3 rows (re-derivation is a no-op in effect, not in row count -- FINAL read confirms it below), got %d", written2)
	}
	gotAfterSecondRun := readTeamRepoOwnership(t, ctx, conn, orgID)
	if len(gotAfterSecondRun) != len(want) {
		t.Fatalf("re-derivation duplicated rows under FINAL dedup: expected %d, got %d: %+v", len(want), len(gotAfterSecondRun), gotAfterSecondRun)
	}
}

// TestTeamRepoOwnershipDerivationResolvesLinearTeamKeyShapedOwnership is the
// CHAOS-4458 part (b) fix, proven against real ClickHouse: a Linear-only
// org's team_project_ownership row is keyed "{org_id}:linear:{team_key}"
// (team_autoimport_linear.py's default-project-key path, when a team has no
// explicit Linear Project associations), while the Linear work item's OWN
// project_id is the raw Linear Project UUID -- a disjoint id space (see
// TeamRepoOwnershipWorkItem's doc comment). Before CHAOS-4458b this derived
// ZERO rows for a Linear-only org (confirmed locally on org 70d529e0: 0 of
// 3168 project-id-bearing Linear work items matched their org's ownership
// row). Exercises BOTH the own-resolution path (the Linear issue itself) and
// the donor walk (a bare GitHub PR with no project_id of its own, linked via
// a relates_to edge), and asserts the resolution-arm tally the worker's
// telemetry now reports.
//
// CHAOS-4537 rewrite: the seeded team_project_ownership row for this
// identity is now a DECOY, pointed at a WRONG team -- resolveWorkItemTeamID
// no longer looks it up at all for the linear_team_key arm, and the resolved
// team_id is the raw NativeTeamKey value ("CHAOS") directly, proven against
// the real ClickHouse read path (loadTeamRepoOwnershipProjectLinks still
// loads that decoy row; the assertion below proves it is never consulted).
func TestTeamRepoOwnershipDerivationResolvesLinearTeamKeyShapedOwnership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4458b-item1b-linear-team-key-org"
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "acme/linear-repo"})
	// CHAOS-4537 codex review P1: "CHAOS" must be a validly-known team
	// (teams row) for the linear_team_key arm to trust NativeTeamKey.
	seedLinearTeam(t, ctx, conn, orgID, "CHAOS", now)
	seedTeamProjectOwnership(t, ctx, conn, orgID, "linear", orgID+":linear:CHAOS", "team-WRONG-decoy", true, now)
	// The Linear issue's OWN project_id is a raw Linear Project UUID -- never
	// matches the ownership row above -- but native_team_key ("CHAOS") is now
	// the resolved team_id directly, no ownership row needed at all.
	seedWorkItemWithNativeTeamKey(
		t, ctx, conn, orgID, "linear:CHAOS-1", "linear", uuid.Nil,
		"11111111-1111-4111-8111-111111111111", "CHAOS", now,
	)
	// A bare GitHub PR: no project_id of its own, reaches "CHAOS" only
	// through the dependency-donor walk onto the Linear issue above.
	seedWorkItem(t, ctx, conn, orgID, "ghpr:acme/linear-repo#9", "github", repoID, "", now)
	seedWorkItemDependency(t, ctx, conn, orgID, "ghpr:acme/linear-repo#9", "linear:CHAOS-1", "relates_to", now)

	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgID, 0)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, retracted, inputsReady, armCounts, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if !inputsReady {
		t.Fatal("expected inputsReady=true")
	}
	if retracted != 0 {
		t.Fatalf("expected 0 rows retracted, got %d", retracted)
	}
	if written != 1 {
		t.Fatalf("expected 1 row written (acme/linear-repo via the donor walk onto the Linear issue's native_team_key identity), got %d", written)
	}
	if got := armCounts[TeamRepoOwnershipResolutionArmLinearTeamKey]; got != 1 {
		t.Fatalf("expected armCounts[linear_team_key] = 1, got %d (%+v)", got, armCounts)
	}
	if got := armCounts[TeamRepoOwnershipResolutionArmProjectID]; got != 0 {
		t.Fatalf("expected armCounts[project_id] = 0 (no direct project_id match exists in this fixture), got %d", got)
	}
	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	row, ok := got["acme/linear-repo"]
	if !ok || row.teamID != "CHAOS" {
		t.Fatalf("expected acme/linear-repo -> CHAOS (the raw native_team_key, not the decoy ownership row's team-WRONG-decoy), got %+v", got)
	}
}

// TestTeamRepoOwnershipDerivationResolvesLinearTeamKeyWithNoProjectOwnershipAtAll
// is CHAOS-4537's own red-first proof for the ClickHouse-loading Derive
// method (not just the pure deriveTeamRepoOwnership function already covered
// above): an org with a real, already-synced Linear work item carrying
// native_team_key, but ZERO team_project_ownership rows of ANY kind -- the
// plausible real-world ordering where work-items sync and team autoimport
// are independent per-config selections (CHAOS-4323) and the latter simply
// has not run yet for this org. Before this fix, Derive's own early return
// on `len(projectLinks) == 0` bailed out before even loading work_items,
// reporting inputsReady=false and deriving nothing -- silently zeroing the
// linear_team_key arm for exactly the org shape CHAOS-4537 was built to
// stop depending on team_project_ownership for.
func TestTeamRepoOwnershipDerivationResolvesLinearTeamKeyWithNoProjectOwnershipAtAll(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4537-linear-team-key-no-tpo-org"
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "acme/linear-repo"})
	// CHAOS-4537 codex review P1: "CHAOS" must be a validly-known team
	// (teams row) for the linear_team_key arm to trust NativeTeamKey -- a
	// SEPARATE table from team_project_ownership, which this test's premise
	// (zero rows, any provider) is specifically about and stays true below.
	seedLinearTeam(t, ctx, conn, orgID, "CHAOS", now)
	// No seedTeamProjectOwnership call at all -- team_project_ownership has
	// zero rows for this org, of any provider.
	seedWorkItemWithNativeTeamKey(
		t, ctx, conn, orgID, "linear:CHAOS-1", "linear", repoID,
		"", "CHAOS", now,
	)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, retracted, inputsReady, armCounts, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if !inputsReady {
		t.Fatal("expected inputsReady=true -- a real, already-synced Linear work item with native_team_key is genuine input, even with zero team_project_ownership rows")
	}
	if retracted != 0 {
		t.Fatalf("expected 0 rows retracted, got %d", retracted)
	}
	if written != 1 {
		t.Fatalf("expected 1 row written via native_team_key alone, got %d", written)
	}
	if got := armCounts[TeamRepoOwnershipResolutionArmLinearTeamKey]; got != 1 {
		t.Fatalf("expected armCounts[linear_team_key] = 1, got %d (%+v)", got, armCounts)
	}
	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	row, ok := got["acme/linear-repo"]
	if !ok || row.teamID != "CHAOS" {
		t.Fatalf("expected acme/linear-repo -> CHAOS via native_team_key with zero team_project_ownership rows, got %+v", got)
	}
}

// TestTeamRepoOwnershipDerivationRejectsUnknownNativeTeamKey is codex review
// (round 2, P1, confirmed real) fix's own red-first proof against the real
// ClickHouse read path: identical fixture to the sibling test above EXCEPT
// no seedLinearTeam call at all -- "CHAOS" never appears in `teams` for this
// org. The linear_team_key arm must resolve nothing rather than mint phantom
// ownership for a native_team_key that names no team currently in the org's
// catalog -- see TeamRepoOwnershipKnownTeam's doc comment.
func TestTeamRepoOwnershipDerivationRejectsUnknownNativeTeamKey(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4537-linear-team-key-unknown-team-org"
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "acme/linear-repo"})
	// No seedLinearTeam call -- "CHAOS" is not a known team for this org.
	seedWorkItemWithNativeTeamKey(
		t, ctx, conn, orgID, "linear:CHAOS-1", "linear", repoID,
		"", "CHAOS", now,
	)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, retracted, _, armCounts, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if written != 0 {
		t.Fatalf("expected 0 rows written -- native_team_key names a team absent from this org's catalog, never guessed, got %d", written)
	}
	if retracted != 0 {
		t.Fatalf("expected 0 rows retracted, got %d", retracted)
	}
	if got := armCounts[TeamRepoOwnershipResolutionArmLinearTeamKey]; got != 0 {
		t.Fatalf("expected armCounts[linear_team_key] = 0, got %d (%+v)", got, armCounts)
	}
	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgID, 0)
}

// TestTeamRepoOwnershipDerivationPreservesReadinessGateForNonLinearOrgsTransientLinkageGap
// is codex review P1 on this PR (confirmed real): removing the
// projectLinks-only early return (the test above) must NOT also remove the
// SEPARATE guard that protects every provider, not just Linear, from a
// different failure mode -- team_project_ownership has already synced for
// this org, but work_items/dependencyEdges/issuePRLinks have not (the
// OPPOSITE ordering from the test above), a plausible transient state
// during a partial sync. Deliberately GitHub-shaped (no NativeTeamKey
// anywhere) per AGENTS.md's provider-matrix rule -- CHAOS-4537's own fix is
// provider-agnostic machinery (the readiness/retraction gate), and this
// proves it protects a non-Linear org, not only the Linear-only scenario
// the sibling test above covers. Before the codex fix, this snapshot would
// have read as inputsReady=true with derived=[], and
// diffTeamRepoOwnershipRetractions would have retracted the pre-existing
// active row below -- silently wiping real, previously-derived ownership
// during a transient sync gap.
func TestTeamRepoOwnershipDerivationPreservesReadinessGateForNonLinearOrgsTransientLinkageGap(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4537-transient-linkage-gap-org"
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "acme/github-repo"})
	seedTeamProjectOwnership(t, ctx, conn, orgID, "github", "proj-1", "team-platform", true, now)
	// No seedWorkItem/seedWorkItemDependency/seedWorkGraphIssuePR calls at
	// all -- work_items, work_item_dependencies, and work_graph_issue_pr are
	// all empty for this org, simulating team_project_ownership having
	// synced strictly before work-items sync reached it.

	// A pre-existing active inferred row, as if a PRIOR Derive() run (before
	// the transient gap) had already resolved and written it -- seeded
	// directly via the same insert shape writeTeamRepoOwnershipRows uses,
	// since this test needs it present WITHOUT the current call ever having
	// derived it itself.
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipInsert)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", "team-platform", repoID, "acme/github-repo",
		"exact", "inferred", uint8(0), teamRepoOwnershipInferredSpecificity, int32(0),
		now, nil, now,
	); err != nil {
		t.Fatalf("append pre-existing team_repo_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send pre-existing team_repo_ownership row: %v", err)
	}

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, retracted, inputsReady, _, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if inputsReady {
		t.Fatal("expected inputsReady=false -- work_items/dependencyEdges/issuePRLinks are all empty, a transient linkage gap, not a genuine no-signal evaluation")
	}
	if written != 0 {
		t.Fatalf("expected 0 rows written during a transient linkage gap, got %d", written)
	}
	if retracted != 0 {
		t.Fatalf("expected 0 rows retracted during a transient linkage gap -- retracting here would wipe real, previously-derived ownership, got %d", retracted)
	}
	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	row, ok := got["acme/github-repo"]
	if !ok || row.teamID != "team-platform" {
		t.Fatalf("expected the pre-existing acme/github-repo -> team-platform row to survive untouched, got %+v", got)
	}
}

// TestTeamRepoOwnershipDerivationNoProjectOwnershipIsNotAnError covers the
// designed-empty case (§0.2): an org with zero team_project_ownership rows
// (a GitHub-only org, or team auto-import never configured) derives zero
// rows and returns no error -- never guessed, never a failure. This is also
// exactly the inputs_not_ready case (team-lead ruling, codex finding #4,
// 2026-08-28): inputsReady must be false here, since it is this exact
// signal -- zero team_project_ownership rows -- the worker's telemetry
// distinguishes from a genuine no-signal evaluation.
func TestTeamRepoOwnershipDerivationNoProjectOwnershipIsNotAnError(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4365-item1b-empty-org"

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, _, inputsReady, _, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive on an org with no project ownership: %v", err)
	}
	if written != 0 {
		t.Fatalf("expected 0 rows written, got %d", written)
	}
	if inputsReady {
		t.Fatal("expected inputsReady=false -- zero team_project_ownership rows is the first-sync gap, not a genuine no-signal evaluation")
	}
	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgID, 0)
}

// TestTeamRepoOwnershipDerivationResolvesGitLabShapedNonPrimaryOwnership is
// the codex adversarial-review fix (2026-08-28, confirmed high-severity),
// proven against real ClickHouse: GitLab's provider_access writer
// (team_autoimport_gitlab.py's _project_ownership_rows) sets is_primary=0
// unconditionally on every team_project_ownership row it ever writes. A
// GitLab-sourced org's donor project must still resolve through the real
// read query, not just the pure Go unit test.
func TestTeamRepoOwnershipDerivationResolvesGitLabShapedNonPrimaryOwnership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4365-item1b-gitlab-org"
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "acme/gitlab-repo"})
	seedTeamProjectOwnership(t, ctx, conn, orgID, "gitlab", "proj-gitlab", "team-gitlab", false, now)
	seedWorkItem(t, ctx, conn, orgID, "gl:acme/gitlab-repo!1", "gitlab", repoID, "proj-gitlab", now)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, _, _, _, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if written != 1 {
		t.Fatalf("expected 1 row from a lone non-primary (GitLab-shaped) ownership claim, got %d", written)
	}
	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	row, ok := got["acme/gitlab-repo"]
	if !ok || row.teamID != "team-gitlab" {
		t.Fatalf("expected acme/gitlab-repo -> team-gitlab, got %+v", got)
	}
}

// TestTeamRepoOwnershipDerivationCollapsesStaleGenerations is the codex
// adversarial-review fix (2026-08-28, confirmed finding), proven against
// real ClickHouse: a re-import writes a NEW valid_from, which is a DISTINCT
// ReplacingMergeTree ORDER BY key FINAL never merges away -- team_repo_
// ownership derivation's own read query (loadTeamRepoOwnershipProjectLinks)
// used FINAL alone, so a stale, higher-scoring generation of one team's
// claim could keep outranking a genuinely newer, lower-scoring correction
// for the SAME (provider, project_id, team_id) forever. Here team-old's
// claim is DOWNGRADED by a newer generation (is_primary true->false,
// specificity 100->0); after the correction, team-new's own (single,
// current) claim should win the project -- proving the read query collapses
// generations via GROUP BY + argMax(field, (updated_at, valid_from)), the
// same shape as metrics/loaders/clickhouse.py's load_team_attribution_context,
// not a naive FINAL read.
func TestTeamRepoOwnershipDerivationCollapsesStaleGenerations(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4365-item1b-stale-generation-org"

	older := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "acme/stale-gen-repo"})

	// team-old's STALE generation: is_primary=true, specificity=100 --
	// would win outright if this were the only row FINAL (or a naive
	// non-collapsing read) ever saw.
	seedTeamProjectOwnershipGeneration(t, ctx, conn, orgID, "linear", "proj-1", "team-old", true, 100, older)
	// team-old's NEWER, correcting generation: downgraded to is_primary=false,
	// specificity=0 -- this is team-old's CURRENT effective claim.
	seedTeamProjectOwnershipGeneration(t, ctx, conn, orgID, "linear", "proj-1", "team-old", false, 0, newer)
	// team-new's own (single, current) claim: is_primary=false, specificity=50
	// -- must now outrank team-old's collapsed (corrected) claim.
	seedTeamProjectOwnershipGeneration(t, ctx, conn, orgID, "linear", "proj-1", "team-new", false, 50, newer)

	seedWorkItem(t, ctx, conn, orgID, "linear:PLAT-1", "linear", repoID, "proj-1", newer)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, _, _, _, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if written != 1 {
		t.Fatalf("expected 1 row, got %d", written)
	}
	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	row, ok := got["acme/stale-gen-repo"]
	if !ok || row.teamID != "team-new" {
		t.Fatalf("expected acme/stale-gen-repo -> team-new (team-old's stale generation must not outrank the newer correction), got %+v", got)
	}
}

// TestTeamRepoOwnershipDerivationRetractsAReassignedRepo is the team-lead
// ruling (2026-08-28, codex R3 finding "removed ownership remains
// authorized indefinitely"), proven against real ClickHouse: a repo whose
// project ownership is REASSIGNED from one team to another must have its
// PRIOR inferred row retracted (valid_to set), not left active forever
// alongside the new team's row -- the exact authorization-exposure shape
// native_status_change.py's _TEAM_REPOSITORIES_SQL would otherwise trust
// (it filters on valid_to, but only if this producer ever sets it).
func TestTeamRepoOwnershipDerivationRetractsAReassignedRepo(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4365-item1b-retraction-org"

	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "acme/retraction-repo"})
	seedTeamProjectOwnershipGeneration(t, ctx, conn, orgID, "linear", "proj-1", "team-old", true, 100, t0)
	seedWorkItem(t, ctx, conn, orgID, "linear:PLAT-1", "linear", repoID, "proj-1", t0)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, retracted, _, _, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("first Derive: %v", err)
	}
	if written != 1 || retracted != 0 {
		t.Fatalf("first Derive: written=%d retracted=%d, want written=1 retracted=0", written, retracted)
	}
	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	if row, ok := got["acme/retraction-repo"]; !ok || row.teamID != "team-old" {
		t.Fatalf("expected acme/retraction-repo -> team-old after the first Derive, got %+v", got)
	}

	// Reassignment: team-new's claim on proj-1 now outranks team-old's
	// (higher specificity, same is_primary) -- exactly what a real
	// team_project_ownership re-import looks like, since no existing writer
	// of that table retracts its own prior rows either (team-lead ruling:
	// pre-existing, cross-writer, tracked separately -- not fixed here).
	seedTeamProjectOwnershipGeneration(t, ctx, conn, orgID, "linear", "proj-1", "team-new", true, 200, t1)

	written, retracted, _, _, err = service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("second Derive: %v", err)
	}
	if written != 1 {
		t.Fatalf("second Derive: expected 1 row written (team-new), got %d", written)
	}
	if retracted != 1 {
		t.Fatalf("second Derive: expected 1 row retracted (team-old), got %d", retracted)
	}

	rows, err := conn.Query(ctx, `
SELECT team_id, is_primary, valid_to IS NULL AS active
FROM team_repo_ownership FINAL
WHERE org_id = ? AND repo_full_name = 'acme/retraction-repo' AND source = 'inferred'
ORDER BY team_id`, orgID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	type resultRow struct {
		teamID    string
		isPrimary uint8
		active    bool
	}
	var results []resultRow
	for rows.Next() {
		var r resultRow
		if err := rows.Scan(&r.teamID, &r.isPrimary, &r.active); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows (team-old retracted + team-new active), got %+v", results)
	}
	if results[0].teamID != "team-new" || !results[0].active || results[0].isPrimary != 0 {
		t.Fatalf("expected team-new active with is_primary=0, got %+v", results[0])
	}
	if results[1].teamID != "team-old" || results[1].active || results[1].isPrimary != 0 {
		t.Fatalf("expected team-old retracted (inactive) with is_primary=0, got %+v", results[1])
	}
}

// TestTeamRepoOwnershipDerivationDoesNotFollowAnotherOrgsDependencyEdge is
// the codex adversarial-review fix (2026-08-28, confirmed high-severity),
// proven against real ClickHouse: work_item_dependencies carries org_id
// (024_add_org_id.sql); a prior version of the read query scoped it
// indirectly through a work_items JOIN (only the SOURCE side's org), which
// does not exclude another tenant's edge row when work_item_id strings
// collide across orgs (they carry no org prefix). Org A's own donor item
// (own project_id, no repo_id of its own) only reaches org A's repo through
// a dependency edge that ORG B planted under org_id=B -- proving the edge
// itself, not just its endpoints' own data, must be org-scoped.
func TestTeamRepoOwnershipDerivationDoesNotFollowAnotherOrgsDependencyEdge(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	orgA := "chaos-4365-item1b-tenant-a"
	orgB := "chaos-4365-item1b-tenant-b"

	repoA := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgA, map[uuid.UUID]string{repoA: "acme/tenant-a-repo"})
	seedTeamProjectOwnership(t, ctx, conn, orgA, "linear", "proj-a", "team-a-legit", true, now)
	// A repo-bearing item with NO project_id of its own: it can only reach
	// a team through a dependency-donor edge.
	seedWorkItem(t, ctx, conn, orgA, "repo-item", "github", repoA, "", now)
	// The donor: resolves to team-a-legit via its own project_id, no repo
	// of its own.
	seedWorkItem(t, ctx, conn, orgA, "donor-item", "linear", uuid.Nil, "proj-a", now)

	// Org B -- a different tenant entirely -- happens to use the SAME two
	// work_item_id strings (no org prefix; a realistic collision) and
	// plants the ONLY edge connecting them, under org_id=B.
	seedWorkItemDependency(t, ctx, conn, orgB, "repo-item", "donor-item", "relates_to", now)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, _, inputsReady, _, err := service.Derive(ctx, orgA)
	if err != nil {
		t.Fatalf("Derive for org A: %v", err)
	}
	if written != 0 {
		t.Fatalf("expected 0 rows for org A -- the ONLY edge connecting repo-item to donor-item belongs to org B and must never resolve org A's donor walk, got %d", written)
	}
	if !inputsReady {
		t.Fatal("expected inputsReady=true -- org A's own project ownership and work_items rows ARE present; only the cross-tenant edge is (correctly) invisible")
	}
	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgA, 0)
}

// TestTeamRepoOwnershipDerivationDoesNotFollowAnotherOrgsIssuePRLink is the
// codex adversarial-review fix's work_graph_issue_pr half, proven against
// real ClickHouse. Org A's own work item resolves to a real team via its
// own project_id but has NO repo_id of its own; the only association
// between it and org A's real repo comes from a work_graph_issue_pr row org
// B planted (repo_id constrained to org A's own known repo set, which an
// attacker could plausibly still guess or brute-force -- org_id is the
// authoritative scope, not repo_id membership alone).
func TestTeamRepoOwnershipDerivationDoesNotFollowAnotherOrgsIssuePRLink(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	orgA := "chaos-4365-item1b-tenant-a"
	orgB := "chaos-4365-item1b-tenant-b"

	repoA := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgA, map[uuid.UUID]string{repoA: "acme/tenant-a-repo"})
	seedTeamProjectOwnership(t, ctx, conn, orgA, "linear", "proj-a", "team-a-legit", true, now)
	// Resolves to team-a-legit via its own project_id; NO repo_id of its
	// own, so Path 1+2 alone produces nothing for repoA.
	seedWorkItem(t, ctx, conn, orgA, "resolver-item", "linear", uuid.Nil, "proj-a", now)

	// Org B fabricates the ONLY association between "resolver-item" and
	// org A's real repo, under org_id=B.
	seedWorkGraphIssuePR(t, ctx, conn, orgB, repoA, "resolver-item", 99, now)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, _, inputsReady, _, err := service.Derive(ctx, orgA)
	if err != nil {
		t.Fatalf("Derive for org A: %v", err)
	}
	if written != 0 {
		t.Fatalf("expected 0 rows for org A -- the ONLY work_graph_issue_pr row associating resolver-item with repoA belongs to org B and must never attribute org A's repo, got %d", written)
	}
	if !inputsReady {
		t.Fatal("expected inputsReady=true -- org A's own project ownership and work_items rows ARE present; only the cross-tenant PR link is (correctly) invisible")
	}
	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgA, 0)
}

func seedTeamRepoOwnershipRepos(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repos map[uuid.UUID]string) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced)`)
	if err != nil {
		t.Fatalf("prepare repos batch: %v", err)
	}
	for id, fullName := range repos {
		if err := batch.Append(id, fullName, orgID, "github", time.Now().UTC()); err != nil {
			t.Fatalf("append repos row: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send repos batch: %v", err)
	}
}

// seedLinearTeam inserts a `teams` row for a Linear team (CHAOS-4537 codex
// review, round 2, P1): loadTeamRepoOwnershipKnownTeams reads this table to
// validate a Linear work item's native_team_key before the linear_team_key
// arm trusts it -- see TeamRepoOwnershipKnownTeam's doc comment. Column
// order/shape mirrors linearReferenceTeamsInsert
// (linear_reference_catalog_effects_clickhouse.go).
func seedLinearTeam(t *testing.T, ctx context.Context, conn driver.Conn, orgID, teamKey string, now time.Time) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id)`)
	if err != nil {
		t.Fatalf("prepare teams batch: %v", err)
	}
	if err := batch.Append(
		teamKey, uuid.New(), teamKey, (*string)(nil),
		[]string{}, []string{}, []string{}, []string{},
		uint8(1), now, orgID, "linear", &teamKey, (*string)(nil),
	); err != nil {
		t.Fatalf("append teams row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send teams batch: %v", err)
	}
}

func seedTeamProjectOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, provider, projectID, teamID string, isPrimary bool, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`)
	if err != nil {
		t.Fatalf("prepare team_project_ownership batch: %v", err)
	}
	primary := uint8(0)
	if isPrimary {
		primary = 1
	}
	if err := batch.Append(orgID, provider, teamID, projectID, "native", primary, uint16(100), int32(0), now, nil, now); err != nil {
		t.Fatalf("append team_project_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_project_ownership batch: %v", err)
	}
}

// seedTeamProjectOwnershipGeneration inserts one team_project_ownership row
// with an explicit specificity and valid_from/updated_at, letting a test
// plant multiple GENERATIONS (repeated-import rows differing in valid_from)
// of the same (org, provider, project, team) claim -- seedTeamProjectOwnership
// always hardcodes specificity=100 and a single timestamp, so it cannot
// express this.
func seedTeamProjectOwnershipGeneration(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, provider, projectID, teamID string, isPrimary bool, specificity uint16, at time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`)
	if err != nil {
		t.Fatalf("prepare team_project_ownership batch: %v", err)
	}
	primary := uint8(0)
	if isPrimary {
		primary = 1
	}
	if err := batch.Append(orgID, provider, teamID, projectID, "native", primary, specificity, int32(0), at, nil, at); err != nil {
		t.Fatalf("append team_project_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_project_ownership batch: %v", err)
	}
}

func seedWorkItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID, provider string, repoID uuid.UUID, projectID string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, project_id, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, provider, projectID, orgID, now); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

// seedWorkItemWithNativeTeamKey inserts a work_items row carrying
// native_team_key (migration 050, Linear only) -- seedWorkItem leaves it at
// ClickHouse's column default (”) for the providers that never set it.
func seedWorkItemWithNativeTeamKey(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID, provider string, repoID uuid.UUID, projectID, nativeTeamKey string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, project_id, native_team_key, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, provider, projectID, nativeTeamKey, orgID, now); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

func seedWorkItemDependency(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, sourceWorkItemID, targetWorkItemID, relationshipType string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id)`)
	if err != nil {
		t.Fatalf("prepare work_item_dependencies batch: %v", err)
	}
	if err := batch.Append(sourceWorkItemID, targetWorkItemID, relationshipType, relationshipType, now, orgID); err != nil {
		t.Fatalf("append work_item_dependencies row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_item_dependencies batch: %v", err)
	}
}

func seedWorkGraphIssuePR(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, workItemID string, prNumber uint32, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)`)
	if err != nil {
		t.Fatalf("prepare work_graph_issue_pr batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, prNumber, float32(1.0), "native", "test-seed", now, orgID); err != nil {
		t.Fatalf("append work_graph_issue_pr row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_issue_pr batch: %v", err)
	}
}

func assertTeamRepoOwnershipRowCount(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, want int) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("count team_repo_ownership: %v", err)
	}
	defer rows.Close()
	var got uint64
	if rows.Next() {
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("scan count: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("count rows.Err: %v", err)
	}
	if int(got) != want {
		t.Fatalf("expected %d team_repo_ownership rows for org %s, got %d", want, orgID, got)
	}
}

type teamRepoOwnershipReadRow struct {
	teamID    string
	provider  string
	source    string
	isPrimary bool
}

func readTeamRepoOwnership(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) map[string]teamRepoOwnershipReadRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT repo_full_name, team_id, provider, source, is_primary
FROM team_repo_ownership FINAL
WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("read team_repo_ownership: %v", err)
	}
	defer rows.Close()
	out := map[string]teamRepoOwnershipReadRow{}
	for rows.Next() {
		var repoFullName, teamID, provider, source string
		var isPrimary uint8
		if err := rows.Scan(&repoFullName, &teamID, &provider, &source, &isPrimary); err != nil {
			t.Fatalf("scan team_repo_ownership row: %v", err)
		}
		out[repoFullName] = teamRepoOwnershipReadRow{
			teamID: teamID, provider: provider, source: source, isPrimary: isPrimary != 0,
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("team_repo_ownership rows.Err: %v", err)
	}
	return out
}
