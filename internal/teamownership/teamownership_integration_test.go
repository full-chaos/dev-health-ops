//go:build integration

package teamownership

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestOwnedRepoIDs runs against the real migrated team_repo_ownership schema
// and pins every boundary CHAOS-4897's follow-up depends on: the bitemporal
// window, the NULL-repo_id PATTERN-UNRESOLVED exclusion (a `match_type =
// 'pattern'` row that never resolved to any concrete repo, name-joined or
// not -- see TestOwnedRepoIDsResolvesProviderAccessRowsByRepoName for the
// OTHER, resolvable shape of a NULL repo_id), org isolation, and that shared
// ownership is membership rather than a resolved single winner.
func TestOwnedRepoIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "org-teamownership"
	const otherOrgID = "org-teamownership-other"
	teamA, teamB := "team-a", "team-b"
	repoOwnedByA := uuid.New()
	repoOwnedByBoth := uuid.New()
	repoExpired := uuid.New()
	repoNotYetActive := uuid.New()
	repoOtherOrg := uuid.New()

	asOf := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)

	insert := func(orgID, teamID string, repoID uuid.UUID, validFrom time.Time, validTo *time.Time) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, ?, ?)
        `, orgID, teamID, repoID, repoID.String(), validFrom, validTo, validFrom); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}
	insertUnresolved := func(orgID, teamID string, validFrom time.Time) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, NULL, 'acme/*', 'pattern', 'inferred', 0, 0, 0, ?, NULL, ?)
        `, orgID, teamID, validFrom, validFrom); err != nil {
			t.Fatalf("seed unresolved team_repo_ownership: %v", err)
		}
	}

	before := asOf.AddDate(0, 0, -30)
	past := asOf.AddDate(0, 0, -1)
	future := asOf.AddDate(0, 0, 30)

	// teamA: one plain active row.
	insert(orgID, teamA, repoOwnedByA, before, nil)
	// A repo BOTH teams claim -- membership, not a resolved single owner:
	// both must see it.
	insert(orgID, teamA, repoOwnedByBoth, before, nil)
	insert(orgID, teamB, repoOwnedByBoth, before, nil)
	// A row that EXPIRED before asOf -- must not count.
	insert(orgID, teamA, repoExpired, before, &past)
	// A row that has not ACTIVATED yet as of asOf -- must not count.
	insert(orgID, teamA, repoNotYetActive, future, nil)
	// A pattern-unresolved row (repo_id NULL, repo_full_name a literal glob
	// no real repo is ever named) -- must not count, and must not error the
	// scan. This fixture seeds no `repos` rows at all, so the name-join
	// TestOwnedRepoIDsResolvesProviderAccessRowsByRepoName exercises can
	// never match here either way; this test's NULL rows are excluded on
	// BOTH grounds (no repo_id AND no join match), that other test isolates
	// the join specifically.
	insertUnresolved(orgID, teamA, before)
	// Same team_id, different org -- must not leak across tenants.
	insert(otherOrgID, teamA, repoOtherOrg, before, nil)

	assertOwned := func(t *testing.T, orgID, teamID string, want ...uuid.UUID) {
		t.Helper()
		got, err := OwnedRepoIDs(ctx, conn, orgID, teamID, asOf)
		if err != nil {
			t.Fatalf("OwnedRepoIDs(%s, %s): %v", orgID, teamID, err)
		}
		sort.Slice(got, func(i, j int) bool { return got[i].String() < got[j].String() })
		sort.Slice(want, func(i, j int) bool { return want[i].String() < want[j].String() })
		if len(got) != len(want) {
			t.Fatalf("OwnedRepoIDs(%s, %s) = %v, want %v", orgID, teamID, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("OwnedRepoIDs(%s, %s) = %v, want %v", orgID, teamID, got, want)
			}
		}
	}

	assertOwned(t, orgID, teamA, repoOwnedByA, repoOwnedByBoth)
	assertOwned(t, orgID, teamB, repoOwnedByBoth)

	// A team with no rows at all owns nothing -- and that must come back as
	// an EMPTY, non-error result, never an error masquerading as "no data".
	empty, err := OwnedRepoIDs(ctx, conn, orgID, "team-with-no-rows", asOf)
	if err != nil {
		t.Fatalf("OwnedRepoIDs for a team with no rows: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("team-with-no-rows owns %v, want none", empty)
	}
}

// TestOwnedRepoIDsSurvivesAPreMergeRevocation is the red-first proof for a
// codex adversarial review P1 (2026-09-04): team_repo_ownership is a
// ReplacingMergeTree(updated_at), and a revocation is written as a
// REPLACEMENT row under the SAME ORDER BY key (org_id, provider,
// repo_full_name, team_id, source, valid_from) with a newer updated_at and
// valid_to=now (retractTeamRepoOwnershipRows in
// team_repo_ownership_derivation_clickhouse.go). Immediately after such a
// write, BEFORE the background merge physically collapses the two rows, a
// query with no FINAL/argMax collapse sees BOTH the stale active row and the
// revocation row -- and a plain `valid_to IS NULL OR valid_to > asOf` WHERE
// admits the STALE one, resurrecting a repo the team no longer owns.
//
// SYSTEM STOP MERGES is what makes this test capable of catching that: this
// table's ReplacingMergeTree would otherwise collapse the two rows into one
// on its own schedule, at which point even a query with NO FINAL/argMax
// safeguard would happen to return the right answer by the time this test's
// assertion runs -- passing for the wrong reason on a fast, quiet CI box, and
// only flaking under load. Stopping merges makes the pre-merge state
// DETERMINISTIC rather than a race this test would only sometimes observe.
func TestOwnedRepoIDsSurvivesAPreMergeRevocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.Exec(ctx, "SYSTEM STOP MERGES team_repo_ownership"); err != nil {
		t.Fatalf("stop merges: %v", err)
	}
	// A FRESH context for the restart: the test's own ctx may be near its
	// deadline or cancelled by the time this deferred call runs, and a
	// restart issued on a dead context is a silent no-op that looks like a
	// restart (same discipline as the loader integration test's
	// seedLoaderFixture).
	defer func() {
		restartCtx, restartCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer restartCancel()
		if err := conn.Exec(restartCtx, "SYSTEM START MERGES team_repo_ownership"); err != nil {
			t.Errorf("restart merges: %v", err)
		}
	}()

	const orgID = "org-teamownership-revocation"
	const teamID = "team-revoked"
	repoID := uuid.New()
	repoFullName := repoID.String()

	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	activeWrittenAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	revokedAt := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	revokedWrittenAt := time.Date(2026, 8, 15, 0, 0, 1, 0, time.UTC)
	asOfAfterRevocation := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)

	insertRow := func(validTo *time.Time, updatedAt time.Time) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, ?, ?)
        `, orgID, teamID, repoID, repoFullName, validFrom, validTo, updatedAt); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}

	// The active row -- same ORDER BY key the revocation below reuses.
	insertRow(nil, activeWrittenAt)
	// The revocation: SAME key, newer updated_at, valid_to set. Written
	// SECOND and merges are stopped, so both physical rows now coexist,
	// exactly as they would immediately after a real Derive() retraction run.
	insertRow(&revokedAt, revokedWrittenAt)

	got, err := OwnedRepoIDs(ctx, conn, orgID, teamID, asOfAfterRevocation)
	if err != nil {
		t.Fatalf("OwnedRepoIDs after revocation: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("OwnedRepoIDs after a pre-merge revocation = %v, want none -- "+
			"the stale active row (valid_to=NULL) was not collapsed away by the "+
			"newer revoked version, so the query is reading un-merged "+
			"ReplacingMergeTree state instead of the latest row per key", got)
	}
}

// TestOwnedRepoIDsResolvesProviderAccessRowsByRepoName is the red-first proof
// for a codex adversarial review P1 (2026-09-04, round 3): GitHub's own
// team-autoimport producer (normalizeGitHubTeamRepoOwnership,
// internal/providersync/github_team_catalog.go) writes EVERY `provider_access`
// row with repo_id=NULL, including exact, fully-resolved matches --
// resolution is deferred to a join against `repos` by (org_id, provider,
// lower(repo_full_name)) at READ time. Before this fix, `OwnedRepoIDs`
// filtered `repo_id IS NOT NULL` outright and returned ZERO owned repos for
// every team using GitHub's own native team-repo permissions -- arguably the
// single most common real-world ownership source, and a materially worse
// outcome than the CHAOS-4897 defect this package exists to close for those
// teams.
func TestOwnedRepoIDsResolvesProviderAccessRowsByRepoName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "org-teamownership-provider-access"
	const otherOrgID = "org-teamownership-provider-access-other"
	const teamID = "team-github-native"
	asOf := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	before := asOf.AddDate(0, 0, -30)

	insertRepo := func(id uuid.UUID, orgID, provider, fullName string) {
		if err := conn.Exec(ctx,
			`INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)`,
			id, fullName, orgID, provider, before,
		); err != nil {
			t.Fatalf("seed repos: %v", err)
		}
	}
	insertProviderAccessRow := func(orgID, teamID, fullName string) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, NULL, ?, 'exact', 'provider_access', 0, 1, 0, ?, NULL, ?)
        `, orgID, teamID, fullName, before, before); err != nil {
			t.Fatalf("seed provider_access team_repo_ownership: %v", err)
		}
	}

	repoAcme := uuid.New()
	insertRepo(repoAcme, orgID, "github", "Acme/API")
	// A DIFFERENT case than the repos row -- lower(...) on both sides is what
	// makes this match, matching the canonical resolver's own case-fold.
	insertProviderAccessRow(orgID, teamID, "acme/api")

	// A repo with the SAME full name in a DIFFERENT org and under a
	// DIFFERENT provider -- both must fail to join, proving the join's
	// org_id/provider predicates are load-bearing, not just its name match.
	// (Same defect shape as CHAOS-4897's own cross-tenant leak concern,
	// applied to this new join.)
	repoOtherOrg := uuid.New()
	insertRepo(repoOtherOrg, otherOrgID, "github", "acme/api")
	repoWrongProvider := uuid.New()
	insertRepo(repoWrongProvider, orgID, "gitlab", "acme/api")

	// A provider_access row whose name NEVER resolves -- must stay excluded,
	// same as a pattern-unresolved row: nothing to join a repo-keyed metrics
	// table against.
	insertProviderAccessRow(orgID, teamID, "acme/never-synced")

	got, err := OwnedRepoIDs(ctx, conn, orgID, teamID, asOf)
	if err != nil {
		t.Fatalf("OwnedRepoIDs: %v", err)
	}
	if len(got) != 1 || got[0] != repoAcme {
		t.Fatalf("OwnedRepoIDs = %v, want [%v] -- the provider_access row "+
			"(repo_id=NULL, repo_full_name=%q) should resolve to the repos "+
			"table's %q by case-insensitive name within (org_id, provider), "+
			"and the same-name rows in the other org/provider and the "+
			"never-synced name must NOT leak in",
			got, repoAcme, "acme/api", "Acme/API")
	}

	// Asserts on the SQL layer's OWN raw output, bypassing OwnedRepoIDs'
	// uuid.Nil defense-in-depth filter entirely (peer review, gate-rounds,
	// 2026-09-04: the assertion above alone passes even with the SQL
	// `matched` sentinel deleted, because that Go-side filter silently
	// absorbs the zero UUID a broken join leaks for an unresolvable name --
	// so it could never prove the SQL layer itself is correct). Removing
	// `r.matched = 1` from the query above and reverting to `r.id IS NOT
	// NULL` must make THIS assertion fail on its own: the never-synced
	// name's row would then read `coalesce(NULL, r.id)` where r.id is the
	// LEFT JOIN's zero-value default for an unmatched row (ClickHouse's
	// behaviour for an unmatched join column, not real NULL) -- landing in
	// this raw list as the literal zero-UUID string.
	rawRepoIDs, err := ownedRepoIDText(ctx, conn, orgID, teamID, asOf)
	if err != nil {
		t.Fatalf("ownedRepoIDText: %v", err)
	}
	for _, rawID := range rawRepoIDs {
		if rawID == uuid.Nil.String() {
			t.Fatalf("ownedRepoIDText raw output = %v, contains the zero UUID "+
				"-- the never-synced provider_access row leaked through the "+
				"join as coalesce(NULL, r.id) with r.id defaulting to the "+
				"zero UUID for its unmatched row. The SQL layer's own "+
				"`matched` sentinel is what is supposed to exclude this "+
				"BEFORE coalesce ever runs on it -- it did not.", rawRepoIDs)
		}
	}
}

// TestAuthoritativeOwnerByRepoRanksIsPrimaryThenSpecificityThenUpdatedAt is
// the red-first proof for CHAOS-5141, #2255 r1 finding 2: a repo claimed by
// more than one team must resolve to the SAME authoritative owner Python's
// load_team_repo_ownership_map picks (src/dev_health_ops/providers/teams.py:281,
// ORDER BY is_primary DESC, specificity DESC, updated_at DESC, team_id ASC --
// first row wins), regardless of which team's ownership row happens to sort
// first by INSERTION order or team_id alone.
//
// team-low is deliberately inserted FIRST and would win under any resolver
// that iterates ownership per-team and keeps whichever is encountered first
// (the exact defect resolveDailyFinalizeRepoToTeam had before this fix) --
// team-primary must win instead, because it is_primary=1.
func TestAuthoritativeOwnerByRepoRanksIsPrimaryThenSpecificityThenUpdatedAt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "org-teamownership-precedence"
	asOf := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	before := asOf.AddDate(0, 0, -30)

	insert := func(teamID string, isPrimary uint8, specificity uint16, updatedAt time.Time, repoID uuid.UUID) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, ?, 0, ?, NULL, ?)
        `, orgID, teamID, repoID, repoID.String(), isPrimary, specificity, before, updatedAt); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}

	multiClaimed := uuid.New()
	soleOwned := uuid.New()
	tiedOnRankDiffersOnUpdatedAt := uuid.New()

	// Inserted FIRST, lowest rank on every axis -- must NOT win.
	insert("team-low", 0, 1, before, multiClaimed)
	// Inserted SECOND, is_primary=1 -- must win regardless of insertion order
	// or team_id (alphabetically "team-low" < "team-primary", so a
	// team_id-only tiebreak would also pick the wrong one here).
	insert("team-primary", 1, 1, before, multiClaimed)
	// A third claim, is_primary=0 but higher specificity than team-low --
	// must still lose to team-primary's is_primary=1 (is_primary outranks
	// specificity entirely, never traded off against it).
	insert("team-specific", 0, 5, before, multiClaimed)
	// An unrelated, singly-owned repo -- proves the function does not just
	// return "the first row it sees" globally, only per repo_id.
	insert("team-solo", 1, 1, before, soleOwned)

	// CHAOS-5141, #2255 r3 finding 1: every claim above shares the SAME
	// updated_at (`before`), so none of them can prove updated_at DESC is
	// actually applied -- a regression dropping it from the ORDER BY would
	// still pass every assertion so far, since is_primary alone already
	// settles multiClaimed and soleOwned has no competing claim at all. Two
	// claims tied on is_primary AND specificity, differing ONLY on
	// updated_at, close that gap: team_id ASC (the tiebreak one step past
	// updated_at) would sort "team-alpha" before "team-zulu" alphabetically,
	// so if updated_at DESC were removed, the OLDER "team-alpha" claim would
	// incorrectly win on the team_id tiebreak. With updated_at DESC intact,
	// the NEWER "team-zulu" claim must win instead.
	older := before
	newer := before.Add(24 * time.Hour)
	insert("team-alpha", 1, 1, older, tiedOnRankDiffersOnUpdatedAt)
	insert("team-zulu", 1, 1, newer, tiedOnRankDiffersOnUpdatedAt)

	owners, err := AuthoritativeOwnerByRepo(ctx, conn, orgID, asOf)
	if err != nil {
		t.Fatalf("AuthoritativeOwnerByRepo: %v", err)
	}

	if got := owners[multiClaimed.String()]; got != "team-primary" {
		t.Fatalf("multi-claimed repo owner=%q, want team-primary -- is_primary "+
			"must outrank both insertion order and specificity", got)
	}
	if got := owners[soleOwned.String()]; got != "team-solo" {
		t.Fatalf("sole-owned repo owner=%q, want team-solo", got)
	}
	if got := owners[tiedOnRankDiffersOnUpdatedAt.String()]; got != "team-zulu" {
		t.Fatalf("repo tied on is_primary/specificity, differing only on "+
			"updated_at: owner=%q, want team-zulu (the NEWER claim) -- "+
			"\"team-alpha\" sorts first alphabetically and would win on the "+
			"team_id tiebreak alone, so this failing means updated_at DESC "+
			"is not actually being applied", got)
	}
}

// TestAuthoritativeOwnerByRepoSkipsAnEmptyTeamIDRow is the red-first proof
// for CHAOS-5141, #2255 r2 finding 1: the schema permits an empty team_id
// (051_team_attribution_dimensions.sql has no non-empty constraint on it),
// and Python's load_team_repo_ownership_map skips such a row entirely
// (`if not repo_id or not team_id: continue`, teams.py:411) rather than let
// an empty string win a repo -- the caller then falls back to the
// pattern-resolver for that repo. A prior revision of AuthoritativeOwnerByRepo
// only guarded against an invalid/zero repo_id, not an empty team_id, so an
// empty-team_id row (however it got there) silently won the repo and
// suppressed both a real lower-ranked claim AND the pattern-resolver
// fallback the caller applies when a repo has no entry at all.
func TestAuthoritativeOwnerByRepoSkipsAnEmptyTeamIDRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "org-teamownership-empty-team-id"
	asOf := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	before := asOf.AddDate(0, 0, -30)

	insert := func(teamID string, isPrimary uint8, specificity uint16, repoID uuid.UUID) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, ?, 0, ?, NULL, ?)
        `, orgID, teamID, repoID, repoID.String(), isPrimary, specificity, before, before); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}

	onlyEmptyClaim := uuid.New()
	emptyClaimWinsRankButLowerRealClaimExists := uuid.New()

	// A repo whose ONLY claim has an empty team_id -- must be ABSENT from the
	// result entirely (so the caller's pattern-resolver fallback applies),
	// never present with team_id="".
	insert("", 1, 1, onlyEmptyClaim)

	// A repo where the TOP-RANKED claim (is_primary=1) has an empty
	// team_id, but a LOWER-ranked claim (is_primary=0) has a real team --
	// the real team must win, the empty one must not suppress it.
	insert("", 1, 1, emptyClaimWinsRankButLowerRealClaimExists)
	insert("team-real", 0, 1, emptyClaimWinsRankButLowerRealClaimExists)

	owners, err := AuthoritativeOwnerByRepo(ctx, conn, orgID, asOf)
	if err != nil {
		t.Fatalf("AuthoritativeOwnerByRepo: %v", err)
	}

	if teamID, present := owners[onlyEmptyClaim.String()]; present {
		t.Fatalf("repo with only an empty-team_id claim: owner=%q present=%v, "+
			"want ABSENT from the map so the caller's pattern-resolver fallback "+
			"applies -- an empty team_id must never win a repo", teamID, present)
	}
	if got := owners[emptyClaimWinsRankButLowerRealClaimExists.String()]; got != "team-real" {
		t.Fatalf("repo with a top-ranked empty-team_id claim and a lower-ranked "+
			"real claim: owner=%q, want team-real -- the empty claim must not "+
			"suppress the real one further down the ranked order", got)
	}
}

// TestOwnedRepoIDsResolvesASameSecondWrite proves a team_repo_ownership row
// written with a valid_from EARLIER than asOf, but sharing its wall-clock
// SECOND, still resolves through OwnedRepoIDs. A positional `?` DateTime64
// parameter round-trips at whole-SECOND precision on this driver (confirmed
// empirically against this same ClickHouse image: `SELECT toString(?)` with
// a sub-second time.Time argument returns the value with its fraction
// dropped), which would compare the row's real millisecond offset against a
// truncated asOf and could read a genuinely-valid row as not-yet-valid for
// up to a full second after it was written. The named
// {as_of:DateTime64(3, 'UTC')} parameter OwnedRepoIDs binds instead
// preserves the DateTime64(3) scale end to end, so this stays exact.
//
// Deterministic, not timing-luck: both instants are hand-authored a fixed
// 400ms apart inside the SAME second, not captured from real execution
// speed. The seed itself binds a native-protocol batch, not a positional
// `?` in an INSERT ... VALUES statement -- the same whole-second rounding
// this test exists to catch on the READ side would just as easily hide
// itself on the WRITE side, storing .000 instead of .100 and making every
// assertion below pass regardless of what OwnedRepoIDs does with asOf. The
// stored valid_from is read back and checked against the seeded value
// before OwnedRepoIDs is ever called, so a regression in either direction
// is caught at its own layer.
func TestOwnedRepoIDsResolvesASameSecondWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "org-teamownership-same-second"
	const teamID = "team-platform"
	repoID := uuid.New()

	second := time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC) // a whole second, no sub-second component
	validFrom := second.Add(100 * time.Millisecond)        // .100 into that second
	asOf := second.Add(500 * time.Millisecond)             // .500 into the SAME second, strictly later

	// Seeded via PrepareBatch (the native binary protocol), never a
	// positional `?` bound in an INSERT ... VALUES statement: `?` round-trips
	// a time.Time argument at whole-SECOND precision on this driver just
	// like a SELECT-side positional parameter does, so seeding validFrom
	// that way would silently store .000 instead of .100 and the assertions
	// below would pass regardless of whether OwnedRepoIDs itself preserves
	// sub-second precision -- exactly the gap a mutation of dateTime64Literal
	// to second precision would otherwise slip through undetected.
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", teamID, repoID, repoID.String(), "exact", "inferred",
		uint8(0), uint16(1), int32(0), validFrom, nil, validFrom,
	); err != nil {
		t.Fatalf("append team_repo_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}

	// Read the stored valid_from back RAW (no FINAL/argMax, no OwnedRepoIDs
	// involved) and assert it still carries the .100 fraction -- proves the
	// seed itself is the sub-second fixture this test claims, before trusting
	// what OwnedRepoIDs makes of it.
	var storedValidFrom time.Time
	if err := conn.QueryRow(ctx, `SELECT valid_from FROM team_repo_ownership WHERE org_id = {org_id:String} AND team_id = {team_id:String}`,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_id", teamID)).Scan(&storedValidFrom); err != nil {
		t.Fatalf("read back valid_from: %v", err)
	}
	if !storedValidFrom.Equal(validFrom) {
		t.Fatalf("seed lost sub-second precision: stored valid_from=%s, want %s (the .100 fraction)", storedValidFrom, validFrom)
	}

	got, err := OwnedRepoIDs(ctx, conn, orgID, teamID, asOf)
	if err != nil {
		t.Fatalf("OwnedRepoIDs: %v", err)
	}
	if len(got) != 1 || got[0] != repoID {
		t.Fatalf("expected OwnedRepoIDs to resolve [%s] for a row written .100 into a second when asOf is .500 into the SAME second, got %v", repoID, got)
	}
}
