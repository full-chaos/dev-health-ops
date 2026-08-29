//go:build integration

package providersync

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func githubTeamCatalogAdapterDoer(t *testing.T) *githubTeamCatalogFixtureDoer {
	t.Helper()
	return &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
		"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
		"/orgs/acme/teams/platform/members": `[{"login":"octocat"}]`,
	}}
}

func githubTeamCatalogAdapterClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGitHubTeamCatalogCollectorWritesTeamsAndMemberships(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-adapter-org-a"
	doer := githubTeamCatalogAdapterDoer(t)
	adapter := GitHubTeamCatalogCollector{
		Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn},
	}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	// MembersWritten stays 0: GitHub has no `members` table writer, only
	// `team_memberships` (MembershipsWritten) -- see the comment at its
	// only call site in github_team_catalog_collector.go.
	if result.TeamsWritten != 1 || result.MembershipsWritten != 1 || result.MembersWritten != 0 ||
		len(result.TeamKeys) != 1 || result.TeamKeys[0] != "platform" ||
		result.ProjectsWritten != 0 || result.RepoOwnershipWritten != 1 {
		t.Fatalf("result=%+v", result)
	}
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	roster, ok := sink.ExistingTeamMembers(ctx, orgID, []string{"gh:platform"})
	if !ok || len(roster["gh:platform"]) != 1 || roster["gh:platform"][0] != "github:octocat" {
		t.Fatalf("roster=%+v ok=%v", roster, ok)
	}

	// CHAOS-4434 scope correction: a native GitHub run MUST refresh
	// team_repo_ownership -- there is no other Go-native writer for it
	// (githubTeamRow's doc comment). This is the red-then-green proof: on
	// origin/main (no native GitHub route at all) this table is never
	// touched by anything but the Python bridge; here, a single
	// CollectTeamCatalog call leaves a real row behind.
	ownershipResult, err := conn.Query(ctx,
		`SELECT repo_full_name, source, match_type FROM team_repo_ownership FINAL `+
			`WHERE org_id = ? AND provider = 'github' AND team_id = ?`,
		orgID, "gh:platform",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer ownershipResult.Close()
	if !ownershipResult.Next() {
		t.Fatal("team_repo_ownership row missing after a native GitHub CollectTeamCatalog call")
	}
	var repoFullName, source, matchType string
	if err := ownershipResult.Scan(&repoFullName, &source, &matchType); err != nil {
		t.Fatal(err)
	}
	if repoFullName != "acme/api" || source != "provider_access" || matchType != "exact" {
		t.Fatalf("repo=%q source=%q match=%q", repoFullName, source, matchType)
	}
}

// TestGitHubTeamCatalogCollectorPreservesRosterOnMembersOffRun proves the
// members-off ("teams" selected, "members" not) path carries forward the
// existing roster instead of overwriting it with [] -- CHAOS-4323 round 2's
// codex-flagged data-loss fix, ported.
func TestGitHubTeamCatalogCollectorPreservesRosterOnMembersOffRun(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-adapter-org-b"
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	// Seed an existing roster as if a prior members-on run had already
	// written it.
	seedTeam, err := normalizeGitHubTeam(orgID, githubTeamPayload{Slug: "platform", Name: "Platform"}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	seedTeam.Members = []string{"github:octocat"}
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{seedTeam}); err != nil {
		t.Fatal(err)
	}

	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                `[{"slug":"platform","name":"Platform Renamed"}]`,
		"/orgs/acme/teams/platform/repos": `[]`,
	}}
	adapter := GitHubTeamCatalogCollector{Sink: sink}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: false}, now.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.TeamsWritten != 1 || result.MembershipsWritten != 0 {
		t.Fatalf("result=%+v", result)
	}
	roster, ok := sink.ExistingTeamMembers(ctx, orgID, []string{"gh:platform"})
	if !ok || len(roster["gh:platform"]) != 1 || roster["gh:platform"][0] != "github:octocat" {
		t.Fatalf("roster must survive a members-off run: roster=%+v ok=%v", roster, ok)
	}
	// The team-level fields (name) still update even while the roster is
	// preserved -- only "members" is carried forward, not the whole row.
	result2, err := conn.Query(ctx, `SELECT name FROM teams FINAL WHERE org_id = ? AND id = ?`, orgID, "gh:platform")
	if err != nil {
		t.Fatal(err)
	}
	defer result2.Close()
	var name string
	if !result2.Next() {
		t.Fatal("team row missing after members-off run")
	}
	if err := result2.Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Platform Renamed" {
		t.Fatalf("name=%q", name)
	}
}

func TestGitHubTeamCatalogCollectorSkipsWhenOrgNameMissing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{}}
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github"}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: "org-x", SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.TeamsWritten != 0 || result.MembershipsWritten != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(doer.requests) != 0 {
		t.Fatalf("no request should be issued without a resolvable org name: requests=%v", doer.requests)
	}
}

// TestGitHubTeamCatalogCollectorFailsClosedOnMissingOrgUnderStrict is the
// strict-mode counterpart of the skip-above test: reference discovery
// (ref.Strict=true) must see a missing org name as a real error, matching
// Python's _populate_async under strict_reference_discovery ("raise
// ValueError(missing GitHub credentials or org...)"), never a silent zero.
func TestGitHubTeamCatalogCollectorFailsClosedOnMissingOrgUnderStrict(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{}}
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github"}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	if _, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: "org-x", SyncRunID: "run-1", Strict: true},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	); err == nil {
		t.Fatal("strict CollectTeamCatalog must fail when no org name resolves, not return a silent zero result")
	}
}

// TestGitHubTeamCatalogCollectorFallsBackToSyncOptionsOrgName proves the
// credentials-then-sync_options fallback order Python's _github_org uses:
// when the credential carries no org, ref.SyncOptions["org"] still resolves
// one.
func TestGitHubTeamCatalogCollectorFallsBackToSyncOptionsOrgName(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-adapter-org-sync-options"
	doer := githubTeamCatalogAdapterDoer(t)
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github"}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1", SyncOptions: map[string]any{"org": "acme"}},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.TeamsWritten != 1 || len(doer.requests) == 0 {
		t.Fatalf("sync_options org fallback did not take effect: result=%+v requests=%v", result, doer.requests)
	}
}

// TestGitHubTeamCatalogCollectorPreservesRosterAfterPerTeamFetchFailure is
// the CHAOS-4461 regression proof: with members globally selected, ONE
// team's member fetch failing must not wipe that team's roster to [] --
// its existing roster must survive, exactly like the members-globally-off
// path already guarantees. A second, healthy team in the same run gets its
// freshly observed roster, proving the fix is per-team, not all-or-nothing.
func TestGitHubTeamCatalogCollectorPreservesRosterAfterPerTeamFetchFailure(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-adapter-org-partial-fetch-failure"
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	// Seed platform's existing roster, as if a prior successful run had
	// already written it. ops has no prior row -- a genuinely new team.
	seedTeam, err := normalizeGitHubTeam(orgID, githubTeamPayload{Slug: "platform", Name: "Platform"}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	seedTeam.Members = []string{"github:octocat"}
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{seedTeam}); err != nil {
		t.Fatal(err)
	}

	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"},{"slug":"ops","name":"Operations"}]`,
		"/orgs/acme/teams/platform/repos":   `[]`,
		"/orgs/acme/teams/ops/repos":        `[]`,
		"/orgs/acme/teams/platform/members": `not json`,
		"/orgs/acme/teams/ops/members":      `[{"login":"monalisa"}]`,
	}}
	adapter := GitHubTeamCatalogCollector{Sink: sink}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Both teams still get written -- platform's failed fetch does not
	// abort the run or exclude it from the teams table.
	if result.TeamsWritten != 2 {
		t.Fatalf("result=%+v", result)
	}

	roster, ok := sink.ExistingTeamMembers(ctx, orgID, []string{"gh:platform", "gh:ops"})
	if !ok {
		t.Fatal("roster readback not-ok")
	}
	if len(roster["gh:platform"]) != 1 || roster["gh:platform"][0] != "github:octocat" {
		t.Fatalf("platform's existing roster was NOT preserved after its member fetch failed: roster=%+v", roster)
	}
	if len(roster["gh:ops"]) != 1 || roster["gh:ops"][0] != "github:monalisa" {
		t.Fatalf("ops (healthy fetch, genuinely new team) got the wrong roster: roster=%+v", roster)
	}
}

// rosterConfirmReadFailingConn wraps a real driver.Conn (a live testcontainer
// ClickHouse) and deliberately fails ONLY the roster-preserve read
// (`SELECT id, members FROM teams FINAL ...`, ExistingTeamMembers) --
// everything else (PrepareBatch/Exec for WriteTeams, WriteMemberships,
// the manual_members preserve-read, etc.) passes through to the real
// connection untouched. This is how TestGitHubTeamCatalogCollectorWrites
// HealthyTeamsEvenWhenAnotherTeamsRosterConfirmReadFails forces
// ExistingTeamMembers's ok=false branch deterministically -- a healthy real
// ClickHouse connection has no organic way to produce that outcome.
type rosterConfirmReadFailingConn struct {
	chdriver.Conn
}

func (c rosterConfirmReadFailingConn) Query(ctx context.Context, query string, args ...any) (chdriver.Rows, error) {
	if strings.Contains(query, "SELECT id, members FROM teams FINAL") {
		return nil, errors.New("injected failure: roster confirm-read")
	}
	return c.Conn.Query(ctx, query, args...)
}

// TestGitHubTeamCatalogCollectorWritesHealthyTeamsEvenWhenAnotherTeamsRosterConfirmReadFails
// is the RED-FIRST proof for codex round 1's P2 finding (team-lead ruling,
// 2026-08-28): when one team's member fetch fails AND its roster
// confirm-read (ExistingTeamMembers) ALSO fails, the collector must still
// write every OTHER, healthy team -- not silently skip the entire `teams`
// write for the whole run. EXPECTED TO FAIL on the pre-fix tip: the old
// gate (`!rosterPreservationFailed`) blocked WriteTeams entirely the moment
// ANY team's confirm-read failed, even though rows.Teams was already
// correctly filtered down to just the safe teams by that point.
func TestGitHubTeamCatalogCollectorWritesHealthyTeamsEvenWhenAnotherTeamsRosterConfirmReadFails(t *testing.T) {
	ctx, realConn := newWorkItemEffectsConn(t)
	conn := rosterConfirmReadFailingConn{Conn: realConn}
	orgID := "github-adapter-org-roster-confirm-read-failure"
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"},{"slug":"ops","name":"Operations"}]`,
		"/orgs/acme/teams/platform/repos":   `[]`,
		"/orgs/acme/teams/ops/repos":        `[]`,
		"/orgs/acme/teams/platform/members": `not json`, // platform's fetch fails
		"/orgs/acme/teams/ops/members":      `[{"login":"monalisa"}]`,
	}}
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.RosterPreservationFailed {
		t.Fatal("want RosterPreservationFailed=true -- platform's roster confirm-read was injected to fail")
	}
	if result.TeamsWritten != 1 {
		t.Fatalf("want ops (the healthy, unaffected team) still written despite platform's confirm-read "+
			"failure -- result=%+v", result)
	}
	roster, ok := GitHubTeamCatalogClickHouseEffects{Conn: realConn}.ExistingTeamMembers(ctx, orgID, []string{"gh:ops"})
	if !ok || len(roster["gh:ops"]) != 1 || roster["gh:ops"][0] != "github:monalisa" {
		t.Fatalf("ops's row was not actually persisted: roster=%+v ok=%v", roster, ok)
	}
}

// TestGitHubTeamCatalogCollectorSkipsNativeMembershipConflictingWithManualPinToADifferentTeam
// is a RED-FIRST proof (team-lead ruling, 2026-08-28): GitHub's collector has
// no equivalent of Linear's CHAOS-4431 codex-review-finding-#6 fail-safe
// membership-conflict guard (team_membership_conflict_guard.go) yet, so an
// admin's manual pin of a member to one team is silently contradicted the
// moment that same member also shows up in a DIFFERENT team's native GitHub
// roster. This test is EXPECTED TO FAIL on the current tip -- the fix (a
// GitHub-typed wrapper over the shared, provider-agnostic
// resolveActiveManualMembershipPairs/resolveActiveMemberAttributionFallback
// Identities resolvers, using the CORRECTED semantics 4431 is landing in its
// next base: an exact (member_id, team_id) match is a CONFIRMATION, not a
// conflict; only a manual pin to a DIFFERENT team is a conflict) lands on the
// next rebase, at which point this goes green.
func TestGitHubTeamCatalogCollectorSkipsNativeMembershipConflictingWithManualPinToADifferentTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-membership-conflict-org"
	validFrom := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	// Ground truth: an admin manually pinned octocat to "gh:other-team", NOT
	// "gh:platform" (the team the fixture doer below reports octocat as a
	// native member of).
	if err := conn.Exec(ctx, `INSERT INTO team_memberships
		(org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		orgID, "github", "gh:other-team", "github:octocat", "octocat", nil, []string{"github:octocat"},
		"manual", uint8(1), uint16(0), int32(0), validFrom, nil, validFrom,
	); err != nil {
		t.Fatalf("seed manual membership: %v", err)
	}

	doer := githubTeamCatalogAdapterDoer(t) // team "platform", member "octocat"
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	if _, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	); err != nil {
		t.Fatal(err)
	}

	var conflictingNativeRows uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM team_memberships FINAL
		WHERE org_id = ? AND team_id = 'gh:platform' AND member_id = 'github:octocat'
		AND source = 'native' AND (valid_to IS NULL OR valid_to > now())`, orgID,
	).Scan(&conflictingNativeRows); err != nil {
		t.Fatal(err)
	}
	if conflictingNativeRows != 0 {
		t.Fatalf("a native membership conflicting with octocat's active manual pin to a DIFFERENT team must be "+
			"skipped, not written -- got %d conflicting row(s)", conflictingNativeRows)
	}
}

// TestGitHubTeamCatalogCollectorSkipsWritingATeamFlaggedForManualSyncPolicy is
// a second RED-FIRST proof (team-lead ruling, 2026-08-28, codex review
// finding #3): GitHub's collector has no equivalent of Linear's
// applyTeamSyncPolicyGuard (team_sync_policy_guard.go) yet, so a team an
// admin has flagged sync_policy != 0 (taken out of auto-apply, e.g. "flagged
// for review" or "manual") gets silently overwritten by the next native sync
// anyway. EXPECTED TO FAIL on the current tip; goes green once GitHub's own
// wrapper over the shared, provider-agnostic resolveTeamSyncPolicies lands on
// the next rebase.
func TestGitHubTeamCatalogCollectorSkipsWritingATeamFlaggedForManualSyncPolicy(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-sync-policy-org"
	updatedAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	// An admin flagged "gh:platform" out of auto-apply (sync_policy=1,
	// "flagged for review") BEFORE this native run.
	if err := conn.Exec(ctx, `INSERT INTO team_sync_policies
		(org_id, team_id, sync_policy, managed_fields, updated_by, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		orgID, "gh:platform", uint8(1), []string{}, nil, updatedAt,
	); err != nil {
		t.Fatalf("seed team_sync_policies: %v", err)
	}

	doer := githubTeamCatalogAdapterDoer(t) // team "platform"
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	if _, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	); err != nil {
		t.Fatal(err)
	}

	// No prior `teams` row exists for this fresh org/team -- if the guard is
	// missing (today), this native run creates one anyway.
	var written uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM teams FINAL
		WHERE org_id = ? AND id = 'gh:platform'`, orgID,
	).Scan(&written); err != nil {
		t.Fatal(err)
	}
	if written != 0 {
		t.Fatalf("a team flagged sync_policy != 0 must be left completely untouched by this native run, "+
			"got %d row(s)", written)
	}
}
