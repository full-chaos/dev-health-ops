//go:build integration

package providersync

import (
	"context"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"net/http"
	"testing"
	"time"

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
		"github", "https://api.github.com", fakehttp.Client(doer),
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
	client := githubTeamCatalogAdapterClient(t, fakehttp.Client(doer))
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

func TestGitHubTeamCatalogCollectorSkipsWhenOrgNameMissing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{}}
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github"}
	client := githubTeamCatalogAdapterClient(t, fakehttp.Client(doer))
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
	client := githubTeamCatalogAdapterClient(t, fakehttp.Client(doer))
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
	client := githubTeamCatalogAdapterClient(t, fakehttp.Client(doer))
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
	client := githubTeamCatalogAdapterClient(t, fakehttp.Client(doer))
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
	client := githubTeamCatalogAdapterClient(t, fakehttp.Client(doer))
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
