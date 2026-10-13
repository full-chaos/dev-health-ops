//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// The main and co-owner rows (rulings D5988, Q2) do not change how an inferred
// owner ranks against a GitHub provider_access grant: one-owner readers rank
// (is_primary = 1) first, then specificity, and an inferred main owner
// (specificity 150) outranks a grant (100), as the one inferred row did before.
// A co-owner is never the one owner: a tie with no main owner leaves the grant
// the owner, as before co-owner rows existed (the tie wrote no row then). The
// grant is written by the real GitHub catalog normalizer and sink (repo_id
// NULL, resolved by the repos name join), the inferred rows by Derive.
func TestAnInferredOwnerStillOutranksAProviderAccessGrant(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	grant := func(org *rankedOwnerOrg) {
		t.Helper()
		row, err := normalizeGitHubTeamRepoOwnership(org.orgID, "grant-team", org.repo, org.at)
		if err != nil {
			t.Fatal(err)
		}
		if err := (GitHubTeamCatalogClickHouseEffects{Conn: conn}).WriteTeamRepoOwnership(ctx, org.orgID, []githubTeamRepoOwnershipRow{row}); err != nil {
			t.Fatalf("write the grant: %v", err)
		}
	}
	owner := func(org *rankedOwnerOrg) string {
		t.Helper()
		owners, err := teamownership.AuthoritativeOwnerByRepo(ctx, conn, org.orgID, time.Now().UTC().Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		return owners[org.repoID.String()]
	}

	t.Run("main owner and co-owner beside a grant", func(t *testing.T) {
		org := newRankedOwnerOrg(t, ctx, conn, "grant-precedence-main")
		grant(org)
		if got := owner(org); got != "gh:grant-team" {
			t.Fatalf("before the derive the owner is %q, want the grant gh:grant-team (the control)", got)
		}
		org.ownProject("jira", "main-team")
		org.link("jira", "main-team", "native", 3)
		org.ownProject("linear", "co-team")
		org.link("linear", "co-team", "native", 1)
		org.derive()
		org.assertOwnerRows(map[string]ownerRow{"main-team": ownerState(1, 0.75), "co-team": ownerState(2, 0.25)})
		if got := owner(org); got != "main-team" {
			t.Fatalf("owner = %q, want the inferred main owner main-team over the grant and the co-owner", got)
		}
	})

	t.Run("a tie with no main owner beside a grant", func(t *testing.T) {
		org := newRankedOwnerOrg(t, ctx, conn, "grant-precedence-tie")
		grant(org)
		org.ownProject("jira", "team-a")
		org.link("jira", "team-a", "native", 1)
		org.ownProject("linear", "team-b")
		org.link("linear", "team-b", "native", 1)
		org.derive()
		org.assertOwnerRows(map[string]ownerRow{"team-a": ownerState(2, 0.5), "team-b": ownerState(2, 0.5)})
		if got := owner(org); got != "gh:grant-team" {
			t.Fatalf("owner = %q, want the grant gh:grant-team: a co-owner is never the one owner, and the tie names no main owner", got)
		}
	})
}
