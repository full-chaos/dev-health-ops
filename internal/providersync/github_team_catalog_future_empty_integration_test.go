//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// An empty interval covers no instant, so the GitHub provider_access readers
// never read one as open, even when it opens in the future.
func TestGitHubProviderAccessReadersSkipAFutureEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const org = "org-gh-future-empty"
	future := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, 'github', 'gh:future-empty', ?, 'acme/api', 'exact', 'provider_access', 1, 100, 10, ?, ?, ?)`,
		org, uuid.New(), future, future, future); err != nil {
		t.Fatal(err)
	}
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	open, err := sink.openProviderAccessRepoOwnership(ctx, org, "acme", []string{"gh:future-empty"})
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 0 {
		t.Fatalf("openProviderAccessRepoOwnership read %d empty interval(s) as open", len(open))
	}
	teams, err := sink.OpenRepoOwnershipTeamsNotListed(ctx, org, "acme", []string{"gh:other"})
	if err != nil {
		t.Fatal(err)
	}
	if len(teams) != 0 {
		t.Fatalf("OpenRepoOwnershipTeamsNotListed read %d empty interval(s) as open", len(teams))
	}
}
