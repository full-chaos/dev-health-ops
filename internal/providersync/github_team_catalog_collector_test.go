package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestGitHubCollectTeamCatalogSkipsCleanlyUnderStrictWithNothingUsableSelected
// is the RED-FIRST proof for codex round 1's P1 finding (team-lead ruling,
// 2026-08-28): GitHub has no unconditional reference surface and no
// "Projects" import concept at all -- a strict call with Teams and Members
// both off (e.g. only Projects selected, or CHAOS-4323 selections that
// leave nothing GitHub can act on) has nothing to do. It must skip cleanly
// BEFORE resolving the org name or touching credentials/ClickHouse, exactly
// like Python's no_categories_selected early return
// (team_autoimport_github.py:81-82).
//
// This is a plain unit test (no build tag, no I/O): the credential has no
// resolvable org anywhere and the ClickHouse sink is unreachableConn (fails
// the test if ever touched, same double github_work_items_direct_effects_
// test.go uses). EXPECTED TO FAIL on the pre-fix tip: the old ordering
// resolved the org name (and, finding none, hard-errored under strict)
// BEFORE ever checking whether Teams/Members were selected at all.
func TestGitHubCollectTeamCatalogSkipsCleanlyUnderStrictWithNothingUsableSelected(t *testing.T) {
	adapter := GitHubTeamCatalogCollector{
		Sink: GitHubTeamCatalogClickHouseEffects{Conn: unreachableConn{t: t}},
	}
	// Valid provider, but deliberately NO org configured anywhere (neither
	// Config nor Secret nor ref.SyncOptions) -- if the old org-resolution
	// path is reached at all, it fails under strict.
	credential := providerfoundation.Credential{Provider: "github"}
	client := &providerfoundation.HTTPClient{Provider: "github"}

	result, err := adapter.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		// Any()==true (Projects is on), but GitHub supports neither Teams
		// nor Members here -- and never supports Projects at all.
		TeamCatalogSelections{Projects: true},
		time.Now(),
	)
	if err != nil {
		t.Fatalf("a strict call with nothing GitHub can act on must skip cleanly, even with no "+
			"resolvable org name and an invalid client -- got err=%v", err)
	}
	if result.TeamsWritten != 0 || result.MembershipsWritten != 0 || result.RepoOwnershipWritten != 0 ||
		len(result.TeamKeys) != 0 {
		t.Fatalf("want a zero result, got %+v", result)
	}
}
