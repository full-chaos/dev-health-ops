//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestWriteWithNoWritableRowReleasesItsConnection: a write whose every derived
// row has a repository the repos snapshot does not know writes nothing. It
// must still release its prepared batch: the pool has four connections, and a
// leaked batch holds one until the process ends.
func TestWriteWithNoWritableRowReleasesItsConnection(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for attempt := range 8 {
		written, armCounts, err := writeTeamRepoOwnershipRows(ctx, conn, "chaos-9351-unwritable", time.Now().UTC(),
			[]DerivedTeamRepoOwnershipRow{{TeamID: "team-a", RepoID: uuid.New().String()}}, map[uuid.UUID]teamRepoOwnershipRepoInfo{})
		if err != nil || written != 0 || armCounts != nil {
			t.Fatalf("write %d: written=%d armCounts=%v err=%v, want 0 nil nil", attempt, written, armCounts, err)
		}
	}
	var one uint8
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("query after 8 unwritable writes: %d %v (a leaked batch per write exhausts the pool)", one, err)
	}
}

// TestDeriveWithOnlyAnUnknownRepositoryReleasesItsConnection: the same leak
// through the production entry point. A pull request of an owned project in
// a repository the repos snapshot does not know derives a row that cannot be
// written, at every run.
func TestDeriveWithOnlyAnUnknownRepositoryReleasesItsConnection(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-9351-derive"
	at := time.Now().UTC().Add(-time.Hour)
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{uuid.New(): "acme/known"})
	seedTeamProjectOwnership(t, ctx, conn, orgID, "jira", "10001", "jira:ops", true, at)
	seedWorkItemOfType(t, ctx, conn, orgID, "jira:PR-1", "jira", "pr", uuid.New(), "10001", at)
	service := TeamRepoOwnershipDerivationService{Conn: conn}
	for attempt := range 8 {
		written, retracted, inputsReady, _, err := service.Derive(ctx, orgID)
		if err != nil || written != 0 || retracted != 0 || !inputsReady {
			t.Fatalf("Derive %d: written=%d retracted=%d inputsReady=%v err=%v, want 0 0 true nil", attempt, written, retracted, inputsReady, err)
		}
	}
}
