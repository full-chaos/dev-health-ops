//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// A GitLab merge request written before the numeric-id anchoring holds its
// path in work_items.project_id. The derivation maps it through its repo's
// provider_repo_id to the catalog id project ownership is keyed by, so it
// resolves like an item written after; a repo that names no project resolves
// nothing.
func TestTeamRepoOwnershipDerivationAnchorsALegacyGitLabItem(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-3383-legacy-gitlab-item-org"
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	anchored, loose := uuid.New(), uuid.New()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO repos (id, repo, org_id, provider, provider_repo_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(anchored, "platform/api-v2", orgID, "gitlab", "77", now); err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(loose, "acme/loose", orgID, "gitlab", "", now); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	seedTeamProjectOwnership(t, ctx, conn, orgID, "gitlab", orgID+":gitlab:77", "team-gitlab", false, now)
	seedWorkItemOfType(t, ctx, conn, orgID, "gitlab:acme/api!1", "gitlab", "merge_request", anchored, "acme/api", now)
	seedWorkItemOfType(t, ctx, conn, orgID, "gitlab:acme/loose!1", "gitlab", "merge_request", loose, "acme/loose", now)

	if _, _, _, _, err := (TeamRepoOwnershipDerivationService{Conn: conn}).Derive(ctx, orgID); err != nil {
		t.Fatalf("Derive: %v", err)
	}
	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	if row, ok := got["platform/api-v2"]; !ok || row.teamID != "team-gitlab" {
		t.Fatalf("the legacy item of project 77 resolved to %+v, want team-gitlab; all rows %+v", got["platform/api-v2"], got)
	}
	if row, ok := got["acme/loose"]; ok {
		t.Fatalf("a repo that names no project resolved to %+v, want no ownership", row)
	}
}
