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

	// Ownership written before the catalog id: the project_id holds the path.
	pathOwned, dupA, dupB := uuid.New(), uuid.New(), uuid.New()
	batch, err = conn.PrepareBatch(ctx, `INSERT INTO repos (id, repo, identity_path, org_id, provider, provider_repo_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id                       uuid.UUID
		repo, identity, provider string
	}{
		{pathOwned, "legacy/owned", "legacy/owned", "88"},
		// Two projects have used x/dup: project 1 minted it and left, project 2 holds it now.
		{dupA, "elsewhere/moved", "x/dup", "1"},
		{dupB, "x/dup", "x/dup@2", "2"},
	} {
		if err := batch.Append(row.id, row.repo, row.identity, orgID, "gitlab", row.provider, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	seedTeamProjectOwnership(t, ctx, conn, orgID, "gitlab", "legacy/owned", "team-path", false, now)
	seedTeamProjectOwnership(t, ctx, conn, orgID, "gitlab", "x/dup", "team-dup", false, now)
	seedWorkItemOfType(t, ctx, conn, orgID, "gitlab:legacy/owned!1", "gitlab", "merge_request", pathOwned, "legacy/owned", now)
	seedWorkItemOfType(t, ctx, conn, orgID, "gitlab:x/dup!1", "gitlab", "merge_request", dupA, "x/dup", now)
	seedWorkItemOfType(t, ctx, conn, orgID, "gitlab:x/dup@2!1", "gitlab", "merge_request", dupB, "x/dup", now)

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
	// A path-keyed ownership row joins the item anchored to the project it names.
	if row, ok := got["legacy/owned"]; !ok || row.teamID != "team-path" {
		t.Fatalf("path-keyed ownership of project 88 resolved to %+v, want team-path; all rows %+v", got["legacy/owned"], got)
	}
	// A path two projects have used is not a grant for either.
	for _, repo := range []string{"elsewhere/moved", "x/dup"} {
		if row, ok := got[repo]; ok {
			t.Fatalf("ownership of the reused path x/dup reached %s: %+v", repo, row)
		}
	}
}
