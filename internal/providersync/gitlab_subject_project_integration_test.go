//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// A GitLab item written before 3b2 holds the path in project_id. The attribution
// read gives it the catalog id of the project its repo names, so it joins the
// same grant as an item written after; an item of a project that took the
// path later joins its own.
func TestAnchorGitLabSubjectProjectsOnARealStore(t *testing.T) {
	store := startGitLabIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))
	oldRepo := mustRepositoryIdentity(t, "acme/api")
	store.syncRepository("456", "acme/api", t0.Add(2*time.Hour))
	newRepo := mustRepositoryIdentity(t, "acme/api@456")
	unknownRepo := mustRepositoryIdentity(t, "ghost/none")

	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{
		"stale":   {WorkItemID: "stale", Provider: "gitlab", OrgID: "org-acme", RepoID: &oldRepo, ProjectID: strPtr("acme/api")},
		"current": {WorkItemID: "current", Provider: "gitlab", OrgID: "org-acme", RepoID: &newRepo, ProjectID: strPtr("org-acme:gitlab:456"), ProjectKey: strPtr("acme/api")},
		"ghost":   {WorkItemID: "ghost", Provider: "gitlab", OrgID: "org-acme", RepoID: &unknownRepo, ProjectID: strPtr("ghost/none")},
		"github":  {WorkItemID: "github", Provider: "github", OrgID: "org-acme", RepoID: &oldRepo, ProjectID: strPtr("acme/api")},
	}
	if err := teamattribution.AnchorGitLabSubjectProjects(store.ctx, store.conn, subjects); err != nil {
		t.Fatal(err)
	}
	stale := subjects["stale"]
	if *stale.ProjectID != "org-acme:gitlab:123" || stale.ProjectKey == nil || *stale.ProjectKey != "acme/api" {
		t.Fatalf("stale item = project_id %v, project_key %v; want org-acme:gitlab:123 and its old path", stale.ProjectID, stale.ProjectKey)
	}
	if got := subjects["current"]; *got.ProjectID != "org-acme:gitlab:456" {
		t.Fatalf("current item project_id = %s, want it untouched", *got.ProjectID)
	}
	if got := subjects["ghost"]; *got.ProjectID != "ghost/none" {
		t.Fatalf("an item of an unknown repo = %s, want its path kept (it joins nothing)", *got.ProjectID)
	}
	if got := subjects["github"]; *got.ProjectID != "acme/api" {
		t.Fatalf("a GitHub item = %s, want it untouched", *got.ProjectID)
	}
}

// The catalog keys a project's milestone sprints by the work-items route's
// path, read in one batch from the store.
func TestResolveGitLabRepoIdentityPathsOnARealStore(t *testing.T) {
	store := startGitLabIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))

	got, err := resolveGitLabRepoIdentityPaths(store.ctx, store.reader, "org-acme", []GitLabProjectPath{
		{ProjectID: 123, Path: "platform/api-v2"},
		{ProjectID: 456, Path: "acme/api"},
		{ProjectID: 789, Path: "fresh/project"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]string{123: "acme/api", 456: "acme/api@456", 789: "fresh/project"}
	for id, path := range want {
		if got[id] != path {
			t.Errorf("project %d: ref path = %q, want %q", id, got[id], path)
		}
	}
}
