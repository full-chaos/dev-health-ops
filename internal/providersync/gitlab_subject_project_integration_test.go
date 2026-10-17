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
	if _, err := teamattribution.AnchorGitLabSubjectProjects(store.ctx, store.conn, subjects); err != nil {
		t.Fatal(err)
	}
	stale := subjects["stale"]
	if *stale.ProjectID != "org-acme:gitlab:123" || stale.ProjectKey == nil || *stale.ProjectKey != "acme/api" {
		t.Fatalf("stale item = project_id %v, project_key %v; want org-acme:gitlab:123 and its old path", stale.ProjectID, stale.ProjectKey)
	}
	// Project 456 holds acme/api now: it is no longer a name of project 123's items.
	if len(stale.GitLabPaths) != 1 || stale.GitLabPaths[0] != "platform/api-v2" {
		t.Fatalf("stale item paths = %v, want only [platform/api-v2]", stale.GitLabPaths)
	}
	if got := subjects["current"]; len(got.GitLabPaths) != 2 || got.GitLabPaths[0] != "acme/api" || got.GitLabPaths[1] != "acme/api@456" {
		t.Fatalf("current item paths = %v, want [acme/api acme/api@456]", got.GitLabPaths)
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
		if got.Paths[id] != path {
			t.Errorf("project %d: ref path = %q, want %q", id, got.Paths[id], path)
		}
	}
}

// A path maps to the one project the repos rows name at it, current or minted;
// a path two projects have used is ambiguous; an unseen path is in neither map.
func TestLoadGitLabProjectNativesByPathOnARealStore(t *testing.T) {
	store := startGitLabIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))
	store.syncRepository("777", "solo/repo", t0)

	natives, ambiguous, err := teamattribution.LoadGitLabProjectNativesByPath(store.ctx, store.conn, "org-acme",
		[]string{"acme/api", "platform/api-v2", "solo/repo", "fresh/none"})
	if err != nil {
		t.Fatal(err)
	}
	if natives["acme/api"] != "123" || natives["platform/api-v2"] != "123" || natives["solo/repo"] != "777" || len(ambiguous) != 0 {
		t.Fatalf("natives = %v ambiguous = %v before the reuse", natives, ambiguous)
	}
	if _, ok := natives["fresh/none"]; ok {
		t.Fatal("an unseen path got a project")
	}
	store.syncRepository("456", "acme/api", t0.Add(2*time.Hour))
	natives, ambiguous, err = teamattribution.LoadGitLabProjectNativesByPath(store.ctx, store.conn, "org-acme", []string{"acme/api", "platform/api-v2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, bad := ambiguous["acme/api"]; !bad || natives["platform/api-v2"] != "123" {
		t.Fatalf("after the reuse: natives = %v ambiguous = %v", natives, ambiguous)
	}
	if _, ok := natives["acme/api"]; ok {
		t.Fatal("a reused path still maps to one project")
	}
}

// The sync-time context source anchors a freshly written GitLab subject through
// the real repos row: the renamed project's paths reach the subject.
func TestSyncTimeContextSourceAnchorsAGitLabSubjectOnARealStore(t *testing.T) {
	store := startGitLabIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))
	repo := mustRepositoryIdentity(t, "acme/api")

	source := githubWorkItemClickHouseDerivationContextSource{Conn: store.conn, Lease: store.lease}
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{
		"gitlab:acme/api#1": {WorkItemID: "gitlab:acme/api#1", Provider: "gitlab", OrgID: "org-acme", RepoID: &repo,
			ProjectID: strPtr("org-acme:gitlab:123"), ProjectKey: strPtr("acme/api")},
	}
	if err := source.AnchorGitLabSubjects(store.ctx, subjects); err != nil {
		t.Fatal(err)
	}
	got := subjects["gitlab:acme/api#1"]
	if len(got.GitLabPaths) != 2 || got.GitLabPaths[0] != "platform/api-v2" || got.GitLabPaths[1] != "acme/api" {
		t.Fatalf("paths = %v, want [platform/api-v2 acme/api]", got.GitLabPaths)
	}
}
