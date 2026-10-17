package providersync

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeGitLabBatchReader struct{ rows []GitLabStoredRepoIdentity }

func (reader fakeGitLabBatchReader) StoredRepoIdentitiesForProjects(context.Context, string, []GitLabProjectPath) ([]GitLabStoredRepoIdentity, error) {
	return reader.rows, nil
}

// The catalog keys a project's milestone sprint by the path the work-items
// route keys it by: the minted path of a held project, else the current path,
// else {path}@{N} for a path another project's identity was minted from.
func TestResolveGitLabRepoIdentityPathsFollowsTheWorkItemsRoute(t *testing.T) {
	heldID := mustRepositoryIdentityForTest(t, "acme/api")
	reader := fakeGitLabBatchReader{rows: []GitLabStoredRepoIdentity{
		{ID: heldID, Repo: "platform/api-v2", IdentityPath: "acme/api", ProviderRepoID: "123"},
	}}
	got, err := resolveGitLabRepoIdentityPaths(context.Background(), reader, "org", []GitLabProjectPath{
		{ProjectID: 123, Path: "platform/api-v2"},
		{ProjectID: 456, Path: "acme/api"},
		{ProjectID: 789, Path: "fresh/project"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]string{123: "acme/api", 456: "acme/api@456", 789: "fresh/project"}
	if len(got.Ambiguous) != 0 {
		t.Fatalf("ambiguous = %v, want none", got.Ambiguous)
	}
	for id, path := range want {
		if got.Paths[id] != path {
			t.Errorf("project %d: ref path = %q, want %q", id, got.Paths[id], path)
		}
	}
}

// A project whose stored rows cannot prove its identity is ambiguous: the
// batched resolver names it, never mints it from its current path, and the work
// items route refuses the same input.
func TestAnAmbiguousGitLabIdentityNeverFallsBackToTheCurrentPath(t *testing.T) {
	// A renamed project (id 123) whose row does not state the path it was minted from
	// and whose id does not prove its current path.
	rows := []GitLabStoredRepoIdentity{{ID: mustRepositoryIdentityForTest(t, "acme/api"), Repo: "platform/api-v2", ProviderRepoID: "123"}}
	reader := fakeGitLabBatchReader{rows: rows}
	got, err := resolveGitLabRepoIdentityPaths(context.Background(), reader, "org",
		[]GitLabProjectPath{{ProjectID: 123, Path: "platform/api-v2"}, {ProjectID: 789, Path: "fresh/project"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ambiguous := got.Ambiguous[123]; !ambiguous {
		t.Fatalf("project 123 not reported ambiguous: %+v", got)
	}
	if _, minted := got.Paths[123]; minted {
		t.Fatalf("project 123 got a path: %+v", got.Paths)
	}
	if got.Paths[789] != "fresh/project" {
		t.Fatalf("an undecided neighbour changed project 789: %+v", got.Paths)
	}
	// The work-items route refuses the same input.
	workItems := &fakeGitLabRepoIdentityReader{rows: rows}
	if _, err := resolveGitLabRepoIdentity(context.Background(), workItems, nativeTestClaim("gitlab", "work-items"), 123, "platform/api-v2"); !errors.Is(err, ErrRepositoryIdentityAmbiguous) {
		t.Fatalf("work-items resolution err = %v, want ErrRepositoryIdentityAmbiguous", err)
	}
	// The catalog skips the project's target instead of keying its sprint ref by the current path.
	refPaths := gitlabProjectRefPaths{keyed: true, paths: map[string]string{"789": "fresh/project"}, ambiguous: map[string]struct{}{"123": {}}}
	if path, ok := refPaths.pathOf("123", "platform/api-v2"); ok || path != "" {
		t.Fatalf("pathOf(ambiguous) = %q, %v; want no path", path, ok)
	}
	if path, ok := refPaths.pathOf("789", "x/y"); !ok || path != "fresh/project" {
		t.Fatalf("pathOf(resolved) = %q, %v", path, ok)
	}
	if _, ok := refPaths.pathOf("5", "x/y"); ok {
		t.Fatal("a project the reader did not resolve must not be provable")
	}
	if path, ok := (gitlabProjectRefPaths{}).pathOf("5", "x/y"); !ok || path != "x/y" {
		t.Fatalf("a handler without a reader mints from the current path: %q, %v", path, ok)
	}
}

func mustRepositoryIdentityForTest(t *testing.T, path string) string {
	t.Helper()
	id, err := repositoryIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// The catalog states no target for a project whose identity is ambiguous: it
// makes no milestone call (a nil client would panic) and leaves the row unread,
// so the write keeps what the key holds.
func TestTheCatalogStatesNoTargetForAnAmbiguousGitLabProject(t *testing.T) {
	handler := GitLabTeamCatalogRouteHandler{RepoIdentity: fakeGitLabBatchReader{rows: []GitLabStoredRepoIdentity{
		{ID: mustRepositoryIdentityForTest(t, "acme/api"), Repo: "platform/api-v2", ProviderRepoID: "123"},
	}}}
	id, ok := GitLabCatalogProjectID("org", "123")
	if !ok {
		t.Fatal("no catalog id")
	}
	path := "platform/api-v2"
	projects := []gitlabTeamCatalogProjectRow{{ID: id, OrgID: "org", Provider: "gitlab", ProjectKey: &path, IsActive: 1}}
	if err := handler.collectProjectTargets(context.Background(), nil, "org", projects,
		map[string]string{id.String(): "123"}, time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if projects[0].TargetDate != nil || projects[0].TargetState != "" {
		t.Fatalf("an ambiguous project stated a target: %+v", projects[0].projectTargetFacts)
	}
}
