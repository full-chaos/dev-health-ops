package providersync

import (
	"context"
	"testing"
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
	for id, path := range want {
		if got[id] != path {
			t.Errorf("project %d: ref path = %q, want %q", id, got[id], path)
		}
	}
	paths := gitlabProjectRefPaths(map[string]string{"123": "acme/api"})
	if paths.pathOf("123", "platform/api-v2") != "acme/api" || paths.pathOf("5", "x/y") != "x/y" {
		t.Error("pathOf does not fall back to the current path")
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
