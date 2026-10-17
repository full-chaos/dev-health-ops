package providersync

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func mustRepositoryIdentity(t *testing.T, path string) string {
	t.Helper()
	id, err := repositoryIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestDecideGitLabRepoIdentityKeepsAHeldProjectAndMintsANewOne(t *testing.T) {
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	acme := mustRepositoryIdentity(t, "acme/api")
	platform := mustRepositoryIdentity(t, "platform/api-v2")
	for _, tc := range []struct {
		name        string
		projectID   int64
		currentPath string
		rows        []GitLabStoredRepoIdentity
		want        GitLabRepoIdentity
		decision    string
	}{
		{
			name: "a store without the project mints from the current path", projectID: 123, currentPath: "acme/api",
			want: GitLabRepoIdentity{RepoID: acme, Path: "acme/api"}, decision: gitLabRepoIdentityMinted,
		},
		{
			name: "a renamed project keeps its id and minted path", projectID: 123, currentPath: "platform/api-v2",
			rows: []GitLabStoredRepoIdentity{{ID: acme, Repo: "acme/api", IdentityPath: "acme/api", ProviderRepoID: "123", LastSynced: t0}},
			want: GitLabRepoIdentity{RepoID: acme, Path: "acme/api"}, decision: gitLabRepoIdentityHeld,
		},
		{
			name: "a row from before identity_path proves its path by its id", projectID: 123, currentPath: "platform/api-v2",
			rows: []GitLabStoredRepoIdentity{{ID: acme, Repo: "acme/api", ProviderRepoID: "123", LastSynced: t0}},
			want: GitLabRepoIdentity{RepoID: acme, Path: "acme/api"}, decision: gitLabRepoIdentityHeld,
		},
		{
			name: "a row from before provider_repo_id is held by its settings project_id", projectID: 123, currentPath: "platform/api-v2",
			rows: []GitLabStoredRepoIdentity{{ID: acme, Repo: "acme/api", SettingsProjectID: 123, LastSynced: t0}},
			want: GitLabRepoIdentity{RepoID: acme, Path: "acme/api"}, decision: gitLabRepoIdentityHeld,
		},
		{
			name: "provider_repo_id wins over a stale settings project_id", projectID: 123, currentPath: "acme/api",
			rows: []GitLabStoredRepoIdentity{{ID: acme, Repo: "acme/api", ProviderRepoID: "456", SettingsProjectID: 123, LastSynced: t0}},
			want: GitLabRepoIdentity{RepoID: mustRepositoryIdentity(t, "acme/api@123"), Path: "acme/api@123"}, decision: gitLabRepoIdentityTakeover,
		},
		{
			name: "a split made before the fix continues its newest row", projectID: 123, currentPath: "team/api-v3",
			rows: []GitLabStoredRepoIdentity{
				{ID: acme, Repo: "acme/api", ProviderRepoID: "123", LastSynced: t0},
				{ID: platform, Repo: "platform/api-v2", ProviderRepoID: "123", LastSynced: t0.Add(time.Hour)},
			},
			want: GitLabRepoIdentity{RepoID: platform, Path: "platform/api-v2"}, decision: gitLabRepoIdentityHeld,
		},
		{
			name: "a path another project holds mints {path}@{N}", projectID: 456, currentPath: "acme/api",
			rows: []GitLabStoredRepoIdentity{{ID: acme, Repo: "platform/api-v2", IdentityPath: "acme/api", ProviderRepoID: "123", LastSynced: t0}},
			want: GitLabRepoIdentity{RepoID: mustRepositoryIdentity(t, "acme/api@456"), Path: "acme/api@456"}, decision: gitLabRepoIdentityTakeover,
		},
		{
			name: "a path held by a row that names no project is not a takeover", projectID: 456, currentPath: "acme/api",
			rows: []GitLabStoredRepoIdentity{{ID: acme, Repo: "acme/api", LastSynced: t0}},
			want: GitLabRepoIdentity{RepoID: acme, Path: "acme/api"}, decision: gitLabRepoIdentityMinted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, decision, err := decideGitLabRepoIdentity(tc.projectID, tc.currentPath, tc.rows)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want || decision != tc.decision {
				t.Fatalf("got %+v (%s), want %+v (%s)", got, decision, tc.want, tc.decision)
			}
		})
	}
}

func TestDecideGitLabRepoIdentityRefusesARowWhosePathItCannotProve(t *testing.T) {
	rows := []GitLabStoredRepoIdentity{{ID: mustRepositoryIdentity(t, "acme/api"), Repo: "platform/api-v2", ProviderRepoID: "123"}}
	if _, _, err := decideGitLabRepoIdentity(123, "platform/api-v2", rows); !errors.Is(err, ErrRepositoryIdentityAmbiguous) {
		t.Fatalf("err = %v, want ErrRepositoryIdentityAmbiguous", err)
	}
}

type fakeGitLabRepoIdentityReader struct {
	rows  []GitLabStoredRepoIdentity
	paths [][]string
}

func (reader *fakeGitLabRepoIdentityReader) StoredRepoIdentities(context.Context, Claim, int64, string) ([]GitLabStoredRepoIdentity, error) {
	return reader.rows, nil
}

func (reader *fakeGitLabRepoIdentityReader) StoredRepoIdentitiesAtPaths(_ context.Context, _ Claim, paths []string) ([]GitLabStoredRepoIdentity, error) {
	reader.paths = append(reader.paths, paths)
	return reader.rows, nil
}

func TestGitLabDependenciesCarryTheIdentityPathOfTheProjectTheyName(t *testing.T) {
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	reader := &fakeGitLabRepoIdentityReader{rows: []GitLabStoredRepoIdentity{
		// Another project, renamed from team/old-name to team/web.
		{ID: mustRepositoryIdentity(t, "team/old-name"), Repo: "team/web", IdentityPath: "team/old-name", ProviderRepoID: "9", LastSynced: t0},
		// A project that took over acme/api after the syncing project moved.
		{ID: mustRepositoryIdentity(t, "acme/api@456"), Repo: "acme/api", IdentityPath: "acme/api@456", ProviderRepoID: "456", LastSynced: t0},
	}}
	claim := nativeTestClaim("gitlab", "work-items")
	own := GitLabRepoIdentity{RepoID: mustRepositoryIdentity(t, "acme/api"), Path: "acme/api"}
	rows := []gitlabWorkItemDependencyRow{
		// "#7" in the syncing project's own text names its current path.
		{SourceWorkItemID: "gitlab:acme/api#5", TargetWorkItemID: "gitlab:platform/api-v2#7", RelationshipType: "relates_to"},
		// "team/web#3" names the other project by its current path.
		{SourceWorkItemID: "gitlab:acme/api#5", TargetWorkItemID: "gitlab:team/web#3", RelationshipType: "blocks"},
		// "acme/api#8" names the project at acme/api now: the one that took it over.
		{SourceWorkItemID: "gitlab:acme/api#5", TargetWorkItemID: "gitlab:acme/api#8", RelationshipType: "relates_to"},
		// An unknown project keeps its literal path; a Jira key is not a GitLab id.
		{SourceWorkItemID: "gitlab:acme/api#5", TargetWorkItemID: "gitlab:else/where!2", RelationshipType: "relates_to"},
		{SourceWorkItemID: "gitlab:acme/api#5", TargetWorkItemID: "jira:OPS-1", RelationshipType: "relates_to"},
		// The own item through its current path is a self link, and dropped.
		{SourceWorkItemID: "gitlab:acme/api#5", TargetWorkItemID: "gitlab:platform/api-v2#5", RelationshipType: "relates_to"},
		// The same link through both path forms is one row.
		{SourceWorkItemID: "gitlab:acme/api#5", TargetWorkItemID: "gitlab:team/old-name#3", RelationshipType: "blocks"},
	}
	owners := appendGitLabDependencyOwner(nil, "gitlab:acme/api#5", len(rows))
	paths, err := resolveGitLabIdentityPaths(context.Background(), reader, claim, "platform/api-v2", own, gitLabDependencyPaths(rows, owners))
	if err != nil {
		t.Fatal(err)
	}
	got := canonicalGitLabDependencies(rows, owners, paths)
	want := []string{
		"gitlab:acme/api#5 -> gitlab:acme/api#7",
		"gitlab:acme/api#5 -> gitlab:team/old-name#3",
		"gitlab:acme/api#5 -> gitlab:acme/api@456#8",
		"gitlab:acme/api#5 -> gitlab:else/where!2",
		"gitlab:acme/api#5 -> jira:OPS-1",
	}
	edges := make([]string, 0, len(got))
	for _, row := range got {
		edges = append(edges, row.SourceWorkItemID+" -> "+row.TargetWorkItemID)
	}
	if !reflect.DeepEqual(edges, want) {
		t.Fatalf("edges = %q, want %q", edges, want)
	}
	if len(reader.paths) != 1 {
		t.Fatalf("the store was read %d times, want once", len(reader.paths))
	}
	for _, path := range reader.paths[0] {
		if path == "platform/api-v2" {
			t.Fatalf("the syncing project's current path was looked up: %q", reader.paths[0])
		}
	}
}

func TestSplitGitLabWorkItemIDReadsOnlyGitLabItemIDs(t *testing.T) {
	for _, tc := range []struct {
		id, path, rest string
		ok             bool
	}{
		{"gitlab:acme/api#5", "acme/api", "#5", true},
		{"gitlab:acme/api@456!12", "acme/api@456", "!12", true},
		{"gitlab:acme/api:milestone:4", "", "", false},
		{"gitlab:acme/api#", "", "", false},
		{"gitlab:#5", "", "", false},
		{"gh:acme/api#5", "", "", false},
	} {
		path, rest, ok := splitGitLabWorkItemID(tc.id)
		if path != tc.path || rest != tc.rest || ok != tc.ok {
			t.Fatalf("%s: got (%q, %q, %v)", tc.id, path, rest, ok)
		}
	}
}
