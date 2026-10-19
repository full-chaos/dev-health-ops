package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type fakeGitHubIdentityReader struct{ rows []GitLabStoredRepoIdentity }

func (reader fakeGitHubIdentityReader) StoredRepoIdentities(context.Context, Claim, int64, string) ([]GitLabStoredRepoIdentity, error) {
	return reader.rows, nil
}

func (reader fakeGitHubIdentityReader) StoredRepoIdentitiesAtPaths(context.Context, Claim, []string) ([]GitLabStoredRepoIdentity, error) {
	return reader.rows, nil
}

// CHAOS-9879: a closing reference and an owner/repo#n in text name another
// repository by the name the payload states; the row carries the identity the
// target's own items carry, and the syncing repository's own current name maps
// to its identity.
func TestCanonicalGitHubForeignRefsFollowARenamedTarget(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	names := githubRepoNames{Current: "platform/api-v2", Identity: "acme/api"}
	reader := fakeGitHubIdentityReader{rows: []GitLabStoredRepoIdentity{{
		ID: mustRepositoryIdentityForTest(t, "acme/target"), Repo: "platform/target-v2", IdentityPath: "acme/target", ProviderRepoID: "9",
	}}}

	text, err := extractGitHubWorkItemDependencies(claim, "gh:acme/api#1", names.Identity,
		"Closes #5\nblocks platform/target-v2#42\nfixes platform/api-v2#6", "", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	node, _ := json.Marshal(map[string]any{"number": 43, "repository": map[string]string{"nameWithOwner": "platform/target-v2"}})
	closing, err := extractGitHubClosingIssueReferences(claim, names, "ghpr:acme/api#2", []json.RawMessage{node}, at)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := canonicalGitHubForeignRefs(context.Background(), reader, claim, names, append(text, closing...))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[row.SourceWorkItemID+" -> "+row.TargetWorkItemID] = true
		if row.foreignID != "" {
			t.Errorf("row %+v keeps its foreign marker", row)
		}
	}
	for _, want := range []string{
		"gh:acme/api#1 -> gh:acme/api#5",
		"gh:acme/api#1 -> gh:acme/target#42",
		"gh:acme/api#1 -> gh:acme/api#6",
		"ghpr:acme/api#2 -> gh:acme/target#43",
	} {
		if !got[want] {
			t.Errorf("missing %q in %v", want, got)
		}
	}
}

// A board item names its repository by the current name; the work item and the
// membership subject carry the identity name and repo_id the repository's own
// sync wrote.
func TestGitHubProjectV2ItemsFollowARenamedRepository(t *testing.T) {
	reader := fakeGitHubIdentityReader{rows: []GitLabStoredRepoIdentity{{
		ID: mustRepositoryIdentityForTest(t, "acme/api"), Repo: "platform/api-v2", IdentityPath: "acme/api", ProviderRepoID: "123",
	}}}
	issue := gitHubProjectV2ItemPayload{Content: gitHubProjectV2ContentPayload{Typename: "Issue", Number: 7}}
	issue.Content.Repository.NameWithOwner = "platform/api-v2"
	pr := gitHubProjectV2ItemPayload{Content: gitHubProjectV2ContentPayload{Typename: "PullRequest", Number: 8}}
	pr.Content.Repository.NameWithOwner = "platform/api-v2"
	items, err := (GitHubProjectV2Fetcher{RepoIdentity: reader}).itemsByRepositoryIdentity(
		context.Background(), githubWorkItemOracleClaim(), []gitHubProjectV2ItemPayload{issue, pr})
	if err != nil {
		t.Fatal(err)
	}
	subject, ok := githubProjectV2ItemSubject(items[0])
	if !ok || subject.SubjectID != "gh:acme/api#7" || subject.RepoID.String() != mustRepositoryIdentityForTest(t, "acme/api") {
		t.Fatalf("issue subject = %+v ok=%t, want gh:acme/api#7 on the minted repo_id", subject, ok)
	}
	subject, ok = githubProjectV2ItemSubject(items[1])
	if !ok || subject.RepoID.String() != mustRepositoryIdentityForTest(t, "acme/api") {
		t.Fatalf("pull request subject = %+v ok=%t, want the minted repo_id", subject, ok)
	}
	if issue.Content.Repository.NameWithOwner != "platform/api-v2" {
		t.Fatal("the fetched item was modified in place")
	}
}
