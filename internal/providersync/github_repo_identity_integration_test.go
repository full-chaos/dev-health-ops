//go:build integration

package providersync

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type gitHubIdentityStore struct {
	t     *testing.T
	ctx   context.Context
	conn  driver.Conn
	lease providerfoundation.LeaseGuard
}

func startGitHubIdentityStore(t *testing.T) *gitHubIdentityStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, reposDDL); err != nil {
		t.Fatal(err)
	}
	return &gitHubIdentityStore{t: t, ctx: ctx, conn: conn,
		lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
}

func gitHubIdentityRepoBody(nativeID, name string) string {
	return `{"id":` + nativeID + `,"name":"x","full_name":"` + name + `","html_url":"https://github.com/` + name + `","default_branch":"main","language":"Go"}`
}

// syncRepository runs the production-selected repo-metadata route for the
// repository nativeID that GitHub now calls name and writes its effect.
func (store *gitHubIdentityStore) syncRepository(nativeID, name string, at time.Time) {
	t := store.t
	t.Helper()
	route, ok := SelectGitFamilyRoute("github", "repo-metadata", GitFamilyDeps{Conn: store.conn, Lease: store.lease})
	if !ok {
		t.Fatal("no github repo-metadata route")
	}
	claim := nativeTestClaim("github", "repo-metadata")
	claim.SourceExternalID, claim.SourceName = name, name
	doer := &gitHubRepositoryDoer{t: t, body: gitHubIdentityRepoBody(nativeID, name)}
	batch, err := route.Handler.Collect(store.ctx, claim, providerfoundation.Credential{},
		gitHubRepositoryClient(t, doer, "https://api.github.com"), at)
	if err != nil {
		t.Fatal(err)
	}
	if err := route.Sink.WriteEffect(store.ctx, claim, batch.Effects[0]); err != nil {
		t.Fatal(err)
	}
}

// workItem42 runs the production-selected GitHub work-items REST collector for
// the repository nativeID that GitHub now calls name and returns issue #42's
// (work_item_id, repo_id, project_id).
func (store *gitHubIdentityStore) workItem42(nativeID, name string) (string, string, string) {
	t := store.t
	t.Helper()
	handler, ok := SelectWorkItemsRouteHandler("github", "work-items", WorkItemsRouteDeps{
		Conn: store.conn, Lease: store.lease, StatusMapping: &StatusMapping{},
	})
	if !ok {
		t.Fatal("no github work-items route")
	}
	collector := handler.(GitHubWorkItemsRouteHandler).REST
	root := "/repos/" + name
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		root:                       {{body: gitHubIdentityRepoBody(nativeID, name)}},
		root + "/issues":           {{body: `[{"number":42,"title":"Issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z","user":{"login":"octocat"}}]`}},
		root + "/issues/42/events": {{body: `[]`}},
	}}
	claim := githubWorkItemsRESTClaim()
	claim.SourceExternalID, claim.SourceName = name, name
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": false, "fetch_comments": false, "fetch_milestones": false,
	}
	result, err := collector.Collect(store.ctx, claim,
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"),
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range result.Rows.WorkItems {
		if row.WorkItemID == "" || row.RepoID == nil || row.ProjectID == nil {
			continue
		}
		if len(row.WorkItemID) > 3 && row.WorkItemID[len(row.WorkItemID)-3:] == "#42" {
			return row.WorkItemID, row.RepoID.String(), *row.ProjectID
		}
	}
	t.Fatal("issue #42 not written")
	return "", "", ""
}

func (store *gitHubIdentityStore) reposRows() map[string][3]string {
	t := store.t
	t.Helper()
	rows, err := store.conn.Query(store.ctx, `SELECT toString(id), repo, identity_path, provider_repo_id FROM repos FINAL WHERE org_id = 'org-acme' AND provider = 'github'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string][3]string{}
	for rows.Next() {
		var id, repo, path, native string
		if err := rows.Scan(&id, &repo, &path, &native); err != nil {
			t.Fatal(err)
		}
		out[id] = [3]string{repo, path, native}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Rename / transfer (CHAOS-9879 red case SPLIT): the same repository under a
// new name continues the same repository and work item on a real store.
func TestGitHubRenameContinuesTheSameRepositoryAndWorkItem(t *testing.T) {
	store := startGitHubIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	beforeID, beforeRepo, beforeProject := store.workItem42("123", "acme/api")
	if beforeID != "gh:acme/api#42" || beforeProject != "acme/api" {
		t.Fatalf("work_item_id, project_id = %s, %s; want gh:acme/api#42, acme/api", beforeID, beforeProject)
	}

	// The work-items unit can run before the next repo-metadata unit.
	afterID, afterRepo, afterProject := store.workItem42("123", "platform/api-v2")
	if afterID != beforeID || afterRepo != beforeRepo {
		t.Fatalf("SPLIT before the repo-metadata sync: (%s, %s) became (%s, %s)", beforeRepo, beforeID, afterRepo, afterID)
	}
	// project_id names the repository now (name-keyed ownership grants join on it).
	if afterProject != "platform/api-v2" {
		t.Fatalf("project_id after the rename = %s, want platform/api-v2", afterProject)
	}
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))
	againID, againRepo, _ := store.workItem42("123", "platform/api-v2")
	if againID != beforeID || againRepo != beforeRepo {
		t.Fatalf("SPLIT after the repo-metadata sync: (%s, %s) became (%s, %s)", beforeRepo, beforeID, againRepo, againID)
	}
	got := store.reposRows()
	if len(got) != 1 || got[beforeRepo] != [3]string{"platform/api-v2", "acme/api", "123"} {
		t.Fatalf("repos = %v, want one row {current name, minted name, numeric id}", got)
	}
}

// Name reuse / delete+recreate (red case TAKEOVER): a different repository at a
// name another repository's id was minted from gets its own identity.
func TestGitHubNameReuseMintsItsOwnIdentity(t *testing.T) {
	store := startGitHubIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	oldID, oldRepo, _ := store.workItem42("123", "acme/api")
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))

	newID, newRepo, newProject := store.workItem42("456", "acme/api")
	if newID == oldID || newRepo == oldRepo {
		t.Fatalf("TAKEOVER: repositories 123 and 456 write the same work_items key (%s, %s)", newRepo, newID)
	}
	wantRepo := mustRepositoryIdentity(t, "acme/api@456")
	if newID != "gh:acme/api@456#42" || newRepo != wantRepo || newProject != "acme/api" {
		t.Fatalf("repository 456: (%s, %s, %s), want the {name}@{id} identity (gh:acme/api@456#42, %s, acme/api)", newRepo, newID, newProject, wantRepo)
	}
	store.syncRepository("456", "acme/api", t0.Add(2*time.Hour))
	again, againRepo, _ := store.workItem42("456", "acme/api")
	if again != newID || againRepo != newRepo {
		t.Fatalf("repository 456 after its repo-metadata sync: (%s, %s), want (%s, %s)", againRepo, again, newRepo, newID)
	}
	got := store.reposRows()
	if got[oldRepo] != [3]string{"platform/api-v2", "acme/api", "123"} ||
		got[newRepo] != [3]string{"acme/api", "acme/api@456", strconv.Itoa(456)} {
		t.Fatalf("repos = %v", got)
	}
}
