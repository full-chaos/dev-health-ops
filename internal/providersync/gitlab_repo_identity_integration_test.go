//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type gitLabIdentityStore struct {
	t      *testing.T
	ctx    context.Context
	conn   driver.Conn
	lease  providerfoundation.LeaseGuard
	reader GitLabRepoIdentityClickHouseReader
}

func startGitLabIdentityStore(t *testing.T) *gitLabIdentityStore {
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
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	return &gitLabIdentityStore{t: t, ctx: ctx, conn: conn, lease: lease,
		reader: GitLabRepoIdentityClickHouseReader{Conn: conn, Lease: lease}}
}

// syncRepository runs the production-selected repo-metadata route for project
// nativeID at path and writes its effect.
func (store *gitLabIdentityStore) syncRepository(nativeID, path string, at time.Time) {
	t := store.t
	t.Helper()
	route, ok := SelectGitFamilyRoute("gitlab", "repo-metadata", GitFamilyDeps{Conn: store.conn, Lease: store.lease})
	if !ok {
		t.Fatal("no gitlab repo-metadata route")
	}
	claim := nativeTestClaim("gitlab", "repo-metadata")
	claim.SourceExternalID, claim.SourceName = nativeID, path
	body := `{"id":` + nativeID + `,"name":"x","path_with_namespace":"` + path + `","web_url":"https://gitlab.example/` + path + `","default_branch":"main"}`
	batch, err := route.Handler.Collect(store.ctx, claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(&gitLabRepositoryDoer{t: t, body: body}), "https://gitlab.example"), at)
	if err != nil {
		t.Fatal(err)
	}
	if err := route.Sink.WriteEffect(store.ctx, claim, batch.Effects[0]); err != nil {
		t.Fatal(err)
	}
}

// workItem42 runs the GitLab work-items route with the store's reader for
// project nativeID at path and returns issue #42's (work_item_id, repo_id,
// project_id).
func (store *gitLabIdentityStore) workItem42(nativeID, path string) (string, string, string) {
	t := store.t
	t.Helper()
	responses := map[string][]string{}
	for key, values := range gitLabWorkItemResponses() {
		key = strings.Replace(key, "/api/v4/projects/123", "/api/v4/projects/"+nativeID, 1)
		for index := range values {
			values[index] = strings.ReplaceAll(values[index], "acme/api", path)
		}
		responses[key] = values
	}
	responses["/api/v4/projects/"+nativeID] = []string{
		`{"id":` + nativeID + `,"path":"x","path_with_namespace":"` + path + `","name":"x"}`,
	}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.SourceExternalID, claim.SourceName = nativeID, path
	handler := GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
		RepoIdentity: store.reader}
	batch, err := handler.Collect(store.ctx, claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: responses})), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "work_items" {
			continue
		}
		for _, raw := range effect.Rows {
			var row gitlabWorkItemRow
			if err := jsonUnmarshalEffect(raw, &row); err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(row.WorkItemID, "#42") {
				return row.WorkItemID, row.RepoID.String(), *row.ProjectID
			}
		}
	}
	t.Fatal("issue #42 not written")
	return "", "", ""
}

func (store *gitLabIdentityStore) reposRows() map[string][3]string {
	t := store.t
	t.Helper()
	rows, err := store.conn.Query(store.ctx, `SELECT toString(id), repo, identity_path, provider_repo_id FROM repos FINAL WHERE org_id = 'org-acme'`)
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
	return out
}

// Rename / transfer (CHAOS-3383 red case SPLIT): the same project under a new
// path continues the same repository and work item on a real store.
func TestGitLabRenameContinuesTheSameRepositoryAndWorkItem(t *testing.T) {
	store := startGitLabIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	beforeID, beforeRepo, beforeProject := store.workItem42("123", "acme/api")

	// The work-items unit can run before the next repo-metadata unit.
	afterID, afterRepo, afterProject := store.workItem42("123", "platform/api-v2")
	if afterID != beforeID || afterRepo != beforeRepo {
		t.Fatalf("SPLIT before the repo-metadata sync: (%s, %s) became (%s, %s)", beforeRepo, beforeID, afterRepo, afterID)
	}
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))
	againID, againRepo, againProject := store.workItem42("123", "platform/api-v2")
	if againID != beforeID || againRepo != beforeRepo {
		t.Fatalf("SPLIT after the repo-metadata sync: (%s, %s) became (%s, %s)", beforeRepo, beforeID, againRepo, againID)
	}
	if beforeID != "gitlab:acme/api#42" {
		t.Fatalf("work_item_id = %s, want the minted path gitlab:acme/api#42", beforeID)
	}
	// project_id is the catalog id of the numeric project, whatever its path.
	if beforeProject != "org-acme:gitlab:123" || afterProject != beforeProject || againProject != beforeProject {
		t.Fatalf("project_id = %s, %s, %s; want org-acme:gitlab:123 across the rename", beforeProject, afterProject, againProject)
	}
	want := map[string][3]string{beforeRepo: {"platform/api-v2", "acme/api", "123"}}
	if got := store.reposRows(); len(got) != 1 || got[beforeRepo] != want[beforeRepo] {
		t.Fatalf("repos = %v, want one row %v (current path, minted path, numeric id)", got, want)
	}
}

// Path reuse / delete+recreate (red case TAKEOVER): a different project at a
// path another project's id was minted from gets its own identity.
func TestGitLabPathReuseMintsItsOwnIdentity(t *testing.T) {
	store := startGitLabIdentityStore(t)
	t0 := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	store.syncRepository("123", "acme/api", t0)
	oldID, oldRepo, oldProject := store.workItem42("123", "acme/api")
	store.syncRepository("123", "platform/api-v2", t0.Add(time.Hour))

	newID, newRepo, newProject := store.workItem42("456", "acme/api")
	if newID == oldID || newRepo == oldRepo {
		t.Fatalf("TAKEOVER: projects 123 and 456 write the same work_items key (%s, %s)", newRepo, newID)
	}
	if newID != "gitlab:acme/api@456#42" || newRepo != mustRepositoryIdentity(t, "acme/api@456") {
		t.Fatalf("project 456: (%s, %s), want the {path}@{N} identity", newRepo, newID)
	}
	// LEAK (CHAOS-3383 3b2): the two projects must not share a project_id, or
	// project ownership of one attributes the other's items.
	if newProject == oldProject || newProject != "org-acme:gitlab:456" || oldProject != "org-acme:gitlab:123" {
		t.Fatalf("project_id of project 123 = %s, of project 456 = %s; want org-acme:gitlab:123 and org-acme:gitlab:456", oldProject, newProject)
	}
	store.syncRepository("456", "acme/api", t0.Add(2*time.Hour))
	again, againRepo, _ := store.workItem42("456", "acme/api")
	if again != newID || againRepo != newRepo {
		t.Fatalf("project 456 after its repo-metadata sync: (%s, %s), want (%s, %s)", againRepo, again, newRepo, newID)
	}
	got := store.reposRows()
	if got[oldRepo] != [3]string{"platform/api-v2", "acme/api", "123"} || got[newRepo] != [3]string{"acme/api", "acme/api@456", "456"} {
		t.Fatalf("repos = %v", got)
	}
}

// A row written before migration 134 (no provider_repo_id) or 138 (no
// identity_path) still decides its project's identity through the reader.
func TestGitLabRepoIdentityReaderReadsRowsFromBeforeTheAnchorColumns(t *testing.T) {
	store := startGitLabIdentityStore(t)
	id := mustRepositoryIdentity(t, "acme/api")
	if err := store.conn.Exec(store.ctx, `INSERT INTO repos (id, repo, created_at, settings, tags, provider, last_synced, org_id)
VALUES (?, 'acme/api', now64(3), ?, '["gitlab"]', 'gitlab', now64(3), 'org-acme')`, id, `{"source":"gitlab","project_id":123}`); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "work-items")
	identity, err := resolveGitLabRepoIdentity(store.ctx, store.reader, claim, 123, "platform/api-v2")
	if err != nil {
		t.Fatal(err)
	}
	if identity != (GitLabRepoIdentity{RepoID: id, Path: "acme/api"}) {
		t.Fatalf("identity = %+v, want the legacy row's", identity)
	}
	taken, err := resolveGitLabRepoIdentity(store.ctx, store.reader, claim, 456, "acme/api")
	if err != nil {
		t.Fatal(err)
	}
	if taken.Path != "acme/api@456" {
		t.Fatalf("project 456 at the legacy row's path: %+v, want acme/api@456", taken)
	}
	held, err := store.reader.StoredRepoIdentitiesAtPaths(store.ctx, claim, []string{"acme/api"})
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 1 || held[0].ID != id || held[0].SettingsProjectID != 123 {
		t.Fatalf("rows at acme/api = %+v", held)
	}
	if _, err := (GitLabRepoIdentityClickHouseReader{Conn: store.conn}).StoredRepoIdentities(store.ctx, claim, 123, "acme/api"); err == nil {
		t.Fatal("a reader without a lease read the store")
	}
}
