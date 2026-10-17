//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A writer of an older build inserts repos rows without provider_repo_id and
// identity_path (main's Python ClickHouseStore.insert_repo column list), so
// its newer version reads both as empty. The next GitLab repo-metadata sync
// states both again through the real route and sink: the anchor heals at the
// next sync of the project.
func TestGitLabRepositorySyncRestatesTheAnchorAnOldWriterErased(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
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
	sink := GitLabRepositoryClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	claim := nativeTestClaim("gitlab", "repo-metadata")
	sync := func(at time.Time) repositoryRow {
		t.Helper()
		doer := &gitLabRepositoryDoer{t: t, body: gitLabRepositoryFixture}
		batch, err := (GitLabRepositoryRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{},
			gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), at)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, claim, batch.Effects[0]); err != nil {
			t.Fatal(err)
		}
		rows, err := decodeEffectRows[repositoryRow](batch.Effects[0])
		if err != nil {
			t.Fatal(err)
		}
		return rows[0]
	}
	read := func(id string) [2]string {
		t.Helper()
		var anchor [2]string
		if err := conn.QueryRow(ctx, `SELECT provider_repo_id, identity_path FROM repos FINAL WHERE org_id = ? AND id = ?`,
			claim.OrgID, id).Scan(&anchor[0], &anchor[1]); err != nil {
			t.Fatal(err)
		}
		return anchor
	}

	first := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	row := sync(first)
	want := [2]string{"123", "Acme/API"}
	if got := read(row.ID); got != want {
		t.Fatalf("after the first GitLab sync the anchor is %v, want %v", got, want)
	}

	// The older build's writer: its own column list, no anchor columns.
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, ref, created_at, settings, tags, provider, last_synced, source_id, org_id)
VALUES (?, 'Acme/API', NULL, ?, '{}', '["gitlab"]', 'gitlab', ?, NULL, ?)`,
		row.ID, first, first.Add(time.Hour), claim.OrgID); err != nil {
		t.Fatal(err)
	}
	if got := read(row.ID); got != [2]string{"", ""} {
		t.Fatalf("the older writer's version reads %v; the case is not set", got)
	}

	sync(first.Add(2 * time.Hour))
	if got := read(row.ID); got != want {
		t.Fatalf("after the next GitLab sync the anchor is %v, want it restated as %v", got, want)
	}
}
