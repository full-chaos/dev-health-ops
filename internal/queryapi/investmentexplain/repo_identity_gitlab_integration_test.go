//go:build integration

package investmentexplain

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-3383: a renamed GitLab project's merge-request work items carry the
// path its repository id was minted from (repos.identity_path), so a PR
// evidence ref resolves to that id, not one built from the current path. A
// GitHub row's identity_path is never read (provider-scoped).
func TestFetchRepoIdentitiesNamesARenamedGitLabRepositoryByItsMintedPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Exec(ctx, `CREATE TABLE repos (
    id UUID, repo String, ref Nullable(String), created_at DateTime64(3, 'UTC'),
    settings Nullable(String), tags Nullable(String), last_synced DateTime64(3, 'UTC'),
    org_id String DEFAULT 'default', provider String DEFAULT 'unknown', source_id Nullable(UUID),
    provider_repo_id String DEFAULT '', identity_path String DEFAULT ''
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`); err != nil {
		t.Fatal(err)
	}
	const org = "chaos-3383"
	renamed, github := "c7198fbc-1945-3717-05d8-eb78866b4e79", "1ebaec64-a77c-6b80-bc85-38e95e16d90e"
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider, provider_repo_id, identity_path) VALUES
    (?, 'platform/api-v2', now64(3), now64(3), ?, 'gitlab', '123', 'acme/api'),
    (?, 'acme/web', now64(3), now64(3), ?, 'github', '', 'never/read')`, renamed, org, github, org); err != nil {
		t.Fatal(err)
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	identities, err := reader.FetchRepoIdentities(ctx, org, []string{renamed, github})
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{renamed: "gitlab:acme/api!5", github: "ghpr:acme/web#5"} {
		got, ok := prRefWorkItemID(id+"#pr5", identities)
		if !ok || got != want {
			t.Errorf("%s#pr5 -> (%q, %v), want %q", id, got, ok, want)
		}
	}
}
