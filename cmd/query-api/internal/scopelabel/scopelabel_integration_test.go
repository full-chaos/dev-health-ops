//go:build integration

package scopelabel

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	idA = "11111111-1111-1111-1111-111111111111"
	idB = "22222222-2222-2222-2222-222222222222"
)

func TestRealClickHouse_Resolve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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
	for _, ddl := range []string{
		`CREATE TABLE repos (id UUID, repo String, org_id String DEFAULT 'default', created_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(last_synced) ORDER BY id`,
		`CREATE TABLE teams (id String, name String, org_id String DEFAULT 'default', updated_at DateTime64(6)) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id`,
	} {
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	exec := func(q string, a ...any) {
		t.Helper()
		if err := conn.Exec(ctx, fmt.Sprintf(q, a...)); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// idA: renamed but not merged (old name older); idB: another org's repo; a repo whose name is a bare UUID.
	exec(`INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/old', 'org-1', now64(3), now64(3) - INTERVAL 1 HOUR`, idA)
	exec(`INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/new', 'org-1', now64(3), now64(3)`, idA)
	exec(`INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'other/web', 'org-2', now64(3), now64(3)`, idB)
	exec(`INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '33333333-3333-3333-3333-333333333333', '33333333-3333-3333-3333-333333333333', 'org-1', now64(3), now64(3)`)
	exec(`INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6)), ('t2', ' Growth ', 'org-1', now64(6))`)

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	merged := Resolve(ctx, client, "org-1", "repo", []string{idA, idB, "33333333-3333-3333-3333-333333333333", ""}, Options{Final: true})
	if len(merged) != 1 || merged[idA] != "acme/new" {
		t.Errorf("merged view: %v (own org only, a UUID-shaped name omitted)", merged)
	}
	raw := Resolve(ctx, client, "org-1", "repo", []string{idA}, Options{})
	if len(raw) != 1 || (raw[idA] != "acme/new" && raw[idA] != "acme/old") {
		t.Errorf("raw view: %v", raw)
	}
	other := Resolve(ctx, client, "org-2", "repo", []string{idA, idB}, Options{Final: true})
	if len(other) != 1 || other[idB] != "other/web" {
		t.Errorf("org-2: %v", other)
	}
	teams := Resolve(ctx, client, "org-1", "team", []string{"t1", "t2", "t9"}, Options{Final: true})
	if len(teams) != 2 || teams["t1"] != "Platform" || teams["t2"] != "Growth" {
		t.Errorf("teams: %v (names are trimmed)", teams)
	}
	if got := Resolve(ctx, client, "org-1", "team", []string{"t1"}, Options{Final: true, Suffix: "SETTINGS max_execution_time = 5"}); got["t1"] != "Platform" {
		t.Errorf("with a settings suffix: %v", got)
	}
	if got := Resolve(ctx, client, "org-1", "work_type", []string{"x"}, Options{}); len(got) != 0 {
		t.Errorf("unknown kind: %v", got)
	}
}
