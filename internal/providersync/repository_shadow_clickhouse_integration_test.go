//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestRepositoryShadowLoadsPythonOwnedClickHouseProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	}()
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE repos (
	id UUID,
	org_id String,
	repo String,
	provider String,
	settings String,
	last_synced DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, id)`); err != nil {
		t.Fatal(err)
	}
	pythonObserved := time.Date(2026, 7, 23, 12, 1, 0, 0, time.UTC)
	if err := conn.Exec(
		ctx,
		"INSERT INTO repos VALUES (?, ?, ?, ?, ?, ?)",
		"77777777-7777-4777-8777-777777777777",
		"org-acme",
		"acme/api",
		"github",
		`{"source":"github","repo_id":1,"url":"https://github.example/acme/api","default_branch":"main"}`,
		pythonObserved,
	); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "repo-metadata")
	native, err := (NativeRESTHandler{}).normalizeRepository(
		claim,
		"github:repo:acme/api",
		repositoryPayload{
			FullName: "acme/api", HTMLURL: "https://github.example/acme/api",
			DefaultBranch: "main", Archived: true,
			UpdatedAt: "2026-07-20T10:00:00Z",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := (NormalizedShadowComparator{
		Python: ClickHouseRepositoryShadowSource{Conn: conn},
	}).Compare(ctx, claim, []providerfoundation.NormalizedEnvelope{native})
	if err != nil || !comparison.Match {
		t.Fatalf("comparison=%+v error=%v", comparison, err)
	}
}
