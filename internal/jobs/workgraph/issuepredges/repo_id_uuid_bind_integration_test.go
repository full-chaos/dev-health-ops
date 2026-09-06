//go:build integration

package issuepredges_test

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestLoadersBindRepoIDAsStringNotRawUUID is the regression test for
// CHAOS-5358: binding a raw uuid.UUID value to a {repo_id:UUID} named
// ClickHouse query parameter fails server-side ("expected 32 hexadecimal
// digits"), while binding its .String() form against the identical
// placeholder succeeds. Every public Loader method in this package that
// accepts a Window with a non-nil RepoID exercises this bind -- covers all
// three (LoadFastPath, LoadTextParseInputs, LoadHeuristicInputs), which
// together hit all four repo_id-bound queries in clickhouse.go.
func TestLoadersBindRepoIDAsStringNotRawUUID(t *testing.T) {
	ctx := context.Background()
	conn := repoIDBindConnect(ctx, t)

	loader, err := issuepredges.NewLoader(conn)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}

	org := "org-5358-" + uuid.NewString()
	repo := uuid.New()
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	window := issuepredges.Window{From: &from, To: &to, RepoID: &repo, HeuristicDaysWindow: 7, HeuristicConfidence: 0.3}

	if _, err := loader.LoadFastPath(ctx, org, window); err != nil {
		t.Errorf("LoadFastPath with a non-nil Window.RepoID: %v -- this is CHAOS-5358: "+
			"binding a raw uuid.UUID to {repo_id:UUID} fails server-side", err)
	}
	if _, _, err := loader.LoadTextParseInputs(ctx, org, window); err != nil {
		t.Errorf("LoadTextParseInputs with a non-nil Window.RepoID: %v -- CHAOS-5358", err)
	}
	if _, _, _, err := loader.LoadHeuristicInputs(ctx, org, window); err != nil {
		t.Errorf("LoadHeuristicInputs with a non-nil Window.RepoID: %v -- CHAOS-5358", err)
	}
}

func repoIDBindConnect(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	opts, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
