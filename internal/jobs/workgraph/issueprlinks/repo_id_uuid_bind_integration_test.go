//go:build integration

package issueprlinks_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// TestLoadBindsRepoIDAsStringNotRawUUID is the regression test for
// CHAOS-5358: binding a raw uuid.UUID value to a {repo_id:UUID} named
// ClickHouse query parameter fails server-side ("expected 32 hexadecimal
// digits"), while binding its .String() form against the identical
// placeholder succeeds. Reuses connect/mustExec from
// provenance_collision_integration_test.go (same package).
func TestLoadBindsRepoIDAsStringNotRawUUID(t *testing.T) {
	ctx := context.Background()
	conn := connect(ctx, t)

	loader, err := issueprlinks.NewLoader(conn)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}

	org := "org-5358-" + uuid.NewString()
	repo := uuid.New()
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)

	if _, err := loader.Load(ctx, org, issueprlinks.Window{From: &from, To: &to, RepoID: &repo}); err != nil {
		t.Errorf("Load with a non-nil Window.RepoID: %v -- this is CHAOS-5358: "+
			"binding a raw uuid.UUID to {repo_id:UUID} fails server-side", err)
	}
}
