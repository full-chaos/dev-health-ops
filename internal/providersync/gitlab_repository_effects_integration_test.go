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

func TestGitLabRepositoryReadbackSeparatesTenantKeyCollision(t *testing.T) {
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
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabRepositoryClickHouseEffects{Conn: conn, Lease: lease}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claimA := nativeTestClaim("gitlab", "repo-metadata")
	claimA.OrgID = "org-a"
	claimB := claimA
	claimB.OrgID = "org-b"
	rowA := repositoryFixtureRow(claimA.OrgID, now)
	rowA.Provider = "gitlab"
	rowA.Settings = `{"source":"gitlab","tenant":"a"}`
	rowA.Tags = `["gitlab"]`
	rowB := rowA
	rowB.OrgID = claimB.OrgID
	rowB.Settings = `{"source":"gitlab","tenant":"b"}`
	effectA := repositoryEffect(t, claimA, rowA)
	effectB := repositoryEffect(t, claimB, rowB)
	if err := sink.WriteEffect(ctx, claimA, effectA); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claimB, effectB); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		claim  Claim
		effect EffectBatch
	}{
		"org-a": {claimA, effectA},
		"org-b": {claimB, effectB},
	} {
		t.Run(name, func(t *testing.T) {
			inspection, err := sink.InspectEffect(ctx, test.claim, test.effect)
			if err != nil || inspection != EffectExact {
				t.Fatalf("inspection=%s error=%v", inspection, err)
			}
		})
	}
}
