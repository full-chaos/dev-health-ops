//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestGitLabPullRequestCommitLinkEffectsWriteAndReadBackNativeRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	claim := nativeTestClaim("gitlab", "prs")
	now := time.Date(2026, 8, 4, 12, 0, 0, 123000000, time.UTC)
	repoID := "11111111-2222-4333-8444-555555555555"
	var raws []json.RawMessage
	for _, hash := range []string{"c0ffee01", "c0ffee02"} {
		raw, err := json.Marshal(prCommitLinkRow{
			RepoID: repoID, PRNumber: 7, CommitHash: hash, Confidence: 1,
			Provenance: "native", Evidence: "api_pr_commits", LastSynced: now, OrgID: claim.OrgID,
		})
		if err != nil {
			t.Fatal(err)
		}
		raws = append(raws, raw)
	}
	effect, err := BuildEffectBatch("work_graph_pr_commit", EffectReadbackRequired, raws)
	if err != nil {
		t.Fatal(err)
	}
	sink := GitLabPullRequestSocialClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("before write: inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("after write: inspection=%s error=%v", inspection, err)
	}
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_graph_pr_commit FINAL WHERE org_id = ? AND provenance = 'native' AND evidence = 'api_pr_commits'`, claim.OrgID).Scan(&stored); err != nil || stored != 2 {
		t.Fatalf("stored native links=%d error=%v want 2", stored, err)
	}

	github := nativeTestClaim("github", "prs")
	githubSink := GitHubPullRequestSocialClickHouseEffects{Conn: conn, Lease: sink.Lease}
	if err := githubSink.WriteEffect(ctx, github, effect); err == nil {
		t.Fatal("a github unit wrote merge request commit links")
	}
}
