//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestGitLabCICDEffectsAreAtomicDeduplicatedTenantScopedAndRetrySafe(t *testing.T) {
	ctx, sink := newGitLabCICDIntegrationSink(t)
	now := time.Date(2026, 8, 3, 10, 0, 0, 123000000, time.UTC)
	claimA := nativeTestClaim("gitlab", "cicd")
	claimB := claimA
	claimB.OrgID = "other-org"
	statusA, statusB := "success", "failed"
	queued := now.Add(-2 * time.Minute)
	finished := now.Add(-time.Minute)
	rowA := gitLabCICDPipelineRow{
		OrgID: claimA.OrgID, RepoID: "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
		RunID: "901", Status: &statusA, QueuedAt: &queued, StartedAt: queued,
		FinishedAt: &finished, LastSynced: now,
	}
	rowB := rowA
	rowB.OrgID = claimB.OrgID
	rowB.Status = &statusB
	effectA := gitLabCICDEffect(t, rowA)
	effectB := gitLabCICDEffect(t, rowB)

	duplicate, err := BuildEffectBatch(
		"ci_pipeline_runs", EffectReadbackRequired,
		[]json.RawMessage{effectA.Rows[0], effectA.Rows[0]},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claimA, duplicate); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate error=%v", err)
	}

	if err := sink.Conn.Exec(ctx, "ALTER TABLE ci_pipeline_runs ADD CONSTRAINT reject_bad CHECK run_id != 'reject'"); err != nil {
		t.Fatal(err)
	}
	badRow := bytes.ReplaceAll(effectA.Rows[0], []byte(`"run_id":"901"`), []byte(`"run_id":"reject"`))
	combined, err := BuildEffectBatch(
		"ci_pipeline_runs", EffectReadbackRequired,
		[]json.RawMessage{effectA.Rows[0], badRow},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claimA, combined); err == nil {
		t.Fatal("server-rejected second row was accepted")
	}
	if got, err := sink.InspectEffect(ctx, claimA, effectA); err != nil || got != EffectAbsent {
		t.Fatalf("partial batch inspection=%s error=%v", got, err)
	}

	for _, write := range []struct {
		claim  Claim
		effect EffectBatch
	}{{claimB, effectB}, {claimA, effectA}, {claimA, effectA}} {
		if err := sink.WriteEffect(ctx, write.claim, write.effect); err != nil {
			t.Fatal(err)
		}
	}
	for _, inspect := range []struct {
		claim  Claim
		effect EffectBatch
	}{{claimA, effectA}, {claimB, effectB}} {
		got, err := sink.InspectEffect(ctx, inspect.claim, inspect.effect)
		if err != nil || got != EffectExact {
			t.Fatalf("tenant=%s inspection=%s error=%v", inspect.claim.OrgID, got, err)
		}
	}
	var count uint64
	if err := sink.Conn.QueryRow(ctx, `
SELECT count() FROM ci_pipeline_runs FINAL
WHERE org_id = ? AND repo_id = ? AND run_id = ?`,
		rowA.OrgID, rowA.RepoID, rowA.RunID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("retry convergence count=%d", count)
	}
}

func gitLabCICDEffect(t *testing.T, row gitLabCICDPipelineRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(
		"ci_pipeline_runs", EffectReadbackRequired, []gitLabCICDPipelineRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func newGitLabCICDIntegrationSink(t *testing.T) (context.Context, GitLabCICDClickHouseEffects) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `
CREATE TABLE ci_pipeline_runs (
  org_id String, repo_id UUID, run_id String, pipeline_name Nullable(String),
  provider String DEFAULT '', status Nullable(String),
  queued_at Nullable(DateTime64(3,'UTC')), started_at DateTime64(3,'UTC'),
  finished_at Nullable(DateTime64(3,'UTC')), duration_seconds Nullable(Float64),
  queue_seconds Nullable(Float64), retry_count UInt32, cancel_reason Nullable(String),
  trigger_source Nullable(String), commit_hash Nullable(String), branch Nullable(String),
  pr_number Nullable(UInt32), team_id Nullable(String), service_id Nullable(String),
  last_synced DateTime64(3,'UTC')
) ENGINE=ReplacingMergeTree(last_synced) ORDER BY (org_id,repo_id,run_id)`); err != nil {
		t.Fatal(err)
	}
	return ctx, GitLabCICDClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
}
