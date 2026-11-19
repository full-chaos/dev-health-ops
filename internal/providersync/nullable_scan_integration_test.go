//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL; v2.49 left the Go zero value. The effect read-backs keep that zero.
func TestEffectReadBacksScanNullColumnsAsTheZeroValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "00000000-0000-4000-8000-000000014218"
	const repoID = "00000000-0000-4000-8000-0000000142c1"
	synced := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

	t.Run("inspectGitCommit", func(t *testing.T) {
		if err := conn.Exec(ctx, `INSERT INTO git_commits (org_id, repo_id, hash, author_name, committer_name, author_when, committer_when, parents, last_synced)
			VALUES (?, ?, 'c0ffee', NULL, NULL, ?, ?, 1, ?)`, orgID, repoID, synced, synced, synced); err != nil {
			t.Fatal(err)
		}
		inspection, err := inspectGitCommit(ctx, conn, gitCommitRow{
			OrgID: orgID, RepoID: repoID, Hash: "c0ffee", AuthorWhen: synced, CommitterWhen: synced, Parents: 1, LastSynced: synced,
		})
		if err != nil {
			t.Fatal(err)
		}
		if inspection != EffectExact {
			t.Fatalf("inspection = %v, want EffectExact: a NULL author and committer name read back as empty", inspection)
		}
	})
	t.Run("scanWinningPullRequestVersion", func(t *testing.T) {
		if err := conn.Exec(ctx, `INSERT INTO git_pull_requests (org_id, repo_id, number, state, author_name, additions, deletions, changed_files, created_at, last_synced)
			VALUES (?, ?, 7, NULL, NULL, NULL, NULL, NULL, ?, ?)`, orgID, repoID, synced, synced); err != nil {
			t.Fatal(err)
		}
		version, err := GitHubPullRequestClickHouseEffects{Conn: conn}.scanWinningPullRequestVersion(ctx, orgID, repoID, 7)
		if err != nil {
			t.Fatal(err)
		}
		row := version.Row
		if !version.Found || row.State != "" || row.AuthorName != "" || row.Additions != 0 || row.Deletions != 0 || row.ChangedFiles != 0 {
			t.Fatalf("version = %+v, want a found row with empty state and author and zero line counts", version)
		}
	})
	t.Run("inspectEdge", func(t *testing.T) {
		if err := conn.Exec(ctx, `INSERT INTO work_graph_edges (edge_id, source_type, source_id, target_type, target_id, edge_type, provider, provenance, confidence, evidence, discovered_at, last_synced, event_ts, day, org_id, is_deleted)
			VALUES ('edge-1', 'feature_flag', 'flag-1', 'file', 'f.go', 'REFERENCES', NULL, 'explicit_text', 1, '', ?, ?, ?, '2026-11-19', ?, 0)`,
			synced, synced, synced, orgID); err != nil {
			t.Fatal(err)
		}
		inspection, err := featureFlagsClickHouseEffects{Conn: conn, Provider: "launchdarkly"}.inspectEdge(ctx, launchDarklyEdgeRow{
			EdgeID: "edge-1", SourceType: "feature_flag", SourceID: "flag-1", TargetType: "file", TargetID: "f.go", EdgeType: "REFERENCES",
			Provenance: "explicit_text", Confidence: 1, DiscoveredAt: synced, LastSynced: synced, EventAt: synced, Day: "2026-11-19", OrgID: orgID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if inspection != EffectExact {
			t.Fatalf("inspection = %v, want EffectExact: a NULL provider reads back as empty", inspection)
		}
	})
}

// A scalar subquery is typed Nullable(T) by ClickHouse. A count() subquery
// always yields one row, so these count reads never scan a NULL, even on an
// organization with no rows.
func TestCountReadsOverScalarSubqueriesNeverScanANull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	const orgID = "00000000-0000-4000-8000-0000000142e1"

	t.Run("teamIDCarryCount", func(t *testing.T) {
		groups, observations, malformed, err := teamIDCarryCount(ctx, conn, clickhouse.Named("org_id", orgID))
		if err != nil {
			t.Fatal(err)
		}
		if groups != 0 || observations != 0 || malformed != 0 {
			t.Fatalf("counts = %d %d %d, want zeros for an empty organization", groups, observations, malformed)
		}
	})
	t.Run("RetireJiraProjectAsTeamRows", func(t *testing.T) {
		outcome, err := RetireJiraProjectAsTeamRows(ctx, conn, orgID, time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC), true)
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Teams != 0 || outcome.OwnershipRows != 0 {
			t.Fatalf("outcome = %+v, want zero counts for an empty organization", outcome)
		}
	})
}

// storedPipelineFinishDaysSQL reads toDate(finished_at) into a time.Time; its
// WHERE drops a NULL finished_at row before the scan.
func TestStoredPipelineFinishDaysNeverScanANullFinish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	const orgID = "00000000-0000-4000-8000-0000000142e2"
	const repoID = "00000000-0000-4000-8000-0000000142e3"
	synced := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, finished_at, last_synced) VALUES (?, ?, 'run-1', NULL, ?)`,
		orgID, repoID, synced); err != nil {
		t.Fatal(err)
	}
	if err := recordMovedPipelineFinishDays(ctx, conn, orgID, []githubTestsPipelineRow{{RepoID: repoID, RunID: "run-1", FinishedAt: &synced}}); err != nil {
		t.Fatal(err)
	}
}
