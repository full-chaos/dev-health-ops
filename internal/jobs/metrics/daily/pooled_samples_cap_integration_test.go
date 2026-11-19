//go:build integration

package daily

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// C8/C9: every pooled writer applies the cap per row, inclusive. At the cap (2 here) a row stores its samples (marker 1);
// over it (3) the row stores none (marker 2, empty arrays), its stored values stay over ALL samples, and the writer logs
// one WARN line per row. No array is ever cut or sampled.
func TestPooledSamplesCapPerRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	previous := pooledSamplesMaxPerRow
	pooledSamplesMaxPerRow = 2
	t.Cleanup(func() { pooledSamplesMaxPerRow = previous })
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	const timeFormat = "2006-01-02 15:04:05.000"
	day := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	at := func(minutes int) string { return day.Add(time.Duration(minutes) * time.Minute).Format(timeFormat) }

	// The median of 1..n (and the mean, the same for 1..n), scaled.
	mid := func(n int, scale float64) float64 { return float64(n+1) / 2 * scale }

	for _, c := range []struct {
		name       string
		samples    int
		wantMarker uint8
	}{{"at-cap", 2, pooledSamplesStored}, {"over-cap", 3, pooledSamplesOverCap}} {
		logs.Reset()
		org := "org-cap-" + c.name
		repo := fmt.Sprintf("14025000-0000-4000-8000-00000000c%03d", c.samples)
		service := "svc-cap-" + c.name
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, now(), now())`, repo, "acme/cap-"+c.name, org)
		exec(`INSERT INTO operational_service_repository_mappings
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, service_id, repo_id, mapping_kind, valid_from, valid_to, is_active)
VALUES (?, 'pagerduty', 'pd', 'repository_mapping', ?, now64(6), 1, '', 1, 2, ?, now64(6), now64(6), ?, toUUID(?), 'repository_derived', NULL, NULL, 1)`,
			org, "map-"+c.name, "map-"+c.name, service, repo)
		for i := 1; i <= c.samples; i++ {
			start := 60 * i
			// A run of i minutes queued 1 minute (TestOps and CI/CD), its suite of 10*i seconds.
			exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id, retry_count)
VALUES (?, ?, 'success', ?, ?, ?, '2026-03-03 00:00:00.000', ?, 0)`, repo, fmt.Sprintf("run-%d", i), at(start-1), at(start), at(start+i), org)
			exec(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count,
    duration_seconds, started_at, finished_at, org_id, last_synced)
VALUES (?, ?, ?, 'suite', 1, 1, 0, 0, ?, ?, ?, ?, '2026-03-03 00:00:00.000')`, repo, fmt.Sprintf("run-%d", i), fmt.Sprintf("suite-%d", i),
				float64(10*i), at(start), at(start+1), org)
			// A deployment of i hours, a merged pull request of 10*i lines, a bug repair of i hours, an incident of i hours.
			exec(`INSERT INTO deployments (repo_id, deployment_id, status, started_at, finished_at, deployed_at, org_id, last_synced)
VALUES (?, ?, 'success', ?, ?, ?, ?, '2026-03-03 00:00:00.000')`, repo, fmt.Sprintf("d-%d", i), at(start), at(start+60*i), at(start+60*i), org)
			exec(`INSERT INTO git_pull_requests (repo_id, number, author_name, author_email, created_at, merged_at, additions, deletions, last_synced, org_id)
VALUES (?, ?, 'dev', 'dev@example.invalid', '2026-02-20 00:00:00.000', ?, ?, 0, '2026-03-03 00:00:00.000', ?)`, repo, uint32(i), at(start), uint32(10*i), org)
			exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, status, created_at, updated_at, started_at, completed_at, last_synced, org_id)
VALUES (?, ?, 'jira', 'bug', 'done', ?, ?, ?, ?, '2026-03-03 00:00:00.000', ?)`, repo, fmt.Sprintf("b-%d", i), at(start), at(start+60*i), at(start), at(start+60*i), org)
			exec(`INSERT INTO operational_incidents
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, normalized_status, service_id, title, started_at, resolved_at, is_deleted)
VALUES (?, 'pagerduty', 'pd', 'incident', ?, now64(6), 1, '', 1, 2, ?, now64(6), now64(6), 'resolved', ?, 'incident', ?, ?, 0)`,
				org, fmt.Sprintf("i-%d", i), fmt.Sprintf("i-%d", i), service, at(start), at(start+60*i))
		}

		run := Run{ID: "cap-" + c.name, OrganizationID: org, TargetDay: day}
		part := Partition{ID: "cap-part-" + c.name, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo)}}
		pipeline, err := NewTestopsPipelineExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		tests, err := NewTestopsTestExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		cicdExecutor, err := NewCICDExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		deploy, err := NewDeployExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		repoUser, err := NewRepoUserCommitExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		incident, err := NewIncidentExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		for _, executor := range []NativeFamilyExecutor{pipeline, tests, cicdExecutor, deploy, repoUser, incident} {
			if _, err := executor.ComputeFamily(ctx, run, part); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
		}

		n := c.samples
		wantLength := uint64(n)
		if c.wantMarker != pooledSamplesStored {
			wantLength = 0
		}
		for _, check := range []struct {
			table, arrays, value string
			want                 float64
		}{
			{"testops_pipeline_metrics_daily", "[length(pooled_service_ids), length(pooled_durations), length(pooled_queues)]", "median_duration_seconds", mid(n, 60)},
			{"testops_test_metrics_daily", "[length(pooled_service_ids), length(pooled_durations)]", "suite_duration_p50_seconds", mid(n, 10)},
			{"cicd_metrics_daily", "[length(pooled_run_minutes), length(pooled_queue_minutes)]", "avg_duration_minutes", mid(n, 1)},
			{"deploy_metrics_daily", "[length(pooled_deploy_hours)]", "deploy_time_p50_hours", mid(n, 1)},
			{"repo_metrics_daily", "[length(pooled_pr_size_loc), length(pooled_bug_repair_hours)]", "pr_size_p50_loc", mid(n, 10)},
			{"repo_metrics_daily", "[length(pooled_pr_size_loc), length(pooled_bug_repair_hours)]", "mttr_hours", mid(n, 1)},
			{"incident_metrics_daily", "[length(pooled_repair_hours)]", "mttr_p50_hours", mid(n, 1)},
		} {
			var marker uint8
			var lengths []uint64
			var value *float64
			if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT pooled_samples_stored, %s, %s FROM %s WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1`,
				check.arrays, check.value, check.table), org).Scan(&marker, &lengths, &value); err != nil {
				t.Fatalf("%s %s: %v", c.name, check.table, err)
			}
			t.Logf("CAP14025 %-8s %-30s marker=%d lengths=%v %s=%v", c.name, check.table, marker, lengths, check.value, capValue(value))
			if marker != c.wantMarker {
				t.Errorf("%s %s: marker %d, want %d", c.name, check.table, marker, c.wantMarker)
			}
			for _, length := range lengths {
				if length != wantLength {
					t.Errorf("%s %s: array lengths %v, want each %d", c.name, check.table, lengths, wantLength)
					break
				}
			}
			if value == nil || math.Abs(*value-check.want) > 1e-9 {
				t.Errorf("%s %s: %s = %v, want %v over all %d samples", c.name, check.table, check.value, capValue(value), check.want, n)
			}
		}
		warnings := strings.Count(logs.String(), "pooled samples over the cap")
		t.Logf("CAP14025 %-8s WARN lines=%d", c.name, warnings)
		if want := map[bool]int{true: 0, false: 6}[c.wantMarker == pooledSamplesStored]; warnings != want {
			t.Errorf("%s: %d over-cap WARN lines, want %d (one per family):\n%s", c.name, warnings, want, logs.String())
		}
	}
}

func capValue(value *float64) any {
	if value == nil {
		return "NULL"
	}
	return *value
}
