//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testOpsTransientEOF is the driver error of a ClickHouse read that hit a closed connection (clickhouse-go
// conn_process.go): PrepareBatch and Query return it before any row of the INSERT reaches the server.
func testOpsTransientEOF() error {
	return fmt.Errorf("query processing: failed to read first block packet from 10.0.0.9:9440 (conn_id=7): %w", io.EOF)
}

// testOpsFaultSink is the real TestOps sink with one transient fault on the first write of one destination.
// commitFirst=false: the INSERT never reached the server. commitFirst=true: it committed and the acknowledgement was lost.
type testOpsFaultSink struct {
	inner       TestOpsClickHouseEffects
	destination string
	commitFirst bool
	fired       *int
}

func (sink testOpsFaultSink) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if effect.Destination == sink.destination && *sink.fired == 0 {
		*sink.fired++
		if sink.commitFirst {
			if err := sink.inner.WriteEffect(ctx, claim, effect); err != nil {
				return err
			}
		}
		return testOpsTransientEOF()
	}
	return sink.inner.WriteEffect(ctx, claim, effect)
}

// newTestOpsMigratedSink is a TestOps sink over a ClickHouse migrated to the chain head, merges stopped on the
// tables whose raw row count a cell reads.
func newTestOpsMigratedSink(t *testing.T, ctx context.Context) TestOpsClickHouseEffects {
	t.Helper()
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
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES ci_pipeline_runs"); err != nil {
		t.Fatal(err)
	}
	return TestOpsClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
}

func testOpsPipelineCounts(t *testing.T, ctx context.Context, sink TestOpsClickHouseEffects, runID string) (raw, final uint64, newest time.Time, status string) {
	t.Helper()
	if err := sink.Conn.QueryRow(ctx, `SELECT count() FROM ci_pipeline_runs WHERE org_id = 'org-acme' AND run_id = ?`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var statusValue *string
	if err := sink.Conn.QueryRow(ctx, `SELECT count(), max(last_synced), any(status) FROM ci_pipeline_runs FINAL WHERE org_id = 'org-acme' AND run_id = ?`, runID).Scan(&final, &newest, &statusValue); err != nil {
		t.Fatal(err)
	}
	if statusValue != nil {
		status = *statusValue
	}
	return raw, final, newest.UTC(), status
}

// testOpsVersionedEffect is one row of a TestOps destination: the natural key from runID, last_synced, and variant in
// a non-key column, so two variants of one key differ only in content.
func testOpsVersionedEffect(t *testing.T, claim Claim, destination string, runIDs []string, lastSynced []time.Time, variants []string) EffectBatch {
	t.Helper()
	repo := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	provider := map[string]string{"github": "github_actions", "gitlab": "gitlab_ci"}[claim.Provider]
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	format := "lcov"
	var effect EffectBatch
	var err error
	switch destination {
	case "ci_pipeline_runs":
		rows := make([]githubTestsPipelineRow, len(runIDs))
		for index := range runIDs {
			status := variants[index]
			rows[index] = githubTestsPipelineRow{OrgID: claim.OrgID, RepoID: repo, RunID: runIDs[index], Provider: provider, Status: &status, StartedAt: base, LastSynced: lastSynced[index]}
		}
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, rows)
	case "ci_job_runs":
		rows := make([]githubTestsJobRow, len(runIDs))
		for index := range runIDs {
			rows[index] = githubTestsJobRow{OrgID: claim.OrgID, RepoID: repo, RunID: runIDs[index], JobID: "11", JobName: variants[index], LastSynced: lastSynced[index]}
		}
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, rows)
	case "ci_acceptance_checks":
		rows := make([]githubTestsAcceptanceRow, len(runIDs))
		for index := range runIDs {
			rows[index] = githubTestsAcceptanceRow{OrgID: claim.OrgID, RepoID: repo, RunID: runIDs[index], CheckKey: provider + ":unit", CheckName: variants[index], Provider: provider, Requirement: "required", Result: "passed", RuleVersion: githubTestsRuleVersion, Provenance: "fixture", ObservedAt: base, LastSynced: lastSynced[index]}
		}
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, rows)
	case "test_suite_results":
		rows := make([]testSuiteResultRow, len(runIDs))
		for index := range runIDs {
			rows[index] = testSuiteResultRow{OrgID: claim.OrgID, RepoID: repo, RunID: runIDs[index], SuiteID: "suite", SuiteName: variants[index], TotalCount: 1, PassedCount: 1, LastSynced: lastSynced[index]}
		}
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, rows)
	case "test_case_results":
		rows := make([]testCaseResultRow, len(runIDs))
		for index := range runIDs {
			rows[index] = testCaseResultRow{OrgID: claim.OrgID, RepoID: repo, RunID: runIDs[index], SuiteID: "suite", CaseID: "case", CaseName: variants[index], Status: "passed", LastSynced: lastSynced[index]}
		}
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, rows)
	case "coverage_snapshots":
		rows := make([]coverageSnapshotRow, len(runIDs))
		for index := range runIDs {
			branch := variants[index]
			rows[index] = coverageSnapshotRow{OrgID: claim.OrgID, RepoID: repo, RunID: runIDs[index], SnapshotID: "snapshot", ReportFormat: &format, Branch: &branch, LastSynced: lastSynced[index]}
		}
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, rows)
	default:
		t.Fatalf("no row builder for %s", destination)
	}
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

var testOpsLastSyncedDestinations = []string{
	"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results", "test_case_results", "coverage_snapshots",
}

// TestTestOpsReadbackDecidesEachKeyByLastSynced pins the readback of the six last_synced TestOps tables (CHAOS-14412):
// each key is decided by the stored ReplacingMergeTree version against the batch version, then the batch. A stored
// OLDER version is a batch that never landed (absent); a stored NEWER version outranks the batch row, which FINAL can
// never return (exact); only the SAME version compares the content (different = conflict).
func TestTestOpsReadbackDecidesEachKeyByLastSynced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	sink := newTestOpsMigratedSink(t, ctx)
	batchAt := time.Date(2026, 11, 20, 19, 46, 0, 0, time.UTC)
	older, newer := batchAt.Add(-time.Hour), batchAt.Add(time.Hour)
	type stored struct {
		at      time.Time
		variant string
	}
	cases := []struct {
		name   string
		stored []*stored
		want   EffectInspection
	}{
		{name: "none", stored: []*stored{nil}, want: EffectAbsent},
		{name: "older", stored: []*stored{{older, "before"}}, want: EffectAbsent},
		{name: "same_version_same_row", stored: []*stored{{batchAt, "batch"}}, want: EffectExact},
		{name: "same_version_other_row", stored: []*stored{{batchAt, "before"}}, want: EffectConflict},
		{name: "newer", stored: []*stored{{newer, "after"}}, want: EffectExact},
		{name: "mixed_older_newer", stored: []*stored{{older, "before"}, {newer, "after"}}, want: EffectAbsent},
		{name: "mixed_same_other_newer", stored: []*stored{{batchAt, "before"}, {newer, "after"}}, want: EffectConflict},
		{name: "mixed_same_other_older", stored: []*stored{{batchAt, "before"}, {older, "before"}}, want: EffectConflict},
		{name: "mixed_same_row_none", stored: []*stored{{batchAt, "batch"}, nil}, want: EffectAbsent},
		{name: "mixed_same_row_newer", stored: []*stored{{batchAt, "batch"}, {newer, "after"}}, want: EffectExact},
	}
	for _, provider := range []string{"github", "gitlab"} {
		claim := nativeTestClaim(provider, "cicd")
		for _, destination := range testOpsLastSyncedDestinations {
			for _, cell := range cases {
				t.Run(provider+"/"+destination+"/"+cell.name, func(t *testing.T) {
					runIDs := make([]string, len(cell.stored))
					batchTimes := make([]time.Time, len(cell.stored))
					batchVariants := make([]string, len(cell.stored))
					for index, row := range cell.stored {
						runIDs[index] = fmt.Sprintf("%s-%s-%d", provider, cell.name, index)
						batchTimes[index], batchVariants[index] = batchAt, "batch"
						if row == nil {
							continue
						}
						prior := testOpsVersionedEffect(t, claim, destination, runIDs[index:index+1], []time.Time{row.at}, []string{row.variant})
						if err := sink.WriteEffect(ctx, claim, prior); err != nil {
							t.Fatalf("write stored %s: %v", destination, err)
						}
					}
					batch := testOpsVersionedEffect(t, claim, destination, runIDs, batchTimes, batchVariants)
					got, err := sink.InspectEffect(ctx, claim, batch)
					if err != nil || got != cell.want {
						t.Fatalf("%s %s %s inspection=%s err=%v, want %s", provider, destination, cell.name, got, err, cell.want)
					}
				})
			}
		}
	}
}

// TestTestOpsCommitterRecoversAWritingEffectByLastSynced drives the shared committer over the real sink and readback:
// attempt 1 leaves the effect Writing on a transient fault, attempt 2 of the same generation decides it from the rows.
func TestTestOpsCommitterRecoversAWritingEffectByLastSynced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	sink := newTestOpsMigratedSink(t, ctx)
	batchAt := time.Date(2026, 11, 20, 19, 46, 0, 0, time.UTC)
	older, newer := batchAt.Add(-time.Hour), batchAt.Add(time.Hour)
	type stored struct {
		at      time.Time
		variant string
	}
	cases := []struct {
		name     string
		provider string
		stored   []*stored
		wantErr  error
		wantRows []string // FINAL status per key after attempt 2
	}{
		{name: "resync_older_replays", provider: "github", stored: []*stored{{older, "before"}}, wantRows: []string{"batch"}},
		{name: "resync_older_replays_gitlab", provider: "gitlab", stored: []*stored{{older, "before"}}, wantRows: []string{"batch"}},
		{name: "mixed_older_newer_replays", provider: "github", stored: []*stored{{older, "before"}, {newer, "after"}}, wantRows: []string{"batch", "after"}},
		{name: "mixed_same_other_newer_conflicts", provider: "github", stored: []*stored{{batchAt, "before"}, {newer, "after"}}, wantErr: ErrEffectRecoveryAmbiguous},
		{name: "same_version_other_row_conflicts", provider: "github", stored: []*stored{{batchAt, "before"}}, wantErr: ErrEffectRecoveryAmbiguous},
	}
	for _, cell := range cases {
		t.Run(cell.name, func(t *testing.T) {
			claim := nativeTestClaim(cell.provider, "cicd")
			claim.ID = uuid.NewString()
			runIDs := make([]string, len(cell.stored))
			batchTimes := make([]time.Time, len(cell.stored))
			batchVariants := make([]string, len(cell.stored))
			for index, row := range cell.stored {
				runIDs[index] = fmt.Sprintf("committer-%s-%d", cell.name, index)
				batchTimes[index], batchVariants[index] = batchAt, "batch"
				if row == nil {
					continue
				}
				prior := testOpsVersionedEffect(t, claim, "ci_pipeline_runs", runIDs[index:index+1], []time.Time{row.at}, []string{row.variant})
				if err := sink.WriteEffect(ctx, claim, prior); err != nil {
					t.Fatal(err)
				}
			}
			batch := testOpsVersionedEffect(t, claim, "ci_pipeline_runs", runIDs, batchTimes, batchVariants)
			ledger := &memoryEffectLedger{}
			fired := 0
			first := EffectCommitter{Ledger: ledger, Sink: testOpsFaultSink{inner: sink, destination: "ci_pipeline_runs", fired: &fired}, Readback: sink, Now: func() time.Time { return batchAt }}
			if _, err := first.Commit(ctx, claim, []EffectBatch{batch}, batchAt); !errors.Is(err, io.EOF) || fired != 1 {
				t.Fatalf("attempt 1 err=%v fired=%d, want the transient fault", err, fired)
			}
			if ledger.state.Effects[0].Status != GenerationBlockWriting {
				t.Fatalf("attempt 1 left the effect %s, want writing", ledger.state.Effects[0].Status)
			}
			second := EffectCommitter{Ledger: ledger, Sink: sink, Readback: sink, Now: func() time.Time { return batchAt.Add(time.Minute) }}
			result, err := second.Commit(ctx, claim, []EffectBatch{batch}, batchAt)
			if cell.wantErr != nil {
				if !errors.Is(err, cell.wantErr) {
					t.Fatalf("attempt 2 err=%v, want %v", err, cell.wantErr)
				}
				return
			}
			if err != nil || result.ResetForReplay != 1 || result.Written != 1 {
				t.Fatalf("attempt 2 result=%+v err=%v, want one replay", result, err)
			}
			for index, runID := range runIDs {
				_, final, _, status := testOpsPipelineCounts(t, ctx, sink, runID)
				if final != 1 || status != cell.wantRows[index] {
					t.Fatalf("key %d FINAL rows=%d status=%q, want 1 %q", index, final, status, cell.wantRows[index])
				}
			}
		})
	}
}

// TestTestOpsChunkEffectResumesAfterATransientWriteFault is the CHAOS-14412 unit loss on the real chunked executor: the
// github cicd route, the Postgres chunk ledger and the TestOps sink and readback as the worker wires them. Attempt 1
// fails on a transient ClickHouse fault while the ci_pipeline_runs effect is Writing; attempt 2 (same claim, new owner)
// must finish the unit. "resync" = an earlier sync already stored the same keys with an older last_synced.
func TestTestOpsChunkEffectResumesAfterATransientWriteFault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	sink := newTestOpsMigratedSink(t, ctx)
	for _, variant := range []struct {
		name        string
		resync      bool
		commitFirst bool
		wantRaw     uint64
		wantReplay  int
		wantMarked  int
	}{
		{name: "never_reached_server_resync", resync: true, wantRaw: 2, wantReplay: 1},
		{name: "ack_lost_resync", resync: true, commitFirst: true, wantRaw: 2, wantMarked: 1},
		{name: "never_reached_server_first_sync", wantRaw: 1, wantReplay: 1},
		{name: "ack_lost_first_sync", commitFirst: true, wantRaw: 1, wantMarked: 1},
	} {
		t.Run(variant.name, func(t *testing.T) {
			for _, table := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results", "test_case_results", "coverage_snapshots", "testops_run_report_completeness", "daily_metrics_touched_days"} {
				if err := sink.Conn.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
					t.Fatal(err)
				}
			}
			pool, repository := testOpsCicdUnitRepository(t, ctx)
			attempt1 := time.Now().UTC().Truncate(time.Millisecond)
			claim := testOpsClaimUnit(t, ctx, repository, attempt1)
			if variant.resync {
				earlier := claim
				earlier.ID = uuid.NewString()
				githubTestsCommitReportWindowPassClaim(t, ctx, sink,
					fakehttp.Client(githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")), earlier, attempt1.Add(-24*time.Hour))
			}
			fired := 0
			faulty := testOpsFaultSink{inner: sink, destination: "ci_pipeline_runs", commitFirst: variant.commitFirst, fired: &fired}
			if _, err := testOpsCicdAttempt(t, ctx, repository, claim, faulty, sink, attempt1); fired != 1 || !errors.Is(err, io.EOF) {
				t.Fatalf("attempt 1 err=%v fired=%d, want the transient fault", err, fired)
			}
			if err := repository.ReleaseForRetry(ctx, claim, attempt1.Add(37*time.Second)); err != nil {
				t.Fatal(err)
			}
			attempt2 := attempt1.Add(38 * time.Second)
			claim2 := testOpsClaimUnit(t, ctx, repository, attempt2)
			result, err := testOpsCicdAttempt(t, ctx, repository, claim2, sink, sink, attempt2)
			if err != nil {
				t.Fatalf("attempt 2 of the same claim did not resume: %v (ErrEffectRecoveryAmbiguous=%v)", err, errors.Is(err, ErrEffectRecoveryAmbiguous))
			}
			if result.Effects.ResetForReplay != variant.wantReplay || result.Effects.MarkedCommitted != variant.wantMarked {
				t.Fatalf("attempt 2 effects=%+v, want replay=%d marked=%d", result.Effects, variant.wantReplay, variant.wantMarked)
			}
			raw, final, newest, _ := testOpsPipelineCounts(t, ctx, sink, "9001")
			if raw != variant.wantRaw || final != 1 || !newest.Equal(attempt1) {
				t.Fatalf("ci_pipeline_runs raw=%d FINAL=%d last_synced=%s, want raw %d FINAL 1 last_synced %s",
					raw, final, newest.Format(time.RFC3339Nano), variant.wantRaw, attempt1.Format(time.RFC3339Nano))
			}
			var status string
			if err := pool.QueryRow(ctx, `SELECT status FROM public.sync_run_units WHERE id=$1`, firstUnitID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "running" {
				t.Fatalf("unit status=%q after attempt 2, want running (Complete belongs to providerunit)", status)
			}
		})
	}
}

func testOpsCicdUnitRepository(t *testing.T, ctx context.Context) (*pgxpool.Pool, *PostgresRepository) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET dataset_key='cicd', mode='backfill',
		since_at='2026-07-22T00:00:00Z', before_at='2026-07-23T00:00:00Z', processor_flags='{"sync_git":true,"sync_cicd":true}'::jsonb
		WHERE id=$1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	return pool, repository
}

func testOpsClaimUnit(t *testing.T, ctx context.Context, repository *PostgresRepository, now time.Time) Claim {
	t.Helper()
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: 5 * time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

// testOpsCicdAttempt runs one attempt as workerservice/provider_sync.go builds it: Ledger = the Postgres repository,
// Sink and Readback = the TestOps effects.
func testOpsCicdAttempt(t *testing.T, ctx context.Context, repository *PostgresRepository, claim Claim, sink EffectSink, readback EffectReadback, now time.Time) (CompleteRouteExecutionResult, error) {
	t.Helper()
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("github/cicd descriptor=%+v ok=%v", descriptor, ok)
	}
	session := &LeaseSession{
		Repository: repository, Claim: claim, LeaseDuration: 5 * time.Minute,
		Deadline: now.Add(10 * time.Minute), Now: func() time.Time { return now },
	}
	return (CompleteRouteExecutor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: githubBlameIntegrationCredentialRepository{},
			Decryptor:  githubBlameIntegrationCredentialDecryptor{},
		},
		Doer:         fakehttp.Client(githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")),
		Retry:        providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		Budget:       executorBudgetStore{},
		BudgetLimits: BudgetLimitsByCostClass(),
		BudgetTTL:    time.Minute,
		Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
			return executorBackoffGate{}
		},
		Handler:           GitHubTestsRouteHandler{},
		Comparator:        ProductionContractComparator{},
		Committer:         EffectCommitter{Ledger: repository, Sink: sink, Readback: readback, Now: func() time.Time { return now }},
		HeartbeatInterval: 30 * time.Second,
		Now:               func() time.Time { return now },
	}).Execute(ctx, session, descriptor)
}
