//go:build integration

package remaining

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const recomputeTestOrg = "00000000-0000-4000-8000-000000009674"

func recomputeTestStore(t *testing.T, ctx context.Context) (*PostgresStore, *pgxpool.Pool) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createRemainingTables(t, ctx, pool)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return store, pool
}

func countRemainingRuns(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.remaining_metric_runs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestStartManualRecomputeRunDispositions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, pool := recomputeTestStore(t, ctx)

	today := time.Now().UTC()
	covered := today.AddDate(0, 0, -3).Format("2006-01-02")
	uncovered := today.AddDate(0, 0, -4).Format("2006-01-02")
	zeroRow := today.AddDate(0, 0, -5).Format("2006-01-02")
	busy := today.AddDate(0, 0, -6).Format("2006-01-02")
	seedSucceededDoraPartition(t, ctx, store, pool, recomputeTestOrg, covered, 7, "post-sync:covered")
	seedSucceededDoraPartition(t, ctx, store, pool, recomputeTestOrg, zeroRow, 0, "post-sync:zero")
	busyRunID := seedPendingDoraPartition(t, ctx, store, pool, recomputeTestOrg, busy, "post-sync:busy")
	seedOutboxRow(t, ctx, pool, deterministicPartitionID(busyRunID, 1), "pending", "")
	runsBefore := countRemainingRuns(t, ctx, pool)

	const basis = "cfr-125"
	wantPreview := map[string]RecomputeDisposition{
		covered: RecomputeWouldStart, uncovered: RecomputeNotCovered,
		zeroRow: RecomputeNotCovered, busy: RecomputeInProgress,
	}
	for day, want := range wantPreview {
		outcome, err := store.PreviewManualRecompute(ctx, "dora", recomputeTestOrg, day, basis)
		if err != nil || outcome.Disposition != want {
			t.Fatalf("preview %s = %+v, %v; want %s", day, outcome, err, want)
		}
	}
	if got := countRemainingRuns(t, ctx, pool); got != runsBefore {
		t.Fatalf("a dry run wrote runs: %d -> %d", runsBefore, got)
	}

	started, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, covered, basis, nopPartitionPublisher{})
	if err != nil || started.Disposition != RecomputeStarted || started.RunID == "" || started.PartitionID == "" {
		t.Fatalf("covered day = %+v, %v; want started", started, err)
	}
	if want := ManualRecomputeGeneration(basis, "dora", recomputeTestOrg); started.Generation != want {
		t.Fatalf("generation %q, want %q", started.Generation, want)
	}
	for day, want := range map[string]RecomputeDisposition{
		uncovered: RecomputeNotCovered, zeroRow: RecomputeNotCovered, busy: RecomputeInProgress,
	} {
		outcome, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, day, basis, nopPartitionPublisher{})
		if err != nil || outcome.Disposition != want || outcome.PartitionID != "" {
			t.Fatalf("start %s = %+v, %v; want %s and no partition", day, outcome, err, want)
		}
	}
	if got := countRemainingRuns(t, ctx, pool); got != runsBefore+1 {
		t.Fatalf("runs = %d, want %d: only the covered day starts a run", got, runsBefore+1)
	}

	again, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, covered, basis, nopPartitionPublisher{})
	if err != nil || again.Disposition != RecomputeAlreadyRan || again.RunID != started.RunID {
		t.Fatalf("second start while pending = %+v, %v; want already_ran on the same run", again, err)
	}

	claim, err := store.ClaimPartition(ctx, started.PartitionID)
	if err != nil || claim == nil {
		t.Fatalf("claim: %v %v", claim, err)
	}
	if err := store.CompletePartition(ctx, *claim, "compatibility_execution:"+started.PartitionID+":rows_written=1"); err != nil {
		t.Fatal(err)
	}
	done, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, covered, basis, nopPartitionPublisher{})
	if err != nil || done.Disposition != RecomputeAlreadyRecomputed || done.RunID != started.RunID {
		t.Fatalf("second start after success = %+v, %v; want already_recomputed", done, err)
	}
	if got := countRemainingRuns(t, ctx, pool); got != runsBefore+1 {
		t.Fatalf("a second recompute inserted a run: %d, want %d", got, runsBefore+1)
	}

	next, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, covered, "cfr-126", nopPartitionPublisher{})
	if err != nil || next.Disposition != RecomputeStarted || next.RunID == started.RunID {
		t.Fatalf("a new basis = %+v, %v; want a new run", next, err)
	}
}

func TestStartManualRecomputeRunRetriesAFailedRunUnderANumberedGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, pool := recomputeTestStore(t, ctx)
	day := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")
	seedSucceededDoraPartition(t, ctx, store, pool, recomputeTestOrg, day, 7, "post-sync:covered")

	first, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, day, "cfr-125", nopPartitionPublisher{})
	if err != nil || first.Disposition != RecomputeStarted {
		t.Fatalf("first = %+v, %v", first, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE public.remaining_metric_runs SET status = 'failed' WHERE id = $1::uuid", first.RunID); err != nil {
		t.Fatal(err)
	}
	retry, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, day, "cfr-125", nopPartitionPublisher{})
	if err != nil || retry.Disposition != RecomputeStarted || retry.RunID == first.RunID ||
		retry.Generation != ManualRecomputeGeneration("cfr-125", "dora", recomputeTestOrg)+":retry-1" {
		t.Fatalf("after a failed run = %+v, %v; want a retry-1 run", retry, err)
	}
	same, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, day, "cfr-125", nopPartitionPublisher{})
	if err != nil || same.Disposition != RecomputeAlreadyRan || same.RunID != retry.RunID {
		t.Fatalf("while the retry is pending = %+v, %v; want already_ran on the retry", same, err)
	}
}

func TestStartManualRecomputeRunRefusesBadInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, _ := recomputeTestStore(t, ctx)
	for name, call := range map[string]func() error{
		"bad basis": func() error {
			_, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, "2026-09-01", "Bad Basis", nil)
			return err
		},
		"bad org": func() error {
			_, err := store.StartManualRecomputeRun(ctx, "dora", "nope", "2026-09-01", "cfr-125", nil)
			return err
		},
		"bad day": func() error {
			_, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, "nope", "cfr-125", nil)
			return err
		},
		"capacity": func() error {
			_, err := store.StartManualRecomputeRun(ctx, "capacity", recomputeTestOrg, "2026-09-01", "cfr-125", nil)
			return err
		},
		"preview too": func() error {
			_, err := store.PreviewManualRecompute(ctx, "dora", recomputeTestOrg, "2026-09-01", "")
			return err
		},
	} {
		if err := call(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestManualRecomputeRewritesACoveredDayToTheNewBasisAndLeavesAnUncoveredDayAlone
// is the rehearsal on real stores (Postgres run ledger, migrated ClickHouse,
// the real DORA executor through the real claim path). Both days hold a
// successful and a canceled deployment. The covered day already has a
// succeeded run and rows written on the old basis (the canceled deployment
// counted: deployment_frequency 2). A recompute rewrites it to the new basis
// (canceled leaves the count: 1); the uncovered day is not touched; a second
// run writes nothing.
func TestManualRecomputeRewritesACoveredDayToTheNewBasisAndLeavesAnUncoveredDayAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store, pool := recomputeTestStore(t, ctx)
	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)

	const repoID = "11111111-2222-4333-8444-555555559674"
	now := time.Now().UTC()
	coveredDay := now.AddDate(0, 0, -3).Truncate(24 * time.Hour)
	uncoveredDay := now.AddDate(0, 0, -4).Truncate(24 * time.Hour)

	deployments, err := conn.PrepareBatch(ctx, `INSERT INTO deployments (repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare deployments: %v", err)
	}
	for _, day := range []time.Time{coveredDay, uncoveredDay} {
		for index, status := range []string{"success", "canceled"} {
			at := day.Add(time.Duration(9+index) * time.Hour)
			id := "deploy-9674-" + day.Format("0102") + "-" + status
			if err := deployments.Append(
				repoID, id, status, "production",
				&at, &at, &at, (*time.Time)(nil), nil, "", 0.0, recomputeTestOrg, at,
			); err != nil {
				t.Fatalf("append deployment: %v", err)
			}
		}
	}
	if err := deployments.Send(); err != nil {
		t.Fatalf("send deployments: %v", err)
	}

	oldComputedAt := now.Add(-72 * time.Hour)
	oldRows, err := conn.PrepareBatch(ctx, "INSERT INTO dora_metrics_daily (repo_id, day, metric_name, value, computed_at, org_id)")
	if err != nil {
		t.Fatalf("prepare old rows: %v", err)
	}
	if err := oldRows.Append(repoID, coveredDay, "deployment_frequency", 2.0, oldComputedAt, recomputeTestOrg); err != nil {
		t.Fatalf("append old row: %v", err)
	}
	if err := oldRows.Send(); err != nil {
		t.Fatalf("send old rows: %v", err)
	}
	seedSucceededDoraPartition(t, ctx, store, pool, recomputeTestOrg, coveredDay.Format("2006-01-02"), 1, "post-sync:old-basis")

	latest := func(day time.Time) (value float64, rows uint64) {
		t.Helper()
		row := conn.QueryRow(ctx, `
			SELECT argMax(value, computed_at), count() FROM dora_metrics_daily
			WHERE org_id = {org_id:String} AND day = {day:Date} AND metric_name = 'deployment_frequency'`,
			clickhouse.Named("org_id", recomputeTestOrg), clickhouse.Named("day", day.Format("2006-01-02")))
		if err := row.Scan(&value, &rows); err != nil {
			t.Fatalf("read %s: %v", day.Format("2006-01-02"), err)
		}
		return value, rows
	}
	if value, rows := latest(coveredDay); value != 2 || rows != 1 {
		t.Fatalf("setup: covered day reads %v over %d rows, want 2 over 1", value, rows)
	}

	if _, err := store.StartManualBackfillRun(ctx, "dora", recomputeTestOrg, coveredDay.Format("2006-01-02"), "manual-backfill:chaos-9674", nopPartitionPublisher{}); !errors.Is(err, ErrDayAlreadyCovered) {
		t.Fatalf("setup: plain start on the covered day = %v, want ErrDayAlreadyCovered (the refusal this recompute path exists to get past)", err)
	}

	executor, err := NewDORAExecutor(ctx, conn, nil, nil)
	if err != nil {
		t.Fatalf("NewDORAExecutor: %v", err)
	}
	handler, err := NewPartitionHandler[jobruntime.RemainingDORAArgs](store, executor, "dora")
	if err != nil {
		t.Fatalf("NewPartitionHandler: %v", err)
	}
	work := func(partitionID string) {
		t.Helper()
		args := jobruntime.RemainingDORAArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RemainingMetricsPartitionPayload]{
				ContractVersion: jobcontract.ContractVersionV1,
				OrganizationID:  strPtr(recomputeTestOrg),
				CorrelationID:   "chaos-9674-rehearsal",
				IdempotencyKey:  "chaos-9674-rehearsal-" + partitionID,
				Domain:          jobcontract.DomainLink{Type: "remaining_metric_partition", ID: partitionID},
				Payload:         jobcontract.NewRemainingMetricsPartitionPayload(jobcontract.KindRemainingDORA, partitionID),
			},
		}
		execution := &jobruntime.Execution[jobruntime.RemainingDORAArgs]{
			JobID: 1, Attempt: 1, Args: args, Envelope: args.ContractEnvelope(),
			CorrelationID: "chaos-9674-rehearsal", OrganizationID: strPtr(recomputeTestOrg),
		}
		if err := handler.Work(ctx, execution); err != nil {
			t.Fatalf("PartitionHandler.Work: %v", err)
		}
	}

	const basis = "cfr-125"
	days := []string{coveredDay.Format("2006-01-02"), uncoveredDay.Format("2006-01-02")}
	var partitions []string
	for index, day := range days {
		outcome, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, day, basis, nopPartitionPublisher{})
		if err != nil {
			t.Fatal(err)
		}
		wantDisposition := []RecomputeDisposition{RecomputeStarted, RecomputeNotCovered}[index]
		if outcome.Disposition != wantDisposition {
			t.Fatalf("%s = %s, want %s", day, outcome.Disposition, wantDisposition)
		}
		if outcome.PartitionID != "" {
			partitions = append(partitions, outcome.PartitionID)
		}
	}
	if len(partitions) != 1 {
		t.Fatalf("partitions to run = %d, want 1 (the covered day)", len(partitions))
	}
	work(partitions[0])

	if value, rows := latest(coveredDay); value != 1 || rows != 2 {
		t.Fatalf("covered day after the recompute reads %v over %d rows, want 1 (canceled leaves the count) over 2 (old row kept, newer row appended)", value, rows)
	}
	if _, rows := latest(uncoveredDay); rows != 0 {
		t.Fatalf("the uncovered day got %d rows, want 0", rows)
	}

	again, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, days[0], basis, nopPartitionPublisher{})
	if err != nil || again.Disposition != RecomputeAlreadyRecomputed {
		t.Fatalf("second run = %+v, %v; want already_recomputed", again, err)
	}
	if value, rows := latest(coveredDay); value != 1 || rows != 2 {
		t.Fatalf("after the second run the covered day reads %v over %d rows, want 1 over 2: the second run is a no-op", value, rows)
	}
}
