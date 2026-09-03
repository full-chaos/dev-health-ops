//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The gate's fixture schema.
//
// The check constraints are carried over from the migrations (0057 for the
// tables, 0113 for the failed_permanent terminal state) rather than omitted,
// because a fixture that cannot REJECT an impossible row also cannot prove that
// the state it seeds is one production could actually reach. Without the
// partition status constraint, a typo'd 'failed_permanant' would seed happily
// and the stuck-partition cells below would silently test nothing.
const readinessFixtureDDL = `
CREATE TABLE daily_metrics_runs (
    id                  uuid PRIMARY KEY,
    org_id              uuid NOT NULL,
    target_day          date NOT NULL,
    generation          varchar(64) NOT NULL,
    status              varchar(16) NOT NULL DEFAULT 'pending',
    finalization_status varchar(16) NOT NULL DEFAULT 'pending',
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL
);
CREATE TABLE daily_metrics_partitions (
    id      uuid PRIMARY KEY,
    run_id  uuid NOT NULL REFERENCES daily_metrics_runs(id) ON DELETE CASCADE,
    ordinal int NOT NULL,
    status  varchar(24) NOT NULL,
    CONSTRAINT ck_daily_metrics_partition_status CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed', 'failed_permanent')
    )
);`

type recordingObserver struct {
	failOpen []string
	skipped  int
}

func (observer *recordingObserver) RecommendationsReadinessFailOpen(class string) {
	observer.failOpen = append(observer.failOpen, class)
}
func (observer *recordingObserver) RecommendationsReadinessSkipped() { observer.skipped++ }

type discardLogger struct{}

func (discardLogger) Error(string, ...any) {}
func (discardLogger) Warn(string, ...any)  {}

// TestReadinessDecisionsMatchTheReferenceAcrossTheStateGrid walks the gate's
// decision surface as a GRID, not a diagonal.
//
// finalization_status and has_stuck_partition are varied INDEPENDENTLY, because
// the port claims the stuck partition is diagnostic only and never part of the
// decision. A fixture that only ever pairs "unfinished + stuck" with
// "succeeded + clean" tests their conjunction and cannot detect the stuck flag
// leaking into the return value -- which would wedge recommendations for any
// org holding one permanently-failed partition.
func TestReadinessDecisionsMatchTheReferenceAcrossTheStateGrid(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, readinessFixtureDDL); err != nil {
		t.Fatalf("create fixture schema: %v", err)
	}

	const orgID = "5f0f5a0c-0000-4000-8000-000000000001"
	day := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	fanoutGeneration := ScheduledFanoutGenerationPrefixForTest + "2026-08-31"

	for _, testCase := range []struct {
		name                string
		runStatus           string
		finalizationStatus  string
		generation          string
		stuckPartition      bool
		seedRun             bool
		wantReady           bool
		wantSkippedObserved bool
	}{
		{
			name: "a finished day proceeds", seedRun: true, runStatus: "running",
			finalizationStatus: "succeeded", generation: fanoutGeneration,
			wantReady: true,
		},
		{
			// THE GRID CELL THAT MATTERS. A finished run whose partition is
			// permanently failed must still proceed: the run finalized, and the
			// stuck partition is an explanation, not a veto.
			name:    "a finished day proceeds even holding a permanently-failed partition",
			seedRun: true, runStatus: "running", finalizationStatus: "succeeded",
			generation: fanoutGeneration, stuckPartition: true,
			wantReady: true,
		},
		{
			name: "an unfinished day is withheld", seedRun: true, runStatus: "running",
			finalizationStatus: "pending", generation: fanoutGeneration,
			wantReady: false, wantSkippedObserved: true,
		},
		{
			// The other off-diagonal cell: unfinished AND stuck is still just
			// unfinished. The existing contract already covers it; the stuck
			// flag only explains why it may never resolve on its own.
			name:    "an unfinished day holding a stuck partition is withheld for being unfinished",
			seedRun: true, runStatus: "running", finalizationStatus: "pending",
			generation: fanoutGeneration, stuckPartition: true,
			wantReady: false, wantSkippedObserved: true,
		},
		{
			// Absence is not evidence of partial data. Blocking here would stop
			// recommendations for every org whose day Go never dispatched.
			name: "no run at all proceeds", seedRun: false, wantReady: true,
		},
		{
			// A non-fan-out generation is invisible to the gate by design. If
			// the prefix ever drifts from the producer's, EVERY day looks like
			// this row and the gate silently stops gating -- which is what the
			// external prefix pin exists to prevent.
			name: "a run under a different generation prefix is not seen", seedRun: true,
			runStatus: "running", finalizationStatus: "pending",
			generation: "adhoc:daily_metrics_fanout:2026-08-31",
			wantReady:  true,
		},
		{
			name: "a canceled run is excluded and the day proceeds", seedRun: true,
			runStatus: "canceled", finalizationStatus: "pending",
			generation: fanoutGeneration, wantReady: true,
		},
		{
			name: "a failed run is excluded and the day proceeds", seedRun: true,
			runStatus: "failed", finalizationStatus: "pending",
			generation: fanoutGeneration, wantReady: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `TRUNCATE daily_metrics_runs CASCADE`); err != nil {
				t.Fatalf("reset: %v", err)
			}
			if testCase.seedRun {
				seedReadinessRun(t, ctx, pool, orgID, day,
					testCase.generation, testCase.runStatus,
					testCase.finalizationStatus, testCase.stuckPartition)
			}

			observer := &recordingObserver{}
			got := DailyMetricsReady(ctx, pool, orgID, day, observer, discardLogger{})

			if got != testCase.wantReady {
				t.Errorf("DailyMetricsReady = %v, want %v", got, testCase.wantReady)
			}
			if (observer.skipped > 0) != testCase.wantSkippedObserved {
				t.Errorf("skipped observed %d times, want observed=%v — a withheld "+
					"org writes no rows, so an uncounted skip is invisible",
					observer.skipped, testCase.wantSkippedObserved)
			}
			if len(observer.failOpen) != 0 {
				t.Errorf("a successful read must not record a fail-open, got %v",
					observer.failOpen)
			}
		})
	}
}

// TestTheLatestGenerationDecidesRatherThanTheKindestOne pins the ORDER BY.
//
// Two generations for one org/day is the ordinary shape of a re-drive. Without
// the ordering the gate picks arbitrarily, so a day would flip between ready
// and withheld across runs ON UNCHANGED DATA -- and the failure would look like
// flakiness rather than a missing clause.
func TestTheLatestGenerationDecidesRatherThanTheKindestOne(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, readinessFixtureDDL); err != nil {
		t.Fatalf("create fixture schema: %v", err)
	}

	const orgID = "5f0f5a0c-0000-4000-8000-000000000002"
	day := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)

	// The OLDER run succeeded; the NEWER re-drive has not finished. The correct
	// answer is "withhold" -- and note the mutant that drops ORDER BY does not
	// merely differ, it returns the MORE PERMISSIVE answer, so the bug's
	// signature is recommendations computed on partial data.
	seedReadinessRunAt(t, ctx, pool, orgID, day,
		ScheduledFanoutGenerationPrefixForTest+"gen-1", "running", "succeeded", false,
		time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC))
	seedReadinessRunAt(t, ctx, pool, orgID, day,
		ScheduledFanoutGenerationPrefixForTest+"gen-2", "running", "pending", false,
		time.Date(2026, 8, 31, 2, 0, 0, 0, time.UTC))

	observer := &recordingObserver{}
	if DailyMetricsReady(ctx, pool, orgID, day, observer, discardLogger{}) {
		t.Error("the gate proceeded on a superseded succeeded run while the latest " +
			"re-drive is unfinished — recommendations would be computed on partial data")
	}
}

func seedReadinessRun(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID string, day time.Time, generation, status, finalizationStatus string, stuck bool,
) {
	t.Helper()
	seedReadinessRunAt(t, ctx, pool, orgID, day, generation, status,
		finalizationStatus, stuck, time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC))
}

func seedReadinessRunAt(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID string, day time.Time, generation, status, finalizationStatus string,
	stuck bool, createdAt time.Time,
) {
	t.Helper()
	var runID string
	if err := pool.QueryRow(ctx, `
        INSERT INTO daily_metrics_runs
            (id, org_id, target_day, generation, status, finalization_status,
             created_at, updated_at)
        VALUES (gen_random_uuid(), CAST($1 AS uuid), CAST($2 AS date), $3, $4, $5, $6, $6)
        RETURNING id`,
		orgID, day.Format("2006-01-02"), generation, status, finalizationStatus,
		createdAt,
	).Scan(&runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	partitionStatus := "succeeded"
	if stuck {
		partitionStatus = "failed_permanent"
	}
	if _, err := pool.Exec(ctx, `
        INSERT INTO daily_metrics_partitions (id, run_id, ordinal, status)
        VALUES (gen_random_uuid(), CAST($1 AS uuid), 0, $2)`,
		runID, partitionStatus,
	); err != nil {
		t.Fatalf("seed partition: %v", err)
	}
}
