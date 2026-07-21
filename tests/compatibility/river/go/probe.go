package rivercompat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

const RiverVersion = "v0.40.0"

type Mode string

const (
	ModeDirect   Mode = "direct"
	ModePollOnly Mode = "poll-only"
)

type Options struct {
	CrashFirstAttempt bool
	CrashMarker       string
	DatabaseURL       string
	ExpectedAttempt   int
	FetchPollInterval time.Duration
	// InsertMarker inserts one Go v1 job without starting a worker. It exists
	// solely so the rolling-version harness can hand current-version work to
	// an N-1 worker on the upgraded schema.
	InsertMarker     string
	JobTimeout       time.Duration
	MaxAttempts      int
	MigrateOnly      bool
	Mode             Mode
	Priority         int
	Queue            string
	RescueStuckAfter time.Duration
	Samples          int
	Schema           string
	Started          func(Start) error
	// ConsumeMarker makes the probe wait for and execute one externally
	// inserted job (for example, from python_enqueue.py) instead of inserting
	// the internal execute/cancel/recovery matrix.
	ConsumeMarker string
}

type Result struct {
	Status              string             `json:"status"`
	Mode                Mode               `json:"mode"`
	GoVersion           string             `json:"go_version"`
	RiverVersion        string             `json:"river_version"`
	PollOnly            bool               `json:"poll_only"`
	FetchPollIntervalMS float64            `json:"fetch_poll_interval_ms"`
	Migration           MigrationResult    `json:"migration"`
	Workload            *WorkloadResult    `json:"workload,omitempty"`
	Postgres            CounterMeasurement `json:"postgres"`
	Pool                PoolMeasurement    `json:"pool"`
	Gates               GateResult         `json:"gates"`
}

type MigrationResult struct {
	AppliedVersions []int   `json:"applied_versions"`
	DurationMS      float64 `json:"duration_ms"`
	LatestVersion   int     `json:"latest_version"`
	VersionCount    int     `json:"version_count"`
}

type WorkloadResult struct {
	Execute             *JobResult                 `json:"execute,omitempty"`
	ExecuteLatencyMS    *LatencySummary            `json:"execute_latency_ms,omitempty"`
	Cancel              *JobResult                 `json:"cancel,omitempty"`
	RunningCancellation *RunningCancellationResult `json:"running_cancellation,omitempty"`
	Recovery            *JobResult                 `json:"recovery,omitempty"`
	Scheduled           *JobResult                 `json:"scheduled,omitempty"`
	External            *JobResult                 `json:"external,omitempty"`
}

type RunningCancellationResult struct {
	CrossClientContextCancelled bool    `json:"cross_client_context_cancelled"`
	CrossClientObservationMS    float64 `json:"cross_client_observation_ms"`
	SameClientAttempted         bool    `json:"same_client_attempted"`
	SameClientContextCancelled  bool    `json:"same_client_context_cancelled"`
	SameClientObservationMS     float64 `json:"same_client_observation_ms,omitempty"`
	ProbeReleaseUsed            bool    `json:"probe_release_used"`
}

type LatencySummary struct {
	Count       int     `json:"count"`
	Max         float64 `json:"max"`
	Min         float64 `json:"min"`
	P50         float64 `json:"p50"`
	P95         float64 `json:"p95"`
	Limit       float64 `json:"limit"`
	WithinLimit bool    `json:"within_limit"`
}

type GateResult struct {
	BackendConnectionDeltaAtMostSix bool  `json:"backend_connection_delta_at_most_six"`
	CanceledAcquiresZero            bool  `json:"canceled_acquires_zero"`
	EnqueueP95WithinLimit           bool  `json:"enqueue_p95_within_limit"`
	NewConnectionsAtMostSix         bool  `json:"new_connections_at_most_six"`
	CrossClientRunningCancel        *bool `json:"cross_client_running_cancel,omitempty"`
	SameClientRunningCancel         *bool `json:"same_client_running_cancel,omitempty"`
}

type JobResult struct {
	Attempt               int      `json:"attempt"`
	EnqueueToStartMS      *float64 `json:"enqueue_to_start_ms,omitempty"`
	ErrorCount            int      `json:"error_count"`
	MaxAttempts           int      `json:"max_attempts"`
	Outcome               string   `json:"outcome"`
	Priority              int      `json:"priority"`
	Queue                 string   `json:"queue"`
	RecoveryFirstToLastMS *float64 `json:"recovery_first_to_last_ms,omitempty"`
	RunDurationMS         *float64 `json:"run_duration_ms,omitempty"`
	Scheduled             bool     `json:"scheduled"`
	Source                string   `json:"source"`
	State                 string   `json:"state"`
}

type DatabaseCounters struct {
	BackendConnections int64 `json:"backend_connections"`
	BlocksHit          int64 `json:"blocks_hit"`
	BlocksRead         int64 `json:"blocks_read"`
	Sessions           int64 `json:"sessions"`
	TuplesDeleted      int64 `json:"tuples_deleted"`
	TuplesInserted     int64 `json:"tuples_inserted"`
	TuplesUpdated      int64 `json:"tuples_updated"`
	XactCommit         int64 `json:"xact_commit"`
	XactRollback       int64 `json:"xact_rollback"`
}

type CounterMeasurement struct {
	Before DatabaseCounters `json:"before"`
	After  DatabaseCounters `json:"after"`
	Delta  DatabaseCounters `json:"delta"`
}

type PoolCounters struct {
	AcquireCount         int64   `json:"acquire_count"`
	AcquireDurationMS    float64 `json:"acquire_duration_ms"`
	AcquiredConnections  int64   `json:"acquired_connections"`
	CanceledAcquireCount int64   `json:"canceled_acquire_count"`
	EmptyAcquireCount    int64   `json:"empty_acquire_count"`
	IdleConnections      int64   `json:"idle_connections"`
	NewConnections       int64   `json:"new_connections"`
	TotalConnections     int64   `json:"total_connections"`
}

type PoolMeasurement struct {
	Before PoolCounters `json:"before"`
	After  PoolCounters `json:"after"`
	Delta  PoolCounters `json:"delta"`
}

type PhaseError struct {
	Phase string
	Err   error
}

func (e *PhaseError) Error() string { return e.Phase + ": " + e.Err.Error() }
func (e *PhaseError) Unwrap() error { return e.Err }

func ErrorPhase(err error) string {
	var phaseErr *PhaseError
	if errors.As(err, &phaseErr) {
		return phaseErr.Phase
	}
	return "unknown"
}

func Run(ctx context.Context, rawOpts Options) (_ Result, retErr error) {
	opts, err := normalizedOptions(rawOpts)
	if err != nil {
		return Result{}, phaseError("validate_options", err)
	}

	poolConfig, err := pgxpool.ParseConfig(opts.DatabaseURL)
	if err != nil {
		return Result{}, phaseError("parse_database_url", err)
	}
	poolConfig.MaxConns = 6
	poolConfig.MinConns = 0
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "chaos3034-river-compat"
	if opts.Mode == ModePollOnly {
		// Transaction-mode PgBouncer cannot retain prepared statements on a
		// backend connection between transactions.
		poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return Result{}, phaseError("open_database", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return Result{}, phaseError("ping_database", err)
	}

	result := Result{
		Status:              "ok",
		Mode:                opts.Mode,
		GoVersion:           runtime.Version(),
		RiverVersion:        RiverVersion,
		PollOnly:            opts.Mode == ModePollOnly,
		FetchPollIntervalMS: milliseconds(opts.FetchPollInterval),
	}

	migrationStarted := time.Now()
	migrator, err := rivermigrate.New(
		riverpgxv5.New(pool),
		&rivermigrate.Config{Logger: discardLogger(), Schema: opts.Schema},
	)
	if err != nil {
		return Result{}, phaseError("create_migrator", err)
	}
	migrated, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return Result{}, phaseError("migrate", err)
	}
	versions, err := migrator.ExistingVersions(ctx)
	if err != nil {
		return Result{}, phaseError("read_migration_versions", err)
	}
	result.Migration = MigrationResult{
		AppliedVersions: make([]int, 0, len(migrated.Versions)),
		DurationMS:      milliseconds(time.Since(migrationStarted)),
		VersionCount:    len(versions),
	}
	for _, migration := range migrated.Versions {
		result.Migration.AppliedVersions = append(result.Migration.AppliedVersions, migration.Version)
	}
	for _, migration := range versions {
		if migration.Version > result.Migration.LatestVersion {
			result.Migration.LatestVersion = migration.Version
		}
	}

	result.Postgres.Before, err = readDatabaseCounters(ctx, pool)
	if err != nil {
		return Result{}, phaseError("read_postgres_counters_before", err)
	}
	result.Pool.Before = readPoolCounters(pool)
	if opts.MigrateOnly {
		result.Postgres.After = result.Postgres.Before
		result.Postgres.Delta = subtractDatabaseCounters(result.Postgres.After, result.Postgres.Before)
		result.Pool.After = readPoolCounters(pool)
		result.Pool.Delta = subtractPoolCounters(result.Pool.After, result.Pool.Before)
		return result, nil
	}

	worker := NewWorker()
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, worker); err != nil {
		return Result{}, phaseError("register_worker", err)
	}
	inserter, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Logger:  discardLogger(),
		Schema:  opts.Schema,
		Workers: workers,
	})
	if err != nil {
		return Result{}, phaseError("create_insert_client", err)
	}
	if opts.InsertMarker != "" {
		inserted, err := inserter.Insert(ctx, JobArgs{
			ContractVersion: ContractVersion,
			Marker:          opts.InsertMarker,
			Source:          "go",
		}, insertOptions(opts, time.Time{}))
		if err != nil {
			return Result{}, phaseError("insert_external_job", err)
		}
		external, err := jobResultFromRow(inserted.Job, nil, nil, nil)
		if err != nil {
			return Result{}, phaseError("observe_inserted_external_job", err)
		}
		external.Outcome = "inserted"
		result.Workload = &WorkloadResult{External: &external}
		result.Postgres.After, err = readDatabaseCounters(ctx, pool)
		if err != nil {
			return Result{}, phaseError("read_postgres_counters_after", err)
		}
		result.Postgres.Delta = subtractDatabaseCounters(result.Postgres.After, result.Postgres.Before)
		result.Pool.After = readPoolCounters(pool)
		result.Pool.Delta = subtractPoolCounters(result.Pool.After, result.Pool.Before)
		result.Gates = calculateGates(result)
		return result, nil
	}

	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		FetchCooldown:        min(25*time.Millisecond, opts.FetchPollInterval),
		FetchPollInterval:    opts.FetchPollInterval,
		ID:                   fmt.Sprintf("chaos3034-%s-%d", opts.Mode, time.Now().UnixNano()),
		Logger:               discardLogger(),
		JobTimeout:           opts.JobTimeout,
		PollOnly:             opts.Mode == ModePollOnly,
		Queues:               map[string]river.QueueConfig{opts.Queue: {MaxWorkers: 1}},
		RescueStuckJobsAfter: opts.RescueStuckAfter,
		Schema:               opts.Schema,
		SoftStopTimeout:      2 * time.Second,
		TestOnly:             true,
		Workers:              workers,
	})
	if err != nil {
		return Result{}, phaseError("create_worker_client", err)
	}

	events, cancelSubscription := client.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
	)
	defer cancelSubscription()
	if err := client.Start(ctx); err != nil {
		return Result{}, phaseError("start_worker_client", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.StopAndCancel(stopCtx)
	}()

	if opts.CrashFirstAttempt {
		if err := runCrashCandidate(ctx, opts, inserter, worker); err != nil {
			return Result{}, phaseError("crash_candidate", err)
		}
		return Result{}, phaseError("crash_candidate", errors.New("blocking job returned without process termination"))
	} else if opts.ConsumeMarker != "" {
		external, err := consumeExternal(ctx, client, worker, events, opts.ConsumeMarker, opts.ExpectedAttempt)
		if err != nil {
			return Result{}, phaseError("consume_external_job", err)
		}
		result.Workload = &WorkloadResult{External: &external}
	} else {
		workload, err := runInternalWorkload(ctx, opts, client, inserter, worker, events)
		if err != nil {
			return Result{}, err
		}
		result.Workload = &workload
	}

	result.Postgres.After, err = readDatabaseCounters(ctx, pool)
	if err != nil {
		return Result{}, phaseError("read_postgres_counters_after", err)
	}
	result.Postgres.Delta = subtractDatabaseCounters(result.Postgres.After, result.Postgres.Before)
	result.Pool.After = readPoolCounters(pool)
	result.Pool.Delta = subtractPoolCounters(result.Pool.After, result.Pool.Before)
	result.Gates = calculateGates(result)

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	if err := client.Stop(stopCtx); err != nil {
		return Result{}, phaseError("stop_worker_client", err)
	}
	return result, nil
}

func runInternalWorkload(
	ctx context.Context,
	opts Options,
	client *river.Client[pgx.Tx],
	inserter *river.Client[pgx.Tx],
	worker *Worker,
	events <-chan *river.Event,
) (WorkloadResult, error) {
	executeLatencies := make([]float64, 0, opts.Samples)
	var execute JobResult
	for sample := range opts.Samples {
		executeMarker := newMarker(fmt.Sprintf("execute-%d", sample), opts.Mode)
		worker.Register(executeMarker, ScenarioExecute)
		executeQueuedAt := time.Now().UTC()
		executeInsert, err := inserter.Insert(ctx, JobArgs{
			ContractVersion: ContractVersion,
			Marker:          executeMarker,
			Source:          "go",
		}, insertOptions(opts, time.Time{}))
		if err != nil {
			return WorkloadResult{}, phaseError("insert_execute_job", err)
		}
		executeStart, err := waitForStart(ctx, worker.Starts(), executeMarker, 1)
		if err != nil {
			return WorkloadResult{}, phaseError("wait_execute_start", err)
		}
		executeEvent, err := waitForEvent(ctx, events, executeInsert.Job.ID, river.EventKindJobCompleted)
		if err != nil {
			return WorkloadResult{}, phaseError("wait_execute_complete", err)
		}
		observed, err := observeJob(ctx, client, executeInsert.Job.ID, executeEvent, executeStart, executeQueuedAt, nil)
		if err != nil {
			return WorkloadResult{}, phaseError("observe_execute_job", err)
		}
		if observed.EnqueueToStartMS == nil {
			return WorkloadResult{}, phaseError("observe_execute_job", errors.New("missing enqueue-to-start latency"))
		}
		executeLatencies = append(executeLatencies, *observed.EnqueueToStartMS)
		if sample == 0 {
			execute = observed
		}
	}
	latencyLimit := 100.0
	if opts.Mode == ModePollOnly {
		latencyLimit = milliseconds(2*opts.FetchPollInterval + 100*time.Millisecond)
	}
	executeLatency := summarizeLatencies(executeLatencies, latencyLimit)

	scheduledMarker := newMarker("scheduled", opts.Mode)
	worker.Register(scheduledMarker, ScenarioExecute)
	scheduledInsert, err := inserter.Insert(ctx, JobArgs{
		ContractVersion: ContractVersion,
		Marker:          scheduledMarker,
		Source:          "go",
	}, insertOptions(opts, time.Now().UTC().Add(10*time.Second)))
	if err != nil {
		return WorkloadResult{}, phaseError("insert_scheduled_job", err)
	}
	if scheduledInsert.Job.State != rivertype.JobStateScheduled {
		return WorkloadResult{}, phaseError("observe_scheduled_job", fmt.Errorf("state = %s, want scheduled", scheduledInsert.Job.State))
	}
	scheduledRow, err := inserter.JobCancel(ctx, scheduledInsert.Job.ID)
	if err != nil {
		return WorkloadResult{}, phaseError("cancel_scheduled_job", err)
	}
	scheduledResult, err := jobResultFromRow(scheduledRow, nil, nil, nil)
	if err != nil {
		return WorkloadResult{}, phaseError("observe_scheduled_job", err)
	}
	if scheduledRow.State != rivertype.JobStateCancelled || !scheduledResult.Scheduled {
		return WorkloadResult{}, phaseError(
			"observe_scheduled_job",
			fmt.Errorf("state/scheduled = %s/%t, want cancelled/true", scheduledRow.State, scheduledResult.Scheduled),
		)
	}
	scheduledResult.Outcome = "scheduled_state_observed"

	cancelMarker := newMarker("cancel", opts.Mode)
	worker.Register(cancelMarker, ScenarioBlockFirst)
	cancelQueuedAt := time.Now().UTC()
	cancelInsert, err := inserter.Insert(ctx, JobArgs{
		ContractVersion: ContractVersion,
		Marker:          cancelMarker,
		Source:          "go",
	}, insertOptions(opts, time.Time{}))
	if err != nil {
		return WorkloadResult{}, phaseError("insert_cancel_job", err)
	}
	cancelStart, err := waitForStart(ctx, worker.Starts(), cancelMarker, 1)
	if err != nil {
		return WorkloadResult{}, phaseError("wait_cancel_start", err)
	}
	if _, err := inserter.JobCancel(ctx, cancelInsert.Job.ID); err != nil {
		return WorkloadResult{}, phaseError("cancel_running_job", err)
	}
	cancelObservationWindow := 5 * time.Second
	if opts.Mode == ModePollOnly {
		cancelObservationWindow = max(3*opts.FetchPollInterval, 500*time.Millisecond)
	}
	crossStarted := time.Now()
	crossCtx, cancelCross := context.WithTimeout(ctx, cancelObservationWindow)
	cancelFinish, crossErr := waitForFinish(crossCtx, worker.Finishes(), cancelMarker, 1)
	cancelCross()
	cancellation := RunningCancellationResult{
		CrossClientContextCancelled: crossErr == nil && errors.Is(cancelFinish.Cause, river.ErrJobCancelledRemotely),
		CrossClientObservationMS:    milliseconds(time.Since(crossStarted)),
	}
	if opts.Mode == ModeDirect {
		if crossErr != nil {
			return WorkloadResult{}, phaseError("wait_cross_client_cancel_context", crossErr)
		}
		if !cancellation.CrossClientContextCancelled {
			return WorkloadResult{}, phaseError(
				"observe_cross_client_cancel_context",
				fmt.Errorf("context cause = %v, want River remote cancellation", cancelFinish.Cause),
			)
		}
	} else if crossErr != nil && !errors.Is(crossErr, context.DeadlineExceeded) {
		return WorkloadResult{}, phaseError("wait_cross_client_cancel_context", crossErr)
	}

	if !cancellation.CrossClientContextCancelled {
		cancellation.SameClientAttempted = true
		if _, err := client.JobCancel(ctx, cancelInsert.Job.ID); err != nil {
			return WorkloadResult{}, phaseError("cancel_running_job_same_client", err)
		}
		sameStarted := time.Now()
		sameCtx, cancelSame := context.WithTimeout(ctx, cancelObservationWindow)
		cancelFinish, err = waitForFinish(sameCtx, worker.Finishes(), cancelMarker, 1)
		cancelSame()
		cancellation.SameClientObservationMS = milliseconds(time.Since(sameStarted))
		cancellation.SameClientContextCancelled = err == nil && errors.Is(cancelFinish.Cause, river.ErrJobCancelledRemotely)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return WorkloadResult{}, phaseError("wait_same_client_cancel_context", err)
		}
		if !cancellation.SameClientContextCancelled {
			if err := worker.Release(cancelMarker); err != nil {
				return WorkloadResult{}, phaseError("release_cancel_probe", err)
			}
			cancelFinish, err = waitForFinish(ctx, worker.Finishes(), cancelMarker, 1)
			if err != nil {
				return WorkloadResult{}, phaseError("wait_cancel_probe_release", err)
			}
			if !errors.Is(cancelFinish.Cause, ErrProbeRelease) {
				return WorkloadResult{}, phaseError("observe_cancel_probe_release", errors.New("unexpected release cause"))
			}
			cancellation.ProbeReleaseUsed = true
		}
	}
	cancelEvent, err := waitForEvent(ctx, events, cancelInsert.Job.ID, river.EventKindJobCancelled)
	if err != nil {
		return WorkloadResult{}, phaseError("wait_cancel_event", err)
	}
	cancelRow, err := client.JobGet(ctx, cancelInsert.Job.ID)
	if err != nil {
		return WorkloadResult{}, phaseError("read_cancel_job", err)
	}
	cancelLatency := milliseconds(cancelStart.Time.Sub(cancelQueuedAt))
	cancelResult, err := jobResultFromRow(cancelRow, cancelEvent, &cancelLatency, nil)
	if err != nil {
		return WorkloadResult{}, phaseError("observe_cancel_job", err)
	}
	if cancelRow.State != rivertype.JobStateCancelled {
		return WorkloadResult{}, phaseError("observe_cancel_job", fmt.Errorf("state = %s, want cancelled", cancelRow.State))
	}
	switch {
	case cancellation.CrossClientContextCancelled:
		cancelResult.Outcome = "running_context_cancelled_cross_client"
	case cancellation.SameClientContextCancelled:
		cancelResult.Outcome = "running_context_cancelled_same_client"
	default:
		cancelResult.Outcome = "running_cancel_not_propagated_probe_released"
	}

	recoveryMarker := newMarker("recovery", opts.Mode)
	worker.Register(recoveryMarker, ScenarioRecover)
	recoveryQueuedAt := time.Now().UTC()
	recoveryInsert, err := inserter.Insert(ctx, JobArgs{
		ContractVersion: ContractVersion,
		Marker:          recoveryMarker,
		Source:          "go",
	}, insertOptions(opts, time.Time{}))
	if err != nil {
		return WorkloadResult{}, phaseError("insert_recovery_job", err)
	}
	recoveryFirstStart, err := waitForStart(ctx, worker.Starts(), recoveryMarker, 1)
	if err != nil {
		return WorkloadResult{}, phaseError("wait_recovery_first_start", err)
	}
	if _, err := waitForEvent(ctx, events, recoveryInsert.Job.ID, river.EventKindJobFailed); err != nil {
		return WorkloadResult{}, phaseError("wait_recovery_failure", err)
	}
	recoveryLastStart, err := waitForStart(ctx, worker.Starts(), recoveryMarker, 2)
	if err != nil {
		return WorkloadResult{}, phaseError("wait_recovery_second_start", err)
	}
	recoveryEvent, err := waitForEvent(ctx, events, recoveryInsert.Job.ID, river.EventKindJobCompleted)
	if err != nil {
		return WorkloadResult{}, phaseError("wait_recovery_complete", err)
	}
	recoveryGap := milliseconds(recoveryLastStart.Time.Sub(recoveryFirstStart.Time))
	recovery, err := observeJob(
		ctx,
		client,
		recoveryInsert.Job.ID,
		recoveryEvent,
		recoveryFirstStart,
		recoveryQueuedAt,
		&recoveryGap,
	)
	if err != nil {
		return WorkloadResult{}, phaseError("observe_recovery_job", err)
	}
	if recovery.Attempt != 2 || recovery.ErrorCount != 1 {
		return WorkloadResult{}, phaseError(
			"observe_recovery_job",
			fmt.Errorf("attempt/errors = %d/%d, want 2/1", recovery.Attempt, recovery.ErrorCount),
		)
	}
	recovery.Outcome = "completed_after_retry"

	return WorkloadResult{
		Execute:             &execute,
		ExecuteLatencyMS:    &executeLatency,
		Cancel:              &cancelResult,
		RunningCancellation: &cancellation,
		Recovery:            &recovery,
		Scheduled:           &scheduledResult,
	}, nil
}

func consumeExternal(
	ctx context.Context,
	client *river.Client[pgx.Tx],
	worker *Worker,
	events <-chan *river.Event,
	marker string,
	expectedAttempt int,
) (JobResult, error) {
	start, err := waitForStart(ctx, worker.Starts(), marker, expectedAttempt)
	if err != nil {
		return JobResult{}, err
	}
	event, err := waitForEvent(ctx, events, start.JobID, river.EventKindJobCompleted)
	if err != nil {
		return JobResult{}, err
	}
	row, err := client.JobGet(ctx, start.JobID)
	if err != nil {
		return JobResult{}, err
	}
	enqueueToStart := milliseconds(start.Time.Sub(row.CreatedAt))
	result, err := jobResultFromRow(row, event, &enqueueToStart, nil)
	if err != nil {
		return JobResult{}, err
	}
	result.Outcome = "external_completed"
	if expectedAttempt > 0 && result.Attempt != expectedAttempt {
		return JobResult{}, fmt.Errorf("attempt = %d, want %d", result.Attempt, expectedAttempt)
	}
	return result, nil
}

func runCrashCandidate(ctx context.Context, opts Options, inserter *river.Client[pgx.Tx], worker *Worker) error {
	marker := opts.CrashMarker
	if marker == "" {
		marker = newMarker("crash", opts.Mode)
	}
	worker.Register(marker, ScenarioBlockFirst)
	inserted, err := inserter.Insert(ctx, JobArgs{
		ContractVersion: ContractVersion,
		Marker:          marker,
		Source:          "go",
	}, insertOptions(opts, time.Time{}))
	if err != nil {
		return err
	}
	start, err := waitForStart(ctx, worker.Starts(), marker, 1)
	if err != nil {
		return err
	}
	if start.JobID != inserted.Job.ID {
		return fmt.Errorf("started job ID %d, want inserted job ID %d", start.JobID, inserted.Job.ID)
	}
	if opts.Started != nil {
		if err := opts.Started(start); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return context.Cause(ctx)
}

func observeJob(
	ctx context.Context,
	client *river.Client[pgx.Tx],
	jobID int64,
	event *river.Event,
	start Start,
	enqueueRequestedAt time.Time,
	recoveryGap *float64,
) (JobResult, error) {
	row, err := client.JobGet(ctx, jobID)
	if err != nil {
		return JobResult{}, err
	}
	enqueueToStart := milliseconds(start.Time.Sub(enqueueRequestedAt))
	result, err := jobResultFromRow(row, event, &enqueueToStart, recoveryGap)
	if err != nil {
		return JobResult{}, err
	}
	result.Outcome = "completed"
	if row.State != rivertype.JobStateCompleted {
		return JobResult{}, fmt.Errorf("state = %s, want completed", row.State)
	}
	return result, nil
}

func jobResultFromRow(
	row *rivertype.JobRow,
	event *river.Event,
	enqueueToStart *float64,
	recoveryGap *float64,
) (JobResult, error) {
	args, err := DecodeJobArgs(row.EncodedArgs)
	if err != nil {
		return JobResult{}, err
	}
	if row.Kind != JobKind {
		return JobResult{}, fmt.Errorf("kind = %q, want %q", row.Kind, JobKind)
	}

	var runDuration *float64
	if event != nil && event.JobStats != nil {
		value := milliseconds(event.JobStats.RunDuration)
		runDuration = &value
	}
	return JobResult{
		Attempt:               row.Attempt,
		EnqueueToStartMS:      enqueueToStart,
		ErrorCount:            len(row.Errors),
		MaxAttempts:           row.MaxAttempts,
		Priority:              row.Priority,
		Queue:                 row.Queue,
		RecoveryFirstToLastMS: recoveryGap,
		RunDurationMS:         runDuration,
		Scheduled:             row.ScheduledAt.After(row.CreatedAt),
		Source:                args.Source,
		State:                 string(row.State),
	}, nil
}

func waitForStart(ctx context.Context, starts <-chan Start, marker string, attempt int) (Start, error) {
	for {
		select {
		case <-ctx.Done():
			return Start{}, context.Cause(ctx)
		case start := <-starts:
			if start.Args.Marker == marker && (attempt == 0 || start.Attempt == attempt) {
				return start, nil
			}
		}
	}
}

func waitForFinish(ctx context.Context, finishes <-chan Finish, marker string, attempt int) (Finish, error) {
	for {
		select {
		case <-ctx.Done():
			return Finish{}, context.Cause(ctx)
		case finish := <-finishes:
			if finish.Marker == marker && (attempt == 0 || finish.Attempt == attempt) {
				return finish, nil
			}
		}
	}
}

func waitForEvent(ctx context.Context, events <-chan *river.Event, jobID int64, kind river.EventKind) (*river.Event, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case event := <-events:
			if event != nil && event.Job != nil && event.Job.ID == jobID && event.Kind == kind {
				return event, nil
			}
		}
	}
}

func insertOptions(opts Options, scheduledAt time.Time) *river.InsertOpts {
	return &river.InsertOpts{
		MaxAttempts: opts.MaxAttempts,
		Priority:    opts.Priority,
		Queue:       opts.Queue,
		ScheduledAt: scheduledAt,
		Tags:        []string{"phase0", "golang"},
	}
}

func normalizedOptions(opts Options) (Options, error) {
	if opts.DatabaseURL == "" {
		return Options{}, errors.New("database URL is required")
	}
	if opts.Mode == "" {
		opts.Mode = ModeDirect
	}
	if opts.Mode != ModeDirect && opts.Mode != ModePollOnly {
		return Options{}, fmt.Errorf("mode must be %q or %q", ModeDirect, ModePollOnly)
	}
	if opts.FetchPollInterval == 0 {
		opts.FetchPollInterval = 250 * time.Millisecond
	}
	if opts.FetchPollInterval < time.Millisecond {
		return Options{}, errors.New("fetch poll interval must be at least 1ms")
	}
	if opts.Queue == "" {
		opts.Queue = "chaos3034"
	}
	if opts.Priority == 0 {
		opts.Priority = 2
	}
	if opts.Priority < 1 || opts.Priority > 4 {
		return Options{}, errors.New("priority must be between 1 and 4")
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	if opts.MaxAttempts < 2 {
		return Options{}, errors.New("max attempts must be at least 2 for the recovery probe")
	}
	if opts.Samples == 0 {
		opts.Samples = 20
	}
	if opts.Samples < 1 || opts.Samples > 1_000 {
		return Options{}, errors.New("samples must be between 1 and 1000")
	}
	if opts.ExpectedAttempt < 0 {
		return Options{}, errors.New("expected attempt cannot be negative")
	}
	operationCount := 0
	for _, selected := range []bool{
		opts.CrashFirstAttempt,
		opts.ConsumeMarker != "",
		opts.InsertMarker != "",
		opts.MigrateOnly,
	} {
		if selected {
			operationCount++
		}
	}
	if operationCount > 1 {
		return Options{}, errors.New("migrate, insert, consume, and crash operations are mutually exclusive")
	}
	if opts.JobTimeout < 0 {
		return Options{}, errors.New("job timeout cannot be negative")
	}
	if opts.RescueStuckAfter < 0 {
		return Options{}, errors.New("rescue stuck after cannot be negative")
	}
	if opts.RescueStuckAfter > 0 && opts.JobTimeout > 0 && opts.RescueStuckAfter < opts.JobTimeout {
		return Options{}, errors.New("rescue stuck after must be at least the job timeout")
	}
	return opts, nil
}

func newMarker(prefix string, mode Mode) string {
	return fmt.Sprintf("go-%s-%s-%d", prefix, mode, time.Now().UnixNano())
}

func readDatabaseCounters(ctx context.Context, pool *pgxpool.Pool) (DatabaseCounters, error) {
	var counters DatabaseCounters
	err := pool.QueryRow(ctx, `
		SELECT
			numbackends,
			blks_hit,
			blks_read,
			sessions,
			tup_deleted,
			tup_inserted,
			tup_updated,
			xact_commit,
			xact_rollback
		FROM pg_stat_database
		WHERE datname = current_database()
	`).Scan(
		&counters.BackendConnections,
		&counters.BlocksHit,
		&counters.BlocksRead,
		&counters.Sessions,
		&counters.TuplesDeleted,
		&counters.TuplesInserted,
		&counters.TuplesUpdated,
		&counters.XactCommit,
		&counters.XactRollback,
	)
	return counters, err
}

func readPoolCounters(pool *pgxpool.Pool) PoolCounters {
	stats := pool.Stat()
	return PoolCounters{
		AcquireCount:         stats.AcquireCount(),
		AcquireDurationMS:    milliseconds(stats.AcquireDuration()),
		AcquiredConnections:  int64(stats.AcquiredConns()),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
		IdleConnections:      int64(stats.IdleConns()),
		NewConnections:       stats.NewConnsCount(),
		TotalConnections:     int64(stats.TotalConns()),
	}
}

func subtractDatabaseCounters(after, before DatabaseCounters) DatabaseCounters {
	return DatabaseCounters{
		BackendConnections: after.BackendConnections - before.BackendConnections,
		BlocksHit:          after.BlocksHit - before.BlocksHit,
		BlocksRead:         after.BlocksRead - before.BlocksRead,
		Sessions:           after.Sessions - before.Sessions,
		TuplesDeleted:      after.TuplesDeleted - before.TuplesDeleted,
		TuplesInserted:     after.TuplesInserted - before.TuplesInserted,
		TuplesUpdated:      after.TuplesUpdated - before.TuplesUpdated,
		XactCommit:         after.XactCommit - before.XactCommit,
		XactRollback:       after.XactRollback - before.XactRollback,
	}
}

func subtractPoolCounters(after, before PoolCounters) PoolCounters {
	return PoolCounters{
		AcquireCount:         after.AcquireCount - before.AcquireCount,
		AcquireDurationMS:    after.AcquireDurationMS - before.AcquireDurationMS,
		AcquiredConnections:  after.AcquiredConnections - before.AcquiredConnections,
		CanceledAcquireCount: after.CanceledAcquireCount - before.CanceledAcquireCount,
		EmptyAcquireCount:    after.EmptyAcquireCount - before.EmptyAcquireCount,
		IdleConnections:      after.IdleConnections - before.IdleConnections,
		NewConnections:       after.NewConnections - before.NewConnections,
		TotalConnections:     after.TotalConnections - before.TotalConnections,
	}
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration.Microseconds()) / 1_000
}

func summarizeLatencies(samples []float64, limit float64) LatencySummary {
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	result := LatencySummary{
		Count: len(sorted),
		Limit: limit,
	}
	if len(sorted) == 0 {
		return result
	}
	result.Min = sorted[0]
	result.P50 = nearestRank(sorted, 0.50)
	result.P95 = nearestRank(sorted, 0.95)
	result.Max = sorted[len(sorted)-1]
	result.WithinLimit = result.P95 <= limit
	return result
}

func nearestRank(sorted []float64, percentile float64) float64 {
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func calculateGates(result Result) GateResult {
	latencyWithinLimit := true
	if result.Workload != nil && result.Workload.ExecuteLatencyMS != nil {
		latencyWithinLimit = result.Workload.ExecuteLatencyMS.WithinLimit
	}
	gates := GateResult{
		BackendConnectionDeltaAtMostSix: result.Postgres.Delta.BackendConnections <= 6,
		CanceledAcquiresZero:            result.Pool.Delta.CanceledAcquireCount == 0,
		EnqueueP95WithinLimit:           latencyWithinLimit,
		NewConnectionsAtMostSix:         result.Pool.Delta.NewConnections <= 6,
	}
	if result.Workload != nil && result.Workload.RunningCancellation != nil {
		crossClient := result.Workload.RunningCancellation.CrossClientContextCancelled
		gates.CrossClientRunningCancel = &crossClient
		if result.Workload.RunningCancellation.SameClientAttempted {
			sameClient := result.Workload.RunningCancellation.SameClientContextCancelled
			gates.SameClientRunningCancel = &sameClient
		}
	}
	return gates
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func phaseError(phase string, err error) error {
	return &PhaseError{Phase: phase, Err: err}
}
