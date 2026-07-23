package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"runtime/debug"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

const (
	resultSchemaVersion = 1
	riverVersion        = "v0.39.0"
	riverDriverVersion  = "v0.39.0"
	pgxVersion          = "v5.9.2"
	latestNMinus1       = 6
)

type options struct {
	consumeExisting bool
	databaseURL     string
	marker          string
	operation       string
	pollOnly        bool
	queue           string
	schema          string
}

type result struct {
	SchemaVersion      int    `json:"schema_version"`
	Status             string `json:"status"`
	Operation          string `json:"operation"`
	GoVersion          string `json:"go_version"`
	PgxVersion         string `json:"pgx_version"`
	RiverDriverVersion string `json:"river_driver_version"`
	RiverVersion       string `json:"river_version"`
	PollOnly           bool   `json:"poll_only"`
	Marker             string `json:"marker,omitempty"`
	Queue              string `json:"queue,omitempty"`
	JobID              int64  `json:"job_id,omitempty"`
	ContractVersion    int    `json:"contract_version,omitempty"`
	Source             string `json:"source,omitempty"`
	Outcome            string `json:"outcome,omitempty"`
	InsertedByWorker   bool   `json:"inserted_by_worker,omitempty"`
	LatestMigration    int    `json:"latest_migration,omitempty"`
	AppliedVersions    []int  `json:"applied_versions,omitempty"`
}

type phaseError struct {
	phase string
	err   error
}

func (e *phaseError) Error() string { return e.phase + ": " + e.err.Error() }
func (e *phaseError) Unwrap() error { return e.err }

func failPhase(phase string, err error) error {
	return &phaseError{phase: phase, err: err}
}

func errorPhase(err error) string {
	var phased *phaseError
	if errors.As(err, &phased) {
		return phased.phase
	}
	return "unknown"
}

func runProbe(ctx context.Context, opts options) (result, error) {
	if err := verifyDependencyVersion("github.com/riverqueue/river", riverVersion); err != nil {
		return result{}, failPhase("verify_river_version", err)
	}
	if err := verifyDependencyVersion("github.com/riverqueue/river/riverdriver/riverpgxv5", riverDriverVersion); err != nil {
		return result{}, failPhase("verify_river_driver_version", err)
	}
	if err := verifyDependencyVersion("github.com/jackc/pgx/v5", pgxVersion); err != nil {
		return result{}, failPhase("verify_pgx_version", err)
	}
	poolConfig, err := pgxpool.ParseConfig(opts.databaseURL)
	if err != nil {
		return result{}, failPhase("parse_database_url", err)
	}
	poolConfig.MaxConns = 4
	poolConfig.MinConns = 0
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "chaos3034-river-n-minus-1"
	if opts.pollOnly {
		// Transaction-mode PgBouncer cannot retain prepared statements between
		// transactions. PollOnly avoids LISTEN and simple protocol avoids the
		// session-scoped prepared-statement path.
		poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return result{}, failPhase("open_database", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return result{}, failPhase("ping_database", err)
	}

	base := result{
		SchemaVersion:      resultSchemaVersion,
		Status:             "ok",
		Operation:          opts.operation,
		GoVersion:          runtime.Version(),
		PgxVersion:         pgxVersion,
		RiverDriverVersion: riverDriverVersion,
		RiverVersion:       riverVersion,
		PollOnly:           opts.pollOnly,
	}

	switch opts.operation {
	case "migrate":
		return runMigrate(ctx, pool, opts, base)
	case "insert":
		return runInsert(ctx, pool, opts, base)
	case "work":
		return runWork(ctx, pool, opts, base)
	default:
		return result{}, failPhase("validate_options", fmt.Errorf("unsupported operation %q", opts.operation))
	}
}

func verifyDependencyVersion(path, expected string) error {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("Go build information is unavailable")
	}
	for _, dependency := range buildInfo.Deps {
		if dependency.Path != path {
			continue
		}
		if dependency.Replace != nil {
			dependency = dependency.Replace
		}
		if dependency.Version != expected {
			return fmt.Errorf("%s version = %q, want %q", path, dependency.Version, expected)
		}
		return nil
	}
	return fmt.Errorf("%s is absent from Go build information", path)
}

func runMigrate(ctx context.Context, pool *pgxpool.Pool, opts options, base result) (result, error) {
	migrator, err := rivermigrate.New(
		riverpgxv5.New(pool),
		&rivermigrate.Config{Logger: discardLogger(), Schema: opts.schema},
	)
	if err != nil {
		return result{}, failPhase("create_migrator", err)
	}

	all := migrator.AllVersions()
	if len(all) == 0 || all[len(all)-1].Version != latestNMinus1 {
		return result{}, failPhase("verify_migration_bundle", errors.New("unexpected N-1 migration bundle"))
	}
	migrated, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return result{}, failPhase("migrate", err)
	}
	existing, err := migrator.ExistingVersions(ctx)
	if err != nil {
		return result{}, failPhase("read_migrations", err)
	}

	base.AppliedVersions = make([]int, 0, len(migrated.Versions))
	for _, migration := range migrated.Versions {
		base.AppliedVersions = append(base.AppliedVersions, migration.Version)
	}
	slices.Sort(base.AppliedVersions)
	for _, migration := range existing {
		base.LatestMigration = max(base.LatestMigration, migration.Version)
	}
	if base.LatestMigration != latestNMinus1 {
		return result{}, failPhase("verify_migration_prefix", errors.New("N-1 migration prefix is incomplete"))
	}
	base.Outcome = "migrated"
	return base, nil
}

func runInsert(ctx context.Context, pool *pgxpool.Pool, opts options, base result) (result, error) {
	workers, err := compatWorkers(nil)
	if err != nil {
		return result{}, failPhase("register_worker", err)
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Logger:  discardLogger(),
		Schema:  opts.schema,
		Workers: workers,
	})
	if err != nil {
		return result{}, failPhase("create_insert_client", err)
	}

	args := jobArgs{ContractVersion: contractVersion, Marker: opts.marker, Source: "go"}
	inserted, err := client.Insert(ctx, args, &river.InsertOpts{Queue: opts.queue})
	if err != nil {
		return result{}, failPhase("insert_job", err)
	}
	base.Marker = args.Marker
	base.Queue = opts.queue
	base.JobID = inserted.Job.ID
	base.ContractVersion = args.ContractVersion
	base.Source = args.Source
	base.Outcome = "inserted"
	return base, nil
}

type workedJob struct {
	args    jobArgs
	attempt int
	id      int64
}

type compatWorker struct {
	river.WorkerDefaults[jobArgs]
	worked chan<- workedJob
}

func (w *compatWorker) Work(ctx context.Context, job *river.Job[jobArgs]) error {
	if err := job.Args.validate(); err != nil {
		return err
	}
	select {
	case w.worked <- workedJob{args: job.Args, attempt: job.Attempt, id: job.ID}:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func compatWorkers(worked chan<- workedJob) (*river.Workers, error) {
	workers := river.NewWorkers()
	if worked == nil {
		worked = make(chan workedJob, 1)
	}
	if err := river.AddWorkerSafely(workers, &compatWorker{worked: worked}); err != nil {
		return nil, err
	}
	return workers, nil
}

func runWork(ctx context.Context, pool *pgxpool.Pool, opts options, base result) (result, error) {
	worked := make(chan workedJob, 16)
	workers, err := compatWorkers(worked)
	if err != nil {
		return result{}, failPhase("register_worker", err)
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		FetchCooldown:     25 * time.Millisecond,
		FetchPollInterval: 100 * time.Millisecond,
		Logger:            discardLogger(),
		PollOnly:          opts.pollOnly,
		Queues:            map[string]river.QueueConfig{opts.queue: {MaxWorkers: 1}},
		Schema:            opts.schema,
		SoftStopTimeout:   2 * time.Second,
		TestOnly:          true,
		Workers:           workers,
	})
	if err != nil {
		return result{}, failPhase("create_worker_client", err)
	}

	events, cancelSubscription := client.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
	)
	defer cancelSubscription()
	if err := client.Start(ctx); err != nil {
		return result{}, failPhase("start_worker_client", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.StopAndCancel(stopCtx)
	}()

	var expectedID int64
	if !opts.consumeExisting {
		args := jobArgs{ContractVersion: contractVersion, Marker: opts.marker, Source: "go"}
		inserted, err := client.Insert(ctx, args, &river.InsertOpts{Queue: opts.queue})
		if err != nil {
			return result{}, failPhase("insert_work_job", err)
		}
		expectedID = inserted.Job.ID
		base.InsertedByWorker = true
	}

	observed, err := waitForWorked(ctx, worked, opts.marker, expectedID)
	if err != nil {
		return result{}, failPhase("wait_for_work", err)
	}
	if err := waitForCompletion(ctx, events, observed.id); err != nil {
		return result{}, failPhase("wait_for_completion", err)
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	if err := client.Stop(stopCtx); err != nil {
		return result{}, failPhase("stop_worker_client", err)
	}

	base.Marker = observed.args.Marker
	base.Queue = opts.queue
	base.JobID = observed.id
	base.ContractVersion = observed.args.ContractVersion
	base.Source = observed.args.Source
	base.Outcome = "completed"
	return base, nil
}

func waitForWorked(ctx context.Context, worked <-chan workedJob, marker string, expectedID int64) (workedJob, error) {
	for {
		select {
		case observed := <-worked:
			if observed.args.Marker == marker && (expectedID == 0 || observed.id == expectedID) {
				return observed, nil
			}
		case <-ctx.Done():
			return workedJob{}, context.Cause(ctx)
		}
	}
}

func waitForCompletion(ctx context.Context, events <-chan *river.Event, jobID int64) error {
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return errors.New("River event subscription closed")
			}
			if event.Job == nil || event.Job.ID != jobID {
				continue
			}
			if event.Kind != river.EventKindJobCompleted {
				return fmt.Errorf("job ended with event %q", event.Kind)
			}
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}
