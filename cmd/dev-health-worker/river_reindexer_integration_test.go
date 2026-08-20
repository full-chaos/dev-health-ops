//go:build integration

package main

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	reindexDomainRole     = "reindex_domain_runtime"
	reindexQueueRole      = "reindex_queue_runtime"
	reindexRolePassword   = "reindex_test_password"
	reindexObservedWindow = 90 * time.Second
)

// TestRiverWorkerClientRunsReindexerWithoutPermissionErrors is the CHAOS-3939
// regression. It asserts the EFFECT the shipped River client configuration has
// against a real least-privilege runtime role -- the reindexer runs and
// rebuilds nothing -- and it proves the harness can see the bug by running the
// same client with River's default index list, which must produce the
// permission-denied ERROR lines production reported every midnight.
func TestRiverWorkerClientRunsReindexerWithoutPermissionErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })

	admin, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	for _, statement := range []string{
		"CREATE SCHEMA river",
		"CREATE ROLE " + reindexDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE " +
			"NOREPLICATION NOBYPASSRLS PASSWORD '" + reindexRolePassword + "'",
		"CREATE ROLE " + reindexQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE " +
			"NOREPLICATION NOBYPASSRLS PASSWORD '" + reindexRolePassword + "'",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: reindexDomainRole, QueueRole: reindexQueueRole,
	}); err != nil {
		t.Fatal(err)
	}

	queuePool, err := pgxpool.New(ctx, reindexRoleURI(t, postgres.URI, reindexQueueRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queuePool.Close)

	// Premise check. If the runtime role could reindex, the bug this test
	// guards would not exist and every assertion below would be vacuous.
	_, reindexErr := queuePool.Exec(ctx, "REINDEX INDEX river.river_job_kind")
	if reindexErr == nil {
		t.Fatal("runtime queue role could REINDEX river_job_kind; the least-privilege premise no longer holds")
	}
	var pgErr *pgconn.PgError
	if !errors.As(reindexErr, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("REINDEX as the runtime role failed for the wrong reason: %v", reindexErr)
	}

	// Control: River's default index list, everything else exactly as shipped.
	// This must reproduce the production failure, or a passing shipped case
	// below would prove nothing.
	controlErrors, controlRuns := runReindexerOnce(t, ctx, queuePool, func(clientConfig *river.Config) {
		clientConfig.ReindexerIndexNames = river.ReindexerIndexNamesDefault()
	})
	if len(controlErrors) == 0 {
		t.Fatalf("control run reindexed cleanly with River's default index list; "+
			"the harness cannot observe the CHAOS-3939 failure (runs=%d)", controlRuns)
	}
	if !strings.Contains(strings.Join(controlErrors, "\n"), "permission denied") {
		t.Fatalf("control run failed for the wrong reason: %v", controlErrors)
	}

	// Shipped configuration: the reindexer still runs on schedule, initiates
	// zero rebuilds, and logs no error.
	shippedErrors, shippedRuns := runReindexerOnce(t, ctx, queuePool, nil)
	if shippedRuns == 0 {
		t.Fatal("shipped configuration never ran the reindexer; the assertion below would be vacuous")
	}
	if len(shippedErrors) != 0 {
		t.Fatalf("shipped configuration produced reindex errors: %v", shippedErrors)
	}
}

// runReindexerOnce starts the shipped worker client configuration, waits for at
// least one reindexer run, and returns the reindex error messages it logged and
// the number of completed runs it observed.
//
// mutate is nil for the shipped case: riverWorkerClientConfig's output is
// passed through untouched, so deleting the ReindexerIndexNames field from it
// fails this test. Only the control run mutates the configuration, and only to
// restore River's default index list. The schedule override is applied to both
// -- the shipped default fires at midnight UTC, which no test can wait for.
func runReindexerOnce(
	t *testing.T,
	ctx context.Context,
	queuePool *pgxpool.Pool,
	mutate func(*river.Config),
) ([]string, int) {
	t.Helper()
	recorder := &reindexLogRecorder{}
	logger := slog.New(slog.NewTextHandler(recorder, &slog.HandlerOptions{Level: slog.LevelDebug}))

	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &reindexProbeWorker{}); err != nil {
		t.Fatal(err)
	}
	clientConfig := riverWorkerClientConfig(
		config.Config{WorkerInstanceID: uuid.NewString(), RiverDatabaseSchema: "river"},
		map[string]river.QueueConfig{"heartbeat": {MaxWorkers: 1}},
		workers,
		logger,
	)
	if mutate != nil {
		mutate(clientConfig)
	}
	clientConfig.ReindexerSchedule = fixedIntervalSchedule{interval: time.Second}

	client, err := river.NewClient(riverpgxv5.New(queuePool), clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	}()

	deadline := time.Now().Add(reindexObservedWindow)
	for time.Now().Before(deadline) {
		if errorMessages, runs := recorder.observed(); runs > 0 || len(errorMessages) > 0 {
			// One further beat so a run that is mid-flight finishes writing
			// its per-index error lines before they are read.
			time.Sleep(2 * time.Second)
			return recorder.observed()
		}
		time.Sleep(250 * time.Millisecond)
	}
	errorMessages, runs := recorder.observed()
	t.Fatalf("reindexer never completed a run within %s (errors=%v)", reindexObservedWindow, errorMessages)
	return errorMessages, runs
}

// reindexLogRecorder collects the two reindexer signals this test asserts on:
// the per-index error line and the end-of-run line.
type reindexLogRecorder struct {
	mu            sync.Mutex
	errorMessages []string
	runs          int
}

func (recorder *reindexLogRecorder) Write(line []byte) (int, error) {
	text := string(line)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	switch {
	case strings.Contains(text, "Reindexer: Error reindexing"):
		recorder.errorMessages = append(recorder.errorMessages, strings.TrimSpace(text))
	case strings.Contains(text, "Reindexer: Ran successfully"):
		recorder.runs++
	}
	return len(line), nil
}

func (recorder *reindexLogRecorder) observed() ([]string, int) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.errorMessages...), recorder.runs
}

// reindexRoleURI rewrites the container URI to connect as one of the
// least-privilege runtime roles created above, leaving the database untouched.
func reindexRoleURI(t *testing.T, rawURI, role string) string {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, reindexRolePassword)
	return parsed.String()
}

type fixedIntervalSchedule struct{ interval time.Duration }

func (schedule fixedIntervalSchedule) Next(from time.Time) time.Time {
	return from.Add(schedule.interval)
}

// reindexProbeArgs exists only so the client has a worker and therefore starts
// its maintenance services; no job of this kind is ever inserted.
type reindexProbeArgs struct{}

func (reindexProbeArgs) Kind() string { return "reindex.probe" }

type reindexProbeWorker struct {
	river.WorkerDefaults[reindexProbeArgs]
}

func (*reindexProbeWorker) Work(context.Context, *river.Job[reindexProbeArgs]) error { return nil }
