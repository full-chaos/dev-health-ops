//go:build integration

package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHandoffDuePostgresSkipsReplicaLockedOccurrence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createSchedulerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	defaultRepository, err := NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := at("2026-01-01T12:00:00Z")
	if _, err := defaultRepository.HandoffDue(ctx, observedAt, 1, NewOccurrenceCoordinator()); !errors.Is(err, ErrSchedulerMutationDisabled) {
		t.Fatalf("default ownership HandoffDue() error = %v", err)
	}
	var defaultOccurrences int
	var defaultNextRunAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM public.scheduled_sync_occurrences),
			next_run_at
		FROM public.scheduled_jobs
		WHERE id = '00000000-0000-4000-8000-000000003039'
	`).Scan(&defaultOccurrences, &defaultNextRunAt); err != nil {
		t.Fatal(err)
	}
	if defaultOccurrences != 0 || defaultNextRunAt != nil {
		t.Fatalf("default ownership mutated occurrences=%d nextRunAt=%v", defaultOccurrences, defaultNextRunAt)
	}
	ownership := reviewedGoMutationOwnershipPolicy()
	firstRepository, err := newRepositoryWithOwnership(pool, ownership)
	if err != nil {
		t.Fatal(err)
	}
	secondRepository, err := newRepositoryWithOwnership(pool, ownership)
	if err != nil {
		t.Fatal(err)
	}
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstResult := make(chan error, 1)
	var firstOccurrence Occurrence

	go func() {
		occurrences, handoffErr := firstRepository.HandoffDue(
			ctx,
			observedAt,
			1,
			CoordinatorFunc(func(
				handoffCtx context.Context,
				transaction HandoffTransaction,
				occurrence Occurrence,
			) error {
				firstOccurrence = occurrence
				if _, err := transaction.Exec(
					handoffCtx,
					"INSERT INTO public.scheduler_handoffs (id) VALUES ($1)",
					occurrence.ID,
				); err != nil {
					return err
				}
				close(firstEntered)
				select {
				case <-releaseFirst:
					return nil
				case <-handoffCtx.Done():
					return handoffCtx.Err()
				}
			}),
		)
		if handoffErr == nil && len(occurrences) != 1 {
			handoffErr = fmt.Errorf("first replica occurrences = %d, want 1", len(occurrences))
		}
		firstResult <- handoffErr
	}()

	select {
	case <-firstEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	secondOccurrences, err := secondRepository.HandoffDue(
		ctx,
		observedAt,
		1,
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) error {
			return fmt.Errorf("second replica reached locked occurrence")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondOccurrences) != 0 {
		t.Fatalf("second replica occurrences = %d, want 0", len(secondOccurrences))
	}
	close(releaseFirst)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	if firstOccurrence.ConfigID != "00000000-0000-4000-8000-000000003038" ||
		firstOccurrence.OrgID != "org-integration" ||
		firstOccurrence.JobID != "00000000-0000-4000-8000-000000003039" ||
		!firstOccurrence.ScheduledFor.Equal(at("2026-01-01T11:00:00Z")) ||
		!firstOccurrence.NextRunAt.Equal(at("2026-01-01T13:00:00Z")) {
		t.Fatalf("decoded occurrence = %#v", firstOccurrence)
	}

	var handoffs int
	var nextRunAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM public.scheduler_handoffs),
			next_run_at
		FROM public.scheduled_jobs
		WHERE id = '00000000-0000-4000-8000-000000003039'
	`).Scan(&handoffs, &nextRunAt); err != nil {
		t.Fatal(err)
	}
	if handoffs != 1 || !nextRunAt.Equal(at("2026-01-01T13:00:00Z")) {
		t.Fatalf("handoffs=%d nextRunAt=%s", handoffs, nextRunAt)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE public.scheduled_jobs SET next_run_at = NULL
		WHERE id = '00000000-0000-4000-8000-000000003039'
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM public.scheduler_handoffs"); err != nil {
		t.Fatal(err)
	}
	coordinatorErr := errors.New("coordinator authorization failed")
	if _, err := firstRepository.HandoffDue(
		ctx,
		observedAt,
		1,
		CoordinatorFunc(func(
			handoffCtx context.Context,
			transaction HandoffTransaction,
			occurrence Occurrence,
		) error {
			if _, err := transaction.Exec(
				handoffCtx,
				"INSERT INTO public.scheduler_handoffs (id) VALUES ($1)",
				occurrence.ID,
			); err != nil {
				return err
			}
			return coordinatorErr
		}),
	); !errors.Is(err, coordinatorErr) {
		t.Fatalf("failed coordinator error = %v", err)
	}
	var rolledBackNextRunAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM public.scheduler_handoffs),
			next_run_at
		FROM public.scheduled_jobs
		WHERE id = '00000000-0000-4000-8000-000000003039'
	`).Scan(&handoffs, &rolledBackNextRunAt); err != nil {
		t.Fatal(err)
	}
	if handoffs != 0 || rolledBackNextRunAt != nil {
		t.Fatalf("rollback left handoffs=%d nextRunAt=%v", handoffs, rolledBackNextRunAt)
	}

	coordinator := NewOccurrenceCoordinator()
	occurrences, err := firstRepository.HandoffDue(ctx, observedAt, 1, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(occurrences) != 1 {
		t.Fatalf("occurrence handoffs = %d, want 1", len(occurrences))
	}
	if _, err := pool.Exec(ctx, `
		UPDATE public.scheduled_jobs SET next_run_at = NULL
		WHERE id = '00000000-0000-4000-8000-000000003039'
	`); err != nil {
		t.Fatal(err)
	}
	retried, err := firstRepository.HandoffDue(ctx, observedAt, 1, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(retried) != 1 || retried[0].ID != occurrences[0].ID {
		t.Fatalf("retried occurrences = %#v, want id %s", retried, occurrences[0].ID)
	}
	var occurrenceRows int
	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM public.scheduled_sync_occurrences",
	).Scan(&occurrenceRows); err != nil {
		t.Fatal(err)
	}
	if occurrenceRows != 1 {
		t.Fatalf("scheduled occurrence rows = %d, want 1", occurrenceRows)
	}
}

func TestHandoffDuePostgresUnsupportedCronFallsBackWithoutMarkerWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createSchedulerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE public.sync_configurations SET sync_options =
		'{"schedule_cron":"R * * * *","timezone":"UTC"}'::jsonb;
		UPDATE public.scheduled_jobs SET schedule_cron = 'R * * * *'
	`); err != nil {
		t.Fatal(err)
	}
	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.HandoffDueResult(ctx, at("2026-01-01T12:00:00Z"), 1, NewOccurrenceCoordinator())
	if !errors.Is(err, ErrSchedulerFallbackRequired) {
		t.Fatalf("HandoffDueResult() error = %v", err)
	}
	if result.UnsupportedCron != 1 || len(result.HandedOff) != 0 {
		t.Fatalf("unsupported result = %#v", result)
	}
	var occurrences int
	var nextRunAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM public.scheduled_sync_occurrences), next_run_at
		FROM public.scheduled_jobs
		WHERE id = '00000000-0000-4000-8000-000000003039'
	`).Scan(&occurrences, &nextRunAt); err != nil {
		t.Fatal(err)
	}
	if occurrences != 0 || nextRunAt != nil {
		t.Fatalf("unsupported fallback mutated occurrences=%d nextRunAt=%v", occurrences, nextRunAt)
	}
}

// TestHandoffDuePostgresRespectsExternalRowLock is the CHAOS-3128 two-owners-
// impossible proof. The "other owner" here is a raw PostgreSQL connection,
// not another Go repository: it takes exactly the lock clause and lock order
// (sync_configurations, then scheduled_jobs, both `FOR UPDATE`) that
// sync_scheduler.py's `_maybe_dispatch_config` takes via SQLAlchemy's
// `.with_for_update(skip_locked=True)`, so it stands in for a live Celery
// Beat transaction without any Go-side cooperation. The property under test
// is enforced by PostgreSQL's row-lock protocol, not by application code on
// either side agreeing to be polite — see TransferScheduleMarkerOwnershipToGo
// and HandoffDueResult's doc comments for the argument this proves.
func TestHandoffDuePostgresRespectsExternalRowLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createSchedulerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}

	// A second, independent connection plays Celery Beat: same lock order,
	// same clause shape, held open across the window Go tries to handoff in.
	beatConn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = beatConn.Close(context.Background()) }()
	beatTx, err := beatConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = beatTx.Rollback(context.Background()) }()
	if _, err := beatTx.Exec(ctx,
		"SELECT 1 FROM public.sync_configurations WHERE id = $1 FOR UPDATE",
		"00000000-0000-4000-8000-000000003038",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := beatTx.Exec(ctx,
		"SELECT 1 FROM public.scheduled_jobs WHERE id = $1 FOR UPDATE",
		"00000000-0000-4000-8000-000000003039",
	); err != nil {
		t.Fatal(err)
	}

	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	observedAt := at("2026-01-01T12:00:00Z")
	occurrences, err := repository.HandoffDue(ctx, observedAt, 1, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	if len(occurrences) != 0 {
		t.Fatalf(
			"Go handed off %d occurrences while an external transaction held the row lock",
			len(occurrences),
		)
	}

	// The simulated Beat transaction now "wins": it advances the marker and
	// commits, exactly as the real Python scheduler would inside the same
	// locked transaction.
	if _, err := beatTx.Exec(ctx,
		"UPDATE public.scheduled_jobs SET next_run_at = $1, updated_at = $2 WHERE id = $3",
		at("2026-01-01T13:00:00Z"), observedAt, "00000000-0000-4000-8000-000000003039",
	); err != nil {
		t.Fatal(err)
	}
	if err := beatTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Go's next poll must observe the committed marker and correctly treat
	// the schedule as not due, rather than acting on a stale read.
	second, err := repository.HandoffDue(ctx, observedAt, 1, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("Go re-handled a schedule Beat had already advanced: %d occurrences", len(second))
	}
	var occurrenceRows int
	if err := pool.QueryRow(
		ctx, "SELECT count(*) FROM public.scheduled_sync_occurrences",
	).Scan(&occurrenceRows); err != nil {
		t.Fatal(err)
	}
	if occurrenceRows != 0 {
		t.Fatalf(
			"occurrence rows = %d, want 0: Beat's dispatch owns this occurrence, not Go's ledger",
			occurrenceRows,
		)
	}
}

// TestHandoffDuePostgresSurvivesTransactionCrashWithoutPartialWrite is the
// transaction-crash gate. It severs the underlying connection mid-handoff —
// after the coordinator has durably written the occurrence but before the
// marker advance or commit — without ever calling Commit or Rollback, so the
// property under test is PostgreSQL's own transaction-abort-on-disconnect
// behavior, not this package's deferred Rollback call.
func TestHandoffDuePostgresSurvivesTransactionCrashWithoutPartialWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createSchedulerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}

	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	observedAt := at("2026-01-01T12:00:00Z")

	crashed := errors.New("simulated process crash")
	_, err = repository.HandoffDue(ctx, observedAt, 1, CoordinatorFunc(func(
		handoffCtx context.Context,
		transaction HandoffTransaction,
		occurrence Occurrence,
	) error {
		if _, execErr := transaction.Exec(
			handoffCtx,
			"INSERT INTO public.scheduler_handoffs (id) VALUES ($1)",
			occurrence.ID,
		); execErr != nil {
			return execErr
		}
		postgresTransaction, ok := transaction.(postgresSchedulerTransaction)
		if !ok {
			t.Fatal("unexpected transaction type")
		}
		// Kill the connection outright, as a crashed process would, instead
		// of calling Rollback. The repository's deferred Rollback still runs
		// on the caller side and will fail or no-op against a dead
		// connection either way -- that is not what leaves no partial write
		// behind. PostgreSQL aborting the transaction server-side on
		// disconnect is.
		if closeErr := postgresTransaction.Tx.Conn().Close(context.Background()); closeErr != nil {
			t.Logf("closing simulated-crash connection: %v", closeErr)
		}
		return crashed
	}))
	if err == nil {
		t.Fatal("HandoffDue() succeeded despite a crashed connection")
	}

	var handoffs int
	var nextRunAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM public.scheduler_handoffs),
			next_run_at
		FROM public.scheduled_jobs
		WHERE id = '00000000-0000-4000-8000-000000003039'
	`).Scan(&handoffs, &nextRunAt); err != nil {
		t.Fatal(err)
	}
	if handoffs != 0 || nextRunAt != nil {
		t.Fatalf("crash left a partial write: handoffs=%d nextRunAt=%v", handoffs, nextRunAt)
	}
	var occurrenceRows int
	if err := pool.QueryRow(
		ctx, "SELECT count(*) FROM public.scheduled_sync_occurrences",
	).Scan(&occurrenceRows); err != nil {
		t.Fatal(err)
	}
	if occurrenceRows != 0 {
		t.Fatalf("crash left %d occurrence rows behind", occurrenceRows)
	}

	// The pool must not be poisoned by one crashed connection: a fresh
	// transaction has to succeed normally afterward.
	occurrences, err := repository.HandoffDue(ctx, observedAt, 1, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	if len(occurrences) != 1 {
		t.Fatalf("post-crash HandoffDue() occurrences = %d, want 1", len(occurrences))
	}
}

func createSchedulerIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		`CREATE TABLE public.sync_configurations (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			is_active boolean NOT NULL,
			sync_options jsonb NOT NULL,
			last_sync_at timestamptz,
			created_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.scheduled_jobs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_config_id uuid NOT NULL,
			job_type text NOT NULL,
			schedule_cron text NOT NULL,
			timezone text NOT NULL,
			status integer NOT NULL,
			is_running boolean NOT NULL,
			last_run_at timestamptz,
			updated_at timestamptz,
			next_run_at timestamptz
		)`,
		`CREATE TABLE public.scheduler_handoffs (id text PRIMARY KEY)`,
		`CREATE TABLE public.scheduled_sync_occurrences (
			occurrence_id text PRIMARY KEY,
			identity_version text NOT NULL,
			org_id text NOT NULL,
			sync_config_id uuid NOT NULL,
			scheduled_job_id uuid NOT NULL,
			scheduled_for timestamptz NOT NULL,
			job_run_id uuid,
			sync_run_id uuid,
			created_at timestamptz NOT NULL,
			UNIQUE (sync_config_id, scheduled_for)
		)`,
		`INSERT INTO public.sync_configurations (
			id, org_id, is_active, sync_options, last_sync_at, created_at
		) VALUES (
			'00000000-0000-4000-8000-000000003038',
			'org-integration',
			TRUE,
			'{"schedule_cron":"0 * * * *","timezone":"UTC"}'::jsonb,
			'2026-01-01T02:00:00-08:00',
			'2026-01-01T01:00:00-08:00'
		)`,
		`INSERT INTO public.scheduled_jobs (
			id, org_id, sync_config_id, job_type, schedule_cron, timezone,
			status, is_running, updated_at
		) VALUES (
			'00000000-0000-4000-8000-000000003039',
			'org-integration',
			'00000000-0000-4000-8000-000000003038',
			'sync',
			'0 * * * *',
			'UTC',
			0,
			FALSE,
			'2026-01-01T01:00:00-08:00'
		)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
