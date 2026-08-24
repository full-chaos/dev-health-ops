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
			) (HandoffOutcome, error) {
				firstOccurrence = occurrence
				if _, err := transaction.Exec(
					handoffCtx,
					"INSERT INTO public.scheduler_handoffs (id) VALUES ($1)",
					occurrence.ID,
				); err != nil {
					return "", err
				}
				close(firstEntered)
				select {
				case <-releaseFirst:
					return OccurrenceMinted, nil
				case <-handoffCtx.Done():
					return "", handoffCtx.Err()
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
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			return "", fmt.Errorf("second replica reached locked occurrence")
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
		) (HandoffOutcome, error) {
			if _, err := transaction.Exec(
				handoffCtx,
				"INSERT INTO public.scheduler_handoffs (id) VALUES ($1)",
				occurrence.ID,
			); err != nil {
				return "", err
			}
			return "", coordinatorErr
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

	// CHAOS-3936 changed what replaying the same observed instant means, so
	// this block now asserts the property that actually needs protecting.
	//
	// Before: the base was last_sync_at, which this fixture never advances, so
	// every replay recomputed the ONE instant already minted and wrote nothing.
	// This block asserted that no-op by requiring the same occurrence ID back.
	// That is the freeze itself: the same behaviour that made a failed run pin
	// its schedule forever also made a replay look idempotent.
	//
	// Now the base is the occurrence ledger, so a replay mints the next instant
	// that is due and absent -- which is how a schedule recovers from a run
	// that never completed. The invariants that must survive, and that this
	// block proves against real PostgreSQL, are that catch-up ADVANCES (never
	// re-mints an instant) and that it TERMINATES (it stops at the first cron
	// instant in the future rather than running away at a fixed now).
	//
	// The ledger is empty here: every handoff above used a coordinator that
	// writes only public.scheduler_handoffs, and the last of them rolled back.
	coordinator := NewOccurrenceCoordinator()
	clearMarker := func() {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			UPDATE public.scheduled_jobs SET next_run_at = NULL
			WHERE id = '00000000-0000-4000-8000-000000003039'
		`); err != nil {
			t.Fatal(err)
		}
	}
	// Window 1: base is last_sync_at 10:00, so 11:00 is the first due instant.
	occurrences, err := firstRepository.HandoffDue(ctx, observedAt, 1, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(occurrences) != 1 || !occurrences[0].ScheduledFor.Equal(at("2026-01-01T11:00:00Z")) {
		t.Fatalf("first window occurrences = %#v, want one at 11:00", occurrences)
	}
	clearMarker()
	// Window 2, same observedAt: the ledger now holds 11:00, so 12:00 is due
	// and absent. Before the fix this returned 11:00 again and wrote nothing.
	retried, err := firstRepository.HandoffDue(ctx, observedAt, 1, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(retried) != 1 || !retried[0].ScheduledFor.Equal(at("2026-01-01T12:00:00Z")) {
		t.Fatalf("second window occurrences = %#v, want one at 12:00", retried)
	}
	if retried[0].ID == occurrences[0].ID {
		t.Fatal("catch-up re-minted the instant it had already minted")
	}
	clearMarker()
	// Window 3, same observedAt: the next instant is 13:00, which is in the
	// future, so catch-up stops. Without this the fix would mint forever at a
	// fixed now, which is the opposite failure to the one it repairs.
	settled, err := firstRepository.HandoffDue(ctx, observedAt, 1, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(settled) != 0 {
		t.Fatalf("catch-up did not terminate: third window occurrences = %#v", settled)
	}
	var occurrenceRows, distinctInstants int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT scheduled_for)
		FROM public.scheduled_sync_occurrences
	`).Scan(&occurrenceRows, &distinctInstants); err != nil {
		t.Fatal(err)
	}
	if occurrenceRows != 2 || distinctInstants != 2 {
		t.Fatalf("scheduled occurrence rows = %d over %d distinct instants, want 2 over 2",
			occurrenceRows, distinctInstants)
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
	) (HandoffOutcome, error) {
		if _, execErr := transaction.Exec(
			handoffCtx,
			"INSERT INTO public.scheduler_handoffs (id) VALUES ($1)",
			occurrence.ID,
		); execErr != nil {
			return "", execErr
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
		return "", crashed
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

// CHAOS-3936: last_sync_at advances only when a sync COMPLETES. This fixture
// never completes one, so before the fix every window recomputed the same
// 11:00 instant, the ON CONFLICT DO NOTHING insert wrote nothing, and the
// coordinator reported idempotent success -- forever, against real PostgreSQL.
// Each window must instead durably add one new instant.
func TestHandoffDuePostgresKeepsMintingWhileLastSyncStaysFrozen(t *testing.T) {
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

	want := []string{"11:00", "12:00", "13:00"}
	for index, observed := range []string{
		"2026-01-01T12:00:00Z", "2026-01-01T13:00:00Z", "2026-01-01T14:00:00Z",
	} {
		result, err := repository.HandoffDueResult(ctx, at(observed), 4, NewOccurrenceCoordinator())
		if err != nil {
			t.Fatalf("window %d HandoffDueResult() err = %v", index, err)
		}
		if result.Minted() != 1 || len(result.Repeated) != 0 {
			t.Fatalf("window %d minted=%d repeated=%d", index, result.Minted(), len(result.Repeated))
		}
		var frozen time.Time
		if err := pool.QueryRow(ctx, `
			SELECT last_sync_at FROM public.sync_configurations
			WHERE id = '00000000-0000-4000-8000-000000003038'
		`).Scan(&frozen); err != nil {
			t.Fatal(err)
		}
		if !frozen.Equal(at("2026-01-01T10:00:00Z")) {
			t.Fatalf("fixture no longer reproduces a run that never completes: last_sync_at = %s", frozen)
		}
		var minted []string
		rows, err := pool.Query(ctx, `
			SELECT to_char(scheduled_for AT TIME ZONE 'UTC', 'HH24:MI')
			FROM public.scheduled_sync_occurrences
			ORDER BY scheduled_for
		`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var instant string
			if err := rows.Scan(&instant); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			minted = append(minted, instant)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(minted) != fmt.Sprint(want[:index+1]) {
			t.Fatalf("after %d windows the occurrence ledger holds %v, want %v", index+1, minted, want[:index+1])
		}
	}
}

// CHAOS-3936 deploy safety: the ledger base reads occurrence rows written by
// the OLD code, so on first deploy a config frozen for many hours has a ledger
// far behind now. Catch-up must therefore be rate-limited by the persisted
// marker, not by the loop's poll interval -- the scheduler loop ticks every
// second by default, so if the marker did not gate re-entry a config that was
// frozen for a day would mint a day of occurrences in seconds.
//
// This asserts the gate against real PostgreSQL: after a window mints, a
// second window before the next cron instant produces nothing. The sibling
// test deliberately clears next_run_at between windows to demonstrate the walk;
// this one leaves the marker alone, which is what production does.
func TestHandoffDuePostgresRateLimitsCatchUpToTheScheduleMarker(t *testing.T) {
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
	coordinator := NewOccurrenceCoordinator()

	minted, err := repository.HandoffDue(ctx, at("2026-01-01T12:00:00Z"), 4, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(minted) != 1 {
		t.Fatalf("first window minted %d occurrences, want 1", len(minted))
	}
	// The marker now sits at 13:00. Every window before that instant must
	// produce nothing, however often the loop ticks.
	for _, observed := range []string{
		"2026-01-01T12:00:01Z", "2026-01-01T12:15:00Z", "2026-01-01T12:59:59Z",
	} {
		later, err := repository.HandoffDue(ctx, at(observed), 4, coordinator)
		if err != nil {
			t.Fatalf("window at %s: %v", observed, err)
		}
		if len(later) != 0 {
			t.Fatalf("window at %s minted %#v; the schedule marker did not rate-limit catch-up", observed, later)
		}
	}
	var occurrenceRows int
	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM public.scheduled_sync_occurrences",
	).Scan(&occurrenceRows); err != nil {
		t.Fatal(err)
	}
	if occurrenceRows != 1 {
		t.Fatalf("ledger holds %d rows after four windows in one cron interval, want 1", occurrenceRows)
	}
}

// The CHAOS-3936 signal -- occurrences_repeated_total, the idle-due-window
// warning, and the WARN naming the config and instant -- all rest on the real
// coordinator mapping PostgreSQL's ON CONFLICT DO NOTHING onto
// OccurrenceRepeated. That mapping was covered only against a fake pgx row, so
// if it were wrong the metric would simply never fire and an outage would be
// invisible again: a measurement layer that fails toward "fine".
//
// It is driven directly here rather than through a scheduler window on
// purpose. With the base derived from the occurrence ledger, a window can no
// longer compute an instant that already exists -- the ledger it just read
// contains it, so the next window computes the NEXT instant. That makes
// OccurrenceRepeated a regression alarm rather than a routine counter: it
// fires if the base ever stops advancing again. Testing it through a window
// would therefore be testing a path the fix deliberately closed; testing the
// coordinator directly tests the mapping the signal actually depends on.
func TestOccurrenceCoordinatorPostgresReportsOnConflictAsRepeated(t *testing.T) {
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

	occurrence := newOccurrence(
		"00000000-0000-4000-8000-000000003038",
		"org-integration",
		"00000000-0000-4000-8000-000000003039",
		at("2026-01-01T11:00:00Z"),
		at("2026-01-01T12:00:00Z"),
		at("2026-01-01T13:00:00Z"),
	)
	coordinator := NewOccurrenceCoordinator()
	handoff := func() (HandoffOutcome, error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return "", err
		}
		outcome, handoffErr := coordinator.Handoff(ctx, postgresSchedulerTransaction{Tx: tx}, occurrence)
		if handoffErr != nil {
			_ = tx.Rollback(ctx)
			return outcome, handoffErr
		}
		return outcome, tx.Commit(ctx)
	}

	first, err := handoff()
	if err != nil {
		t.Fatal(err)
	}
	if first != OccurrenceMinted {
		t.Fatalf("first handoff outcome = %q, want %q", first, OccurrenceMinted)
	}
	// Identical envelope, real unique constraint, real ON CONFLICT DO NOTHING.
	second, err := handoff()
	if err != nil {
		t.Fatal(err)
	}
	if second != OccurrenceRepeated {
		t.Fatalf("second handoff outcome = %q, want %q -- the repeat signal cannot fire", second, OccurrenceRepeated)
	}
	var rows int
	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM public.scheduled_sync_occurrences",
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("ledger holds %d rows after two identical handoffs, want 1", rows)
	}
}

func createSchedulerIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		`CREATE TABLE public.sync_configurations (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			is_active boolean NOT NULL,
			-- CHAOS-4174: defaults TRUE here (unlike prod migration 0018's
			-- server_default FALSE) so the many existing tests in this file that
			-- insert a config without naming the column keep exercising the
			-- minting path they were written for. Tests of the new refusal gate
			-- insert planner_managed explicitly.
			planner_managed boolean NOT NULL DEFAULT TRUE,
			sync_targets jsonb NOT NULL DEFAULT '[]'::jsonb,
			sync_options jsonb NOT NULL,
			last_sync_at timestamptz,
			created_at timestamptz NOT NULL
		)`,
		// The Coordinator's pre-mint organization guard reads this table. This
		// fixture's org_id ("org-integration") is deliberately NOT a UUID, and
		// the guard admits a non-UUID org without any lookup because Python
		// does (workers/org_guard.py:18-20) -- so these tests keep exercising
		// the minting path they were written for. The table is created anyway
		// so the guard has somewhere to look if that org_id ever becomes a UUID.
		`CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text)`,
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

// TestHandoffDuePostgresRefusesNotPlannerManagedConfig is the CHAOS-4174
// red-first proof. Before this change a fixture-style config
// (planner_managed=false) with a due schedule dispatched exactly like a real
// config: minted an occurrence and advanced its marker. chris ruled
// (2026-08-23): "That column is useless past something being a fixture
// trigger to not use. Fixtures will never be able to be run on a schedule."
//
// This asserts the OUTCOME, not the guard mechanics: a planner_managed=false
// config with an otherwise-due schedule never mints a run, its marker stays
// due (so it stays eligible for whenever it becomes planner-managed), and the
// refusal is counted -- while a planner_managed=true config in the SAME
// window still mints, proving the gate does not regress ordinary scheduling.
func TestHandoffDuePostgresRefusesNotPlannerManagedConfig(t *testing.T) {
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
	// createSchedulerIntegrationFixture seeds one planner_managed=true config
	// (the DDL's default) -- config 00000000-0000-4000-8000-000000003038 /
	// job 00000000-0000-4000-8000-000000003039, due at observedAt below. That
	// config is this test's regression proof: it must still mint.
	if err := createSchedulerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}

	const (
		fixtureConfigID = "00000000-0000-4000-8000-00000000f001"
		fixtureJobID    = "00000000-0000-4000-8000-00000000f002"
	)
	for _, statement := range []string{
		`INSERT INTO public.sync_configurations (
			id, org_id, is_active, planner_managed, sync_options, last_sync_at, created_at
		) VALUES (
			'` + fixtureConfigID + `', 'org-integration', TRUE, FALSE,
			'{"schedule_cron":"0 * * * *","timezone":"UTC"}'::jsonb,
			'2026-01-01T02:00:00-08:00', '2026-01-01T01:00:00-08:00'
		)`,
		`INSERT INTO public.scheduled_jobs (
			id, org_id, sync_config_id, job_type, schedule_cron, timezone,
			status, is_running, updated_at
		) VALUES (
			'` + fixtureJobID + `', 'org-integration', '` + fixtureConfigID + `',
			'sync', '0 * * * *', 'UTC', 0, FALSE, '2026-01-01T01:00:00-08:00'
		)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	observedAt := at("2026-01-01T12:00:00Z")
	result, err := repository.HandoffDueResult(ctx, observedAt, 4, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatalf("HandoffDueResult() error = %v", err)
	}

	if result.SkippedNotPlannerManaged != 1 {
		t.Errorf("SkippedNotPlannerManaged = %d, want 1", result.SkippedNotPlannerManaged)
	}
	for _, occurrence := range result.HandedOff {
		if occurrence.ConfigID == fixtureConfigID {
			t.Fatalf("planner_managed=false config %s minted an occurrence", fixtureConfigID)
		}
	}
	if result.Minted() != 1 {
		t.Errorf("Minted() = %d, want 1 (only the planner_managed=true config)", result.Minted())
	}

	var fixtureNextRunAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT next_run_at FROM public.scheduled_jobs WHERE id = $1`, fixtureJobID,
	).Scan(&fixtureNextRunAt); err != nil {
		t.Fatal(err)
	}
	if fixtureNextRunAt != nil {
		t.Errorf("planner_managed=false config's marker advanced to %v; a refused schedule must stay due", fixtureNextRunAt)
	}
	var fixtureOccurrences int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.scheduled_sync_occurrences WHERE sync_config_id = $1`, fixtureConfigID,
	).Scan(&fixtureOccurrences); err != nil {
		t.Fatal(err)
	}
	if fixtureOccurrences != 0 {
		t.Errorf("planner_managed=false config has %d occurrence rows, want 0", fixtureOccurrences)
	}

	var realNextRunAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT next_run_at FROM public.scheduled_jobs WHERE id = '00000000-0000-4000-8000-000000003039'`,
	).Scan(&realNextRunAt); err != nil {
		t.Fatal(err)
	}
	if realNextRunAt == nil {
		t.Error("planner_managed=true config's marker did not advance; the gate must not regress ordinary scheduling")
	}
}

// TestScheduledCandidatesDeprioritizeRefusedRowsUnderLimit is a codex-review
// finding (validated here before being accepted): a planner_managed=false
// config never advances its own ordering key once refused -- evaluateContext
// refuses it before the marker write, so next_run_at/last_sync_at stay
// exactly as overdue as they were. Without deprioritizing it in
// schedulerHandoffCandidatesSQL's ORDER BY, the most-overdue fixture-style
// config would occupy the SAME bounded window slot on every single poll
// forever, permanently starving a real planner_managed=true config that is
// also due but ranks after it.
//
// This seeds a planner_managed=false config that is MORE overdue than the
// createSchedulerIntegrationFixture config (planner_managed=true, the
// default), then runs three consecutive windows with limit=1 -- the tightest
// possible bound. If the refused row were not deprioritized, all three
// windows would spend their one slot on it and the real config would never
// mint. With the fix, the real config must win the very first window despite
// being less overdue by wall-clock time.
func TestScheduledCandidatesDeprioritizeRefusedRowsUnderLimit(t *testing.T) {
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
	// createSchedulerIntegrationFixture's config (...3038 / job ...3039) is
	// planner_managed=true (the DDL default) with last_sync_at
	// 2026-01-01T10:00:00Z. This is the "real" config that must not starve.
	if err := createSchedulerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}

	const (
		staleConfigID = "00000000-0000-4000-8000-00000000e101"
		staleJobID    = "00000000-0000-4000-8000-00000000e102"
	)
	for _, statement := range []string{
		// last_sync_at is a full 10 hours older than the real config's, so it
		// ranks first in schedulerHandoffCandidatesSQL's ORDER BY on plain
		// due-ness -- exactly the condition that starves without the fix.
		`INSERT INTO public.sync_configurations (
			id, org_id, is_active, planner_managed, sync_options, last_sync_at, created_at
		) VALUES (
			'` + staleConfigID + `', 'org-integration', TRUE, FALSE,
			'{"schedule_cron":"0 * * * *","timezone":"UTC"}'::jsonb,
			'2026-01-01T00:00:00Z', '2025-12-31T00:00:00Z'
		)`,
		`INSERT INTO public.scheduled_jobs (
			id, org_id, sync_config_id, job_type, schedule_cron, timezone,
			status, is_running, updated_at
		) VALUES (
			'` + staleJobID + `', 'org-integration', '` + staleConfigID + `',
			'sync', '0 * * * *', 'UTC', 0, FALSE, '2025-12-31T00:00:00Z'
		)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	observedAt := at("2026-01-01T12:00:00Z")

	first, err := repository.HandoffDueResult(ctx, observedAt, 1, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatalf("first window: HandoffDueResult() error = %v", err)
	}
	if first.Minted() != 1 || len(first.HandedOff) != 1 || first.HandedOff[0].ConfigID != "00000000-0000-4000-8000-000000003038" {
		t.Fatalf("first window did not prioritize the real config despite being "+
			"less overdue: Minted=%d HandedOff=%v (want the real config minted "+
			"first, ahead of the more-overdue but refused config)",
			first.Minted(), first.HandedOff)
	}
	if first.SkippedNotPlannerManaged != 0 {
		t.Errorf("first window SkippedNotPlannerManaged = %d, want 0: with only "+
			"one slot it must go to the real config, not spend the window "+
			"reading and refusing the stale one", first.SkippedNotPlannerManaged)
	}

	// The second window's one slot is free to reach the refused config now
	// that the real one no longer occupies the front of the order.
	second, err := repository.HandoffDueResult(ctx, observedAt, 1, NewOccurrenceCoordinator())
	if err != nil {
		t.Fatalf("second window: HandoffDueResult() error = %v", err)
	}
	if second.SkippedNotPlannerManaged != 1 {
		t.Errorf("second window SkippedNotPlannerManaged = %d, want 1", second.SkippedNotPlannerManaged)
	}
}
