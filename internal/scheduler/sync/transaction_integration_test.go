//go:build integration

package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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
	firstRepository, err := NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	secondRepository, err := NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := at("2026-01-01T12:00:00Z")
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
