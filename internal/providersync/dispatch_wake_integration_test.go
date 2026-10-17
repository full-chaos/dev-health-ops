//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func withDispatchWakeFixture(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	fn(ctx, pool, repository)
}

// seedRunUnit adds one more unit of the fixture run in the given status.
func seedRunUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string) string {
	t.Helper()
	unitID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
	id, org_id, sync_run_id, integration_id, source_id, provider,
	dataset_key, cost_class, mode, since_at, before_at, status,
	processor_flags, updated_at
) VALUES (
	$1, 'org-acme', $2, $3, $4, 'github', 'commits', 'medium',
	'incremental', '2026-07-22T12:00:00Z', '2026-07-23T12:00:00Z',
	$5, '{"sync_git":true,"sync_commits":true}', NOW()
)`, unitID, firstRunID, firstIntegrationID, firstSourceID, status); err != nil {
		t.Fatal(err)
	}
	return unitID
}

type dispatchWakeRow struct {
	rows        int
	status      string
	availableAt time.Time
}

func readDispatchWake(t *testing.T, ctx context.Context, pool *pgxpool.Pool) dispatchWakeRow {
	t.Helper()
	var row dispatchWakeRow
	if err := pool.QueryRow(ctx, `
SELECT count(*), COALESCE(min(status), ''), COALESCE(min(available_at), 'epoch'::timestamptz)
FROM public.sync_dispatch_outbox
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`, firstRunID).Scan(&row.rows, &row.status, &row.availableAt); err != nil {
		t.Fatal(err)
	}
	return row
}

// markDispatchWakeDelivered stands in for the relay: it publishes the row and
// leaves it 'dispatched' with the instant it was armed for, the state a pass's
// own delivery is in while it runs.
func markDispatchWakeDelivered(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET status = 'dispatched', dispatched_at = available_at,
    dispatched_transport = 'river', dispatched_route_generation = 1
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`, firstRunID); err != nil {
		t.Fatal(err)
	}
}

func claimFixtureUnit(t *testing.T, ctx context.Context, repository *PostgresRepository, unitID string, now time.Time) Claim {
	t.Helper()
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

// TestUnitTerminalCommitWakesDispatchForItsRun pins CHAOS-10013 fix (A) at the
// unit commit: a unit leaving its concurrency slot -- by success or by failure
// -- arms its run's dispatch_sync_run wakeup at the completion instant, while a
// unit of the run is still waiting for a slot.
//
// RED CONTROL: remove the wakeDispatch call from Complete (or failTx) and the
// matching half fails with rows=0 (or the row stays 'dispatched'): the unit
// commit armed only finalize_sync_run, which is the 2-units-per-countdown
// pacing measured on prod.
func TestUnitTerminalCommitWakesDispatchForItsRun(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		seedRunUnit(t, ctx, pool, "planned")
		failingUnitID := seedRunUnit(t, ctx, pool, "dispatching")
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)

		armedBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeArmed)
		completedAt := now.Add(2 * time.Second)
		if err := repository.Complete(ctx, claimFixtureUnit(t, ctx, repository, firstUnitID, now),
			map[string]any{"records": 1}, nil, now, completedAt); err != nil {
			t.Fatal(err)
		}
		got := readDispatchWake(t, ctx, pool)
		if got.rows != 1 || got.status != "pending" || !got.availableAt.Equal(completedAt) {
			t.Fatalf("after Complete: dispatch wake rows=%d status=%q available_at=%s, want 1 pending at %s",
				got.rows, got.status, got.availableAt, completedAt)
		}
		if armed := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeArmed); armed != armedBefore+1 {
			t.Fatalf("armed wake counter moved %d, want 1", armed-armedBefore)
		}

		markDispatchWakeDelivered(t, ctx, pool)
		failedAt := now.Add(5 * time.Second)
		if err := repository.Fail(ctx, claimFixtureUnit(t, ctx, repository, failingUnitID, now),
			"provider_error", now, failedAt); err != nil {
			t.Fatal(err)
		}
		got = readDispatchWake(t, ctx, pool)
		if got.rows != 1 || got.status != "pending" {
			t.Fatalf("after Fail: dispatch wake rows=%d status=%q, want the delivered row re-armed to pending", got.rows, got.status)
		}
	})
}

// TestUnitTerminalCommitDoesNotWakeDispatchWhenNothingWaits is the guard's own
// pin: with no unit of the run 'planned' or 'retrying' there is nothing a pass
// could claim, so the commit arms no dispatch row (finalize closes the run).
//
// RED CONTROL: drop the EXISTS clause of syncrunrollup.WakeDispatchSQL and this
// fails with rows=1.
func TestUnitTerminalCommitDoesNotWakeDispatchWhenNothingWaits(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		seedRunUnit(t, ctx, pool, "success")
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
		notWaitingBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeNotWaiting)
		if err := repository.Complete(ctx, claimFixtureUnit(t, ctx, repository, firstUnitID, now),
			map[string]any{"records": 1}, nil, now, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if got := readDispatchWake(t, ctx, pool); got.rows != 0 {
			t.Fatalf("dispatch wake rows=%d status=%q, want none: no unit of the run waits for a slot", got.rows, got.status)
		}
		if moved := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeNotWaiting) - notWaitingBefore; moved != 1 {
			t.Fatalf("not_waiting counter moved %d, want 1", moved)
		}
	})
}

// TestBurstOfUnitCompletionsArmsOnePendingWakeupPerRun pins the dedupe the
// brief asks for: ten completions of one run inside one second leave ONE
// pending dispatch_sync_run row, at the earliest completion instant.
//
// RED CONTROL: give WakeDispatchSQL a plain INSERT with no ON CONFLICT tail
// and the second completion fails the unique key; change the tail's LEAST to
// the new value and the earliest instant is lost.
func TestBurstOfUnitCompletionsArmsOnePendingWakeupPerRun(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		seedRunUnit(t, ctx, pool, "planned")
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
		units := []string{firstUnitID}
		for len(units) < 10 {
			units = append(units, seedRunUnit(t, ctx, pool, "dispatching"))
		}
		claims := make([]Claim, 0, len(units))
		for _, unitID := range units {
			claims = append(claims, claimFixtureUnit(t, ctx, repository, unitID, now))
		}
		earliest := now.Add(time.Second)
		for index, claim := range claims {
			completedAt := earliest.Add(time.Duration(9-index) * 100 * time.Millisecond)
			if index == len(claims)-1 {
				completedAt = earliest
			}
			if err := repository.Complete(ctx, claim, map[string]any{"records": 1}, nil, now, completedAt); err != nil {
				t.Fatal(err)
			}
		}
		got := readDispatchWake(t, ctx, pool)
		if got.rows != 1 || got.status != "pending" || !got.availableAt.Equal(earliest) {
			t.Fatalf("after 10 completions: rows=%d status=%q available_at=%s, want ONE pending row at the earliest completion %s",
				got.rows, got.status, got.availableAt, earliest)
		}
	})
}
