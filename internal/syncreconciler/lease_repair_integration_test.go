//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	leaseRepairRunA  = "00000000-0000-4000-8000-000000003901"
	leaseRepairRunB  = "00000000-0000-4000-8000-000000003902"
	leaseRepairRunC  = "00000000-0000-4000-8000-000000003903"
	leaseRepairUnitA = "00000000-0000-4000-8000-000000003911"
	leaseRepairUnitB = "00000000-0000-4000-8000-000000003912"
	leaseRepairUnitC = "00000000-0000-4000-8000-000000003913"
)

type gatedLeaseRepairTx struct {
	pgx.Tx
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (tx *gatedLeaseRepairTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	command, err := tx.Tx.Exec(ctx, sql, args...)
	if err != nil || sql != "SELECT pg_advisory_xact_lock($1)" {
		return command, err
	}
	tx.once.Do(func() { close(tx.entered) })
	select {
	case <-tx.release:
		return command, nil
	case <-ctx.Done():
		return pgconn.CommandTag{}, ctx.Err()
	}
}

type failingLeaseRepairTx struct {
	pgx.Tx
}

func (tx *failingLeaseRepairTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.HasPrefix(strings.TrimSpace(sql), "UPDATE") {
		return pgconn.CommandTag{}, errors.New("injected lease-repair write failure")
	}
	return tx.Tx.Exec(ctx, sql, args...)
}

func TestLeaseRepairPostgresContract(t *testing.T) {
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
	if err := createLeaseRepairIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)

	t.Run("two replicas skip a locked candidate and one owns the retry", func(t *testing.T) {
		resetLeaseRepairTables(t, ctx, pool)
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunA, "org-a", "running")
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitA, leaseRepairRunA, "org-a", "linear", "backfill", "work_items", 0, now.Add(-time.Minute))

		entered := make(chan struct{})
		release := make(chan struct{})
		first, err := newLeaseRepair(func(beginCtx context.Context) (pgx.Tx, error) {
			tx, beginErr := pool.Begin(beginCtx)
			if beginErr != nil {
				return nil, beginErr
			}
			return &gatedLeaseRepairTx{Tx: tx, entered: entered, release: release}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewLeaseRepair(pool)
		if err != nil {
			t.Fatal(err)
		}
		firstDone := make(chan struct {
			result LeaseRepairResult
			err    error
		}, 1)
		go func() {
			result, stepErr := first.Step(ctx, now, 1)
			firstDone <- struct {
				result LeaseRepairResult
				err    error
			}{result, stepErr}
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		secondDone := make(chan struct {
			result LeaseRepairResult
			err    error
		}, 1)
		go func() {
			result, stepErr := second.Step(ctx, now, 1)
			secondDone <- struct {
				result LeaseRepairResult
				err    error
			}{result, stepErr}
		}()
		select {
		case secondResult := <-secondDone:
			t.Fatalf("second replica bypassed bucket lock: %#v, %v", secondResult.result, secondResult.err)
		case <-time.After(100 * time.Millisecond):
		}
		close(release)
		firstResult := <-firstDone
		secondResult := <-secondDone
		if firstResult.err != nil || firstResult.result != (LeaseRepairResult{Selected: 1, Retried: 1}) {
			t.Fatalf("first replica Step() = %#v, %v", firstResult.result, firstResult.err)
		}
		if secondResult.err != nil || secondResult.result != (LeaseRepairResult{Selected: 1}) {
			t.Fatalf("second replica Step() = %#v, %v", secondResult.result, secondResult.err)
		}
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitA, "retrying", nil, leaseTimePointer(now.Add(expiredLeaseDefaultRetryBackoff)), "worker_lost", false, 1)
	})

	t.Run("a live owner and terminal run are excluded", func(t *testing.T) {
		resetLeaseRepairTables(t, ctx, pool)
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunA, "org-a", "running")
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunB, "org-b", "success")
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitA, leaseRepairRunA, "org-a", "linear", "backfill", "work_items", 0, now.Add(time.Minute))
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitB, leaseRepairRunB, "org-b", "linear", "backfill", "work_items", 0, now.Add(-time.Minute))
		repair, err := NewLeaseRepair(pool)
		if err != nil {
			t.Fatal(err)
		}
		result, err := repair.Step(ctx, now, 2)
		if err != nil || result != (LeaseRepairResult{}) {
			t.Fatalf("live/terminal Step() = %#v, %v", result, err)
		}
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitA, "running", leaseStringPointer("worker-a"), nil, "", false, 0)
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitB, "running", leaseStringPointer("worker-a"), nil, "", false, 0)
	})

	t.Run("retry, exhaustion, and noneligible surfaces have exact terminal metadata", func(t *testing.T) {
		resetLeaseRepairTables(t, ctx, pool)
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunA, "org-a", "running")
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunB, "org-b", "running")
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunC, "org-c", "running")
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitA, leaseRepairRunA, "org-a", "linear", "backfill", "work_items", 0, now.Add(-3*time.Minute))
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitB, leaseRepairRunB, "org-b", "linear", "backfill", "work_items", expiredLeaseDefaultMaximumRetries, now.Add(-2*time.Minute))
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitC, leaseRepairRunC, "org-c", "github", "backfill", "work_items", 0, now.Add(-time.Minute))
		repair, err := NewLeaseRepair(pool)
		if err != nil {
			t.Fatal(err)
		}
		result, err := repair.Step(ctx, now, 3)
		if err != nil || result != (LeaseRepairResult{Selected: 3, Retried: 1, Failed: 2}) {
			t.Fatalf("Step() = %#v, %v", result, err)
		}
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitA, "retrying", nil, leaseTimePointer(now.Add(expiredLeaseDefaultRetryBackoff)), "worker_lost", false, 1)
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitB, "failed", nil, nil, leaseRepairRetryExhaustedCategory, true, expiredLeaseDefaultMaximumRetries)
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitC, "failed", nil, nil, leaseRepairWorkerLostCategory, false, 0)
	})

	t.Run("transaction fault rolls back and tenant fence plus limit leave other rows untouched", func(t *testing.T) {
		resetLeaseRepairTables(t, ctx, pool)
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunA, "org-a", "running")
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunB, "org-b", "running")
		seedLeaseRepairRun(t, ctx, pool, leaseRepairRunC, "org-c", "running")
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitA, leaseRepairRunA, "org-a", "linear", "backfill", "work_items", 0, now.Add(-3*time.Minute))
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitB, leaseRepairRunB, "org-b", "linear", "backfill", "work_items", 0, now.Add(-2*time.Minute))
		// A malformed cross-tenant unit must not be selected even though its run
		// is nonterminal and its lease is expired.
		seedLeaseRepairUnit(t, ctx, pool, leaseRepairUnitC, leaseRepairRunC, "org-other", "linear", "backfill", "work_items", 0, now.Add(-4*time.Minute))

		faulting, err := newLeaseRepair(func(beginCtx context.Context) (pgx.Tx, error) {
			tx, beginErr := pool.Begin(beginCtx)
			if beginErr != nil {
				return nil, beginErr
			}
			return &failingLeaseRepairTx{Tx: tx}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := faulting.Step(ctx, now, 1); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("faulting Step() error = %v", err)
		}
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitA, "running", leaseStringPointer("worker-a"), nil, "", false, 0)

		repair, err := NewLeaseRepair(pool)
		if err != nil {
			t.Fatal(err)
		}
		result, err := repair.Step(ctx, now, 1)
		if err != nil || result != (LeaseRepairResult{Selected: 1, Retried: 1}) {
			t.Fatalf("bounded Step() = %#v, %v", result, err)
		}
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitA, "retrying", nil, leaseTimePointer(now.Add(expiredLeaseDefaultRetryBackoff)), "worker_lost", false, 1)
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitB, "running", leaseStringPointer("worker-a"), nil, "", false, 0)
		assertLeaseRepairState(t, ctx, pool, leaseRepairUnitC, "running", leaseStringPointer("worker-a"), nil, "", false, 0)
	})
}

func createLeaseRepairIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			status text NOT NULL
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			provider text NOT NULL,
			dataset_key text NOT NULL,
			cost_class text NOT NULL,
			mode text NOT NULL,
			status text NOT NULL,
			attempts integer NOT NULL DEFAULT 0,
			available_at timestamptz,
			rate_limit_deferrals integer NOT NULL DEFAULT 0,
			rate_limit_first_seen_at timestamptz,
			expired_lease_retry_count integer NOT NULL DEFAULT 0,
			last_retry_reason text,
			retry_exhausted_at timestamptz,
			error text,
			result jsonb,
			lease_owner text,
			lease_expires_at timestamptz,
			updated_at timestamptz NOT NULL
		)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func resetLeaseRepairTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, "TRUNCATE public.sync_run_units, public.sync_runs"); err != nil {
		t.Fatal(err)
	}
}

func seedLeaseRepairRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, orgID, status string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO public.sync_runs (id, org_id, status) VALUES ($1, $2, $3)`, id, orgID, status); err != nil {
		t.Fatal(err)
	}
}

func seedLeaseRepairUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, runID, orgID, provider, mode, dataset string, retries int64, expiresAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, provider, dataset_key, cost_class, mode, status,
			rate_limit_deferrals, rate_limit_first_seen_at, expired_lease_retry_count,
			lease_owner, lease_expires_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'standard', $6, 'running', 7, $7, $8, 'worker-a', $9, $7)`,
		id, orgID, runID, provider, dataset, mode, expiresAt.Add(-time.Hour), retries, expiresAt); err != nil {
		t.Fatal(err)
	}
}

func assertLeaseRepairState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, wantStatus string, wantOwner *string, wantAvailable *time.Time, wantCategory string, wantExhausted bool, wantRetries int64) {
	t.Helper()
	var (
		status      string
		owner       *string
		availableAt *time.Time
		retries     int64
		deferrals   int
		firstSeen   *time.Time
		exhaustedAt *time.Time
		resultRaw   []byte
	)
	if err := pool.QueryRow(ctx, `
		SELECT status, lease_owner, available_at, expired_lease_retry_count,
			rate_limit_deferrals, rate_limit_first_seen_at, retry_exhausted_at, result
		FROM public.sync_run_units WHERE id = $1`, id).Scan(
		&status, &owner, &availableAt, &retries, &deferrals, &firstSeen, &exhaustedAt, &resultRaw,
	); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || !equalStringPointers(owner, wantOwner) || !equalTimePointers(availableAt, wantAvailable) || retries != wantRetries {
		t.Fatalf("unit %s state status=%s owner=%v available=%v retries=%d", id, status, owner, availableAt, retries)
	}
	if wantStatus == "retrying" && (deferrals != 0 || firstSeen != nil) {
		t.Fatalf("retry unit %s retained rate-limit episode: deferrals=%d first_seen=%v", id, deferrals, firstSeen)
	}
	if wantExhausted && exhaustedAt == nil {
		t.Fatalf("exhausted unit %s lacks retry_exhausted_at", id)
	}
	if !wantExhausted && exhaustedAt != nil {
		t.Fatalf("nonexhausted unit %s has retry_exhausted_at=%v", id, exhaustedAt)
	}
	if wantCategory == "" {
		if resultRaw != nil {
			t.Fatalf("untouched unit %s result=%s", id, resultRaw)
		}
		return
	}
	var result map[string]any
	if err := json.Unmarshal(resultRaw, &result); err != nil {
		t.Fatal(err)
	}
	if result["error_category"] != wantCategory || result["retry_reason"] != leaseRepairRetryReason ||
		result["retry_exhausted"] != wantExhausted || result["last_lease_expired_at"] == nil ||
		result["retry_count"] != float64(wantRetries) {
		t.Fatalf("unit %s result=%v", id, result)
	}
	wantSurfaces := []string{}
	if wantStatus == "retrying" || wantCategory == leaseRepairRetryExhaustedCategory {
		wantSurfaces = linearBackfillRetrySurfaces
	}
	gotSurfaces, ok := result["retry_surfaces"].([]any)
	if !ok || len(gotSurfaces) != len(wantSurfaces) {
		t.Fatalf("unit %s retry surfaces=%v want=%v", id, result["retry_surfaces"], wantSurfaces)
	}
	for index, surface := range wantSurfaces {
		if gotSurfaces[index] != surface {
			t.Fatalf("unit %s retry surfaces=%v want=%v", id, gotSurfaces, wantSurfaces)
		}
	}
	if got, want := result["last_lease_expired_at"], "2026-07-23T12:00:00+00:00"; got != want {
		t.Fatalf("unit %s last lease expiry=%v, want %s", id, got, want)
	}
}

func leaseStringPointer(value string) *string { return &value }

func leaseTimePointer(value time.Time) *time.Time { return &value }

func equalStringPointers(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func equalTimePointers(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.UTC().Equal(right.UTC())
}
