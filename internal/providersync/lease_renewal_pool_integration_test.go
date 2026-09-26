//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/workersignals"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestLeaseRenewalDoesNotQueueBehindAnExhaustedWorkPool is the heartbeat half of
// CHAOS-6889. The work pool is small (4 by default) and dispatch passes or sync
// units can hold every connection for as long as they run. A lease renewal that
// had to acquire from that pool ran into its deadline, was read as a lost lease
// and cancelled a healthy unit. The repository renews on a connection of its own.
//
// RED CONTROL: on the code this replaces, Renew returns ErrLeaseLost after its
// 1.5s budget while both work connections are held.
func TestLeaseRenewalDoesNotQueueBehindAnExhaustedWorkPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repository.Close)
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Every connection of the work pool is held, as by concurrent passes.
	held := make([]*pgxpool.Conn, 0, int(config.MaxConns))
	for len(held) < int(config.MaxConns) {
		connection, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, connection)
	}
	t.Cleanup(func() {
		for _, connection := range held {
			connection.Release()
		}
	})
	if pool.Stat().IdleConns() != 0 || pool.Stat().AcquiredConns() != config.MaxConns {
		t.Fatalf("fixture: %d acquired / %d idle of %d, want the work pool exhausted", pool.Stat().AcquiredConns(), pool.Stat().IdleConns(), config.MaxConns)
	}

	renewContext, cancelRenew := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancelRenew()
	started := time.Now()
	if err := repository.Renew(renewContext, claim, now.Add(time.Second), now.Add(time.Minute+time.Second)); err != nil {
		t.Fatalf("Renew with the work pool exhausted = %v after %s, want nil: the heartbeat must not queue behind the work", err, time.Since(started))
	}
	var expiresAt time.Time
	reader, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	if err := reader.QueryRow(ctx, `SELECT lease_expires_at FROM public.sync_run_units WHERE id = $1::uuid`, firstUnitID).Scan(&expiresAt); err != nil {
		t.Fatal(err)
	}
	if !expiresAt.Equal(now.Add(time.Minute + time.Second)) {
		t.Fatalf("lease_expires_at = %s, want the renewed %s", expiresAt, now.Add(time.Minute+time.Second))
	}
}

func leaseRenewalFailures(t *testing.T, reason string) uint64 {
	t.Helper()
	var scraped strings.Builder
	if err := workersignals.MetricsSource().WritePrometheus(&scraped); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("dev_health_provider_sync_lease_renewal_failed_total{reason=%q} ", reason)
	for _, line := range strings.Split(scraped.String(), "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			count, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return count
		}
	}
	t.Fatalf("no %s sample", prefix)
	return 0
}

// CHAOS-6920: a failed lease renewal is a counter by reason, so a quiet prod window is a
// measurable zero instead of an absent series.
func TestLeaseRenewalFailuresAreCountedByReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The statement runs but the unit is no longer this owner's: lease_lost.
	lostBefore, errorBefore := leaseRenewalFailures(t, "lease_lost"), leaseRenewalFailures(t, "error")
	stranger := claim
	stranger.Owner = uuid.NewString()
	if err := repository.Renew(ctx, stranger, now.Add(time.Second), now.Add(time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Renew by a non-owner = %v, want ErrLeaseLost", err)
	}
	if got := leaseRenewalFailures(t, "lease_lost"); got != lostBefore+1 {
		t.Fatalf("lease_lost = %d, want %d", got, lostBefore+1)
	}
	if got := leaseRenewalFailures(t, "error"); got != errorBefore {
		t.Fatalf("error moved to %d on a lease-lost renewal, want %d", got, errorBefore)
	}

	// The statement cannot run (its pool is gone): error.
	repository.Close()
	if err := repository.Renew(ctx, claim, now.Add(time.Second), now.Add(time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Renew on a closed pool = %v, want ErrLeaseLost", err)
	}
	if got := leaseRenewalFailures(t, "error"); got != errorBefore+1 {
		t.Fatalf("error = %d, want %d", got, errorBefore+1)
	}
}
