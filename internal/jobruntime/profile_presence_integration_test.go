//go:build integration

package jobruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProfilePresenceCountsIndependentReplicasAndDrainExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_profile_instances (
			instance_id uuid PRIMARY KEY,
			profile varchar(32) NOT NULL,
			state varchar(16) NOT NULL CHECK (state IN ('active', 'draining')),
			started_at timestamptz NOT NULL,
			heartbeat_at timestamptz NOT NULL,
			expires_at timestamptz NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	first, err := NewProfilePresence(pool, "heavy", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProfilePresence(pool, "heavy", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	first.ttl = time.Minute
	second.ttl = time.Minute
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	assertPresenceSummary(t, ctx, pool, ProfilePresenceSummary{Profile: "heavy", Live: 2})
	if err := first.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	assertPresenceSummary(t, ctx, pool, ProfilePresenceSummary{Profile: "heavy", Live: 2, Draining: 1})
	if err := first.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	assertPresenceSummary(t, ctx, pool, ProfilePresenceSummary{Profile: "heavy", Live: 1})
	if _, err := pool.Exec(ctx, `
		UPDATE public.worker_profile_instances
		SET expires_at = statement_timestamp() - interval '1 second'
		WHERE instance_id = $1`, second.instanceID); err != nil {
		t.Fatal(err)
	}
	assertPresenceSummary(t, ctx, pool, ProfilePresenceSummary{})
	if err := second.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertPresenceSummary(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	want ProfilePresenceSummary,
) {
	t.Helper()
	got, err := ReadProfilePresence(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if want.Profile == "" {
		if len(got) != 0 {
			t.Fatalf("presence = %#v, want none", got)
		}
		return
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("presence = %#v, want %#v", got, want)
	}
}
