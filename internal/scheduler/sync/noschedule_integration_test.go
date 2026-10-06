//go:build integration

package sync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-8768: the report SELECT runs against real rows in the migrated schema.
// Counted: active + planner-managed + no schedule_cron. Not counted: the same
// with a cron, an inactive config, and a config that is not planner-managed.
func TestUnscheduledConfigsPostgresCountsOnlyActivePlannerManagedWithoutCron(t *testing.T) {
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
	// Seeds config ...3038: active, planner-managed, WITH a schedule_cron: not counted.
	if err := createSchedulerIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	const (
		noCronID      = "00000000-0000-4000-8000-00000000a001"
		inactiveID    = "00000000-0000-4000-8000-00000000a002"
		unmanagedID   = "00000000-0000-4000-8000-00000000a003"
		withCronOther = "00000000-0000-4000-8000-00000000a004"
	)
	seed := func(configID, jobID string, managed bool, options string) {
		t.Helper()
		if err := insertPlannerFixture(ctx, pool, plannerFixture{
			configID: configID, jobID: jobID, orgID: "org-integration", plannerManaged: managed,
			options: options, createdAt: "2026-01-01T01:00:00-08:00",
		}); err != nil {
			t.Fatal(err)
		}
	}
	seed(noCronID, "00000000-0000-4000-8000-00000000b001", true, `{}`)
	seed(inactiveID, "00000000-0000-4000-8000-00000000b002", true, `{}`)
	seed(unmanagedID, "00000000-0000-4000-8000-00000000b003", false, `{}`)
	seed(withCronOther, "00000000-0000-4000-8000-00000000b004", true, `{"schedule_cron":"0 * * * *"}`)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_configurations SET is_active = FALSE WHERE id = $1::uuid`, inactiveID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE public.sync_configurations SET provider = 'jira' WHERE id = $1::uuid`, noCronID); err != nil {
		t.Fatal(err)
	}
	// An empty-string cron is "no cron" too (COALESCE(... , '') <> '' in the handoff SQL).
	if _, err := pool.Exec(ctx, `UPDATE public.sync_configurations SET sync_options = '{"schedule_cron":""}'::json WHERE id = $1::uuid`, noCronID); err != nil {
		t.Fatal(err)
	}

	repository, err := NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	got, err := repository.UnscheduledConfigs(ctx)
	if err != nil {
		t.Fatalf("UnscheduledConfigs() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("UnscheduledConfigs() = %#v, want exactly the active planner-managed config without a cron", got)
	}
	if got[0].Provider != "jira" || got[0].Hash != ConfigHash(noCronID) {
		t.Fatalf("UnscheduledConfigs()[0] = %#v, want provider jira and hash %s", got[0], ConfigHash(noCronID))
	}
}
