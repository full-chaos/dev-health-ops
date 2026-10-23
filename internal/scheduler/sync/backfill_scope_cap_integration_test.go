//go:build integration

package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestNativeMaterializerBackfillScopeKeepsAPlanUnderTheUnitCap is the planner
// side of `dho backfill run --source-ids --dataset-keys`: the same backfill
// window of the same configuration is refused with ErrInvalidPlan when its
// trigger names no scope (every enabled source, so over the cap) and is
// planned when the trigger names one source and one dataset (the columns the
// verb writes). The cap is lowered to ten units so that six sources of one
// dataset over three chunks cross it; the default 1000 is the same arithmetic
// at the size of a 61-repository integration.
func TestNativeMaterializerBackfillScopeKeepsAPlanUnderTheUnitCap(t *testing.T) {
	t.Setenv("SYNC_RUN_MAX_UNITS", "10")
	fixture := startMaterializerPostgres(t)
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	occurrence := startBackfillTriggerFixture(t, fixture, since, before)
	ctx := context.Background()
	// The org's tier must not set its own cap: the env cap is the one measured.
	if _, err := fixture.pool.Exec(ctx, `DELETE FROM public.tier_limits WHERE limit_key = 'max_sync_units'`); err != nil {
		t.Fatal(err)
	}
	const (
		jiraConfigID = "00000000-0000-4000-8000-000000002001"
		jiraSourceID = "00000000-0000-4000-8000-000000002003"
	)
	for index := 1; index <= 5; index++ {
		if _, err := fixture.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
SELECT $1::uuid,org_id,integration_id,provider,source_type,'PROJ%[1]d','PROJ%[1]d','PROJ%[1]d',TRUE,metadata,now(),now()
FROM public.integration_sources WHERE id = $2::uuid`, index), fmt.Sprintf("00000000-0000-4000-8000-0000000021%02d", index), jiraSourceID); err != nil {
			t.Fatal(err)
		}
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}

	_, err = materializeAndCommit(t, fixture, materializer, occurrence)
	if !errors.Is(err, ErrInvalidPlan) || !strings.Contains(err.Error(), "over cap 10") {
		t.Fatalf("the unscoped plan of six sources: err = %v, want ErrInvalidPlan over cap 10", err)
	}

	if _, err := fixture.pool.Exec(ctx,
		`UPDATE public.sync_manual_triggers SET source_ids = ARRAY[$1]::text[], dataset_keys = ARRAY['work-items']::text[] WHERE occurrence_id = $2`,
		jiraSourceID, occurrence.ID); err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, occurrence)
	if err != nil {
		t.Fatalf("the plan scoped to one source and one dataset: %v", err)
	}
	if plan.PlannedUnits != 3 {
		t.Fatalf("plan.PlannedUnits = %d, want 3 (one source, three 7-day chunks)", plan.PlannedUnits)
	}
}

// TestNativeMaterializerPlansABackfillOverTheDefaultOffUnitCap is CHAOS-10955:
// 400 sources of one dataset over three 7-day chunks are 1200 units. With the
// opt-in cap off (SYNC_RUN_MAX_UNITS unset or 0) the plan is not refused, even
// when the org's tier carries a max_sync_units row of 1000 (that row no longer
// sets the cap): every unit is committed.
func TestNativeMaterializerPlansABackfillOverTheDefaultOffUnitCap(t *testing.T) {
	for _, test := range []struct {
		name, env string
		set       bool
	}{
		{name: "env unset"},
		{name: "env zero", set: true, env: "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("SYNC_RUN_MAX_UNITS", test.env)
			if !test.set {
				if err := os.Unsetenv("SYNC_RUN_MAX_UNITS"); err != nil {
					t.Fatal(err)
				}
			}
			fixture := startMaterializerPostgres(t)
			since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			before := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
			occurrence := startBackfillTriggerFixture(t, fixture, since, before)
			ctx := context.Background()
			if _, err := fixture.pool.Exec(ctx, `DELETE FROM public.tier_limits WHERE limit_key = 'max_sync_units'`); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.pool.Exec(ctx, `INSERT INTO public.tier_limits (tier, limit_key, limit_value)
SELECT t, 'max_sync_units', '1000' FROM unnest(ARRAY['community','team','enterprise']) AS t`); err != nil {
				t.Fatal(err)
			}
			const jiraSourceID = "00000000-0000-4000-8000-000000002003"
			if _, err := fixture.pool.Exec(ctx, `INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
SELECT gen_random_uuid(),org_id,integration_id,provider,source_type,'PROJ'||n,'PROJ'||n,'PROJ'||n,TRUE,metadata,now(),now()
FROM public.integration_sources, generate_series(1, 399) AS n WHERE id = $1::uuid`, jiraSourceID); err != nil {
				t.Fatal(err)
			}
			materializer, err := NewNativeMaterializer(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := materializeAndCommit(t, fixture, materializer, occurrence)
			if err != nil {
				t.Fatalf("the unscoped plan of 400 sources: %v, want it planned", err)
			}
			if plan.PlannedUnits != 1200 {
				t.Fatalf("plan.PlannedUnits = %d, want 1200 (400 sources, three 7-day chunks)", plan.PlannedUnits)
			}
		})
	}
}
