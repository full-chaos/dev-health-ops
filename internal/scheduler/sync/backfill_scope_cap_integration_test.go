//go:build integration

package sync

import (
	"context"
	"errors"
	"fmt"
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
