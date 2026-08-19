//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestPagerDutySchedulesEffectUsesMigratedClickHouseTombstonesAndExactReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	// Apply the production migration chain, including the canonical schedules
	// table, rather than creating a test-only relaxed schema.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	claim := nativeTestClaim("pagerduty", "schedules")
	initialAt := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	rowA, err := normalizePagerDutySchedule(
		claim, "acme", pagerDutySchedulePayload{
			ID: "PS1", Name: "Primary", TimeZone: pagerDutyStringPtr("America/Los_Angeles"),
		}, initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	rowB, err := normalizePagerDutySchedule(
		claim, "acme", pagerDutySchedulePayload{ID: "PS2", Name: "Support"}, initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	initialEffect, err := effectBatchFromValues(
		"operational_on_call_schedules", EffectReadbackRequired,
		[]pagerDutyScheduleRow{rowA, rowB},
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := PagerDutySchedulesClickHouseEffects{
		Conn: conn, ProviderInstanceID: "Acme",
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Now:   func() time.Time { return initialAt },
	}
	if inspection, err := sink.InspectEffect(ctx, claim, initialEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("before write inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, initialEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, initialEffect); err != nil || inspection != EffectExact {
		t.Fatalf("after initial write inspection=%s error=%v", inspection, err)
	}

	// A later complete snapshot omits PS2. The sink must fence that omission
	// with a strictly newer tombstone while retaining active PS1.
	updatedAt := initialAt.Add(time.Hour)
	rowAUpdated, err := normalizePagerDutySchedule(
		claim, "acme", pagerDutySchedulePayload{
			ID: "PS1", Name: "Primary updated", UpdatedAt: updatedAt.Format(time.RFC3339Nano),
			TimeZone: pagerDutyStringPtr("America/Los_Angeles"),
		}, updatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	partialEffect, err := effectBatchFromValues(
		"operational_on_call_schedules", EffectReadbackRequired,
		[]pagerDutyScheduleRow{rowAUpdated},
	)
	if err != nil {
		t.Fatal(err)
	}
	sink.Now = func() time.Time { return updatedAt }
	if err := sink.WriteEffect(ctx, claim, partialEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, partialEffect); err != nil || inspection != EffectExact {
		t.Fatalf("tombstone write inspection=%s error=%v", inspection, err)
	}
	var deleted bool
	var deletedAt time.Time
	if err := conn.QueryRow(ctx, `
SELECT is_deleted, deleted_at
FROM operational_on_call_schedules FINAL
WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND external_id = ?`,
		claim.OrgID, "pagerduty", "acme", "schedule", "PS2",
	).Scan(&deleted, &deletedAt); err != nil {
		t.Fatal(err)
	}
	if !deleted || !deletedAt.Equal(updatedAt) {
		t.Fatalf("missing-schedule tombstone deleted=%v deleted_at=%s want %s", deleted, deletedAt, updatedAt)
	}

	// The durable effect payload can be replayed after the write. Exact
	// readback must remain stable and must not create a second logical row.
	if err := sink.WriteEffect(ctx, claim, partialEffect); err != nil {
		t.Fatalf("replay write: %v", err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, partialEffect); err != nil || inspection != EffectExact {
		t.Fatalf("after replay inspection=%s error=%v", inspection, err)
	}

	otherClaim := claim
	otherClaim.OrgID = "org-other"
	otherRow := rowAUpdated
	otherRow.OrgID = otherClaim.OrgID
	if err := fillPagerDutyScheduleOrdering(&otherRow); err != nil {
		t.Fatal(err)
	}
	otherEffect, err := effectBatchFromValues(
		"operational_on_call_schedules", EffectReadbackRequired,
		[]pagerDutyScheduleRow{otherRow},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, otherClaim, otherEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, partialEffect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign row changed readback inspection=%s error=%v", inspection, err)
	}
}
