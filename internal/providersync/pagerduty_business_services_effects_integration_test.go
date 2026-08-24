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

func TestPagerDutyBusinessServicesEffectUsesMigratedClickHouseTombstonesAndExactReplay(t *testing.T) {
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
	// Apply the production migration chain, including the canonical operational
	// services table, rather than creating a test-only relaxed schema.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	claim := nativeTestClaim("pagerduty", "business-services")
	initialAt := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	rowA, err := normalizePagerDutyBusinessService(
		claim, "acme", pagerDutyBusinessServicePayload{
			ID: "PBS1", Name: "Payments", Description: pagerDutyStringPtr("Checkout"),
		}, initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	rowB, err := normalizePagerDutyBusinessService(
		claim, "acme", pagerDutyBusinessServicePayload{ID: "PBS2", Name: "Support"},
		initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	initialEffect, err := effectBatchFromValues(
		"operational_services", EffectReadbackRequired,
		[]pagerDutyBusinessServiceRow{rowA, rowB},
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := PagerDutyBusinessServicesClickHouseEffects{
		Entitlement: allowIncidentEntitlement,
		Conn:        conn, ProviderInstanceID: "Acme",
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

	// A later complete snapshot omits PBS2. The sink must fence that omission
	// with a strictly newer tombstone while retaining the active PBS1 row.
	updatedAt := initialAt.Add(time.Hour)
	rowAUpdated, err := normalizePagerDutyBusinessService(
		claim, "acme", pagerDutyBusinessServicePayload{
			ID: "PBS1", Name: "Payments updated", UpdatedAt: updatedAt.Format(time.RFC3339Nano),
		}, updatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	partialEffect, err := effectBatchFromValues(
		"operational_services", EffectReadbackRequired,
		[]pagerDutyBusinessServiceRow{rowAUpdated},
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
FROM operational_services FINAL
WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND external_id = ?`,
		claim.OrgID, "pagerduty", "acme", "business_service", "PBS2",
	).Scan(&deleted, &deletedAt); err != nil {
		t.Fatal(err)
	}
	if !deleted || !deletedAt.Equal(updatedAt) {
		t.Fatalf("missing-business-service tombstone deleted=%v deleted_at=%s want %s", deleted, deletedAt, updatedAt)
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
	if err := fillPagerDutyBusinessServiceOrdering(&otherRow); err != nil {
		t.Fatal(err)
	}
	otherEffect, err := effectBatchFromValues(
		"operational_services", EffectReadbackRequired,
		[]pagerDutyBusinessServiceRow{otherRow},
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
