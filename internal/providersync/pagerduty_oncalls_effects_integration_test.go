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

func TestPagerDutyOnCallsEffectUsesMigratedClickHouseUpsertsAndExactReplay(t *testing.T) {
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
	// Use the production migration chain so nullable Int32/DateTime64 values
	// and the exact assignment table shape are exercised, not a test-only DDL.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	claim := nativeTestClaim("pagerduty", "on-calls")
	initialAt := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	levelOne := int32(1)
	rowA, err := normalizePagerDutyOnCall(
		claim, "acme", pagerDutyOnCallPayload{
			ID: "OC1", Start: "2026-08-01T10:00:00.123456Z", End: "2026-08-01T18:00:00.123456Z",
			EscalationLevel: &levelOne, User: &pagerDutyOnCallReference{ID: "PU1"},
			Schedule: &pagerDutyOnCallReference{ID: "PS1"}, EscalationPolicy: &pagerDutyOnCallReference{ID: "PE1"},
		}, initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	rowB, err := normalizePagerDutyOnCall(
		claim, "acme", pagerDutyOnCallPayload{
			Start: "2026-08-02T10:00:00Z", End: "2026-08-02T18:00:00Z",
			EscalationLevel: int32Pointer(2), User: &pagerDutyOnCallReference{ID: "PU2"},
			Schedule: &pagerDutyOnCallReference{ID: "PS2"}, EscalationPolicy: &pagerDutyOnCallReference{ID: "PE2"},
		}, initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	initialEffect, err := effectBatchFromValues(
		"operational_on_call_assignments", EffectReadbackRequired,
		[]pagerDutyOnCallRow{rowA, rowB},
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := PagerDutyOnCallsClickHouseEffects{
		Entitlement: allowIncidentEntitlement,
		Conn:        conn, ProviderInstanceID: "Acme",
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
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

	// The Python on-calls branch is an upsert stream. A later payload for OC1
	// must update that assignment without treating omitted OC2 as a deletion.
	updatedAt := initialAt.Add(time.Hour)
	levelTwo := int32(2)
	rowAUpdated, err := normalizePagerDutyOnCall(
		claim, "acme", pagerDutyOnCallPayload{
			ID: "OC1", Start: "2026-08-01T11:00:00.123456Z", End: "2026-08-01T19:00:00.123456Z",
			EscalationLevel: &levelTwo, User: &pagerDutyOnCallReference{ID: "PU1"},
			Schedule: &pagerDutyOnCallReference{ID: "PS1"}, EscalationPolicy: &pagerDutyOnCallReference{ID: "PE1"},
			UpdatedAt: updatedAt.Format(time.RFC3339Nano),
		}, updatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	updatedEffect, err := effectBatchFromValues(
		"operational_on_call_assignments", EffectReadbackRequired,
		[]pagerDutyOnCallRow{rowAUpdated},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, updatedEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, updatedEffect); err != nil || inspection != EffectExact {
		t.Fatalf("after update inspection=%s error=%v", inspection, err)
	}
	var retained uint64
	if err := conn.QueryRow(ctx, `
SELECT count()
FROM operational_on_call_assignments FINAL
WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ?`,
		claim.OrgID, "pagerduty", "acme", "oncall", rowB.ID,
	).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatalf("upsert stream removed omitted OC2: count=%d", retained)
	}
	if err := sink.WriteEffect(ctx, claim, updatedEffect); err != nil {
		t.Fatalf("replay write: %v", err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, updatedEffect); err != nil || inspection != EffectExact {
		t.Fatalf("after replay inspection=%s error=%v", inspection, err)
	}

	otherClaim := claim
	otherClaim.OrgID = "org-other"
	otherRow := rowAUpdated
	otherRow.OrgID = otherClaim.OrgID
	if err := fillPagerDutyOnCallOrdering(&otherRow); err != nil {
		t.Fatal(err)
	}
	otherEffect, err := effectBatchFromValues(
		"operational_on_call_assignments", EffectReadbackRequired,
		[]pagerDutyOnCallRow{otherRow},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, otherClaim, otherEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, updatedEffect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign tenant changed readback inspection=%s error=%v", inspection, err)
	}
}

func int32Pointer(value int32) *int32 { return &value }
