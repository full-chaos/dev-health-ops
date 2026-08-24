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

func TestPagerDutyEscalationPoliciesEffectUsesMigratedClickHouseAndExactReplay(t *testing.T) {
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
	// Apply the production migration chain, including Python migration steps,
	// before opening the Go connection. This keeps the proof against the real
	// operational_escalation_policies schema rather than a test-only DDL copy.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	claim := nativeTestClaim("pagerduty", "escalation-policies")
	normalizedAt := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	row, err := normalizePagerDutyEscalationPolicy(
		claim, "acme", pagerDutyEscalationPolicyPayload{
			ID: "PESCAL1", Name: "Primary", UpdatedAt: "2026-08-01T10:00:00Z",
		}, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := effectBatchFromValues(
		"operational_escalation_policies", EffectReadbackRequired,
		[]pagerDutyEscalationPolicyRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := PagerDutyEscalationPoliciesClickHouseEffects{
		Entitlement: allowIncidentEntitlement,
		Conn:        conn,
		Lease:       providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("before write inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("after write inspection=%s error=%v", inspection, err)
	}
	// A recovery replay uses the durable effect payload and must classify as
	// exact without manufacturing a new source revision.
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatalf("replay write: %v", err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("after replay inspection=%s error=%v", inspection, err)
	}

	otherClaim := claim
	otherClaim.OrgID = "org-other"
	otherRow := row
	otherRow.OrgID = otherClaim.OrgID
	if err := fillPagerDutyEscalationPolicyOrdering(&otherRow); err != nil {
		t.Fatal(err)
	}
	otherEffect, err := effectBatchFromValues(
		"operational_escalation_policies", EffectReadbackRequired,
		[]pagerDutyEscalationPolicyRow{otherRow},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, otherClaim, otherEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign row changed tenant readback inspection=%s error=%v", inspection, err)
	}
}
