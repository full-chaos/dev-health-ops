package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestPagerDutyEscalationPoliciesEffectRejectsForeignTenantAndOrderingTamper(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "escalation-policies")
	row, err := normalizePagerDutyEscalationPolicy(
		claim, "acme", pagerDutyEscalationPolicyPayload{ID: "PESCAL1", Name: "Primary"},
		time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	foreign := row
	foreign.OrgID = "org-other"
	if err := validatePagerDutyEscalationPolicyRows(claim, []pagerDutyEscalationPolicyRow{foreign}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error=%v", err)
	}
	tampered := row
	tampered.Name = "forged"
	if err := validatePagerDutyEscalationPolicyRows(claim, []pagerDutyEscalationPolicyRow{tampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering tamper error=%v", err)
	}
	if err := validatePagerDutyEscalationPolicyRows(claim, []pagerDutyEscalationPolicyRow{row, row}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate row error=%v", err)
	}
}

func TestPagerDutyEscalationPoliciesEffectRejectsWrongRequestBeforeSinkAccess(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "escalation-policies")
	sink := PagerDutyEscalationPoliciesClickHouseEffects{}
	if err := sink.WriteEffect(nil, claim, EffectBatch{Destination: "operational_escalation_policies"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid request error=%v", err)
	}
	if err := sink.WriteEffect(
		context.Background(), claim, EffectBatch{Destination: "other"},
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong destination error=%v", err)
	}
}
