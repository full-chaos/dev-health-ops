package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestPagerDutyOnCallsEffectRejectsForeignScopeOrderingTamperAndDuplicates(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "on-calls")
	level := int32(1)
	row, err := normalizePagerDutyOnCall(
		claim, "acme", pagerDutyOnCallPayload{
			ID: "OC1", EscalationLevel: &level,
			User:             &pagerDutyOnCallReference{ID: "PU1"},
			Schedule:         &pagerDutyOnCallReference{ID: "PS1"},
			EscalationPolicy: &pagerDutyOnCallReference{ID: "PE1"},
		}, time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	foreign := row
	foreign.OrgID = "org-other"
	if err := validatePagerDutyOnCallRows(claim, []pagerDutyOnCallRow{foreign}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error=%v", err)
	}
	foreignInstance := row
	foreignInstance.ProviderInstanceID = "other"
	if err := fillPagerDutyOnCallOrdering(&foreignInstance); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyOnCallRows(claim, []pagerDutyOnCallRow{foreignInstance}); err != nil {
		t.Fatalf("row instance should be structurally valid before sink fence: %v", err)
	}
	if _, err := (PagerDutyOnCallsClickHouseEffects{}).providerInstance(
		[]pagerDutyOnCallRow{row, foreignInstance},
	); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("mixed instance error=%v", err)
	}
	tampered := row
	levelTwo := int32(2)
	tampered.EscalationLevel = &levelTwo
	if err := validatePagerDutyOnCallRows(claim, []pagerDutyOnCallRow{tampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering tamper error=%v", err)
	}
	conflictTampered := row
	conflictTampered.SourceConflictKey = "00"
	if err := validatePagerDutyOnCallRows(claim, []pagerDutyOnCallRow{conflictTampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("source conflict tamper error=%v", err)
	}
	if err := validatePagerDutyOnCallRows(claim, []pagerDutyOnCallRow{row, row}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate row error=%v", err)
	}
}

func TestPagerDutyOnCallsEffectKeepsUpsertRowsWithoutTombstoneState(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "on-calls")
	row, err := normalizePagerDutyOnCall(
		claim, "acme", pagerDutyOnCallPayload{ID: "OC1"},
		time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.SourceEntityType != "oncall" || row.ScheduleID != nil || row.UserID != nil ||
		row.EscalationPolicyID != nil || row.EscalationLevel != nil || row.StartsAt != nil || row.EndsAt != nil {
		t.Fatalf("unexpected on-call row=%+v", row)
	}
	if len(pagerDutyOnCallValues(row)) != 27 || pagerDutyOnCallsColumns == "" {
		t.Fatalf("assignment schema projection changed unexpectedly: values=%d columns=%q", len(pagerDutyOnCallValues(row)), pagerDutyOnCallsColumns)
	}
}

func TestPagerDutyOnCallsEffectRejectsUnsafeEmptySnapshotWithoutInstance(t *testing.T) {
	if _, err := (PagerDutyOnCallsClickHouseEffects{}).providerInstance(nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("empty snapshot provider instance error=%v", err)
	}
}

func TestPagerDutyOnCallsEffectRejectsWrongRequestBeforeSinkAccess(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "on-calls")
	sink := PagerDutyOnCallsClickHouseEffects{}
	if err := sink.WriteEffect(nil, claim, EffectBatch{Destination: "operational_on_call_assignments"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid request error=%v", err)
	}
	if err := sink.WriteEffect(
		context.Background(), claim, EffectBatch{Destination: "other"},
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong destination error=%v", err)
	}
}
