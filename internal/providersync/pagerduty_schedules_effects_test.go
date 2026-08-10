package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestPagerDutySchedulesEffectRejectsForeignScopeOrderingTamperAndMixedInstances(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "schedules")
	row, err := normalizePagerDutySchedule(
		claim, "acme", pagerDutySchedulePayload{
			ID: "PS1", Name: "Primary", TimeZone: stringPtr("UTC"),
		}, time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	foreign := row
	foreign.OrgID = "org-other"
	if err := validatePagerDutyScheduleRows(claim, []pagerDutyScheduleRow{foreign}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error=%v", err)
	}
	foreignInstance := row
	foreignInstance.ProviderInstanceID = "other"
	if err := fillPagerDutyScheduleOrdering(&foreignInstance); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyScheduleRows(claim, []pagerDutyScheduleRow{foreignInstance}); err != nil {
		t.Fatalf("row instance should be structurally valid before sink fence: %v", err)
	}
	if _, err := (PagerDutySchedulesClickHouseEffects{}).providerInstance(
		[]pagerDutyScheduleRow{row, foreignInstance},
	); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("mixed instance error=%v", err)
	}
	tampered := row
	tampered.Timezone = stringPtr("America/New_York")
	if err := validatePagerDutyScheduleRows(claim, []pagerDutyScheduleRow{tampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering tamper error=%v", err)
	}
	inconsistent := row
	inconsistent.IsDeleted = true
	if err := fillPagerDutyScheduleOrdering(&inconsistent); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyScheduleRows(claim, []pagerDutyScheduleRow{inconsistent}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("deleted flag consistency error=%v", err)
	}
	if err := validatePagerDutyScheduleRows(claim, []pagerDutyScheduleRow{row, row}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate row error=%v", err)
	}
}

func TestPagerDutySchedulesTombstoneBumpsSourceVersionAndPreservesIdentity(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "schedules")
	row, err := normalizePagerDutySchedule(
		claim, "acme", pagerDutySchedulePayload{ID: "PS1", Name: "Primary"},
		time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	tombstone, err := pagerDutyScheduleTombstone(row, row.SourceVersionAt)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.ID != row.ID || tombstone.ExternalID != row.ExternalID ||
		!tombstone.SourceVersionAt.Equal(row.SourceVersionAt.Add(time.Microsecond)) ||
		!tombstone.ObservedAt.Equal(tombstone.SourceVersionAt) ||
		!tombstone.LastSynced.Equal(tombstone.SourceVersionAt) || !tombstone.IsDeleted ||
		tombstone.DeletedAt == nil || !tombstone.DeletedAt.Equal(tombstone.SourceVersionAt) ||
		tombstone.SourceRevision == nil || tombstone.IngestRevision == nil ||
		tombstone.OrderingContract != 2 {
		t.Fatalf("tombstone=%+v", tombstone)
	}
	if tombstone.SourceRevision.Cmp(row.SourceRevision) <= 0 {
		t.Fatalf("tombstone source revision did not advance: old=%s new=%s", row.SourceRevision, tombstone.SourceRevision)
	}
}

func TestPagerDutySchedulesEffectRejectsUnsafeEmptySnapshotWithoutInstance(t *testing.T) {
	if _, err := (PagerDutySchedulesClickHouseEffects{}).providerInstance(nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("empty snapshot provider instance error=%v", err)
	}
}

func TestPagerDutySchedulesEffectRejectsWrongRequestBeforeSinkAccess(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "schedules")
	sink := PagerDutySchedulesClickHouseEffects{}
	if err := sink.WriteEffect(nil, claim, EffectBatch{Destination: "operational_on_call_schedules"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid request error=%v", err)
	}
	if err := sink.WriteEffect(
		context.Background(), claim, EffectBatch{Destination: "other"},
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong destination error=%v", err)
	}
}
