package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestPagerDutyBusinessServicesEffectRejectsForeignScopeOrderingTamperAndMixedInstances(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "business-services")
	row, err := normalizePagerDutyBusinessService(
		claim, "acme", pagerDutyBusinessServicePayload{
			ID: "PBS1", Name: "Payments", Description: pagerDutyStringPtr("Checkout"),
		}, time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	foreign := row
	foreign.OrgID = "org-other"
	if err := validatePagerDutyBusinessServiceRows(claim, []pagerDutyBusinessServiceRow{foreign}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error=%v", err)
	}
	foreignInstance := row
	foreignInstance.ProviderInstanceID = "other"
	if err := fillPagerDutyBusinessServiceOrdering(&foreignInstance); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyBusinessServiceRows(claim, []pagerDutyBusinessServiceRow{foreignInstance}); err != nil {
		t.Fatalf("row instance should be structurally valid before sink fence: %v", err)
	}
	if _, err := (PagerDutyBusinessServicesClickHouseEffects{}).providerInstance(
		[]pagerDutyBusinessServiceRow{row, foreignInstance},
	); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("mixed instance error=%v", err)
	}
	tampered := row
	tampered.Name = "forged"
	if err := validatePagerDutyBusinessServiceRows(claim, []pagerDutyBusinessServiceRow{tampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering tamper error=%v", err)
	}
	inconsistent := row
	inconsistent.IsDeleted = true
	if err := fillPagerDutyBusinessServiceOrdering(&inconsistent); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyBusinessServiceRows(claim, []pagerDutyBusinessServiceRow{inconsistent}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("deleted flag consistency error=%v", err)
	}
	if err := validatePagerDutyBusinessServiceRows(claim, []pagerDutyBusinessServiceRow{row, row}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate row error=%v", err)
	}
}

func TestPagerDutyBusinessServicesTombstoneBumpsSourceVersionAndPreservesIdentity(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "business-services")
	row, err := normalizePagerDutyBusinessService(
		claim, "acme", pagerDutyBusinessServicePayload{ID: "PBS1", Name: "Payments"},
		time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	tombstone, err := pagerDutyBusinessServiceTombstone(row, row.SourceVersionAt)
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

func TestPagerDutyBusinessServicesEffectRejectsUnsafeEmptySnapshotWithoutInstance(t *testing.T) {
	if _, err := (PagerDutyBusinessServicesClickHouseEffects{}).providerInstance(nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("empty snapshot provider instance error=%v", err)
	}
}

func TestPagerDutyBusinessServicesEffectRejectsWrongRequestBeforeSinkAccess(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "business-services")
	sink := PagerDutyBusinessServicesClickHouseEffects{}
	if err := sink.WriteEffect(nil, claim, EffectBatch{Destination: "operational_services"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid request error=%v", err)
	}
	if err := sink.WriteEffect(
		context.Background(), claim, EffectBatch{Destination: "other"},
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong destination error=%v", err)
	}
}
