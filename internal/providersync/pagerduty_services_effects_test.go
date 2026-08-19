package providersync

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestPagerDutyServicesEffectsRejectForeignScopeOrderingTamperAndMixedInstances(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "services")
	now := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	service, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{
		ID: "PS1", Name: "Payments", EscalationPolicy: &pagerDutyServiceReference{ID: "PE1"},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	foreign := service
	foreign.OrgID = "org-other"
	if err := fillPagerDutyServiceOrdering(&foreign); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyServiceRows(claim, []pagerDutyServiceRow{foreign}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error=%v", err)
	}
	foreignInstance := service
	foreignInstance.ProviderInstanceID = "other"
	if err := fillPagerDutyServiceOrdering(&foreignInstance); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyServiceRows(claim, []pagerDutyServiceRow{foreignInstance}); err != nil {
		t.Fatalf("row instance should be structurally valid before sink fence: %v", err)
	}
	if _, err := (PagerDutyServicesClickHouseEffects{}).providerInstance(service.ProviderInstanceID, foreignInstance.ProviderInstanceID); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("mixed instance error=%v", err)
	}
	tampered := service
	tampered.Name = "forged"
	if err := validatePagerDutyServiceRows(claim, []pagerDutyServiceRow{tampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering tamper error=%v", err)
	}
	if err := validatePagerDutyServiceRows(claim, []pagerDutyServiceRow{service, service}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate service error=%v", err)
	}

	mapping, err := pagerDutyServiceMappingFromReference(
		service, pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"},
		pagerDutyMappingMetadata, now, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	foreignMapping := mapping
	foreignMapping.OrgID = "org-other"
	if err := validatePagerDutyServiceMappingRows(claim, []pagerDutyServiceRepositoryMappingRow{foreignMapping}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign mapping tenant error=%v", err)
	}
	foreignMapping = mapping
	foreignMapping.ProviderInstanceID = "other"
	if err := fillPagerDutyServiceMappingOrdering(&foreignMapping); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyServiceMappingRows(claim, []pagerDutyServiceRepositoryMappingRow{foreignMapping}); err != nil {
		t.Fatalf("mapping instance structural validation: %v", err)
	}
	badMapping := mapping
	badMapping.RelationshipProvenance = pagerDutyStringPtr("admin_configuration")
	if err := fillPagerDutyServiceMappingOrdering(&badMapping); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyServiceMappingRows(claim, []pagerDutyServiceRepositoryMappingRow{badMapping}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("mapping provenance tamper error=%v", err)
	}
}

func TestPagerDutyServicesTombstonesAdvanceVersionsAndPreserveMappingOwnership(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "services")
	now := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	service, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{ID: "PS1", Name: "Payments"}, now)
	if err != nil {
		t.Fatal(err)
	}
	serviceTombstone, err := pagerDutyServiceTombstone(service, service.SourceVersionAt)
	if err != nil {
		t.Fatal(err)
	}
	if serviceTombstone.ID != service.ID || serviceTombstone.ExternalID != service.ExternalID ||
		!serviceTombstone.SourceVersionAt.Equal(service.SourceVersionAt.Add(time.Microsecond)) ||
		!serviceTombstone.ObservedAt.Equal(serviceTombstone.SourceVersionAt) ||
		!serviceTombstone.LastSynced.Equal(serviceTombstone.SourceVersionAt) || !serviceTombstone.IsDeleted ||
		serviceTombstone.DeletedAt == nil || !serviceTombstone.DeletedAt.Equal(serviceTombstone.SourceVersionAt) ||
		serviceTombstone.SourceRevision == nil || serviceTombstone.SourceRevision.Cmp(service.SourceRevision) <= 0 {
		t.Fatalf("service tombstone=%+v", serviceTombstone)
	}
	if got := new(big.Int).And(new(big.Int).Rsh(new(big.Int).Set(serviceTombstone.SourceRevision), 56), big.NewInt(255)); got.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("service tombstone operation rank=%s want 2", got)
	}
	mapping, err := pagerDutyServiceMappingFromReference(
		service, pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"},
		pagerDutyMappingMetadata, now, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	mappingTombstone, err := pagerDutyServiceMappingTombstone(mapping, now)
	if err != nil {
		t.Fatal(err)
	}
	if mappingTombstone.ID != mapping.ID || mappingTombstone.IsActive || mappingTombstone.ValidTo == nil ||
		!mappingTombstone.ValidTo.Equal(now) || !mappingTombstone.ObservedAt.Equal(now) ||
		!mappingTombstone.LastSynced.Equal(now) ||
		!mappingTombstone.SourceVersionAt.Equal(mapping.SourceVersionAt.Add(time.Microsecond)) ||
		mappingTombstone.SourceRevision == nil || mappingTombstone.SourceRevision.Cmp(mapping.SourceRevision) <= 0 ||
		mappingTombstone.SourceEntityType != string(pagerDutyMappingMetadata) {
		t.Fatalf("mapping tombstone=%+v", mappingTombstone)
	}
	if got := new(big.Int).And(new(big.Int).Rsh(new(big.Int).Set(mappingTombstone.SourceRevision), 56), big.NewInt(255)); got.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("mapping tombstone operation rank=%s want 2", got)
	}
}

func TestPagerDutyServicesEffectsRejectUnsafeEmptySnapshotAndWrongDestination(t *testing.T) {
	if _, err := (PagerDutyServicesClickHouseEffects{}).providerInstance(); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("empty snapshot provider instance error=%v", err)
	}
	claim := nativeTestClaim("pagerduty", "services")
	sink := PagerDutyServicesClickHouseEffects{}
	if err := sink.WriteEffect(nil, claim, EffectBatch{Destination: "operational_services"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil context error=%v", err)
	}
	if err := sink.WriteEffect(context.Background(), claim, EffectBatch{Destination: "other"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong destination error=%v", err)
	}
}
