//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestPagerDutyServicesEffectsUseMigratedClickHouseForReplayTombstonesTenantAndLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	// The current production writer contract is v2; make the migration runner
	// inherit that explicit admission instead of silently materializing its
	// legacy default.
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
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
	// This is the production migration chain, including the v2 ordering
	// contract, rather than a test-only table that could omit readback fields.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	claim := nativeTestClaim("pagerduty", "services")
	initialAt := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	serviceA, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{ID: "PS1", Name: "Payments"}, initialAt)
	if err != nil {
		t.Fatal(err)
	}
	serviceB, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{ID: "PS2", Name: "Support"}, initialAt)
	if err != nil {
		t.Fatal(err)
	}
	mappingA, err := pagerDutyServiceMappingFromReference(serviceA, pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"}, pagerDutyMappingMetadata, initialAt, "")
	if err != nil {
		t.Fatal(err)
	}
	mappingB, err := pagerDutyServiceMappingFromReference(serviceB, pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/support"}, pagerDutyMappingMetadata, initialAt, "")
	if err != nil {
		t.Fatal(err)
	}
	serviceEffect, err := effectBatchFromValues("operational_services", EffectReadbackRequired, []pagerDutyServiceRow{serviceA, serviceB})
	if err != nil {
		t.Fatal(err)
	}
	mappingEffect, err := effectBatchFromValues("operational_service_repository_mappings", EffectReadbackRequired, []pagerDutyServiceRepositoryMappingRow{mappingA, mappingB})
	if err != nil {
		t.Fatal(err)
	}
	sink := PagerDutyServicesClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		ProviderInstanceID: "Acme", Now: func() time.Time { return initialAt },
	}
	if inspection, err := sink.InspectEffect(ctx, claim, serviceEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("services before write inspection=%s error=%v", inspection, err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, mappingEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("mappings before write inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, serviceEffect); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, mappingEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, serviceEffect); err != nil || inspection != EffectExact {
		t.Fatalf("services after write inspection=%s error=%v", inspection, err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, mappingEffect); err != nil || inspection != EffectExact {
		t.Fatalf("mappings after write inspection=%s error=%v", inspection, err)
	}
	// Replaying the durable effect is the recovery path: no new source fetch or
	// generated identity is involved, and both destinations must read back exact.
	if err := sink.WriteEffect(ctx, claim, serviceEffect); err != nil {
		t.Fatalf("service replay: %v", err)
	}
	if err := sink.WriteEffect(ctx, claim, mappingEffect); err != nil {
		t.Fatalf("mapping replay: %v", err)
	}

	updatedAt := initialAt.Add(time.Hour)
	serviceAUpdated, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{ID: "PS1", Name: "Payments updated", UpdatedAt: updatedAt.Format(time.RFC3339Nano)}, updatedAt)
	if err != nil {
		t.Fatal(err)
	}
	updatedServiceEffect, err := effectBatchFromValues("operational_services", EffectReadbackRequired, []pagerDutyServiceRow{serviceAUpdated})
	if err != nil {
		t.Fatal(err)
	}
	mappingAUpdated, err := pagerDutyServiceMappingFromReference(serviceAUpdated, pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"}, pagerDutyMappingMetadata, updatedAt, "")
	if err != nil {
		t.Fatal(err)
	}
	updatedMappingEffect, err := effectBatchFromValues("operational_service_repository_mappings", EffectReadbackRequired, []pagerDutyServiceRepositoryMappingRow{mappingAUpdated})
	if err != nil {
		t.Fatal(err)
	}
	sink.Now = func() time.Time { return updatedAt }
	if err := sink.WriteEffect(ctx, claim, updatedServiceEffect); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, updatedMappingEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, updatedServiceEffect); err != nil || inspection != EffectExact {
		t.Fatalf("updated services inspection=%s error=%v", inspection, err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, updatedMappingEffect); err != nil || inspection != EffectExact {
		t.Fatalf("updated mappings inspection=%s error=%v", inspection, err)
	}
	var deleted bool
	var deletedAt time.Time
	if err := conn.QueryRow(ctx, `
SELECT is_deleted, deleted_at
FROM (
  SELECT org_id, id, external_id, is_deleted, deleted_at, source_revision, source_conflict_key, ingest_revision
  FROM operational_services
  WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ?
  ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC
  LIMIT 1 BY org_id, id
)
WHERE external_id = ?`,
		claim.OrgID, "pagerduty", "acme", "service", "PS2").Scan(&deleted, &deletedAt); err != nil {
		t.Fatal(err)
	}
	if !deleted || !deletedAt.Equal(updatedAt) {
		t.Fatalf("service tombstone deleted=%v deleted_at=%s want %s", deleted, deletedAt, updatedAt)
	}
	var active bool
	var validTo time.Time
	if err := conn.QueryRow(ctx, `
SELECT is_active, valid_to
FROM (
  SELECT org_id, id, is_active, valid_to, source_revision, source_conflict_key, ingest_revision
  FROM operational_service_repository_mappings
  WHERE org_id = ? AND provider = ? AND provider_instance_id = ?
  ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC
  LIMIT 1 BY org_id, id
)
WHERE id = ?`,
		claim.OrgID, "pagerduty", "acme", mappingB.ID).Scan(&active, &validTo); err != nil {
		t.Fatal(err)
	}
	if active || !validTo.Equal(updatedAt) {
		t.Fatalf("mapping tombstone active=%v valid_to=%s want %s", active, validTo, updatedAt)
	}

	// A foreign tenant may write its own identity, but it cannot alter this
	// tenant's readback because every sink query includes org/provider/instance.
	otherClaim := claim
	otherClaim.OrgID = "org-other"
	foreign := serviceAUpdated
	foreign.OrgID = otherClaim.OrgID
	if err := fillPagerDutyServiceOrdering(&foreign); err != nil {
		t.Fatal(err)
	}
	foreignEffect, err := effectBatchFromValues("operational_services", EffectReadbackRequired, []pagerDutyServiceRow{foreign})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, otherClaim, foreignEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, updatedServiceEffect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign tenant changed service inspection=%s error=%v", inspection, err)
	}

	// A lease loss at the durable write boundary must fail closed; the batch is
	// prepared but never acknowledged as written.
	leaseSink := PagerDutyServicesClickHouseEffects{
		Conn: conn, ProviderInstanceID: "acme",
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return providerfoundation.ErrLeaseLost }),
	}
	if err := leaseSink.WriteEffect(ctx, claim, updatedServiceEffect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease failure=%v", err)
	}
}
