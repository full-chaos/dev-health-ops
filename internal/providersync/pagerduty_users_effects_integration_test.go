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

func TestPagerDutyUsersEffectUsesMigratedClickHouseTombstonesAndExactReplay(t *testing.T) {
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
	// users table, instead of creating a test-only table with a relaxed schema.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	claim := nativeTestClaim("pagerduty", "users")
	initialAt := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	rowA, err := normalizePagerDutyUser(
		claim, "acme", pagerDutyUserPayload{ID: "PU1", Name: "Alice"}, initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	rowB, err := normalizePagerDutyUser(
		claim, "acme", pagerDutyUserPayload{ID: "PU2", Name: "Bob"}, initialAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	initialEffect, err := effectBatchFromValues(
		"operational_users", EffectReadbackRequired,
		[]pagerDutyUserRow{rowA, rowB},
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := PagerDutyUsersClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		ProviderInstanceID: "Acme",
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

	updatedAt := initialAt.Add(time.Hour)
	rowAUpdated, err := normalizePagerDutyUser(
		claim, "acme", pagerDutyUserPayload{
			ID: "PU1", Name: "Alice updated", UpdatedAt: updatedAt.Format(time.RFC3339Nano),
		}, updatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	partialEffect, err := effectBatchFromValues(
		"operational_users", EffectReadbackRequired, []pagerDutyUserRow{rowAUpdated},
	)
	if err != nil {
		t.Fatal(err)
	}
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
FROM operational_users FINAL
WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND external_id = ?`,
		claim.OrgID, "pagerduty", "acme", "user", "PU2",
	).Scan(&deleted, &deletedAt); err != nil {
		t.Fatal(err)
	}
	if !deleted || !deletedAt.Equal(updatedAt) {
		t.Fatalf("missing-user tombstone deleted=%v deleted_at=%s want %s", deleted, deletedAt, updatedAt)
	}

	// The durable effect payload can be replayed after the write. The exact
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
	if err := fillPagerDutyUserOrdering(&otherRow); err != nil {
		t.Fatal(err)
	}
	otherEffect, err := effectBatchFromValues(
		"operational_users", EffectReadbackRequired, []pagerDutyUserRow{otherRow},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, otherClaim, otherEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, partialEffect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign tenant changed readback inspection=%s error=%v", inspection, err)
	}
}
