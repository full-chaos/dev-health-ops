//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresIncidentEntitlementHonorsRevocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// The migrated schema (CHAOS-6769 ledger): the hand-written entitlement tables lacked the real
	// tables' NOT NULL columns, and the migrations already register canonical_incident_ingestion.
	pgschema.Apply(ctx, t, pool)
	orgID, featureID := uuid.NewString(), uuid.NewString()
	pgseed.Org(ctx, t, pool, orgID, "community")
	pgseed.SetFeatureFlag(ctx, t, pool, featureID, "canonical_incident_ingestion", "community", true)
	entitlement := PostgresIncidentEntitlement{
		Pool: pool,
		Now: func() time.Time {
			return time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
		},
	}
	if err := entitlement.Require(ctx, orgID); err != nil {
		t.Fatalf("tier grant: %v", err)
	}
	pgseed.OrgOverride(ctx, t, pool, orgID, featureID, false)
	if err := entitlement.Require(ctx, orgID); !errors.Is(err, ErrIncidentEntitlementDisabled) {
		t.Fatalf("revoked grant error=%v", err)
	}
}

// TestPostgresIncidentEntitlementNonObjectLicenseOverrideStillDecidesByTier
// is the regression proof for CHAOS-6286: before this consumed
// internal/api/licensing, a valid-but-non-object org_licenses.features_override
// (e.g. an empty JSON array) made loadCanonicalIncidentFeatureState's local
// decode fail and require() return ErrIncidentEntitlementDisabled directly --
// denying an org whose tier alone qualifies it, which Python's own
// isinstance(dict) guard would never do (it treats non-dict JSON as "no
// override" and falls through to the tier check). This org has no
// org_feature_overrides row at all, tier-qualifies, and its license's
// features_override is `[]` -- Require must succeed.
func TestPostgresIncidentEntitlementNonObjectLicenseOverrideStillDecidesByTier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// The migrated schema (CHAOS-6769 ledger): the hand-written entitlement tables lacked the real
	// tables' NOT NULL columns, and the migrations already register canonical_incident_ingestion.
	pgschema.Apply(ctx, t, pool)
	orgID := uuid.NewString()
	pgseed.Org(ctx, t, pool, orgID, "community")
	pgseed.SetFeatureFlag(ctx, t, pool, uuid.NewString(), "canonical_incident_ingestion", "community", true)
	pgseed.OrgLicense(ctx, t, pool, orgID, "community", `[]`)

	entitlement := PostgresIncidentEntitlement{
		Pool: pool,
		Now:  func() time.Time { return time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC) },
	}
	if err := entitlement.Require(ctx, orgID); err != nil {
		t.Fatalf("non-object features_override must not deny a tier-qualified org: %v", err)
	}
}
