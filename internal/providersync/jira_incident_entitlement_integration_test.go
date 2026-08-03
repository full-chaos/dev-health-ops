//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresJiraIncidentEntitlementHonorsRevocation(t *testing.T) {
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
	for _, statement := range []string{
		`CREATE TABLE organizations (id uuid PRIMARY KEY, tier text NOT NULL)`,
		`CREATE TABLE feature_flags (
  id uuid PRIMARY KEY, key text UNIQUE NOT NULL, min_tier text NOT NULL,
  is_enabled boolean NOT NULL)`,
		`CREATE TABLE org_feature_overrides (
  org_id uuid NOT NULL, feature_id uuid NOT NULL, is_enabled boolean NOT NULL,
  expires_at timestamptz, PRIMARY KEY (org_id, feature_id))`,
		`CREATE TABLE org_licenses (
  org_id uuid PRIMARY KEY, tier text NOT NULL, features_override json)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	orgID, featureID := uuid.NewString(), uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO organizations (id, tier) VALUES ($1, 'community')`, orgID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO feature_flags (id, key, min_tier, is_enabled)
VALUES ($1, 'canonical_incident_ingestion', 'community', true)`, featureID); err != nil {
		t.Fatal(err)
	}
	entitlement := PostgresJiraIncidentEntitlement{
		Pool: pool,
		Now: func() time.Time {
			return time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
		},
	}
	if err := entitlement.Require(ctx, orgID); err != nil {
		t.Fatalf("tier grant: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled)
VALUES ($1, $2, false)`, orgID, featureID); err != nil {
		t.Fatal(err)
	}
	if err := entitlement.Require(ctx, orgID); !errors.Is(err, ErrJiraIncidentEntitlementDisabled) {
		t.Fatalf("revoked grant error=%v", err)
	}
}
