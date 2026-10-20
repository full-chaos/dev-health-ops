//go:build integration

package acr

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The acr entitlement route serves platform_model_fallback beside
// acr_service_model, from the real store on the migrated schema. Only an
// active org override opens it: no tier, no license features_override, no
// expired override, and acr_service_model does not imply it.
func TestEntitlementRouteServesPlatformModelFallbackOnlyForAnActiveOrgOverride(t *testing.T) {
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
	pgschema.Apply(ctx, t, pool)

	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	server := newTestServer(PostgresEntitlementStore{Pool: pool, Now: func() time.Time { return now }})
	defer server.Close()

	for _, key := range []string{"platform_model_fallback", "acr_service_model"} {
		if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = $1`, key); err != nil {
			t.Fatal(err)
		}
	}
	fallbackID, serviceModelID := uuid.NewString(), uuid.NewString()
	pgseed.FeatureFlag(ctx, t, pool, fallbackID, "platform_model_fallback", "community", true)
	pgseed.FeatureFlag(ctx, t, pool, serviceModelID, "acr_service_model", "community", true)

	org := func(tier string) string {
		id := uuid.NewString()
		pgseed.Org(ctx, t, pool, id, tier)
		return id
	}
	enterprise := org("enterprise")
	licensed := org("enterprise")
	pgseed.OrgLicense(ctx, t, pool, licensed, "enterprise", `{"platform_model_fallback": true}`)
	expired := org("enterprise")
	pgseed.OrgOverride(ctx, t, pool, expired, fallbackID, true)
	if _, err := pool.Exec(ctx, `UPDATE org_feature_overrides SET expires_at = $1 WHERE org_id = $2::uuid AND feature_id = $3::uuid`, now.Add(-time.Hour), expired, fallbackID); err != nil {
		t.Fatal(err)
	}
	serviceModelOnly := org("enterprise")
	pgseed.OrgOverride(ctx, t, pool, serviceModelOnly, serviceModelID, true)
	granted := org("community")
	pgseed.OrgOverride(ctx, t, pool, granted, fallbackID, true)

	for _, tc := range []struct {
		name         string
		org          string
		fallback     bool
		serviceModel bool
	}{
		{name: "enterprise tier, no override", org: enterprise},
		{name: "license features_override true", org: licensed},
		{name: "expired org override", org: expired},
		{name: "acr_service_model only", org: serviceModelOnly, serviceModel: true},
		{name: "active org override on community tier", org: granted, fallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := getAuthed(server.URL + "/api/v1/internal/acr/entitlements/" + tc.org)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", response.StatusCode)
			}
			var body map[string]any
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["platform_model_fallback"] != tc.fallback || body["acr_service_model"] != tc.serviceModel {
				t.Fatalf("platform_model_fallback = %v acr_service_model = %v, want %t and %t (body %v)", body["platform_model_fallback"], body["acr_service_model"], tc.fallback, tc.serviceModel, body)
			}
		})
	}
}
