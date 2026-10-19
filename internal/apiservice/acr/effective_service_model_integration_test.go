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

// The acr entitlement route serves the EFFECTIVE acr_service_model: the org's
// explicit override when it has one, else ON when the org has a saved BYO LLM
// configuration (auto-enrolment), else the plan or license decision. A
// globally disabled feature reads OFF for every org.
func TestEntitlementRouteServesTheEffectiveServiceModel(t *testing.T) {
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

	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'acr_service_model'`); err != nil {
		t.Fatal(err)
	}
	serviceModelID := uuid.NewString()
	pgseed.FeatureFlag(ctx, t, pool, serviceModelID, "acr_service_model", "community", true)

	org := func(provider string, override *bool) string {
		id := uuid.NewString()
		pgseed.Org(ctx, t, pool, id, "team")
		if provider != "" {
			pgseed.Setting(ctx, t, pool, id, "llm", "provider", provider, false)
			pgseed.Setting(ctx, t, pool, id, "llm", "model", "saved-model", false)
			pgseed.Setting(ctx, t, pool, id, "llm", "api_key", "sealed-placeholder", true)
		}
		if override != nil {
			pgseed.OrgOverride(ctx, t, pool, id, serviceModelID, *override)
		}
		return id
	}
	on, off := true, false
	savedNoOverride := org("anthropic", nil)
	savedOverrideOff := org("anthropic", &off)
	savedOverrideOn := org("anthropic", &on)
	autoProvider := org("auto", nil)
	nothing := org("", nil)
	expiredOff := org("anthropic", &off)
	if _, err := pool.Exec(ctx, `UPDATE org_feature_overrides SET expires_at = $1 WHERE org_id = $2::uuid AND feature_id = $3::uuid`, now.Add(-time.Hour), expiredOff, serviceModelID); err != nil {
		t.Fatal(err)
	}

	serviceModel := func(t *testing.T, orgID string) any {
		t.Helper()
		response, err := getAuthed(server.URL + "/api/v1/internal/acr/entitlements/" + orgID)
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
		return body["acr_service_model"]
	}

	for _, tc := range []struct {
		name string
		org  string
		want bool
	}{
		{name: "saved BYO config and no override: auto-enrolled ON", org: savedNoOverride, want: true},
		{name: "saved BYO config and an expired override: auto-enrolled ON", org: expiredOff, want: true},
		{name: "saved BYO config and an org override OFF: OFF", org: savedOverrideOff, want: false},
		{name: "saved BYO config and an org override ON: ON", org: savedOverrideOn, want: true},
		{name: "provider auto is no BYO config: plan decision OFF", org: autoProvider, want: false},
		{name: "no BYO config and no override: plan decision OFF", org: nothing, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := serviceModel(t, tc.org); got != tc.want {
				t.Fatalf("acr_service_model = %v, want %t", got, tc.want)
			}
		})
	}

	t.Run("globally disabled feature is OFF even with a saved BYO config and an override ON", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE feature_flags SET is_enabled = false WHERE id = $1::uuid`, serviceModelID); err != nil {
			t.Fatal(err)
		}
		for _, orgID := range []string{savedNoOverride, savedOverrideOn} {
			if got := serviceModel(t, orgID); got != false {
				t.Fatalf("acr_service_model = %v, want false", got)
			}
		}
	})
}
