//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// An org admin turned a saved BYO config off (settings row llm/own_model_enabled
// = false): the acr llm-config route serves no key and names the reason
// byo_llm_turned_off. A closed byo_llm gate still wins; the toggle beats an
// unreadable stored key and saved keys for other purposes only; with nothing
// saved there is nothing to turn off. The toggle never changes the effective
// acr_service_model the entitlement route serves.
func TestInternalLLMConfigOwnModelTurnedOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key IN ('byo_llm', 'acr_service_model')`); err != nil {
		t.Fatal(err)
	}
	pgseed.FeatureFlag(ctx, t, pool, uuid.NewString(), "byo_llm", "team", true)
	serviceModelFlag := uuid.NewString()
	pgseed.FeatureFlag(ctx, t, pool, serviceModelFlag, "acr_service_model", "community", true)

	const savedKey = "placeholder-own-model-value"
	seedOrg := func(tier, toggle string) string {
		org := uuid.NewString()
		pgseed.Org(ctx, t, pool, org, tier)
		sealed, err := decryptor.Encrypt([]byte(savedKey))
		if err != nil {
			t.Fatal(err)
		}
		pgseed.Setting(ctx, t, pool, org, "llm", "provider", "anthropic", false)
		pgseed.Setting(ctx, t, pool, org, "llm", "model", "saved-model", false)
		pgseed.Setting(ctx, t, pool, org, "llm", "api_key", sealed.Reveal(), true)
		if toggle != "" {
			pgseed.Setting(ctx, t, pool, org, "llm", "own_model_enabled", toggle, false)
		}
		return org
	}
	turnedOff := seedOrg("enterprise", "false")
	turnedOn := seedOrg("enterprise", "true")
	lapsedOff := seedOrg("community", "false")
	overrideOff := seedOrg("enterprise", "false")
	pgseed.OrgOverride(ctx, t, pool, overrideOff, serviceModelFlag, false)

	unreadableOff := uuid.NewString()
	pgseed.Org(ctx, t, pool, unreadableOff, "enterprise")
	pgseed.Setting(ctx, t, pool, unreadableOff, "llm", "provider", "anthropic", false)
	pgseed.Setting(ctx, t, pool, unreadableOff, "llm", "model", "saved-model", false)
	pgseed.Setting(ctx, t, pool, unreadableOff, "llm", "api_key", "not-a-fernet-token", true)
	pgseed.Setting(ctx, t, pool, unreadableOff, "llm", "own_model_enabled", "false", false)

	explainOnlyOff := uuid.NewString()
	pgseed.Org(ctx, t, pool, explainOnlyOff, "enterprise")
	pgseed.Setting(ctx, t, pool, explainOnlyOff, "llm", "purpose:explain:provider", "openai", false)
	pgseed.Setting(ctx, t, pool, explainOnlyOff, "llm", "purpose:explain:model", "explain-model", false)
	pgseed.Setting(ctx, t, pool, explainOnlyOff, "llm", "own_model_enabled", "false", false)

	nothingOff := uuid.NewString()
	pgseed.Org(ctx, t, pool, nothingOff, "enterprise")
	pgseed.Setting(ctx, t, pool, nothingOff, "llm", "own_model_enabled", "false", false)

	acrToken := "svc_acr_" + uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials
		(id, service_name, token_hash, token_prefix, scopes, created_at)
		VALUES (gen_random_uuid(), 'acr', $1, $2, '["llm_config:read", "entitlements:read"]'::json, now())`,
		admin.HashServiceToken(acrToken), acrToken[:16]); err != nil {
		t.Fatal(err)
	}
	server, err := NewInternalServer(config.Config{APIInternalAddress: "127.0.0.1:0"}, quietLog(),
		InternalRoutes(Deps{Pool: pool, Decryptor: decryptor}, quietLog()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+acrToken)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	llmConfig := func(org string) *httptest.ResponseRecorder {
		return get("/api/v1/internal/acr/orgs/" + org + "/llm-config")
	}

	for _, tc := range []struct {
		name   string
		org    string
		reason string
	}{
		{name: "saved config turned off", org: turnedOff, reason: "byo_llm_turned_off"},
		{name: "turned off with an explicit acr_service_model override off", org: overrideOff, reason: "byo_llm_turned_off"},
		{name: "turned off behind a closed byo_llm gate: the closed gate wins", org: lapsedOff, reason: "byo_llm_gate_closed"},
		{name: "turned off with an unreadable stored key: no 409", org: unreadableOff, reason: "byo_llm_turned_off"},
		{name: "turned off with keys for other purposes only", org: explainOnlyOff, reason: "byo_llm_turned_off"},
		{name: "turned off with nothing saved: plain no config", org: nothingOff, reason: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := llmConfig(tc.org)
			if rec.Code != http.StatusNotFound || rec.Header().Get("X-Org-Llm-Config") != "none" || rec.Header().Get("X-Org-Llm-Config-Reason") != tc.reason {
				t.Fatalf("answered %d marker %q reason %q, want the marked 404 with reason %q", rec.Code,
					rec.Header().Get("X-Org-Llm-Config"), rec.Header().Get("X-Org-Llm-Config-Reason"), tc.reason)
			}
			if strings.Contains(rec.Body.String(), savedKey) || strings.Contains(rec.Body.String(), "saved-model") {
				t.Fatal("the body carries the saved config")
			}
		})
	}
	t.Run("turned on serves the saved config", func(t *testing.T) {
		rec := llmConfig(turnedOn)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), savedKey) {
			t.Fatalf("answered %d, want 200 with the saved config", rec.Code)
		}
	})
	serviceModel := func(t *testing.T, org string) any {
		t.Helper()
		rec := get("/api/v1/internal/acr/entitlements/" + org)
		if rec.Code != http.StatusOK {
			t.Fatalf("entitlement route answered %d", rec.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body["acr_service_model"]
	}
	t.Run("the toggle keeps the effective acr_service_model on", func(t *testing.T) {
		if got := serviceModel(t, turnedOff); got != true {
			t.Fatalf("acr_service_model = %v, want true", got)
		}
	})
	t.Run("an explicit override off still reads off", func(t *testing.T) {
		if got := serviceModel(t, overrideOff); got != false {
			t.Fatalf("acr_service_model = %v, want false", got)
		}
	})
}
