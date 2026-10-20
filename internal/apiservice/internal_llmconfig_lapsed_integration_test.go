//go:build integration

package apiservice

import (
	"context"
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

// An org that keeps a saved BYO config but no longer holds byo_llm is served
// nothing on either internal llm-config route: the marked 404, never the key.
// The tier floor wins over a positive org override. An entitled org with the
// same kind of row is the positive control.
func TestInternalLLMConfigLapsedOrgServesNothing(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'byo_llm'`); err != nil {
		t.Fatal(err)
	}
	flagID := uuid.NewString()
	pgseed.FeatureFlag(ctx, t, pool, flagID, "byo_llm", "team", true)

	const savedKey = "placeholder-lapsed-org-value"
	seedOrg := func(tier string, override *bool) string {
		org := uuid.NewString()
		pgseed.Org(ctx, t, pool, org, tier)
		if override != nil {
			pgseed.OrgOverride(ctx, t, pool, org, flagID, *override)
		}
		sealed, err := decryptor.Encrypt([]byte(savedKey))
		if err != nil {
			t.Fatal(err)
		}
		pgseed.Setting(ctx, t, pool, org, "llm", "provider", "anthropic", false)
		pgseed.Setting(ctx, t, pool, org, "llm", "model", "saved-model", false)
		pgseed.Setting(ctx, t, pool, org, "llm", "api_key", sealed.Reveal(), true)
		return org
	}
	yes, no := true, false
	entitled := seedOrg("enterprise", nil)
	lapsed := seedOrg("community", nil)
	lapsedOverride := seedOrg("community", &yes)
	switchedOff := seedOrg("enterprise", &no)

	acrToken := "svc_acr_" + uuid.NewString()
	for _, c := range []struct{ service, token string }{{"acr", acrToken}} {
		if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials
			(id, service_name, token_hash, token_prefix, scopes, created_at)
			VALUES (gen_random_uuid(), $1, $2, $3, '["llm_config:read"]'::json, now())`,
			c.service, admin.HashServiceToken(c.token), c.token[:16]); err != nil {
			t.Fatal(err)
		}
	}
	server, err := NewInternalServer(config.Config{APIInternalAddress: "127.0.0.1:0"}, quietLog(),
		InternalRoutes(Deps{Pool: pool, Decryptor: decryptor}, quietLog()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	routes := []struct{ name, path, token string }{
		{"acr", "/api/v1/internal/acr/orgs/%s/llm-config", acrToken},
	}
	var reason string
	get := func(route int, org string) (int, string, string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, strings.Replace(routes[route].path, "%s", org, 1), nil)
		req.Header.Set("Authorization", "Bearer "+routes[route].token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		reason = rec.Header().Get("X-Org-Llm-Config-Reason")
		return rec.Code, rec.Header().Get("X-Org-Llm-Config"), rec.Body.String()
	}
	noConfig := uuid.NewString()
	pgseed.Org(ctx, t, pool, noConfig, "enterprise")
	explainOnly := uuid.NewString()
	pgseed.Org(ctx, t, pool, explainOnly, "enterprise")
	pgseed.Setting(ctx, t, pool, explainOnly, "llm", "purpose:explain:provider", "openai", false)
	pgseed.Setting(ctx, t, pool, explainOnly, "llm", "purpose:explain:model", "explain-model", false)
	if code, marker, _ := get(0, explainOnly); code != http.StatusNotFound || marker != "none" || reason != "byo_no_investigate_config" {
		t.Fatalf("acr route, BYO keys for explain only: answered %d marker %q reason %q, want the marked 404 with reason byo_no_investigate_config", code, marker, reason)
	}
	lapsedExplainOnly := uuid.NewString()
	pgseed.Org(ctx, t, pool, lapsedExplainOnly, "community")
	pgseed.Setting(ctx, t, pool, lapsedExplainOnly, "llm", "purpose:explain:provider", "openai", false)
	pgseed.Setting(ctx, t, pool, lapsedExplainOnly, "llm", "purpose:explain:model", "explain-model", false)
	if code, marker, _ := get(0, lapsedExplainOnly); code != http.StatusNotFound || marker != "none" || reason != "byo_llm_gate_closed" {
		t.Fatalf("acr route, lapsed tier with BYO keys for explain only: answered %d marker %q reason %q, want the marked 404 with reason byo_llm_gate_closed", code, marker, reason)
	}
	for i, route := range routes {
		if code, marker, _ := get(i, noConfig); code != http.StatusNotFound || marker != "none" || reason != "" {
			t.Fatalf("%s route, no config: answered %d marker %q reason %q, want the marked 404 with no reason", route.name, code, marker, reason)
		}
		code, _, body := get(i, entitled)
		if code != http.StatusOK || !strings.Contains(body, savedKey) {
			t.Fatalf("%s route: an entitled org with a saved config answered %d, want 200 with its config", route.name, code)
		}
		for name, org := range map[string]string{"community tier": lapsed, "community tier with a positive override": lapsedOverride, "org override off": switchedOff} {
			code, marker, body := get(i, org)
			if code != http.StatusNotFound || marker != "none" || reason != "byo_llm_gate_closed" {
				t.Fatalf("%s route, %s: answered %d marker %q reason %q, want the marked 404 with reason byo_llm_gate_closed", route.name, name, code, marker, reason)
			}
			if strings.Contains(body, savedKey) || strings.Contains(body, "saved-model") {
				t.Fatalf("%s route, %s: the body carries the saved config", route.name, name)
			}
		}
	}
}
