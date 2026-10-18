//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// The internal worker route serves the categorize config for a worker-operator
// credential holding llm_config:read: the categorize override when stored, else
// the default row, 404 when the org has none; the acr credential and the acr
// route do not cross over.
func TestInternalWorkerCategorizeConfigRoute(t *testing.T) {
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
	pgseed.FeatureFlag(ctx, t, pool, uuid.NewString(), "byo_llm", "team", true)
	org, bare := uuid.NewString(), uuid.NewString()
	pgseed.Org(ctx, t, pool, org, "enterprise")
	pgseed.Org(ctx, t, pool, bare, "enterprise")

	workerToken := "svc_wllm_" + uuid.NewString()
	acrToken := "svc_acr_" + uuid.NewString()
	for _, c := range []struct{ service, token string }{{"worker-llm-config", workerToken}, {"acr", acrToken}} {
		if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials
			(id, service_name, token_hash, token_prefix, scopes, created_at)
			VALUES (gen_random_uuid(), $1, $2, $3, '["llm_config:read"]'::json, now())`,
			c.service, admin.HashServiceToken(c.token), c.token[:16]); err != nil {
			t.Fatal(err)
		}
	}
	plain := func(key, value string) { pgseed.Setting(ctx, t, pool, org, "llm", key, value, false) }
	secret := func(key, value string) {
		sealed, err := decryptor.Encrypt([]byte(value))
		if err != nil {
			t.Fatal(err)
		}
		pgseed.Setting(ctx, t, pool, org, "llm", key, sealed.Reveal(), true)
	}
	plain("provider", "anthropic")
	plain("model", "default-model")
	secret("api_key", "placeholder-default-value")
	plain("purpose:investigate:provider", "openai")
	plain("purpose:investigate:model", "investigate-model")

	server, err := NewInternalServer(config.Config{APIInternalAddress: "127.0.0.1:0"}, quietLog(),
		InternalRoutes(Deps{Pool: pool, Decryptor: decryptor}, quietLog()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	get := func(path, token string) (int, map[string]string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var out map[string]string
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	route := func(o string) string { return "/api/v1/internal/workers/orgs/" + o + "/llm-config/categorize" }

	code, got := get(route(org), workerToken)
	if code != 200 || got["model"] != "default-model" || got["api_key"] != "placeholder-default-value" {
		t.Fatalf("no categorize override must serve the default row (the investigate override must not leak): %d model=%q", code, got["model"])
	}
	plain("purpose:categorize:provider", "anthropic")
	plain("purpose:categorize:model", "categorize-model")
	code, got = get(route(org), workerToken)
	if code != 200 || got["model"] != "categorize-model" || got["api_key"] != "placeholder-default-value" {
		t.Fatalf("same-provider categorize override: %d model=%q key inherited=%v", code, got["model"], got["api_key"] == "placeholder-default-value")
	}
	if code, _ := get(route(bare), workerToken); code != 404 {
		t.Fatalf("an org with no config answered %d, want 404", code)
	}
	if code, _ := get(route(org), acrToken); code != 401 {
		t.Fatalf("the acr credential on the worker route answered %d, want 401", code)
	}
	if code, _ := get("/api/v1/internal/acr/orgs/"+org+"/llm-config", workerToken); code != 401 {
		t.Fatalf("the worker credential on the acr route answered %d, want 401", code)
	}
	if code, _ := get(route(org), "svc_wllm_"+uuid.NewString()); code != 401 {
		t.Fatalf("an unknown token answered %d, want 401", code)
	}
}
