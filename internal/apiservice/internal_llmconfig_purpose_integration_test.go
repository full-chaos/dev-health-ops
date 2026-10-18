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

// The internal llm-config route, built by InternalRoutes over a real store,
// serves purpose investigate: the override wins over the default row, a
// missing override falls back to the default row, an override for the
// default's provider with no key of its own is served the default key, and
// another purpose's override does not leak in.
func TestInternalLLMConfigServesInvestigatePurpose(t *testing.T) {
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
	org := uuid.NewString()
	pgseed.Org(ctx, t, pool, org, "enterprise")
	const token = "svc_acr_purpose-0123456789abcdef"
	if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials
		(id, service_name, token_hash, token_prefix, scopes, created_at)
		VALUES (gen_random_uuid(), 'acr', $1, $2, '["llm_config:read"]'::json, now())`,
		admin.HashServiceToken(token), token[:16]); err != nil {
		t.Fatal(err)
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
	secret("api_key", "sk-default")
	plain("purpose:explain:provider", "openai")
	plain("purpose:explain:model", "explain-model")

	server, err := NewInternalServer(config.Config{APIInternalAddress: "127.0.0.1:0"}, quietLog(),
		InternalRoutes(Deps{Pool: pool, Decryptor: decryptor}, quietLog()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	fetch := func() map[string]string {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/acr/orgs/"+org+"/llm-config", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		var out map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	base := fetch()
	if base["model"] != "default-model" || base["provider"] != "anthropic" || base["api_key"] != "sk-default" {
		t.Fatalf("no investigate override must serve the default row (an explain override must not leak): %v", base["model"])
	}

	plain("purpose:investigate:provider", "anthropic")
	plain("purpose:investigate:model", "investigate-model")
	inherited := fetch()
	if inherited["model"] != "investigate-model" || inherited["api_key"] != "sk-default" || inherited["version"] == base["version"] {
		t.Fatalf("same-provider override: model=%q key inherited=%v version moved=%v",
			inherited["model"], inherited["api_key"] == "sk-default", inherited["version"] != base["version"])
	}

	if _, err := pool.Exec(ctx, `UPDATE settings SET value = 'openai' WHERE org_id = $1 AND key = 'purpose:investigate:provider'`, org); err != nil {
		t.Fatal(err)
	}
	secret("purpose:investigate:api_key", "sk-investigate")
	own := fetch()
	if own["provider"] != "openai" || own["model"] != "investigate-model" || own["api_key"] != "sk-investigate" {
		t.Fatalf("override must win over the default row: provider=%q model=%q", own["provider"], own["model"])
	}
}
