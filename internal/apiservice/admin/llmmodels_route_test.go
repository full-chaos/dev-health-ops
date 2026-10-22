//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestLLMSettingsModelsRouteServesListCachesAndDropsOnDelete(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-models-32-bytes!!!"
	orgID, bareOrgID, adminUserID := uuid.New(), uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for slug, id := range map[string]uuid.UUID{"set": orgID, "bare": bareOrgID} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, id, "llmmodels-route-"+slug)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, adminUserID, "llmmodels-route-admin@example.com")
			for key, value := range map[string]string{"provider": "openai", "model": "gpt-5-mini", "api_key": "sk-models-route", "base_url": "https://example.invalid/v1"} {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, 'llm', $3, $4, false, NULL, now(), now())`, uuid.New(), orgID.String(), key, value)
			}
			tokens := map[string]map[string]any{}
			for slug, org := range map[string]uuid.UUID{"set": orgID, "bare": bareOrgID} {
				tokens[slug] = map[string]any{"user_id": adminUserID.String(), "email": "llmmodels-route-admin@example.com", "org_id": org.String(), "role": "admin"}
			}
			return tokens
		},
	})
	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}

	var calls atomic.Int32
	var lastAuth atomic.Value
	var lastURL atomic.Value
	doer := fakeReadinessDoer(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		lastAuth.Store(req.Header.Get("Authorization"))
		lastURL.Store(req.URL.String())
		return jsonResponse(200, `{"object":"list","data":[{"id":"gpt-5-mini"},{"id":"gpt-5"}]}`), nil
	})
	base, _ := startGoServer(t, ctx, venue, jwtKey, func(d *apiservice.Deps) { d.HTTPDoer = fakehttp.Client(doer) })

	decode := func(resp routeResponse) map[string]any {
		t.Helper()
		if resp.status != 200 {
			t.Fatalf("status=%d body=%s", resp.status, resp.body)
		}
		if strings.Contains(resp.body, "sk-models-route") {
			t.Fatalf("response carries the api key: %s", resp.body)
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
			t.Fatalf("decode: %v\n%s", err, resp.body)
		}
		return out
	}
	ids := func(out map[string]any) string {
		var parts []string
		for _, item := range out["models"].([]any) {
			parts = append(parts, item.(map[string]any)["id"].(string))
		}
		return strings.Join(parts, ",")
	}

	first := decode(doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/models", auth("set")))
	if first["source"] != "live" || ids(first) != "gpt-5,gpt-5-mini,gpt-5-nano" || first["reason"] != nil || first["provider"] != "openai" {
		t.Fatalf("first read = %v", first)
	}
	if lastAuth.Load() != "Bearer sk-models-route" || lastURL.Load() != "https://example.invalid/v1/models" || calls.Load() != 1 {
		t.Fatalf("upstream call: auth=%v url=%v calls=%d", lastAuth.Load(), lastURL.Load(), calls.Load())
	}

	second := decode(doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/models", auth("set")))
	if second["source"] != "cache" || ids(second) != "gpt-5,gpt-5-mini,gpt-5-nano" || calls.Load() != 1 {
		t.Fatalf("second read = %v calls=%d", second, calls.Load())
	}

	other := decode(doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/models?provider=%20Anthropic%20", auth("set")))
	if other["source"] != "catalog" || other["provider"] != "anthropic" || ids(other) != "claude-haiku-5-5,claude-opus-5-5,claude-sonnet-5-5" || other["reason"] != nil || calls.Load() != 1 {
		t.Fatalf("another provider = %v calls=%d: the saved key must not reach it", other, calls.Load())
	}
	unknown := doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/models?provider=ollama", auth("set"))
	if unknown.status != 400 || strings.TrimSpace(unknown.body) != `{"detail":{"error":"unknown_provider","feature":"byo_llm","message":"provider must be one of anthropic, gemini, openai, qwen"}}` || calls.Load() != 1 {
		t.Fatalf("unknown provider: status=%d body=%s calls=%d", unknown.status, unknown.body, calls.Load())
	}

	deleted := doRouteRequest(t, base, "DELETE", "/api/v1/admin/llm-settings", auth("set"))
	if deleted.status != 200 {
		t.Fatalf("DELETE: status=%d body=%s", deleted.status, deleted.body)
	}
	after := decode(doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/models", auth("set")))
	if after["source"] != "unavailable" || after["reason"] != "not_configured" || ids(after) != "" || calls.Load() != 1 {
		t.Fatalf("after delete = %v calls=%d", after, calls.Load())
	}

	bare := decode(doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/models", auth("bare")))
	if bare["source"] != "unavailable" || bare["reason"] != "not_configured" {
		t.Fatalf("bare org = %v", bare)
	}

	venueoracle.WriteGoOnlyProof(t, "route-handler plumbing only (read saved config, list through the injected doer, cache, drop on delete) -- no provider is called")
}
