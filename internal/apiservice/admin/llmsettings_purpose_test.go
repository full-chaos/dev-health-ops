//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	defaultCred = "placeholder-default-value"
	explainCred = "placeholder-explain-value"
)

// TestLLMSettingsPerPurposeOverrides is CHAOS-10039's persisted-state proof
// over the real route: the default row and the per-purpose overrides share the
// llm category, a purpose write/read/delete touches only its own rows, and a
// model is required with every purpose override. Go-only: Python has no
// purposes.
func TestLLMSettingsPerPurposeOverrides(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-purpose-32-bytes!"
	org, adminUserID := uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			for _, stmt := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'llmpurpose', 'llmpurpose', 'team', 'stripe', true, now(), now())`, []any{org}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'llmpurpose-admin@example.com', true, true, false, 0, now(), now())`, []any{adminUserID}},
			} {
				if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			return map[string]map[string]any{"a": {"user_id": adminUserID.String(), "email": "llmpurpose-admin@example.com", "org_id": org.String(), "role": "admin"}}
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(llmVenueEncryptionKey), "")
		if err != nil {
			t.Fatalf("build decryptor: %v", err)
		}
		deps.Decryptor = decryptor
	})

	call := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest(method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+venue.Tokens["a"])
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}
	keys := func() map[string]bool {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT key FROM settings WHERE org_id = $1 AND category = 'llm'`, org.String())
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				t.Fatal(err)
			}
			out[key] = true
		}
		return out
	}
	const route = "/api/v1/admin/llm-settings"

	if status, _ := call("PUT", route, fmt.Sprintf(`{"provider":"anthropic","model":"big-model","api_key":%q}`, defaultCred)); status != 200 {
		t.Fatalf("default PUT: %d", status)
	}

	t.Run("a purpose override requires a model", func(t *testing.T) {
		if status, body := call("PUT", route, fmt.Sprintf(`{"purpose":"explain","provider":"openai","api_key":%q}`, explainCred)); status != 422 {
			t.Fatalf("status=%d body=%v, want 422", status, body)
		}
		if status, body := call("PUT", route, fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"","api_key":%q}`, explainCred)); status != 422 {
			t.Fatalf("empty model: status=%d body=%v, want 422", status, body)
		}
		if keys()["purpose:explain:provider"] {
			t.Fatal("a refused write left rows behind")
		}
	})

	t.Run("same provider reuses the default key; a different provider needs its own", func(t *testing.T) {
		if status, body := call("PUT", route, `{"purpose":"explain","provider":"anthropic","model":"small-model"}`); status != 200 {
			t.Fatalf("same provider without a key: %d %v, want 200", status, body)
		}
		if keys()["purpose:explain:api_key"] {
			t.Fatal("the default key was copied into the override instead of reused")
		}
		if status, body := call("PUT", route, `{"purpose":"explain","provider":"openai","model":"small-model"}`); status != 422 {
			t.Fatalf("other provider without a key: %d %v, want 422", status, body)
		}
		if status, body := call("PUT", route, `{"purpose":"explain","provider":"ollama","model":"llama"}`); status != 200 {
			t.Fatalf("keyless provider: %d %v, want 200", status, body)
		}
		if status, _ := call("DELETE", route+"?purpose=explain", ""); status != 200 {
			t.Fatalf("cleanup: %d", status)
		}
	})

	t.Run("categorize is not a purpose: every verb is refused with its own reason", func(t *testing.T) {
		for _, c := range []struct{ verb, path, body string }{
			{"PUT", route, `{"purpose":"categorize","provider":"anthropic","model":"small-model"}`},
			{"GET", route + "?purpose=categorize", ""},
			{"DELETE", route + "?purpose=categorize", ""},
		} {
			status, body := call(c.verb, c.path, c.body)
			if status != 400 || fmt.Sprint(body["error"]) != "purpose_not_configurable" {
				t.Fatalf("%s: status=%d body=%v, want 400 purpose_not_configurable", c.verb, status, body)
			}
		}
		if keys()["purpose:categorize:provider"] {
			t.Fatal("a refused categorize write left rows behind")
		}
	})

	t.Run("an unknown purpose is refused", func(t *testing.T) {
		if status, _ := call("PUT", route, `{"purpose":"summarize","provider":"openai","model":"m"}`); status != 400 {
			t.Fatalf("PUT status=%d, want 400", status)
		}
		if status, _ := call("GET", route+"?purpose=summarize", ""); status != 400 {
			t.Fatalf("GET status=%d, want 400", status)
		}
		if status, _ := call("DELETE", route+"?purpose=summarize", ""); status != 400 {
			t.Fatalf("DELETE status=%d, want 400", status)
		}
	})

	t.Run("budget and concurrency are default-only", func(t *testing.T) {
		if status, _ := call("PUT", route, `{"purpose":"explain","provider":"openai","model":"m","concurrency":4}`); status != 400 {
			t.Fatalf("status=%d, want 400", status)
		}
	})

	t.Run("write, read, list, delete one purpose", func(t *testing.T) {
		status, put := call("PUT", route, fmt.Sprintf(`{"purpose":"explain","provider":" OpenAI ","model":"small-explain","api_key":%q}`, explainCred))
		if status != 200 || put["purpose"] != "explain" || put["provider"] != "openai" || put["model"] != "small-explain" {
			t.Fatalf("PUT: %d %v", status, put)
		}
		if got, _ := put["api_key"].(string); got == "" || got == explainCred || strings.Contains(got, "placeholder-explain") {
			t.Fatalf("api_key not masked: %q", got)
		}
		if status, get := call("GET", route+"?purpose=explain", ""); status != 200 || get["model"] != "small-explain" {
			t.Fatalf("GET purpose: %d %v", status, get)
		}
		if status, _ := call("GET", route+"?purpose=investigate", ""); status != 404 {
			t.Fatalf("GET empty purpose: %d, want 404", status)
		}
		status, def := call("GET", route, "")
		purposes, _ := def["purposes"].(map[string]any)
		if status != 200 || def["model"] != "big-model" || purposes["explain"] == nil || purposes["categorize"] != nil {
			t.Fatalf("GET default: %d %v", status, def)
		}
		var secret string
		if err := pool.QueryRow(ctx, `SELECT value FROM settings WHERE org_id = $1 AND category = 'llm' AND key = 'purpose:explain:api_key'`, org.String()).Scan(&secret); err != nil || strings.Contains(secret, "placeholder-explain") {
			t.Fatalf("override api_key stored in the clear or missing: %v", err)
		}
		if status, _ := call("DELETE", route+"?purpose=explain", ""); status != 200 {
			t.Fatalf("DELETE purpose: %d", status)
		}
		if status, _ := call("DELETE", route+"?purpose=explain", ""); status != 404 {
			t.Fatalf("second DELETE purpose: %d, want 404", status)
		}
		for key := range keys() {
			if strings.HasPrefix(key, "purpose:") {
				t.Fatalf("override row %q survived its delete", key)
			}
		}
		if !keys()["provider"] || !keys()["api_key"] {
			t.Fatal("a purpose delete removed the default row")
		}
	})

	t.Run("the default delete leaves the overrides alone", func(t *testing.T) {
		if status, _ := call("PUT", route, fmt.Sprintf(`{"purpose":"investigate","provider":"openai","model":"mid","api_key":%q}`, explainCred)); status != 200 {
			t.Fatalf("PUT investigate: %d", status)
		}
		if status, _ := call("DELETE", route, ""); status != 200 {
			t.Fatalf("default DELETE: %d", status)
		}
		got := keys()
		if got["provider"] || !got["purpose:investigate:provider"] {
			t.Fatalf("rows after default delete: %v", got)
		}
		if status, _ := call("DELETE", route+"?purpose=investigate", ""); status != 200 {
			t.Fatalf("DELETE investigate: %d", status)
		}
		for key := range keys() {
			t.Errorf("row %q left behind", key)
		}
	})

	venueoracle.WriteGoOnlyProof(t, "CHAOS-10039 per-purpose overrides: Python has no purposes")
}
