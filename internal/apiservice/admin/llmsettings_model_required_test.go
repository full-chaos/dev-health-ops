//go:build integration

package admin_test

import (
	"context"
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

// TestLLMSettingsRefusesABYOProviderWithoutAModel is CHAOS-10538's rule test over
// the real route: a BYO provider saved with no usable model answers 400
// model_required and changes nothing (a read after it equals the read before),
// while a "no BYO" provider needs no model. Go-only: the Python plane saved such
// a row, which the venue oracle retires and pins.
func TestLLMSettingsRefusesABYOProviderWithoutAModel(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-model-rule-32-bytes!"
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
VALUES ($1, 'llmmodelrule', 'llmmodelrule', 'team', 'stripe', true, now(), now())`, []any{org}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'llmmodelrule-admin@example.com', true, true, false, 0, now(), now())`, []any{adminUserID}},
			} {
				if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			return map[string]map[string]any{"a": {"user_id": adminUserID.String(), "email": "llmmodelrule-admin@example.com", "org_id": org.String(), "role": "admin"}}
		},
	})
	base, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(llmVenueEncryptionKey), "")
		if err != nil {
			t.Fatalf("build decryptor: %v", err)
		}
		deps.Decryptor = decryptor
	})

	call := func(method, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, base+"/api/v1/admin/llm-settings", strings.NewReader(body))
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
		return resp.StatusCode, strings.TrimSpace(string(raw))
	}
	rows := func() string {
		t.Helper()
		return venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
			`SELECT category, key, value, is_encrypted, (updated_at)::text FROM settings WHERE org_id = '`+org.String()+`' ORDER BY category, key`)
	}

	cred := "placeholder-model-rule-value"
	if status, body := call("PUT", `{"provider":"openai","model":"m1","api_key":"`+cred+`"}`); status != 200 {
		t.Fatalf("a BYO provider with a model: status %d %s", status, body)
	}
	_, before := call("GET", "")
	rowsBefore := rows()

	const modelRequired = `{"detail":{"error":"model_required","feature":"byo_llm","message":"model is required when a BYO LLM provider is saved"}}`
	for name, body := range map[string]string{
		"key rotation, model absent": `{"provider":"openai","api_key":"` + cred + `-rotated"}`,
		"provider only":              `{"provider":"openai"}`,
		"model null":                 `{"provider":"openai","model":null,"api_key":"` + cred + `-null"}`,
		"model empty":                `{"provider":"anthropic","model":""}`,
		"model blank":                `{"provider":"anthropic","model":"   "}`,
		"unknown provider":           `{"provider":"ollama"}`,
		"budget and concurrency":     `{"provider":"openai","concurrency":3,"budget_limit_micro_usd":5}`,
	} {
		status, got := call("PUT", body)
		if status != 400 || got != modelRequired {
			t.Errorf("%s: status %d body %s, want 400 %s", name, status, got, modelRequired)
		}
		if after := func() string { _, g := call("GET", ""); return g }(); after != before {
			t.Errorf("%s: a refused save changed the settings read: %s -> %s", name, before, after)
		}
		if after := rows(); after != rowsBefore {
			t.Errorf("%s: a refused save changed the rows", name)
		}
		if strings.Contains(got, cred) {
			t.Errorf("%s: the refusal carries the key", name)
		}
	}

	if status, got := call("PUT", `{"provider":"openai","base_url":"http://127.0.0.1:1/v1"}`); status != 400 || strings.Contains(got, "model_required") {
		t.Errorf("an invalid base_url is refused first: status %d %s", status, got)
	}
	for _, provider := range []string{"auto", "none", "mock"} {
		if status, got := call("PUT", `{"provider":"`+provider+`"}`); status != 200 {
			t.Errorf("provider %q needs no model: status %d %s", provider, status, got)
		}
	}

	venueoracle.WriteGoOnlyProof(t, "Go-only rule test: the Python plane saved a model-less BYO provider, so there is no Python answer to compare; the route's refusals and the rows after them are asserted directly")
}
