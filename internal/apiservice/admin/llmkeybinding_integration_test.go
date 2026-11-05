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

// TestAProviderChangeNeverKeepsTheOldVendorsKey is CHAOS-13096 over the real
// route and rows: a PUT that changes the provider of the default row or of a
// purpose override, with no api_key, is refused (400
// provider_change_requires_api_key) and changes no row; a change with a key
// replaces it; a change to a known keyless provider, or an override switched to
// the default row's provider, deletes the old key row. Go-only: Python has no
// such rule.
func TestAProviderChangeNeverKeepsTheOldVendorsKey(t *testing.T) {
	ctx := context.Background()
	const jwtKey = "venue-oracle-test-secret-key-for-llm-binding-32-bytes!"
	org, adminUserID := uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   repoRoot(t),
		JWTKey: jwtKey,
		Seed: withTokenMemberships(func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			for _, stmt := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'llmbinding', 'llmbinding', 'team', 'stripe', true, now(), now())`, []any{org}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'llmbinding-admin@example.com', true, true, false, 0, now(), now())`, []any{adminUserID}},
			} {
				if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			return map[string]map[string]any{"a": {"user_id": adminUserID.String(), "email": "llmbinding-admin@example.com", "org_id": org.String(), "role": "admin"}}
		}),
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(llmVenueEncryptionKey), "")
		if err != nil {
			t.Fatalf("build decryptor: %v", err)
		}
		deps.Decryptor = decryptor
	})
	call := func(body string) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest("PUT", base+"/api/v1/admin/llm-settings", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+venue.Tokens["a"])
		req.Header.Set("Content-Type", "application/json")
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
	// rows is every llm row of the org, values and updated_at, as one text.
	rows := func() string {
		t.Helper()
		got, err := pool.Query(ctx, `SELECT key, coalesce(value, ''), updated_at::text FROM settings WHERE org_id = $1 AND category = 'llm' ORDER BY key`, org.String())
		if err != nil {
			t.Fatal(err)
		}
		defer got.Close()
		var out []string
		for got.Next() {
			var key, value, at string
			if err := got.Scan(&key, &value, &at); err != nil {
				t.Fatal(err)
			}
			out = append(out, key+"="+value+"@"+at)
		}
		return strings.Join(out, "\n")
	}
	stored := func(key string) bool {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM settings WHERE org_id = $1 AND category = 'llm' AND key = $2 AND coalesce(value, '') <> ''`, org.String(), key).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	refused := func(name, body string) {
		t.Helper()
		before := rows()
		status, out := call(body)
		detail, _ := out["detail"].(map[string]any)
		t.Logf("%s: status=%d error=%v", name, status, detail["error"])
		if status != 400 || detail["error"] != "provider_change_requires_api_key" {
			t.Fatalf("%s: status=%d body=%v, want 400 provider_change_requires_api_key", name, status, out)
		}
		if after := rows(); after != before {
			t.Fatalf("%s: a refused write changed rows:\n%s\n---\n%s", name, before, after)
		}
	}
	ok := func(name, body string) {
		t.Helper()
		if status, out := call(body); status != 200 {
			t.Fatalf("%s: status=%d body=%v, want 200", name, status, out)
		}
	}
	const openAIKey, anthropicKey, explainKey = "sk-openai-" + "binding-0123456789", "sk-ant-" + "binding-0123456789", "sk-explain-" + "binding-0123456789"

	ok("default openai with its key", fmt.Sprintf(`{"provider":"openai","model":"gpt-5","api_key":%q}`, openAIKey))
	refused("default -> anthropic, no key", `{"provider":"anthropic","model":"claude-haiku-5-5"}`)
	refused("default -> anthropic, null key", `{"provider":"anthropic","model":"claude-haiku-5-5","api_key":null}`)
	ok("default same provider, no key", `{"provider":" OpenAI ","model":"gpt-5-mini"}`)
	if !stored("api_key") {
		t.Fatal("a same-provider write without a key dropped the key")
	}
	ok("default -> anthropic with its key", fmt.Sprintf(`{"provider":"anthropic","model":"claude-haiku-5-5","api_key":%q}`, anthropicKey))

	ok("explain openai with its key", fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"gpt-5","api_key":%q}`, explainKey))
	refused("explain -> gemini, no key", `{"purpose":"explain","provider":"gemini","model":"gemini-x"}`)
	ok("explain -> the default provider, no key", `{"purpose":"explain","provider":"anthropic","model":"claude-haiku-5-5"}`)
	if stored("purpose:explain:api_key") {
		t.Fatal("an override switched to the default provider kept its own old key: it would block the default key")
	}
	ok("investigate openai with its key", fmt.Sprintf(`{"purpose":"investigate","provider":"openai","model":"gpt-5","api_key":%q}`, explainKey))
	ok("investigate -> ollama (no key needed)", `{"purpose":"investigate","provider":"ollama","model":"llama"}`)
	if stored("purpose:investigate:api_key") {
		t.Fatal("a switch to a keyless provider kept the old vendor's key")
	}

	ok("default -> ollama (no key needed)", `{"provider":"ollama","model":"llama"}`)
	if stored("api_key") {
		t.Fatal("a default switch to a keyless provider kept the old vendor's key")
	}
	venueoracle.WriteGoOnlyProof(t, "CHAOS-13096 provider change and key: Python has no such rule")
}
