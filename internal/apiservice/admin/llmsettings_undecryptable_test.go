//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestLLMSettingsUndecryptableAfterKeyRotation is CHAOS-12172 over the real
// route and real Postgres. One server writes the BYO settings under one
// SETTINGS_ENCRYPTION_KEY; a second server on the same database runs with a
// replaced key, as after a rotation or a restore. Its reads must answer the
// distinct 409 llm_settings_undecryptable (naming the settings, never a
// value), not the generic 500; Remove and Replace must work without the old
// value; a server with no key at all stays a 500. Go-only: Python has no such
// code.
func TestLLMSettingsUndecryptableAfterKeyRotation(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-rotation-32-bytes!"
	const (
		oldDefault = "placeholder-old-default-value"
		oldExplain = "placeholder-old-explain-value"
		newDefault = "placeholder-new-default-value"
		rotatedKey = "venue-llm-settings-rotated-fernet-key!!"
	)
	orgReplace, orgRemove, adminUserID := uuid.New(), uuid.New(), uuid.New()

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
			for slug, id := range map[string]uuid.UUID{"replace": orgReplace, "remove": orgRemove} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, id, "llmrotation-"+slug)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'llmrotation-admin@example.com', true, true, false, 0, now(), now())`, adminUserID)
			tokens := map[string]map[string]any{}
			for slug, org := range map[string]uuid.UUID{"replace": orgReplace, "remove": orgRemove} {
				tokens[slug] = map[string]any{"user_id": adminUserID.String(), "email": "llmrotation-admin@example.com", "org_id": org.String(), "role": "admin"}
			}
			return tokens
		},
	})
	withKey := func(key string) adminDepsExtra {
		return func(deps *apiservice.Deps) {
			decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
			if err != nil {
				t.Fatalf("build decryptor: %v", err)
			}
			deps.Decryptor = decryptor
		}
	}
	before, pool := startGoServer(t, ctx, venue, jwtKey, withKey(llmVenueEncryptionKey))
	after, _ := startGoServer(t, ctx, venue, jwtKey, withKey(rotatedKey))
	keyless, _ := startGoServer(t, ctx, venue, jwtKey)

	type reply struct {
		status int
		raw    string
		detail map[string]any
	}
	call := func(base, token, method, path, body string) reply {
		t.Helper()
		req, err := http.NewRequest(method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+venue.Tokens[token])
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out struct {
			Detail map[string]any `json:"detail"`
		}
		_ = json.Unmarshal(raw, &out)
		return reply{status: resp.StatusCode, raw: string(raw), detail: out.Detail}
	}
	storedValue := func(org uuid.UUID, key string) string {
		t.Helper()
		var value string
		if err := pool.QueryRow(ctx, `SELECT coalesce(value, '') FROM settings WHERE org_id = $1 AND category = 'llm' AND key = $2`, org.String(), key).Scan(&value); err != nil {
			return ""
		}
		return value
	}
	const route = "/api/v1/admin/llm-settings"

	// Saved under the old key: a default config and an explain override with its own key.
	for _, token := range []string{"replace", "remove"} {
		if got := call(before, token, "PUT", route, fmt.Sprintf(`{"provider":"anthropic","model":"big-model","api_key":%q}`, oldDefault)); got.status != 200 {
			t.Fatalf("%s: default PUT under the old key: %d %s", token, got.status, got.raw)
		}
		if got := call(before, token, "PUT", route, fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"small-model","api_key":%q}`, oldExplain)); got.status != 200 {
			t.Fatalf("%s: explain PUT under the old key: %d %s", token, got.status, got.raw)
		}
	}
	oldCipher := storedValue(orgReplace, "api_key")
	if oldCipher == "" || strings.Contains(oldCipher, oldDefault) {
		t.Fatalf("the default key was not stored encrypted")
	}

	assertUndecryptable := func(t *testing.T, got reply, settings, purposes []string) {
		t.Helper()
		if got.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s, want 409", got.status, got.raw)
		}
		if got.detail["error"] != "llm_settings_undecryptable" || got.detail["feature"] != "byo_llm" {
			t.Fatalf("detail=%v, want error llm_settings_undecryptable feature byo_llm", got.detail)
		}
		if got.detail["message"] != "The saved key can no longer be read. Replace or remove it." {
			t.Fatalf("message=%v", got.detail["message"])
		}
		strs := func(v any) []string {
			list, _ := v.([]any)
			out := make([]string, 0, len(list))
			for _, item := range list {
				out = append(out, fmt.Sprint(item))
			}
			return out
		}
		if gotSettings := strs(got.detail["settings"]); !slices.Equal(gotSettings, settings) {
			t.Fatalf("settings=%v, want %v", gotSettings, settings)
		}
		if gotPurposes := strs(got.detail["purposes"]); !slices.Equal(gotPurposes, purposes) {
			t.Fatalf("purposes=%v, want %v", gotPurposes, purposes)
		}
		for _, secret := range []string{oldDefault, oldExplain, oldCipher, "gAAAAA"} {
			if strings.Contains(got.raw, secret) {
				t.Fatalf("the body holds key material: %s", got.raw)
			}
		}
	}

	t.Run("after rotation a read names the unreadable settings with its own 409", func(t *testing.T) {
		assertUndecryptable(t, call(after, "replace", "GET", route, ""), []string{"api_key", "purpose:explain:api_key"}, []string{"explain"})
		assertUndecryptable(t, call(after, "replace", "GET", route+"?purpose=explain", ""), []string{"purpose:explain:api_key"}, []string{"explain"})
	})

	t.Run("every other BYO route that reads the key answers the same 409", func(t *testing.T) {
		for _, route := range []struct{ method, path string }{
			{"GET", route + "/models"},
			{"GET", route + "/status"},
			{"POST", route + "/readiness"},
			{"GET", route + "/spend"},
		} {
			got := call(after, "replace", route.method, route.path, "")
			if got.status != http.StatusConflict || got.detail["error"] != "llm_settings_undecryptable" {
				t.Errorf("%s %s: status=%d body=%s, want 409 llm_settings_undecryptable", route.method, route.path, got.status, got.raw)
			}
			for _, secret := range []string{oldDefault, oldExplain, oldCipher} {
				if strings.Contains(got.raw, secret) {
					t.Errorf("%s %s: the body holds key material", route.method, route.path)
				}
			}
		}
	})

	t.Run("a server with no key at all is still the generic 500", func(t *testing.T) {
		if got := call(keyless, "replace", "GET", route, ""); got.status != http.StatusInternalServerError {
			t.Fatalf("status=%d body=%s, want 500", got.status, got.raw)
		}
	})

	t.Run("a write that keeps the unreadable key is refused with the same 409 and changes nothing", func(t *testing.T) {
		got := call(after, "replace", "PUT", route, `{"provider":"openai","model":"other-model","base_url":"https://example.invalid/v1"}`)
		assertUndecryptable(t, got, []string{"api_key", "purpose:explain:api_key"}, []string{"explain"})
		if storedValue(orgReplace, "api_key") != oldCipher || storedValue(orgReplace, "model") != "big-model" {
			t.Fatalf("a refused write changed the stored config")
		}
	})

	t.Run("Replace: the unreadable override is removed, then a new key replaces the old one", func(t *testing.T) {
		if got := call(after, "replace", "DELETE", route+"?purpose=explain", ""); got.status != 200 {
			t.Fatalf("purpose DELETE: %d %s", got.status, got.raw)
		}
		got := call(after, "replace", "PUT", route, fmt.Sprintf(`{"provider":"openai","model":"other-model","api_key":%q,"base_url":"https://example.invalid/v1"}`, newDefault))
		if got.status != 200 {
			t.Fatalf("replace PUT: %d %s, want 200", got.status, got.raw)
		}
		read := call(after, "replace", "GET", route, "")
		if read.status != 200 || !strings.Contains(read.raw, `"provider":"openai"`) || strings.Contains(read.raw, newDefault) {
			t.Fatalf("GET after replace: %d %s", read.status, read.raw)
		}
	})

	t.Run("Remove: both deletes work without the old value", func(t *testing.T) {
		if got := call(after, "remove", "DELETE", route, ""); got.status != 200 {
			t.Fatalf("DELETE: %d %s", got.status, got.raw)
		}
		if storedValue(orgRemove, "api_key") != "" {
			t.Fatalf("the unreadable default key survived DELETE")
		}
		// The override is its own config: still unreadable until it is removed too.
		assertUndecryptable(t, call(after, "remove", "GET", route, ""), []string{"purpose:explain:api_key"}, []string{"explain"})
		if got := call(after, "remove", "DELETE", route+"?purpose=explain", ""); got.status != 200 {
			t.Fatalf("purpose DELETE: %d %s", got.status, got.raw)
		}
		if got := call(after, "remove", "GET", route, ""); got.status != 200 {
			t.Fatalf("GET after both removes: %d %s, want 200", got.status, got.raw)
		}
	})

	venueoracle.WriteGoOnlyProof(t, "CHAOS-12172: an undecryptable BYO setting answers 409 llm_settings_undecryptable; Python has no such code")
}
