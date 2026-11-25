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

// TestLLMSettingsDefaultReadListsSavedOverrides: the default read lists, under
// "purposes", each saved per-purpose override of the org, the categorize
// override of an opted-in org included.
func TestLLMSettingsDefaultReadListsSavedOverrides(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	org, adminUserID := uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   root,
		Seed: withTokenMemberships(func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			for _, stmt := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'llmlist', 'llmlist', 'team', 'stripe', true, now(), now())`, []any{org}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'llmlist-admin@example.com', true, true, false, 0, now(), now())`, []any{adminUserID}},
			} {
				if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			return map[string]map[string]any{"a": {"user_id": adminUserID.String(), "email": "llmlist-admin@example.com", "org_id": org.String(), "role": "admin"}}
		}),
	})
	base, _ := startGoServer(t, ctx, venue, func(deps *apiservice.Deps) {
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
	const route = "/api/v1/admin/llm-settings"

	if status, _ := call("PUT", route, fmt.Sprintf(`{"provider":"anthropic","model":"big-model","api_key":%q}`, defaultCred)); status != 200 {
		t.Fatalf("default PUT: %d", status)
	}
	if status, _ := call("PUT", route, `{"purpose":"explain","provider":"anthropic","model":"small-model"}`); status != 200 {
		t.Fatalf("explain PUT: %d", status)
	}
	if status, body := call("PUT", route, `{"purpose":"categorize","provider":"anthropic","model":"cat-model","categorize_byo":true}`); status != 200 {
		t.Fatalf("categorize PUT: %d %v", status, body)
	}
	if status, body := call("GET", route+"?purpose=categorize", ""); status != 200 || body["model"] != "cat-model" {
		t.Fatalf("control: GET purpose=categorize: %d %v", status, body)
	}

	status, def := call("GET", route, "")
	purposes, _ := def["purposes"].(map[string]any)
	if status != 200 || purposes["explain"] == nil {
		t.Fatalf("control: GET default lists the explain override: %d %v", status, def)
	}
	if purposes["categorize"] == nil {
		t.Errorf("GET default purposes has no categorize override that PUT stored and GET purpose=categorize serves: purposes=%v", purposes)
	}

	venueoracle.WriteGoOnlyProof(t, "default read lists the saved categorize override: Python has no purposes")
}
