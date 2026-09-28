//go:build integration

package admin_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	adminsvc "github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestLLMSettingsStatusRouteVenueOracle is the venue-oracle proof for GET
// /api/v1/admin/llm-settings/status (CHAOS-6252a). It covers every base-field
// state from evaluate_org_llm_status (not_configured/unknown_provider/
// missing_credentials/invalid_base_url/active), every gate (tier, flag kill
// switch, downgrade), and the identity/org-shape refusals -- all of which
// this port reproduces byte-for-byte against live Python.
//
// The readiness/binary_transport_readiness/readiness_checked_at/
// readiness_safe_failure_reason fields are DELIBERATELY normalized out of the
// two "readiness record" cases below (D2715 ruling): this port surfaces the
// persisted ask_dev_agent_readiness record read-only, trusting it only while
// its fingerprint equals this port's credential-only fingerprint of the org's
// current BYO config (a mismatch is never_checked here, "stale" in Python --
// a named divergence asserted below). Python's full currency/role-
// certification state machine is Ask Dev-role machinery
// (CHAOS-6252b, folded into the CHAOS-6262 Ask Dev deletion class; prod Ask
// Dev is OFF until the MCP project lands). A synthetic seeded record cannot
// satisfy Python's real currency check, so Python and Go are EXPECTED to
// disagree on those four fields for those two cases specifically -- normalized
// out here, then asserted directly (Inspect) against this port's own
// documented, narrower contract instead.
func TestLLMSettingsStatusRouteVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-status-32-bytes!"

	adminID, memberID, superID := uuid.New(), uuid.New(), uuid.New()
	type orgSpec struct {
		id   uuid.UUID
		slug string
		tier string
	}
	orgs := map[string]*orgSpec{}
	for _, spec := range []orgSpec{
		{slug: "empty", tier: "team"},          // not_configured
		{slug: "unknown", tier: "team"},        // unknown_provider
		{slug: "missing", tier: "team"},        // missing_credentials
		{slug: "invalidurl", tier: "team"},     // invalid_base_url (SSRF)
		{slug: "active", tier: "team"},         // active, no readiness row -> never_checked
		{slug: "ready", tier: "team"},          // active, readiness=ready
		{slug: "failed", tier: "team"},         // active, readiness=failed
		{slug: "community", tier: "community"}, // tier gate, 402
		{slug: "off", tier: "team"},            // flag kill switch, 403
		{slug: "mismatch", tier: "team"},       // active, readiness record certified against a DIFFERENT config -> never_checked here (Python: stale)
		{slug: "incomplete", tier: "team"},     // active, readiness blob missing required keys -> never_checked
		{slug: "fallback", tier: "team"},       // invalid_base_url with a matching audit_logs fallback row -> last_fallback_at set
	} {
		spec := spec
		spec.id = uuid.New()
		orgs[spec.slug] = &spec
	}
	ghostOrg := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
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
			for _, org := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, now(), now())`, org.id, "llmstatus-"+org.slug, org.tier)
			}
			exec(`UPDATE feature_flags SET created_at = '2020-01-01T00:00:00+00:00', updated_at = '2020-01-01T00:00:00+00:00'`)
			exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags WHERE key = 'byo_llm'), false, NULL, NULL, 'kill switch', NULL, now(), now())`,
				uuid.New(), orgs["off"].id)
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(adminID, "llmstatus-admin@example.com", false)
			user(memberID, "llmstatus-member@example.com", false)
			user(superID, "llmstatus-super@example.com", true)

			row := func(org, category, key, value string) {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, false, NULL, '2026-02-01T00:00:00+00:00', '2026-02-01T00:00:00+00:00')`,
					uuid.New(), orgs[org].id.String(), category, key, value)
			}
			row("unknown", "llm", "provider", "not-a-real-provider")
			row("missing", "llm", "provider", "openai")
			row("invalidurl", "llm", "provider", "openai")
			row("invalidurl", "llm", "base_url", "https://10.0.0.1/v1")
			row("invalidurl", "llm", "api_key", "sk-anything")
			row("active", "llm", "provider", "openai")
			row("active", "llm", "api_key", "sk-anything")
			row("ready", "llm", "provider", "openai")
			row("ready", "llm", "api_key", "sk-anything")
			row("failed", "llm", "provider", "openai")
			row("failed", "llm", "api_key", "sk-anything")
			row("mismatch", "llm", "provider", "openai")
			row("mismatch", "llm", "api_key", "sk-anything")

			// The Go port only trusts a record whose fingerprint equals the
			// org's CURRENT BYO config (codex r3 P1), so the seeded ready/failed
			// records carry that fingerprint (provider openai, api_key
			// sk-anything, no model/base_url) -- the same value a real
			// POST /llm-settings/readiness would have stored. The mismatch org
			// carries a fingerprint that does not match.
			currentFingerprint := adminsvc.ReadinessFingerprint("openai", "", "", "sk-anything")
			readinessRecord := func(fingerprint, outcome, safeErrorCode string) string {
				payload := map[string]any{
					"fingerprint":       fingerprint,
					"readiness_version": "venue-oracle-synthetic-version",
					"checked_at":        "2026-01-15T12:00:00+00:00",
					"outcome":           outcome,
					"safe_error_code":   nil,
				}
				if safeErrorCode != "" {
					payload["safe_error_code"] = safeErrorCode
				}
				encoded, err := json.Marshal(payload)
				if err != nil {
					t.Fatalf("encode readiness record: %v", err)
				}
				return string(encoded)
			}
			row("ready", "llm", "ask_dev_agent_readiness", readinessRecord(currentFingerprint, "ready", ""))
			row("failed", "llm", "ask_dev_agent_readiness", readinessRecord(currentFingerprint, "failed", "provider_unavailable"))
			row("mismatch", "llm", "ask_dev_agent_readiness", readinessRecord("venue-oracle-stale-fingerprint", "ready", ""))

			// round-1 review finding (codex, P2): a readiness blob missing
			// fingerprint/readiness_version/checked_at must be treated as
			// absent (never_checked), matching Python's load() raising
			// KeyError on the missing dict key -- not merely "falsy", so a
			// present empty string would NOT trip this, only an absent key.
			row("incomplete", "llm", "provider", "openai")
			row("incomplete", "llm", "api_key", "sk-anything")
			incompleteRecord, err := json.Marshal(map[string]any{"outcome": "ready"})
			if err != nil {
				t.Fatalf("encode incomplete readiness record: %v", err)
			}
			row("incomplete", "llm", "ask_dev_agent_readiness", string(incompleteRecord))

			// round-1 review finding (codex, P2): last_fallback_at was never
			// exercised positively (no seeded audit_logs row), so a wrong
			// resource_id/JSON key/window/predicate would still pass. Seed a
			// matching fallback audit row: same provider/base_url_hash/
			// reason_code credentials.py's own _audit_changes_match compares.
			fallbackBaseURL := "https://10.0.0.1/v1"
			row("fallback", "llm", "provider", "openai")
			row("fallback", "llm", "base_url", fallbackBaseURL)
			row("fallback", "llm", "api_key", "sk-anything")
			fallbackHashSum := sha256.Sum256([]byte(fallbackBaseURL))
			fallbackBaseURLHash := hex.EncodeToString(fallbackHashSum[:])[:16]
			fallbackChanges, err := json.Marshal(map[string]any{
				"provider":      "openai",
				"base_url":      fallbackBaseURL,
				"base_url_hash": fallbackBaseURLHash,
				"reason":        "LLM base_url host resolves to a non-public address",
				"reason_code":   "invalid_base_url",
			})
			if err != nil {
				t.Fatalf("encode fallback audit changes: %v", err)
			}
			exec(`INSERT INTO audit_logs (id, org_id, action, resource_type, resource_id, changes, request_metadata, status, created_at)
VALUES ($1, $2, 'other', 'setting', 'llm.base_url', $3::json, '{}'::json, 'failure', now() - interval '1 hour')`,
				uuid.New(), orgs["fallback"].id.String(), string(fallbackChanges))

			tokens := map[string]map[string]any{
				"member":  {"user_id": memberID.String(), "email": "llmstatus-member@example.com", "org_id": orgs["active"].id.String(), "role": "member"},
				"super":   {"user_id": superID.String(), "email": "llmstatus-super@example.com", "is_superuser": true},
				"notuuid": {"user_id": adminID.String(), "email": "llmstatus-admin@example.com", "org_id": "not-a-uuid", "role": "admin"},
				"ghost":   {"user_id": adminID.String(), "email": "llmstatus-admin@example.com", "org_id": ghostOrg.String(), "role": "admin"},
			}
			for slug, org := range orgs {
				tokens[slug] = map[string]any{"user_id": adminID.String(), "email": "llmstatus-admin@example.com", "org_id": org.id.String(), "role": "admin"}
			}
			return tokens
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	const path = "/api/v1/admin/llm-settings/status"
	get := func(name, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: path, Headers: auth(token)}
	}

	requests := []venueoracle.Request{
		get("not configured", "empty"),
		get("unknown provider", "unknown"),
		get("missing credentials", "missing"),
		get("invalid base_url", "invalidurl"),
		get("active never_checked", "active"),
		get("readiness record ready", "ready"),
		get("readiness record failed", "failed"),
		get("readiness record fingerprint mismatch (python stale, go never_checked)", "mismatch"),
		get("community tier gate", "community"),
		get("kill switch gate", "off"),
		get("incomplete readiness blob never_checked", "incomplete"),
		get("invalid base_url with fallback audit row", "fallback"),
		get("org not a uuid", "notuuid"),
		get("org missing", "ghost"),
		get("superuser without org", "super"),
		get("member refused", "member"),
		{Name: "unauthenticated", Method: "GET", Path: path},
		{Name: "post is 405", Method: "POST", Path: path, Headers: auth("active")},
	}

	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {})
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			if strings.HasPrefix(request.Name, "readiness record ") {
				for _, field := range []string{"readiness", "binary_transport_readiness", "readiness_checked_at", "readiness_safe_failure_reason"} {
					body = redactField(t, body, field)
				}
			}
			return body
		},
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			switch request.Name {
			case "readiness record ready":
				assertJSONField(t, request.Name, goResponse.Body, "readiness", "ready")
				assertJSONField(t, request.Name, goResponse.Body, "binary_transport_readiness", "ready")
				assertJSONField(t, request.Name, goResponse.Body, "readiness_checked_at", "2026-01-15T12:00:00Z")
				assertJSONField(t, request.Name, goResponse.Body, "readiness_safe_failure_reason", nil)
			case "readiness record fingerprint mismatch (python stale, go never_checked)":
				// NAMED DIVERGENCE from Python, which answers "stale" for a
				// record whose fingerprint no longer matches (settings.py
				// is_current). This port has no "stale" state (CHAOS-6252b) and
				// reports never_checked instead, so a superseded certification
				// is never displayed as current.
				assertJSONField(t, request.Name, goResponse.Body, "readiness", "never_checked")
				assertJSONField(t, request.Name, goResponse.Body, "binary_transport_readiness", "never_checked")
				assertJSONField(t, request.Name, goResponse.Body, "readiness_checked_at", nil)
				assertJSONField(t, request.Name, goResponse.Body, "readiness_safe_failure_reason", nil)
			case "readiness record failed":
				assertJSONField(t, request.Name, goResponse.Body, "readiness", "failed")
				assertJSONField(t, request.Name, goResponse.Body, "binary_transport_readiness", "failed")
				assertJSONField(t, request.Name, goResponse.Body, "readiness_checked_at", "2026-01-15T12:00:00Z")
				assertJSONField(t, request.Name, goResponse.Body, "readiness_safe_failure_reason", "The configured Ask Dev model endpoint is unavailable.")
			}
		},
	})
	t.Log(receipt)
}

// assertJSONField decodes body and checks one top-level field against want
// (nil means the field must be JSON null).
func assertJSONField(t *testing.T, name, body, key string, want any) {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("%s: decode go response: %v\n%s", name, err, body)
	}
	got, present := decoded[key]
	if want == nil {
		if present && got != nil {
			t.Errorf("%s: %s = %v, want null", name, key, got)
		}
		return
	}
	if got != want {
		t.Errorf("%s: %s = %v, want %v", name, key, got, want)
	}
}
