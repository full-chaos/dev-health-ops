//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// fakeReadinessDoer is providerfoundation.HTTPDoer without importing that
// package's own test-only helper: a plain function value satisfies the
// one-method interface directly.
type fakeReadinessDoer func(*http.Request) (*http.Response, error)

func (f fakeReadinessDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

const readyRound1Body = `{"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,` +
	`"tool_calls":[{"id":"route-test-call","type":"function","function":{"name":"readiness_echo_v1","arguments":"{\"nonce\":\"ready-v1\"}"}}]}}]}`

const readyRound2Body = `{"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant",` +
	`"content":"{\"kind\":\"final_answer\",\"value\":{\"nonce\":\"ready-v1\"}}"}}]}`

// readyDoer answers every request as a successful readiness_echo round trip
// (round 1 vs round 2 told apart by whether the request body carries a
// "tool" role message, same detector the wire-level oracle stub uses).
func readyDoer() fakeReadinessDoer {
	return func(req *http.Request) (*http.Response, error) {
		defer req.Body.Close()
		raw, _ := io.ReadAll(req.Body)
		var decoded struct {
			Messages []struct {
				Role string `json:"role"`
			} `json:"messages"`
		}
		_ = json.Unmarshal(raw, &decoded)
		round2 := false
		for _, m := range decoded.Messages {
			if m.Role == "tool" {
				round2 = true
			}
		}
		if round2 {
			return jsonResponse(200, readyRound2Body), nil
		}
		return jsonResponse(200, readyRound1Body), nil
	}
}

// failingDoer answers every request with a permanent, non-retryable
// failure (401), certifying "failed"/"provider_not_configured".
func failingDoer() fakeReadinessDoer {
	return func(*http.Request) (*http.Response, error) {
		return jsonResponse(401, `{"error":{"type":"invalid_request_error","code":"invalid_api_key","message":"Incorrect API key provided"}}`), nil
	}
}

// poisonDoer records whether it was ever invoked, for the SSRF route-wiring
// subtest below. TestReadinessSSRFGuardCoversEveryPythonRefusalClass
// (llmreadinessprobe_ssrf_test.go) proves llmorgsettings.ValidateBaseURLChecked
// itself refuses every Python refusal class, called DIRECTLY -- codex r2's P3
// finding is that nothing proves the ROUTE still calls that guard: a route
// that stopped calling it, or called it and ignored the result, would not be
// caught by that unit test alone. The route's HTTP handler runs the request
// on the http.Server's own goroutine, not the test's, so this cannot call
// t.Fatalf directly (testing.T requires FailNow on the test's own
// goroutine) -- it records the request URL on an atomic pointer instead, and
// the test asserts it's still nil AFTER the POST call returns.
type poisonDoer struct {
	invokedURL atomic.Pointer[string]
}

func (p *poisonDoer) Do(req *http.Request) (*http.Response, error) {
	url := req.URL.String()
	p.invokedURL.Store(&url)
	// Answer as if the SSRF target were "ready" -- the worst case, proving
	// this isn't refused only by accident of an otherwise-unreachable stub.
	return jsonResponse(200, readyRound1Body), nil
}

// TestLLMSettingsReadinessRouteSuccessPath is D2908 condition 3 (the P3
// r1 named: "0.0% coverage" on postLLMSettingsReadiness/
// loadReadinessBYOConfig). It is a Go-only proof (no Python response is
// compared -- see venueoracle.WriteGoOnlyProof): the wire-fidelity of the
// probe itself is ALREADY the differential oracle's job
// (llmreadinessprobe_live_python_oracle_test.go); this test's job is the
// ROUTE HANDLER's own plumbing -- read the saved BYO config, run the
// prober, persist the outcome, and hand it back -- against a REAL
// Postgres, through the REAL HTTP route, twice: once for the POST itself,
// once more for a SEPARATE GET /llm-settings/status call proving the
// record the POST wrote is what a later read actually sees.
//
// The prober's own HTTP transport is faked via Deps.HTTPDoer (threaded to
// handlers.upstreamDoer, the same injection point pagerduty_oauth_callback.go's
// calls already use -- see newOpenAICompatibleReadinessProber's doc
// comment) rather than a real network call: base_url is set to
// https://example.invalid/v1, which llmorgsettings.ValidateBaseURLChecked
// (the route's own SSRF gate, unchanged and already live-python-oracled)
// accepts as an unresolvable-therefore-not-an-SSRF-target name -- see
// TestReadinessSSRFGuardAcceptsAnUnresolvableName -- so the gate is
// exercised for real, not bypassed, while the actual network round trip
// is the fake.
func TestLLMSettingsReadinessRouteSuccessPath(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-readiness-32-bytes!"

	readyOrgID, failOrgID, ssrfOrgID := uuid.New(), uuid.New(), uuid.New()
	adminUserID := uuid.New()

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
			for slug, id := range map[string]uuid.UUID{"ready": readyOrgID, "fail": failOrgID, "ssrf": ssrfOrgID} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, id, "llmreadiness-route-"+slug)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, adminUserID, "llmreadiness-route-admin@example.com")

			row := func(org uuid.UUID, key, value string) {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, 'llm', $3, $4, false, NULL, now(), now())`, uuid.New(), org.String(), key, value)
			}
			for _, org := range []uuid.UUID{readyOrgID, failOrgID} {
				row(org, "provider", "openai")
				row(org, "model", "gpt-5-mini")
				row(org, "api_key", "sk-route-test")
				row(org, "base_url", "https://example.invalid/v1")
			}
			// ssrfOrgID: an otherwise-valid config (provider/model/api_key all
			// set) whose base_url is a real SSRF refusal class -- IP-literal
			// loopback, class 8 of TestReadinessSSRFGuardCoversEveryPythonRefusalClass
			// -- proving the ROUTE's own call site still blocks it end to end.
			row(ssrfOrgID, "provider", "openai")
			row(ssrfOrgID, "model", "gpt-5-mini")
			row(ssrfOrgID, "api_key", "sk-route-test")
			row(ssrfOrgID, "base_url", "https://127.0.0.1/v1")

			tokens := map[string]map[string]any{}
			for slug, org := range map[string]uuid.UUID{"ready": readyOrgID, "fail": failOrgID, "ssrf": ssrfOrgID} {
				tokens[slug] = map[string]any{"user_id": adminUserID.String(), "email": "llmreadiness-route-admin@example.com", "org_id": org.String(), "role": "admin"}
			}
			return tokens
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}

	t.Run("ready", func(t *testing.T) {
		base, _ := startGoServer(t, ctx, venue, jwtKey, func(d *apiservice.Deps) { d.HTTPDoer = readyDoer() })

		postResp := doRouteRequest(t, base, "POST", "/api/v1/admin/llm-settings/readiness", auth("ready"))
		if postResp.status != 200 {
			t.Fatalf("POST readiness: status=%d body=%s", postResp.status, postResp.body)
		}
		var postDecoded map[string]any
		if err := json.Unmarshal([]byte(postResp.body), &postDecoded); err != nil {
			t.Fatalf("decode POST body: %v\n%s", err, postResp.body)
		}
		if postDecoded["readiness"] != "ready" {
			t.Fatalf("POST readiness field = %v, want %q: %s", postDecoded["readiness"], "ready", postResp.body)
		}
		if postDecoded["readiness_safe_failure_reason"] != nil {
			t.Fatalf("POST readiness_safe_failure_reason = %v, want null: %s", postDecoded["readiness_safe_failure_reason"], postResp.body)
		}
		checkedAt, _ := postDecoded["readiness_checked_at"].(string)
		if checkedAt == "" {
			t.Fatalf("POST readiness_checked_at is empty: %s", postResp.body)
		}

		// The persistence + readback proof: a SEPARATE GET call, its own
		// HTTP round trip against the SAME real Postgres row the POST
		// above wrote, must see the identical outcome.
		getResp := doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/status", auth("ready"))
		if getResp.status != 200 {
			t.Fatalf("GET status: status=%d body=%s", getResp.status, getResp.body)
		}
		var getDecoded map[string]any
		if err := json.Unmarshal([]byte(getResp.body), &getDecoded); err != nil {
			t.Fatalf("decode GET body: %v\n%s", err, getResp.body)
		}
		if getDecoded["readiness"] != "ready" {
			t.Fatalf("GET status readiness = %v, want %q (persisted by the POST above): %s", getDecoded["readiness"], "ready", getResp.body)
		}
		if getDecoded["readiness_checked_at"] != checkedAt {
			t.Fatalf("GET status readiness_checked_at = %v, want the POST's own %q (readback of the SAME row): %s", getDecoded["readiness_checked_at"], checkedAt, getResp.body)
		}

		// Codex r3 P1 (CHAOS-6976): a certified "ready" record must stop
		// being reported as ready once the credential it certified has
		// changed underneath it. Rotate the org's api_key directly (same
		// shape the seed row above used: category=llm, is_encrypted=false)
		// and prove a SUBSEQUENT GET no longer reports the stale "ready".
		admin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
		if err != nil {
			t.Fatalf("open admin pool for credential rotation: %v", err)
		}
		defer admin.Close()
		if _, err := admin.Exec(ctx,
			`UPDATE settings SET value = $1, updated_at = now() WHERE org_id = $2 AND category = 'llm' AND key = 'api_key'`,
			"sk-route-test-ROTATED", readyOrgID.String(),
		); err != nil {
			t.Fatalf("rotate api_key: %v", err)
		}

		staleResp := doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/status", auth("ready"))
		if staleResp.status != 200 {
			t.Fatalf("GET status after credential rotation: status=%d body=%s", staleResp.status, staleResp.body)
		}
		var staleDecoded map[string]any
		if err := json.Unmarshal([]byte(staleResp.body), &staleDecoded); err != nil {
			t.Fatalf("decode post-rotation GET body: %v\n%s", err, staleResp.body)
		}
		if staleDecoded["readiness"] == "ready" {
			t.Fatalf("GET status still reports readiness=ready after the certified BYO api_key changed (stale certification not detected): %s", staleResp.body)
		}
	})

	t.Run("failed", func(t *testing.T) {
		base, _ := startGoServer(t, ctx, venue, jwtKey, func(d *apiservice.Deps) { d.HTTPDoer = failingDoer() })

		postResp := doRouteRequest(t, base, "POST", "/api/v1/admin/llm-settings/readiness", auth("fail"))
		if postResp.status != 200 {
			t.Fatalf("POST readiness: status=%d body=%s", postResp.status, postResp.body)
		}
		var postDecoded map[string]any
		if err := json.Unmarshal([]byte(postResp.body), &postDecoded); err != nil {
			t.Fatalf("decode POST body: %v\n%s", err, postResp.body)
		}
		if postDecoded["readiness"] != "failed" {
			t.Fatalf("POST readiness field = %v, want %q: %s", postDecoded["readiness"], "failed", postResp.body)
		}
		reason, _ := postDecoded["readiness_safe_failure_reason"].(string)
		if reason == "" {
			t.Fatalf("POST readiness_safe_failure_reason is empty on a failed outcome: %s", postResp.body)
		}

		getResp := doRouteRequest(t, base, "GET", "/api/v1/admin/llm-settings/status", auth("fail"))
		if getResp.status != 200 {
			t.Fatalf("GET status: status=%d body=%s", getResp.status, getResp.body)
		}
		var getDecoded map[string]any
		if err := json.Unmarshal([]byte(getResp.body), &getDecoded); err != nil {
			t.Fatalf("decode GET body: %v\n%s", err, getResp.body)
		}
		if getDecoded["readiness"] != "failed" {
			t.Fatalf("GET status readiness = %v, want %q (persisted by the POST above): %s", getDecoded["readiness"], "failed", getResp.body)
		}
		if getDecoded["readiness_safe_failure_reason"] != reason {
			t.Fatalf("GET status readiness_safe_failure_reason = %v, want the POST's own %q (readback of the SAME row): %s", getDecoded["readiness_safe_failure_reason"], reason, getResp.body)
		}
	})

	t.Run("ssrf_refused_base_url_never_reaches_the_network", func(t *testing.T) {
		doer := &poisonDoer{}
		base, _ := startGoServer(t, ctx, venue, jwtKey, func(d *apiservice.Deps) { d.HTTPDoer = doer })

		postResp := doRouteRequest(t, base, "POST", "/api/v1/admin/llm-settings/readiness", auth("ssrf"))
		if postResp.status != 404 {
			t.Fatalf("POST readiness with an SSRF-refused base_url: status=%d body=%s, want 404 (guard must refuse before any network call)", postResp.status, postResp.body)
		}
		var postDecoded map[string]any
		if err := json.Unmarshal([]byte(postResp.body), &postDecoded); err != nil {
			t.Fatalf("decode POST body: %v\n%s", err, postResp.body)
		}
		const wantDetail = "No BYO LLM configuration is saved for this organization."
		if postDecoded["detail"] != wantDetail {
			t.Fatalf("POST readiness detail = %v, want %q: %s", postDecoded["detail"], wantDetail, postResp.body)
		}
		if invoked := doer.invokedURL.Load(); invoked != nil {
			t.Fatalf("SSRF guard did not block the route: the prober reached the network at %s", *invoked)
		}
	})

	venueoracle.WriteGoOnlyProof(t, "route-handler plumbing only (read config, call the prober, persist, read back) -- "+
		"the probe's own wire fidelity vs live Python is llmreadinessprobe_live_python_oracle_test.go's job, not this test's")
}

type routeResponse struct {
	status int
	body   string
}

func doRouteRequest(t *testing.T, base, method, path string, headers map[string]string) routeResponse {
	t.Helper()
	req, err := http.NewRequest(method, base+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return routeResponse{status: resp.StatusCode, body: string(body)}
}
