package acr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	testOrg    = "6f1c2a52-6d0e-4a5e-9d57-3f4b8d1a7c11"
	testKey    = "sk-live-VERY-SECRET-key-material"
	testToken  = "svc_acr_valid-token-value-0123456789"
	testCredID = "cred-1"
)

type fakeVerifier struct {
	err      error
	gotToken string
	gotScope string
}

func (f *fakeVerifier) Verify(_ context.Context, token, scope string) (string, error) {
	f.gotToken, f.gotScope = token, scope
	if token != testToken && f.err == nil {
		return "", ErrCredentialInvalid
	}
	return testCredID, f.err
}

type fakeSource struct {
	cfg   llmorgsettings.OrgConfig
	found bool
	err   error
	got   string
}

func (f *fakeSource) OrgConfig(_ context.Context, orgID string) (llmorgsettings.OrgConfig, bool, error) {
	f.got = orgID
	return f.cfg, f.found, f.err
}

func configured() *fakeSource {
	return &fakeSource{found: true, cfg: llmorgsettings.OrgConfig{
		Provider: "openai", Model: "gpt-5-mini", BaseURL: "https://api.openai.com/v1",
		APIKey: secrets.NewHidden(testKey), Version: "2026-10-16T18:00:00Z",
	}}
}

func call(t *testing.T, v CredentialVerifier, s LLMConfigSource, logs io.Writer, org, auth string) (*http.Response, string) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Logger: logger, LLMConfig: s, Credentials: v}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/internal/acr/orgs/"+org+"/llm-config", nil)
	if auth != "" {
		request.Header.Set("Authorization", auth)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	response := recorder.Result()
	body, _ := io.ReadAll(response.Body)
	return response, string(body)
}

func TestLLMConfigServesTheStoredConfigWithTheKey(t *testing.T) {
	verifier, source := &fakeVerifier{}, configured()
	response, body := call(t, verifier, source, io.Discard, testOrg, "Bearer "+testToken)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, body)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"schema_version": "acr_llm_config.v1", "org_id": testOrg, "provider": "openai", "model": "gpt-5-mini",
		"base_url": "https://api.openai.com/v1", "api_key": testKey, "version": "2026-10-16T18:00:00Z",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("body = %v, want %v", got, want)
	}
	if verifier.gotScope != LLMConfigScope || verifier.gotToken != testToken {
		t.Fatalf("verifier got token=%q scope=%q", verifier.gotToken, verifier.gotScope)
	}
	if source.got != testOrg {
		t.Fatalf("source asked for %q", source.got)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") || response.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("headers = %v", response.Header)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("the key response must be no-store")
	}
}

func TestLLMConfigServesThePromptCacheOptInOnlyWhenOptedIn(t *testing.T) {
	for _, tc := range []struct {
		name     string
		optedIn  bool
		wantKey  bool
		wantBool bool
	}{
		{"opted in", true, true, true},
		{"not opted in: the field is omitted, not false", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := configured()
			source.cfg.PromptCache = tc.optedIn
			response, body := call(t, &fakeVerifier{}, source, io.Discard, testOrg, "Bearer "+testToken)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d body=%s", response.StatusCode, body)
			}
			var got map[string]any
			if err := json.Unmarshal([]byte(body), &got); err != nil {
				t.Fatal(err)
			}
			value, present := got["prompt_cache"]
			if present != tc.wantKey || (present && value != tc.wantBool) {
				t.Fatalf("prompt_cache = %v (present %v), want present %v value %v; body=%s", value, present, tc.wantKey, tc.wantBool, body)
			}
			if got["schema_version"] != "acr_llm_config.v1" || got["api_key"] != testKey {
				t.Fatalf("the rest of the body changed: %s", body)
			}
		})
	}
}

func TestLLMConfigRefusals(t *testing.T) {
	cases := []struct {
		name     string
		verifier CredentialVerifier
		source   LLMConfigSource
		org      string
		auth     string
		want     int
	}{
		{"missing header", &fakeVerifier{}, configured(), testOrg, "", 401},
		{"not a bearer header", &fakeVerifier{}, configured(), testOrg, "Basic abc", 401},
		{"wrong token", &fakeVerifier{}, configured(), testOrg, "Bearer svc_acr_wrong", 401},
		{"revoked or expired", &fakeVerifier{err: ErrCredentialInvalid}, configured(), testOrg, "Bearer " + testToken, 401},
		{"wrong scope", &fakeVerifier{err: ErrCredentialScope}, configured(), testOrg, "Bearer " + testToken, 403},
		{"credential lookup down", &fakeVerifier{err: errors.New("db down")}, configured(), testOrg, "Bearer " + testToken, 503},
		{"malformed org", &fakeVerifier{}, configured(), "not-a-uuid", "Bearer " + testToken, 404},
		{"no config or gate closed", &fakeVerifier{}, &fakeSource{}, testOrg, "Bearer " + testToken, 404},
		{"store down", &fakeVerifier{}, &fakeSource{err: errors.New("db down")}, testOrg, "Bearer " + testToken, 503},
		{"stored config unreadable", &fakeVerifier{}, &fakeSource{err: fmt.Errorf("%w: setting %q", llmorgsettings.ErrStoredSettingUnreadable, "api_key")}, testOrg, "Bearer " + testToken, 409},
		{"no verifier wired", nil, configured(), testOrg, "Bearer " + testToken, 503},
		{"no source wired", &fakeVerifier{}, nil, testOrg, "Bearer " + testToken, 503},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, body := call(t, tc.verifier, tc.source, io.Discard, tc.org, tc.auth)
			if response.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", response.StatusCode, tc.want, body)
			}
			if strings.Contains(body, testKey) {
				t.Fatalf("refusal body carries the key: %s", body)
			}
			if tc.want == 401 && response.Header.Get("WWW-Authenticate") != "Bearer" {
				t.Fatal("401 must carry WWW-Authenticate")
			}
		})
	}
}

func TestLLMConfigNeverReachesAnyLog(t *testing.T) {
	var logs bytes.Buffer
	leaky := errors.New("dial postgres://user:" + testKey + "@db: refused")
	for _, source := range []*fakeSource{configured(), {}, {err: leaky}} {
		call(t, &fakeVerifier{}, source, &logs, testOrg, "Bearer "+testToken)
	}
	call(t, &fakeVerifier{err: leaky}, configured(), &logs, testOrg, "Bearer "+testToken)
	call(t, &fakeVerifier{}, configured(), &logs, testOrg, "Bearer svc_acr_wrong-token")
	if logs.Len() == 0 {
		t.Fatal("no log output captured: the test measured nothing")
	}
	for _, secret := range []string{testKey, testToken, "svc_acr_wrong-token"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("log output carries a secret: %s", logs.String())
		}
	}
	if !strings.Contains(logs.String(), testOrg) || !strings.Contains(logs.String(), "2026-10-16T18:00:00Z") {
		t.Fatalf("served line lacks org id and version: %s", logs.String())
	}
}

func TestOrgConfigStructRedactsTheKeyInLogsAndJSON(t *testing.T) {
	cfg := configured().cfg
	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("x", slog.Any("cfg", cfg))
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{logs.String(), string(encoded), fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg)} {
		if strings.Contains(out, testKey) {
			t.Fatalf("OrgConfig leaked the key: %s", out)
		}
	}
}

func TestLLMConfigResponseMarshalsTheKey(t *testing.T) {
	encoded, err := json.Marshal(llmConfigBody(testOrg, configured().cfg))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"api_key":"`+testKey+`"`) {
		t.Fatalf("response lost the key: %s", encoded)
	}
}

func TestLLMConfigCountsEachOutcome(t *testing.T) {
	outcomes := func(t *testing.T) map[string]int64 {
		return counterByAttribute(t, "devhealth_acr_llm_config_requests_total", func(p metricdata.DataPoint[int64]) (string, bool) {
			value, _ := p.Attributes.Value("outcome")
			return value.AsString(), true
		})
	}
	bearer := "Bearer " + testToken
	unreadable := fmt.Errorf("%w: setting %q", llmorgsettings.ErrStoredSettingUnreadable, "api_key")
	cases := []struct {
		name     string
		outcome  string
		verifier CredentialVerifier
		source   LLMConfigSource
		org      string
		auth     string
	}{
		{"served", "served", &fakeVerifier{}, configured(), testOrg, bearer},
		{"unauthorized_missing", "unauthorized_missing", &fakeVerifier{}, configured(), testOrg, ""},
		{"unauthorized_invalid", "unauthorized_invalid", &fakeVerifier{err: ErrCredentialInvalid}, configured(), testOrg, bearer},
		{"forbidden", "forbidden", &fakeVerifier{err: ErrCredentialScope}, configured(), testOrg, bearer},
		{"credential_unavailable", "credential_unavailable", &fakeVerifier{err: errors.New("db down")}, configured(), testOrg, bearer},
		{"bad_org", "bad_org", &fakeVerifier{}, configured(), "not-a-uuid", bearer},
		{"unreadable", "unreadable", &fakeVerifier{}, &fakeSource{err: unreadable}, testOrg, bearer},
		{"store_unavailable", "store_unavailable", &fakeVerifier{}, &fakeSource{err: errors.New("db down")}, testOrg, bearer},
		{"not_found", "not_found", &fakeVerifier{}, &fakeSource{}, testOrg, bearer},
		{"byo_base_url_carries_key", "byo_base_url_carries_key", &fakeVerifier{}, sourceWithBaseURL("https://p.example.invalid/" + testKey), testOrg, bearer},
		{"unconfigured", "unconfigured", nil, configured(), testOrg, bearer},
	}
	acrMetricReader()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := outcomes(t)
			call(t, tc.verifier, tc.source, io.Discard, tc.org, tc.auth)
			after := outcomes(t)
			for label, count := range after {
				want := int64(0)
				if label == tc.outcome {
					want = 1
				}
				if got := count - before[label]; got != want {
					t.Errorf("outcome %q moved by %d, want %d", label, got, want)
				}
			}
			if after[tc.outcome]-before[tc.outcome] != 1 {
				t.Fatalf("outcome %q not counted: before %v after %v", tc.outcome, before, after)
			}
		})
	}
}

// The categorize worker route no longer exists: an org's BYO config never
// reaches categorization.
func TestWorkerCategorizeRouteIsGone(t *testing.T) {
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LLMConfig: configured(), Credentials: &fakeVerifier{},
	}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/internal/workers/orgs/"+testOrg+"/llm-config/categorize", nil)
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("the categorize route answered %d, want 404", recorder.Code)
	}
}

// The no-config 404 carries the marker a worker needs to tell it from a missing
// route; a bad org id's 404 does not.
func TestNoConfigNotFoundIsMarked(t *testing.T) {
	source := &fakeSource{found: false}
	response, _ := call(t, &fakeVerifier{}, source, io.Discard, testOrg, "Bearer "+testToken)
	if response.StatusCode != http.StatusNotFound || response.Header.Get(NoConfigHeader) != NoConfigValue {
		t.Fatalf("no config: status %d header %q", response.StatusCode, response.Header.Get(NoConfigHeader))
	}
	response, _ = call(t, &fakeVerifier{}, source, io.Discard, "not-a-uuid", "Bearer "+testToken)
	if response.StatusCode != http.StatusNotFound || response.Header.Get(NoConfigHeader) != "" {
		t.Fatalf("bad org: status %d header %q", response.StatusCode, response.Header.Get(NoConfigHeader))
	}
}

func TestLLMConfigNamesTheClosedGateInTheLog(t *testing.T) {
	cases := []struct {
		name   string
		source *fakeSource
		want   string
	}{
		{"gate closed", &fakeSource{cfg: llmorgsettings.OrgConfig{GateClosed: true}}, `"reason":"byo_llm_gate_closed"`},
		{"nothing stored", &fakeSource{}, `"reason":"none_configured"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			response, body := call(t, &fakeVerifier{}, tc.source, &logs, testOrg, "Bearer "+testToken)
			if response.StatusCode != 404 || response.Header.Get(NoConfigHeader) != NoConfigValue {
				t.Fatalf("status %d marker %q, want the marked 404", response.StatusCode, response.Header.Get(NoConfigHeader))
			}
			if !strings.Contains(logs.String(), tc.want) || strings.Contains(body, testKey) {
				t.Fatalf("log %q lacks %s", logs.String(), tc.want)
			}
		})
	}
}
