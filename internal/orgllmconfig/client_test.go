package orgllmconfig

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

const (
	org   = "6f1c2a52-6d0e-4a5e-9d57-3f4b8d1a7c11"
	token = "svc_wllm_test-token-value-0123456789"
	key   = "placeholder-org-key-value"
)

func serve(t *testing.T, status int, body string) (*Client, *http.Request) {
	t.Helper()
	var seen http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = *r.Clone(r.Context())
		if status == 404 && strings.Contains(body, "no-config") {
			w.Header().Set("X-Org-Llm-Config", "none")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, err := New(server.URL, secrets.NewHidden(token), nil)
	if err != nil {
		t.Fatal(err)
	}
	// seen is read after Fetch returns.
	return client, &seen
}

func TestFetchServesTheResolvedConfigAndSendsTheWorkerToken(t *testing.T) {
	body := fmt.Sprintf(`{"schema_version":"acr_llm_config.v1","org_id":%q,"provider":"Anthropic","model":"small","base_url":"","api_key":%q,"version":"v1"}`, org, key)
	client, seen := serve(t, 200, body)
	cfg, err := client.Fetch(context.Background(), org)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider != "anthropic" || cfg.Model != "small" || cfg.APIKey.Reveal() != key || cfg.Version != "v1" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if seen.URL.Path != "/api/v1/internal/workers/orgs/"+org+"/llm-config/categorize" || seen.Header.Get("Authorization") != "Bearer "+token {
		t.Fatalf("request path %q auth ok=%v", seen.URL.Path, seen.Header.Get("Authorization") == "Bearer "+token)
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg), key) {
		t.Fatal("the key is printable from Config")
	}
}

func TestFetchClassifiesEveryOutcome(t *testing.T) {
	good := fmt.Sprintf(`{"schema_version":"acr_llm_config.v1","org_id":%q,"provider":"openai","api_key":%q}`, org, key)
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   error
	}{
		{"none configured", 404, `{"detail":"no-config"}`, ErrNotConfigured},
		{"generic missing route 404", 404, `{"detail":"Not found"}`, ErrUnavailable},
		{"unauthorized", 401, ``, ErrRefused},
		{"forbidden", 403, ``, ErrRefused},
		{"stored config unreadable", 409, ``, ErrRefused},
		{"server error", 500, ``, ErrUnavailable},
		{"unavailable", 503, ``, ErrUnavailable},
		{"garbage body", 200, `not json`, ErrUnavailable},
		{"wrong schema", 200, `{"schema_version":"x","org_id":"` + org + `","provider":"openai"}`, ErrRefused},
		{"other org", 200, `{"schema_version":"acr_llm_config.v1","org_id":"someone-else","provider":"openai"}`, ErrRefused},
		{"no provider", 200, `{"schema_version":"acr_llm_config.v1","org_id":"` + org + `","provider":""}`, ErrRefused},
		{"ok", 200, good, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := serve(t, tc.status, tc.body)
			_, err := client.Fetch(context.Background(), org)
			if !errors.Is(err, tc.want) && !(tc.want == nil && err == nil) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if err != nil && (strings.Contains(err.Error(), key) || strings.Contains(err.Error(), token)) {
				t.Fatalf("error carries a secret: %v", err)
			}
		})
	}
}

func TestFetchUnreachableApiIsUnavailable(t *testing.T) {
	client, err := New("http://127.0.0.1:1", secrets.NewHidden(token), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Fetch(context.Background(), org); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestFromEnvOffHalfAndOn(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if c, err := FromEnv(env(nil)); c != nil || err != nil {
		t.Fatalf("both unset: %v %v", c, err)
	}
	for name, m := range map[string]map[string]string{
		"url only":   {EnvURL: "http://api:8080"},
		"token only": {EnvToken: token},
		"bad scheme": {EnvURL: "ftp://api", EnvToken: token},
		"no host":    {EnvURL: "http://", EnvToken: token},
	} {
		if c, err := FromEnv(env(m)); c != nil || err == nil {
			t.Errorf("%s: client=%v err=%v, want a refusal", name, c, err)
		}
	}
	if c, err := FromEnv(env(map[string]string{EnvURL: "http://api:8080/", EnvToken: token})); c == nil || err != nil {
		t.Fatalf("both set: %v %v", c, err)
	}
}

// The default client and a supplied one of the default redirect policy never
// carry the worker token to another origin.
func TestTheClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	for name, supplied := range map[string]*http.Client{"default": nil, "supplied": probe.Client()} {
		client, err := New(probe.Base.URL, secrets.NewHidden(token), supplied)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = client.Fetch(context.Background(), org)
		t.Run(name, func(t *testing.T) { probe.Assert(t) })
	}
}
