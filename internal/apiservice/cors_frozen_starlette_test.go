package apiservice

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyheaders"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

type corsCase struct {
	Method  string            `json:"method"`
	Vary    int               `json:"vary"`
	Headers map[string]string `json:"headers"`
}

// TestCORSMatchesFrozenStarlette pins the Go CORS middleware against the
// Starlette CORSMiddleware answers, frozen from the build that carried the api's own arguments:
// every Origin shape (absent, empty, allowed, disallowed) crossed with 0, 1
// or 2 handler-set Vary headers, on a simple request and on a preflight, for
// the default, list and "*" origin configurations. Compared: status, every
// Vary value and every Access-Control-* header.
func TestCORSMatchesFrozenStarlette(t *testing.T) {
	golden := venueoracle.OpenGolden(t, programGolden("cors-starlette", t.Name(), "b9b95dc21c75108e286bb853e53a8158b7b50aae8fec00a9d94c72828cb09074"))

	configs := map[string]*string{
		"default": nil,
		"list":    ptr("https://a.example, https://b.example,,"),
		"star":    ptr("*"),
		"empty":   ptr(""),
		"blank":   ptr("   "),
	}
	var cases []corsCase
	for _, origin := range []*string{nil, ptr(""), ptr("https://a.example"), ptr("http://localhost:3000"), ptr("https://evil.example")} {
		for vary := 0; vary <= 2; vary++ {
			for _, preflight := range []bool{false, true} {
				headers := map[string]string{}
				if origin != nil {
					headers["Origin"] = *origin
				}
				method := http.MethodGet
				if preflight {
					method = http.MethodOptions
					headers["Access-Control-Request-Method"] = "POST"
				}
				cases = append(cases, corsCase{Method: method, Vary: vary, Headers: headers})
			}
			cases = append(cases, corsCase{Method: http.MethodPost, Vary: vary, Headers: withOrigin(origin)})
		}
	}
	input, _ := json.Marshal(map[string]any{"configs": configs, "cases": cases})
	request := venueoracle.ProgramRequest("cors matrix", input)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	output := []byte(answers[0].Body)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct {
		Starlette string `json:"starlette"`
		Configs   map[string]struct {
			AllowOrigins []string `json:"allow_origins"`
			Answers      []struct {
				Status  int                 `json:"status"`
				Headers map[string][]string `json:"headers"`
			} `json:"answers"`
		} `json:"configs"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	compared := 0
	if len(cases) != 45 || len(want.Configs) != 5 {
		t.Fatalf("matrix is %d cases x %d configs, want 45 x 5", len(cases), len(want.Configs))
	}
	for name, config := range want.Configs {
		origins, known := configs[name]
		if !known {
			t.Fatalf("python answered an unknown config %q", name)
		}
		// Go's origin list comes from the production loader, never from
		// Python's parse: an unset, empty or blank variable must reach the
		// same allow-list on both planes.
		goOrigins := loadCORSOrigins(t, origins)
		if !reflect.DeepEqual(append([]string{}, goOrigins...), append([]string{}, config.AllowOrigins...)) {
			t.Errorf("config=%s allow_origins: go %q, python %q", name, goOrigins, config.AllowOrigins)
		}
		cors := pyheaders.NewCORS(goOrigins)
		for index, c := range cases {
			vary := c.Vary
			handler := cors.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for _, value := range []string{"Accept-Encoding", "X-Two"}[:vary] {
					w.Header().Add("Vary", value)
				}
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				_, _ = w.Write([]byte("ok"))
			}))
			request := httptest.NewRequest(c.Method, "/v", nil)
			for key, value := range c.Headers {
				request.Header.Set(key, value)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			got := map[string][]string{}
			for key, values := range recorder.Result().Header {
				key = strings.ToLower(key)
				if key == "vary" || strings.HasPrefix(key, "access-control-") {
					got[key] = values
				}
			}
			expected := config.Answers[index]
			if recorder.Code != expected.Status || !reflect.DeepEqual(got, normalizeEmpty(expected.Headers)) {
				t.Errorf("config=%s %s vary=%d headers=%v:\n go     %d %v\n python %d %v", name, c.Method, c.Vary,
					sortedHeaders(c.Headers), recorder.Code, got, expected.Status, expected.Headers)
			}
			compared++
		}
	}
	if compared != 225 || !strings.HasPrefix(want.Starlette, "1.7.") {
		t.Fatalf("compared %d of 225 cases against starlette %s", compared, want.Starlette)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

func ptr(value string) *string { return &value }

// loadCORSOrigins is the allow-list the real config.Load builds for one
// CORS_ALLOWED_ORIGINS setting (nil: the variable is absent).
func loadCORSOrigins(t *testing.T, origins *string) []string {
	t.Helper()
	env := map[string]string{}
	if origins != nil {
		env["CORS_ALLOWED_ORIGINS"] = *origins
	}
	cfg, err := config.Load(config.Spec{
		Service:   config.APIServiceName,
		LookupEnv: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
	})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return cfg.CORSAllowedOrigins
}

func withOrigin(origin *string) map[string]string {
	if origin == nil {
		return map[string]string{}
	}
	return map[string]string{"Origin": *origin}
}

func normalizeEmpty(headers map[string][]string) map[string][]string {
	if headers == nil {
		return map[string][]string{}
	}
	return headers
}

func sortedHeaders(headers map[string]string) []string {
	out := make([]string, 0, len(headers))
	for key, value := range headers {
		out = append(out, key+"="+value)
	}
	sort.Strings(out)
	return out
}
