package externalingest_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

const pushPrefix = "/api/v1/external-ingest"

// route finds the handler Routes mounts for a method and pattern.
func route(t *testing.T, deps externalingest.Deps, method, pattern string) http.Handler {
	t.Helper()
	for _, candidate := range externalingest.Routes(deps) {
		if candidate.Method == method && candidate.Pattern == pattern {
			return candidate.Handler
		}
	}
	t.Fatalf("no %s %s route", method, pattern)
	return nil
}

func newPushRequest(method, pattern, authorization string, serial int) *http.Request {
	var body *bytes.Reader
	if method == http.MethodPost {
		body = bytes.NewReader([]byte(`{}`))
	} else {
		body = bytes.NewReader(nil)
	}
	target := strings.ReplaceAll(pattern, "{ingestion_id}", "11111111-1111-4111-8111-111111111111")
	target = strings.ReplaceAll(target, "{schema_version}", "external-ingest.v1")
	r := httptest.NewRequest(method, target, body)
	r.SetPathValue("ingestion_id", "11111111-1111-4111-8111-111111111111")
	r.SetPathValue("schema_version", "external-ingest.v1")
	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}
	// One address per request: the auth-failure throttle counts per address and would answer 429 to a
	// long run from one, which is not the refusal under test.
	r.Header.Set("X-Forwarded-For", fmt.Sprintf("10.%d.%d.%d", serial/65536%256, serial/256%256, serial%256))
	r.Header.Set("Content-Type", "application/json")
	return r
}

// The five authenticated external-push routes refuse every kind of user token before any lookup: 401
// invalid_token from the token-prefix gate, no database connection attempted, no credential echoed.
func TestExternalPushAuthenticatedRoutesRefuseUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	pool, connects := usertokens.CountingPool(t)
	var logs bytes.Buffer
	deps := externalingest.Deps{Pool: pool, Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	routes := []struct{ method, pattern string }{
		{"GET", pushPrefix + "/availability"},
		{"POST", pushPrefix + "/validate"},
		{"POST", pushPrefix + "/batches"},
		{"GET", pushPrefix + "/batches"},
		{"GET", pushPrefix + "/batches/{ingestion_id}"},
	}
	serial := 0
	for _, rt := range routes {
		handler := route(t, deps, rt.method, rt.pattern)
		for _, tok := range tokens {
			serial++
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, newPushRequest(rt.method, rt.pattern, "Bearer "+tok.Value, serial))
			if recorder.Code != http.StatusUnauthorized {
				t.Errorf("%s %s with %s: status %d, want 401; body %s", rt.method, rt.pattern, tok.Kind, recorder.Code, recorder.Body.String())
				continue
			}
			var body struct {
				Error struct{ Code, Message string } `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || body.Error.Code != "invalid_token" || body.Error.Message != "Missing or invalid ingest token" {
				t.Errorf("%s %s with %s: body %s, want the invalid_token envelope", rt.method, rt.pattern, tok.Kind, recorder.Body.String())
			}
			usertokens.NoLeak(t, rt.method+" "+rt.pattern+" body", recorder.Body.String(), tokens)
		}
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the database: %d connection attempt(s), want 0", got)
	}
	usertokens.NoLeak(t, "the logs", logs.String(), tokens)
}

// The control of the test above: a push token (fcpush_ prefix) passes the gate and goes to the database,
// so the same pool shows a non-zero count and the answer is no longer the 401 refusal. If this ever read 0
// or 401, the zero above would prove nothing.
func TestExternalPushControlAPushTokenReachesTheLookup(t *testing.T) {
	pool, connects := usertokens.CountingPool(t)
	deps := externalingest.Deps{Pool: pool, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	recorder := httptest.NewRecorder()
	route(t, deps, "GET", pushPrefix+"/availability").ServeHTTP(recorder, newPushRequest("GET", pushPrefix+"/availability", "Bearer fcpush_control_token", 1))
	if recorder.Code == http.StatusUnauthorized {
		t.Fatalf("a push-shaped token was refused at the gate: %s", recorder.Body.String())
	}
	if connects.Count() == 0 {
		t.Fatal("a push-shaped token never reached the database lookup: the counting pool detects nothing")
	}
}

// The two schema-discovery routes carry the push profile but the handlers perform no credential check: a
// user token changes nothing, so the answer equals the anonymous one (no lookup, no echo of the credential).
func TestExternalPushSchemaDiscoveryIgnoresUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	pool, connects := usertokens.CountingPool(t)
	deps := externalingest.Deps{Pool: pool, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	for _, rt := range []struct{ method, pattern string }{
		{"GET", pushPrefix + "/schemas"},
		{"GET", pushPrefix + "/schemas/{schema_version}"},
	} {
		handler := route(t, deps, rt.method, rt.pattern)
		anonymous := httptest.NewRecorder()
		handler.ServeHTTP(anonymous, newPushRequest(rt.method, rt.pattern, "", 900))
		serial := 1000
		for _, tok := range tokens {
			serial++
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, newPushRequest(rt.method, rt.pattern, "Bearer "+tok.Value, serial))
			if recorder.Code != anonymous.Code || recorder.Body.String() != anonymous.Body.String() {
				t.Errorf("%s with %s: %d %s differs from the anonymous %d %s", rt.pattern, tok.Kind, recorder.Code, recorder.Body.String(), anonymous.Code, anonymous.Body.String())
			}
			usertokens.NoLeak(t, rt.pattern+" body", recorder.Body.String(), tokens)
		}
	}
	if connects.Count() != 0 {
		t.Errorf("schema discovery touched the database: %d attempt(s)", connects.Count())
	}
}
