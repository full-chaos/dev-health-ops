package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func mountedPaths(t *testing.T, mux *http.ServeMux, path string) bool {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, nil)
	_, pattern := mux.Handler(request)
	return pattern != ""
}

// Default posture: no flag, no route. And the log must SAY so with an
// explicit zero -- "the proof route is off" and "the proof route is on
// but nothing used it" are different states.
func TestProofRouteIsNotRegisteredByDefault(t *testing.T) {
	t.Setenv(proofRouteEnabledEnv, "")
	t.Setenv(deploymentEnvEnv, "")

	mux := http.NewServeMux()
	output := captureLog(t, func() {
		mountProofRoute(mux, func(http.ResponseWriter, *http.Request) {})
	})

	if mountedPaths(t, mux, "/query/proof") {
		t.Fatal("/query/proof must not be registered without an explicit opt-in")
	}
	if !strings.Contains(output, "routes_registered=0") {
		t.Fatalf("the absent case must log an explicit zero, got %q", output)
	}
}

func TestProofRouteRegistersOnExplicitOptIn(t *testing.T) {
	t.Setenv(proofRouteEnabledEnv, "true")
	t.Setenv(deploymentEnvEnv, "local")

	mux := http.NewServeMux()
	output := captureLog(t, func() {
		mountProofRoute(mux, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	})

	if !mountedPaths(t, mux, "/query/proof") {
		t.Fatalf("/query/proof must register on an explicit opt-in; log was %q", output)
	}
	if !strings.Contains(output, "routes_registered=1") {
		t.Fatalf("the registered case must say so, got %q", output)
	}
}

// The production refusal is not a courtesy: a measurement route that can
// execute an operation an operator deliberately did NOT canary must not
// exist in production, and the env flag must not be the only thing
// standing between it and a production process.
func TestProofRouteRefusesInAProductionPosture(t *testing.T) {
	for _, posture := range []string{"prod", "production", "PRODUCTION", " Prod "} {
		t.Run(posture, func(t *testing.T) {
			t.Setenv(proofRouteEnabledEnv, "true")
			t.Setenv(deploymentEnvEnv, posture)

			mux := http.NewServeMux()
			output := captureLog(t, func() {
				mountProofRoute(mux, func(http.ResponseWriter, *http.Request) {})
			})

			if mountedPaths(t, mux, "/query/proof") {
				t.Fatal("the flag must be ignored in a production posture")
			}
			if !strings.Contains(output, "production posture") {
				t.Fatalf("the refusal must name its reason, got %q", output)
			}
		})
	}
}

// A nil handler registered on a mux panics at request time. Refusing at
// boot is strictly better than a proof route that 500s.
func TestProofRouteRefusesANilHandler(t *testing.T) {
	t.Setenv(proofRouteEnabledEnv, "true")
	t.Setenv(deploymentEnvEnv, "local")

	mux := http.NewServeMux()
	output := captureLog(t, func() { mountProofRoute(mux, nil) })
	if mountedPaths(t, mux, "/query/proof") {
		t.Fatal("a nil handler must not be registered")
	}
	if !strings.Contains(output, "routes_registered=0") {
		t.Fatalf("expected an explicit zero, got %q", output)
	}
}

// Mounting /query must not accidentally answer for /query/proof: with
// Go's ServeMux an exact pattern matches only itself, and this asserts
// that rather than trusting it.
func TestQueryPatternDoesNotSwallowTheProofPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", func(http.ResponseWriter, *http.Request) {})
	if mountedPaths(t, mux, "/query/proof") {
		t.Fatal("/query must not match /query/proof -- the two routes have different reachability")
	}
}

func TestBuildInfoHandlerRefusesWithoutABearer(t *testing.T) {
	handler := newBuildInfoHandler(nil)
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/buildinfo", nil))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("an unauthenticated /buildinfo read must be 401, got %d", recorder.Code)
	}
}

func TestBuildInfoHandlerRejectsNonGet(t *testing.T) {
	handler := newBuildInfoHandler(nil)
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/buildinfo", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("got %d", recorder.Code)
	}
}
