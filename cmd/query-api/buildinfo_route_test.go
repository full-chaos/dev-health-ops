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

// The proof route's own responses must carry the plane and the build that
// served them. Without this, a proof-route response was indistinguishable
// from any other endpoint returning a plausible 200, and go-api-prove
// skipped the plane check on that route for exactly that reason.
func TestProofRouteStampsPlaneAndBuild(t *testing.T) {
	handler := withProofProvenance(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) },
		"b18e56fa79cfe20ce0f75df148144b832d92be36",
	)
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/query/proof", nil))

	if got := recorder.Header().Get(planeHeaderName); got != "go" {
		t.Fatalf("proof responses must name their plane, got %q", got)
	}
	if got := recorder.Header().Get(buildHeaderName); got != "b18e56fa79cfe20ce0f75df148144b832d92be36" {
		t.Fatalf("proof responses must name their build, got %q", got)
	}
}

// A build the process cannot name must not be stamped as an empty claim --
// an empty header reads as "this build is the empty string", which the
// runner would then compare against a real commit.
func TestProofRouteOmitsAnUnknowableBuild(t *testing.T) {
	handler := withProofProvenance(func(w http.ResponseWriter, _ *http.Request) {}, "")
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/query/proof", nil))

	if _, present := recorder.Header()[http.CanonicalHeaderKey(buildHeaderName)]; present {
		t.Fatal("an unknown build must be absent, not an empty header")
	}
	if got := recorder.Header().Get(planeHeaderName); got != "go" {
		t.Fatalf("the plane is still knowable, got %q", got)
	}
}

// The production refusal must fail CLOSED on an undeclared posture: an
// unset DEV_HEALTH_ENV is not evidence of a non-production deployment.
func TestProofRouteRefusesAnUndeclaredPosture(t *testing.T) {
	t.Setenv(proofRouteEnabledEnv, "true")
	t.Setenv(deploymentEnvEnv, "")

	mux := http.NewServeMux()
	output := captureLog(t, func() {
		mountProofRoute(mux, func(http.ResponseWriter, *http.Request) {})
	})

	if mountedPaths(t, mux, "/query/proof") {
		t.Fatal("an undeclared posture must refuse, not fall through to registration")
	}
	if !strings.Contains(output, "routes_registered=0") {
		t.Fatalf("the refusal must carry an explicit zero, got %q", output)
	}
	if !strings.Contains(output, "has not declared a non-production posture") {
		t.Fatalf("the refusal must name its reason, got %q", output)
	}
}

// r3 P1, and the reason this PR touches cmd/query-api at all.
//
// Only /query/proof carried provenance headers; the NORMAL /query route
// was mounted raw. So the Python edge's CHAOS-5479 pass-through forwarded
// a header the Go handler never set, every canary/primary measurement was
// unbindable, and the prover -- correctly refusing to certify what it
// cannot bind -- downgraded all of them to `unsupported`. Nothing could
// ever be proven.
//
// Both routes now use the same wrapper, so they cannot drift into
// disagreeing about what they claim.
func TestTheNormalQueryRouteCarriesProvenanceHeaders(t *testing.T) {
	const commit = "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e"
	served := false
	handler := withProofProvenance(func(w http.ResponseWriter, _ *http.Request) {
		served = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"featureFlags":[]}}`))
	}, commit)

	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/query", nil))

	if !served {
		t.Fatal("the wrapped handler must still run")
	}
	if got := recorder.Header().Get(buildHeaderName); got != commit {
		t.Fatalf("the serving build must be on a NORMAL /query response, got %q -- without it no canary or primary operation can ever be proven", got)
	}
	if got := recorder.Header().Get(planeHeaderName); got != "go" {
		t.Fatalf("plane header = %q, want go", got)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
}

// An unstamped build must not produce an empty header that reads as a
// binding. Absent is a true statement; empty is a claim of nothing.
func TestAnUnstampedBuildSetsNoBuildHeader(t *testing.T) {
	handler := withProofProvenance(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}, "")

	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/query", nil))

	if _, present := recorder.Header()[http.CanonicalHeaderKey(buildHeaderName)]; present {
		t.Fatal("an unstamped build must set NO build header rather than an empty one")
	}
}
