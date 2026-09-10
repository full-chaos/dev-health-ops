package main

import (
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
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

	if got := recorder.Result().Header.Get(planeHeaderName); got != "go" {
		t.Fatalf("proof responses must name their plane, got %q", got)
	}
	if got := recorder.Result().Header.Get(buildHeaderName); got != "b18e56fa79cfe20ce0f75df148144b832d92be36" {
		t.Fatalf("proof responses must name their build, got %q", got)
	}
}

// A build the process cannot name must not be stamped as an empty claim --
// an empty header reads as "this build is the empty string", which the
// runner would then compare against a real commit.
func TestProofRouteOmitsAnUnknowableBuild(t *testing.T) {
	handler := withProofProvenance(func(w http.ResponseWriter, _ *http.Request) {}, unstampedBuild)
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/query/proof", nil))

	if _, present := recorder.Result().Header[http.CanonicalHeaderKey(buildHeaderName)]; present {
		t.Fatal("an unknown build must be absent, not an empty header")
	}
	if got := recorder.Result().Header.Get(planeHeaderName); got != "go" {
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
	if got := recorder.Result().Header.Get(buildHeaderName); got != commit {
		t.Fatalf("the serving build must be on a NORMAL /query response, got %q -- without it no canary or primary operation can ever be proven", got)
	}
	if got := recorder.Result().Header.Get(planeHeaderName); got != "go" {
		t.Fatalf("plane header = %q, want go", got)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
}

// An unstamped build must not produce a header that reads as a binding.
// Absent is a true statement; empty -- or the placeholder "unknown" --
// is a claim of nothing dressed as a claim of something. goapiproof
// binds a receipt's candidate_build to whatever this header says, and
// `proven` is keyed on that column, so a row reading "unknown" can never
// be matched by any deployment yet LOOKS bound.
//
// r10 item (4): this used to pin only "", a value version.Current never
// produces -- so it pinned a case that cannot happen while leaving the
// case that does happen (an image built with no -ldflags and no VCS
// stamp) entirely untested.
func TestAnUnstampedBuildSetsNoBuildHeader(t *testing.T) {
	for _, commit := range []string{"", unstampedBuild, "  " + unstampedBuild + "  "} {
		handler := withProofProvenance(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}, commit)

		recorder := httptest.NewRecorder()
		handler(recorder, httptest.NewRequest(http.MethodPost, "/query", nil))

		if got, present := recorder.Result().Header[http.CanonicalHeaderKey(buildHeaderName)]; present {
			t.Fatalf("commit %q stamped %v: a process that cannot identify itself must set NO build header at all", commit, got)
		}
		// The plane header is unconditional -- WHICH implementation
		// served the request is knowable either way.
		if got := recorder.Result().Header.Get(planeHeaderName); got != "go" {
			t.Fatalf("commit %q dropped the plane header too: %q", commit, got)
		}
	}

	// Control, and the killer for a guard mutated to stamp NOTHING: a
	// real commit is still stamped, verbatim.
	const real = "a2a6703c1e4bb1942f1d36490c037fbd6b91b463"
	handler := withProofProvenance(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}, real)
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/query", nil))
	if got := recorder.Result().Header.Get(buildHeaderName); got != real {
		t.Fatalf("a real commit must still be stamped, got %q", got)
	}
}

// r5/r6/r7: the production /query registration must actually stamp the
// build, and this asserts it by DRIVING the real mux rather than reading
// main.go's source.
//
// The source-text version was defeated twice -- once by wrapping the
// registration in `if false` and registering the raw handler instead, once
// by handing the wrapper an empty build. Both left the pin green, because
// reading source is not running it. mountQueryRoute exists so this test
// can register the real route on a real mux and serve a real request.
func TestTheProductionQueryRouteStampsTheBuild(t *testing.T) {
	commit := stampedBuild(t)

	mux := http.NewServeMux()
	served := false
	mountQueryRoute(mux, func(w http.ResponseWriter, _ *http.Request) {
		served = true
		w.WriteHeader(http.StatusOK)
	})

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/query", nil))

	if !served {
		t.Fatal("the production handler did not run")
	}
	// r8: asserting only the PLANE let an empty build stamp pass. The
	// build is the header a proof receipt is bound by; the plane is not.
	if got := recorder.Result().Header.Get(buildHeaderName); got != commit {
		t.Fatalf("the production /query route stamped %q, want the running build %q: a route that stamps anything else leaves every measurement unbindable", got, commit)
	}
	if got := recorder.Result().Header.Get(planeHeaderName); got != "go" {
		t.Fatalf("plane header = %q, want go", got)
	}
}

// F1, the strongest form: a real server and a real client, so the
// assertion is over headers as RECEIVED rather than over any recorder
// view of them.
//
// `withProofProvenance` sets the headers BEFORE calling the wrapped
// handler, and must: once the handler calls WriteHeader the map is on the
// wire and a later write is silently dropped. Every pin for that ordering
// read `recorder.Header()` -- the recorder's LIVE map, which keeps
// accepting writes after WriteHeader -- so moving the two Set calls after
// `handler(w, r)` left the whole suite green while the header vanished in
// production. The two views diverge exactly under that mutant:
// `recorder.Header()` shows the build, `recorder.Result().Header` does not.
//
// This test cannot be fooled that way, because there is no recorder.
func TestProvenanceHeadersReachTheWire(t *testing.T) {
	commit := stampedBuild(t)

	mux := http.NewServeMux()
	mountQueryRoute(mux, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"featureFlags":[]}}`))
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	response, err := server.Client().Post(server.URL+"/query", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /query: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })

	if got := response.Header.Get(buildHeaderName); got != commit {
		t.Fatalf("ON THE WIRE the build header is %q, want %q -- the header must be set before the handler writes, or it never leaves the process", got, commit)
	}
	if got := response.Header.Get(planeHeaderName); got != "go" {
		t.Fatalf("ON THE WIRE the plane header is %q, want go", got)
	}
}

// F1b: main's CALL is what stamps the build, and it was untested --
// `mountQueryRoute(mux, handlers.Query, "")` compiled and left the suite
// green, because the pin passed its own constant and proved only that the
// helper stamps what it is handed.
//
// `runningBuild()` gives that value one named source. This asserts main
// mounts with THAT value and that it is the process's own identity, so
// substituting a literal at the call site fails here.
func TestTheProductionRouteMountsWithTheRunningBuild(t *testing.T) {
	commit := stampedBuild(t)

	if runningBuild() != version.Current("query-api").Commit {
		t.Fatalf("runningBuild() = %q, want the process's own commit", runningBuild())
	}

	mux := http.NewServeMux()
	mountQueryRoute(mux, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	response, err := server.Client().Post(server.URL+"/query", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /query: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })

	// The WIRE must carry the process's OWN identity. Comparing against
	// runningBuild() is the whole point -- substituting a literal at the
	// call site fails here -- but only while runningBuild() is something
	// a wrong value could differ from, which is what stampedBuild buys.
	got := response.Header.Get(buildHeaderName)
	if got != commit {
		t.Fatalf("the mounted route stamped %q but the process reports %q", got, commit)
	}
	if got != runningBuild() {
		t.Fatalf("the mounted route stamped %q but runningBuild() says %q: main mounts a value that is not this process's identity", got, runningBuild())
	}
}

// unstampedBuild is what version.Current("query-api").Commit ACTUALLY
// returns in a binary built without -ldflags and without a VCS stamp --
// which is every `go test` binary, and every image built the same way.
//
// r10 item (4): the two unknowable-build pins used to feed "", a value
// version.Current never produces. That made them pin a case that cannot
// happen while leaving the case that does happen untested -- and the
// production guard was `commit != ""`, so an unstamped process stamped
// `x-dev-health-build: unknown` on every response.
const unstampedBuild = "unknown"

// stampedBuild makes THIS process report a known build for the duration of
// one test, and returns it.
//
// r11 S2: the three wire pins compared the header against runningBuild(),
// which in an ordinary `go test` binary is "unknown" -- so once the route
// correctly suppressed that value, every pin was asserting the header is
// ABSENT. `withProofProvenance(query, "")` at the real call site then
// survived all three again, and the pins only regained teeth when the
// suite happened to be built with `-ldflags -X ...version.Commit=<sha>`.
// A test must not depend on a build flag to be able to fail.
//
// version.Commit is the same package variable -ldflags writes, so setting
// it here drives the production read path (runningBuild -> version.Current)
// rather than bypassing it. No production lever is added: mountQueryRoute
// still takes no build argument.
func stampedBuild(t *testing.T) string {
	t.Helper()
	const commit = "a2a6703c1e4bb1942f1d36490c037fbd6b91b463"
	previous := version.Commit
	version.Commit = commit
	t.Cleanup(func() { version.Commit = previous })

	// Guard the seam itself: if version.Current ever stops reading this
	// variable, every pin below would silently go back to asserting
	// absence.
	if got := runningBuild(); got != commit {
		t.Fatalf("runningBuild() = %q after stamping %q: the pins below can no longer tell a real stamp from a missing one", got, commit)
	}
	return commit
}

func TestTheUnstampedDefaultIsWhatVersionActuallyReturns(t *testing.T) {
	// If this ever fails, the two pins above are testing a fiction again.
	if got := version.Current("query-api").Commit; got != unstampedBuild && len(got) != 40 {
		t.Fatalf("version.Current(...).Commit = %q in a test binary: the unknowable-build pins are keyed on %q, so they no longer describe the real default", got, unstampedBuild)
	}
}

// The /query/proof registration has the same lever the /query one had, at
// buildinfo_route.go's mountProofRoute: it passes the build to
// withProofProvenance itself, so `withProofProvenance(handler, "")` there
// is a writable mutation. It survived the whole suite, because every
// proof-route pin called withProofProvenance DIRECTLY with its own
// constant and so proved only that the helper stamps what it is handed --
// exactly the gap r11 named on the routing command's credential.
//
// This drives the real registration over a real server instead.
func TestTheProofRouteMountsWithTheRunningBuild(t *testing.T) {
	commit := stampedBuild(t)
	t.Setenv(proofRouteEnabledEnv, "true")
	t.Setenv(deploymentEnvEnv, "local")

	mux := http.NewServeMux()
	served := false
	mountProofRoute(mux, func(w http.ResponseWriter, _ *http.Request) {
		served = true
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	response, err := server.Client().Post(server.URL+"/query/proof", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /query/proof: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })

	if !served {
		t.Fatal("the proof handler did not run")
	}
	if got := response.Header.Get(buildHeaderName); got != commit {
		t.Fatalf("ON THE WIRE /query/proof stamped %q, want the running build %q: a proof-route receipt is bound by this header, so a wrong or missing stamp makes every proof-route measurement unbindable", got, commit)
	}
	if got := response.Header.Get(buildHeaderName); got != runningBuild() {
		t.Fatalf("/query/proof stamped %q but runningBuild() says %q: the registration mounts a value that is not this process's identity", got, runningBuild())
	}
	if got := response.Header.Get(planeHeaderName); got != "go" {
		t.Fatalf("ON THE WIRE the plane header is %q, want go", got)
	}
}
