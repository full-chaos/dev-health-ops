package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// CHAOS-7096: /query/proof-write is registered ONLY on
// the internal route set, never the public one. mountProofWriteRoute is the
// single place that decision is made, so it is proven directly here rather
// than through a full Build() (which needs live ClickHouse/Postgres/envelope
// config to populate handlers.ProofWrite at all) -- and pinned a second way,
// at the call-site level, by TestBuildCallsMountProofWriteRouteOnInternalMuxOnly
// below, so a future edit that passes Build's PUBLIC mux by mistake fails even
// before any HTTP request is made.

func mpwGetenv(enabled string) func(string) string {
	return func(key string) string {
		if key == proofWriteRouteEnabledEnv {
			return enabled
		}
		return ""
	}
}

func mpwProbe(hit *int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		*hit++
		w.WriteHeader(http.StatusTeapot)
	}
}

func TestMountProofWriteRouteRefusesWhenTheEnvVarIsNotSetToTrue(t *testing.T) {
	for _, raw := range []string{"", "false", "1", "yes"} {
		t.Run(strings.TrimSpace("case_"+raw), func(t *testing.T) {
			hit := new(int)
			mux := http.NewServeMux()
			mountProofWriteRoute(mpwGetenv(raw), mux, mpwProbe(hit))

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/query/proof-write", nil))
			if rec.Code == http.StatusTeapot {
				t.Fatalf("%q must not register the route (got 418 -- the handler ran)", raw)
			}
			if *hit != 0 {
				t.Fatalf("%q: handler ran %d time(s), want 0", raw, *hit)
			}
		})
	}
}

// Tolerant of case and surrounding whitespace, the same as proofRouteEnabledEnv's
// existing check -- pinned as a decision, not left to be discovered by accident.
func TestMountProofWriteRouteToleratesCaseAndWhitespace(t *testing.T) {
	hit := new(int)
	mux := http.NewServeMux()
	mountProofWriteRoute(mpwGetenv("TRUE "), mux, mpwProbe(hit))

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/query/proof-write", nil))
	if rec.Code != http.StatusTeapot || *hit != 1 {
		t.Fatalf("code=%d hit=%d, want 418/1", rec.Code, *hit)
	}
}

func TestMountProofWriteRouteRefusesANilHandlerEvenWhenEnabled(t *testing.T) {
	mux := http.NewServeMux()
	mountProofWriteRoute(mpwGetenv("true"), mux, nil)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/query/proof-write", nil))
	if rec.Code == http.StatusTeapot {
		t.Fatal("a nil handler must not register the route")
	}
}

func TestMountProofWriteRouteRegistersOnlyOnTheMuxItIsGiven(t *testing.T) {
	hit := new(int)
	targetMux := http.NewServeMux()
	otherMux := http.NewServeMux()
	mountProofWriteRoute(mpwGetenv("true"), targetMux, mpwProbe(hit))

	rec := httptest.NewRecorder()
	targetMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/query/proof-write", nil))
	if rec.Code != http.StatusTeapot || *hit != 1 {
		t.Fatalf("the mux passed in: code=%d hit=%d, want 418/1", rec.Code, *hit)
	}

	rec2 := httptest.NewRecorder()
	otherMux.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/query/proof-write", nil))
	if rec2.Code == http.StatusTeapot {
		t.Fatal("a DIFFERENT mux answered 418 -- mountProofWriteRoute must only touch the mux it was given")
	}
}

// TestBuildCallsMountProofWriteRouteOnInternalMuxOnly is a source-text pin,
// the same technique posture_readiness_test.go already uses in this package:
// it fails the moment a future edit passes Build's PUBLIC mux to
// mountProofWriteRoute instead of internalMux, without needing a full,
// live-dependency Build() to prove it at the HTTP level.
func TestBuildCallsMountProofWriteRouteOnInternalMuxOnly(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	if strings.Count(text, "mountProofWriteRoute(getenv, internalMux, handlers.ProofWrite)") != 1 {
		t.Fatal(`server.go must call mountProofWriteRoute(getenv, internalMux, handlers.ProofWrite) exactly once`)
	}
	if strings.Count(text, "mountProofWriteRoute(getenv, internalMux, nil)") != 1 {
		t.Fatal(`server.go must call mountProofWriteRoute(getenv, internalMux, nil) exactly once (the unconfigured branch)`)
	}
	if strings.Contains(text, "mountProofWriteRoute(getenv, mux,") {
		t.Fatal("server.go must NEVER call mountProofWriteRoute with the public mux")
	}
}
