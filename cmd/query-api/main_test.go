package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzAlwaysOK(t *testing.T) {
	rec := httptest.NewRecorder()
	healthzHandler()(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: got status %d, want 200", rec.Code)
	}
}

func TestReadyzOKOnceProcessIsUp(t *testing.T) {
	rec := httptest.NewRecorder()
	readyzHandler()(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz: got status %d, want 200", rec.Code)
	}
}

// TestExecutableSchemaBuildsAndLinks proves the gqlgen-generated schema and
// the (all-panicking) resolver stubs actually compose into a working
// graphql.ExecutableSchema -- a real, if narrow, build-time proof that the
// schema-first codegen and the canonical SDL pin stay in sync. It does NOT
// exercise a resolver (every field panics by design, see main.go's package
// doc) -- only that construction succeeds.
func TestExecutableSchemaBuildsAndLinks(t *testing.T) {
	handler := newExecutableSchemaHandler()
	if handler == nil {
		t.Fatal("newExecutableSchemaHandler returned nil")
	}
}

func TestAddrDefaultsWhenEnvUnset(t *testing.T) {
	t.Setenv("QUERY_API_ADDR", "")
	if got := addr(); got != defaultAddr {
		t.Fatalf("addr() = %q, want default %q", got, defaultAddr)
	}
}

func TestAddrHonorsEnvOverride(t *testing.T) {
	t.Setenv("QUERY_API_ADDR", ":9999")
	if got := addr(); got != ":9999" {
		t.Fatalf("addr() = %q, want :9999", got)
	}
}
