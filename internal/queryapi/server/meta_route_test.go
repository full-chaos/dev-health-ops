package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// TestMetaSwitchFromEnvDefaultsDisabled mirrors
// TestFilterOptionsSwitchFromEnvDefaultsDisabled: with no env var set,
// metaOperation is NOT enabled.
func TestMetaSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := metaSwitchFromEnv(os.Getenv)
	if sw.Enabled(metaOperation) {
		t.Fatal("expected metaOperation to be disabled with no env var set")
	}
}

// TestMetaSwitchFromEnvEnabledViaEnvVar is the other half.
func TestMetaSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(metaEnabledEnvVar, "true")
	sw := metaSwitchFromEnv(os.Getenv)
	if !sw.Enabled(metaOperation) {
		t.Fatal("expected metaOperation to be enabled with GO_API_META_ENABLED=true")
	}
}

// TestMetaRouteUnreachableWhenSwitchDisabled mirrors
// TestFilterOptionsRouteUnreachableWhenSwitchDisabled: a registered
// handler dispatched through a Mux whose Switch reports the operation
// disabled must never run and must answer 404.
func TestMetaRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(metaOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(metaOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestMetaRouteReachableWhenSwitchEnabled is the other half.
func TestMetaRouteReachableWhenSwitchEnabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	sw.Set(metaOperation, true)
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(metaOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(metaOperation, rec, req)

	if !reached {
		t.Fatal("registered handler did not run despite the switch being enabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// TestNewMetaWorkHandlerRequiresNoAuthContext pins that this route's work
// handler runs with NO claims attached at all -- unlike every sibling
// route's newXWorkHandler, there is no authctx.FromContext check here
// because the Python route this ports has no auth (see meta_route.go's
// package doc comment for the citation trail).
func TestNewMetaWorkHandlerRequiresNoAuthContext(t *testing.T) {
	handler := newMetaWorkHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// TestNewMetaWorkHandlerHappyPathShape pins the response Content-Type and
// full body shape.
func TestNewMetaWorkHandlerHappyPathShape(t *testing.T) {
	handler := newMetaWorkHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var decoded struct {
		Backend            string         `json:"backend"`
		LastIngestAt       *string        `json:"last_ingest_at"`
		Coverage           map[string]any `json:"coverage"`
		Limits             map[string]int `json:"limits"`
		SupportedEndpoints []string       `json:"supported_endpoints"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Backend != "clickhouse" {
		t.Fatalf("backend = %q, want clickhouse", decoded.Backend)
	}
	if decoded.LastIngestAt != nil {
		t.Fatalf("last_ingest_at = %v, want nil", decoded.LastIngestAt)
	}
	if decoded.Coverage == nil || len(decoded.Coverage) != 0 {
		t.Fatalf("coverage = %v, want present-and-empty", decoded.Coverage)
	}
	if decoded.Limits["max_days"] != 365 || decoded.Limits["max_repos"] != 1000 {
		t.Fatalf("limits = %v, want max_days=365 max_repos=1000", decoded.Limits)
	}
	if len(decoded.SupportedEndpoints) != 9 {
		t.Fatalf("supported_endpoints len = %d, want 9", len(decoded.SupportedEndpoints))
	}
}

// TestBuildMetaRouteEntryHandlerRejectsNonGET pins the same method-guard
// shape every sibling entryHandler uses: 405, not 404, with Starlette's
// own default {"detail": "Method Not Allowed"} body -- confirmed live
// against the real FastAPI app (see this route set's TEST-EVIDENCE for
// the capture command).
func TestBuildMetaRouteEntryHandlerRejectsNonGET(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	handler, cleanup, ok, err := buildMetaRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildMetaRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildMetaRoute: ok = false, want true with CLICKHOUSE_URI set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q, want application/json", ct)
	}
	if got, want := rec.Body.String(), `{"detail":"Method Not Allowed"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestLoadMetaRouteConfigRequiresClickHouseURI pins the "stay unmounted"
// contract: with no CLICKHOUSE_URI set, buildMetaRoute must return
// ok=false and a nil error, never fail to build the binary.
func TestLoadMetaRouteConfigRequiresClickHouseURI(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "")
	_, ok := loadMetaRouteConfig(os.Getenv)
	if ok {
		t.Fatal("expected loadMetaRouteConfig to report ok=false with CLICKHOUSE_URI unset")
	}
}
