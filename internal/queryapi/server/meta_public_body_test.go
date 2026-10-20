package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestMetaPublicBodyHasNoVersionKey(t *testing.T) {
	handler := newMetaWorkHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.Bytes()
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, body)
	}
	if _, present := decoded["version"]; present {
		t.Fatalf("public meta body carries a version key: %s", body)
	}
	for _, key := range []string{"backend", "last_ingest_at", "coverage", "limits", "supported_endpoints"} {
		if _, present := decoded[key]; !present {
			t.Fatalf("public meta body lost key %q: %s", key, body)
		}
	}
}

func TestMetaPublicRouteServesWithUnreachableClickHouse(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://127.0.0.1:1/default")
	t.Setenv(metaEnabledEnvVar, "true")
	handler, cleanup, ok, err := buildMetaRoute(os.Getenv)
	if err != nil || !ok {
		t.Fatalf("buildMetaRoute: ok=%v err=%v", ok, err)
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "version") {
		t.Fatalf("public meta body mentions a version: %s", rec.Body.String())
	}
}
