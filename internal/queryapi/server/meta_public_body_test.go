package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type countingMetaClient struct {
	inner fixedVersionMetaClient
	calls int
}

func (c *countingMetaClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.calls++
	return c.inner.Query(ctx, statement, bindings)
}

func serveMetaBody(t *testing.T, client *countingMetaClient) []byte {
	t.Helper()
	handler := newMetaWorkHandler(client)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes()
}

func TestMetaPublicBodyHasNoVersionKey(t *testing.T) {
	body := serveMetaBody(t, &countingMetaClient{inner: fixedVersionMetaClient{version: "24.3.1.2672"}})
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, body)
	}
	if _, present := decoded["version"]; present {
		t.Fatalf("public meta body carries a version key: %s", body)
	}
	if strings.Contains(string(body), "24.3.1") {
		t.Fatalf("public meta body names the ClickHouse build: %s", body)
	}
	for _, key := range []string{"backend", "last_ingest_at", "coverage", "limits", "supported_endpoints"} {
		if _, present := decoded[key]; !present {
			t.Fatalf("public meta body lost key %q: %s", key, body)
		}
	}
}

func TestMetaPublicHandlerIssuesNoQuery(t *testing.T) {
	client := &countingMetaClient{inner: fixedVersionMetaClient{version: "24.3.1.2672"}}
	serveMetaBody(t, client)
	if client.calls != 0 {
		t.Fatalf("public meta handler issued %d ClickHouse queries, want 0", client.calls)
	}
}
