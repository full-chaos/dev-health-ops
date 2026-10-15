package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
)

// graphQLAccessLine drives one request through the real /graphql chain under
// the same TraceHandler the query-api listeners use and returns the decoded
// "http request" line.
func graphQLAccessLine(t *testing.T, c edgeCell, body string) (map[string]any, string) {
	t.Helper()
	handler, _, member, nonmember, envelope := edgeHarness(t)
	mux := http.NewServeMux()
	mux.Handle("/graphql", internalidentity.Public(handler))
	logs := &bytes.Buffer{}
	traced := httpapi.TraceHandler(mux, httpapi.TraceOptions{
		Listener: "query-public",
		Logger:   slog.New(slog.NewJSONHandler(logs, nil)),
	})
	request := edgeRequest(t, c, member, nonmember, envelope)
	if body != "" {
		request = httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+member)
	}
	traced.ServeHTTP(httptest.NewRecorder(), request)
	for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err == nil && line["msg"] == "http request" {
			return line, logs.String()
		}
	}
	t.Fatalf("no http request line in %q", logs.String())
	return nil, ""
}

func TestGraphQLAccessLineCarriesDigestOperationAndStatus(t *testing.T) {
	line, _ := graphQLAccessLine(t, edgeCell{method: http.MethodPost, carrier: "member", document: "query"}, "")
	if line["route"] != "/graphql" || line["status"] != float64(http.StatusOK) {
		t.Fatalf("route/status = %v/%v", line["route"], line["status"])
	}
	if line["graphql_digest"] != digestHex(iaDocument) || line["graphql_operation"] != "probe" || line["graphql_digest_reason"] != "registered" {
		t.Fatalf("graphql fields = %v", line)
	}
}

func TestGraphQLAccessLineNamesAnUnregisteredOrUnresolvedDocument(t *testing.T) {
	line, _ := graphQLAccessLine(t, edgeCell{method: http.MethodPost, carrier: "member", document: "unregistered"}, "")
	if line["status"] != float64(http.StatusNotFound) || line["graphql_digest"] != digestHex("query Other { probe }") ||
		line["graphql_operation"] != "" || line["graphql_digest_reason"] != "unregistered" {
		t.Fatalf("unregistered line = %v", line)
	}
	line, _ = graphQLAccessLine(t, edgeCell{method: http.MethodPost, carrier: "none", document: "query"}, "")
	if line["status"] != float64(http.StatusUnauthorized) || line["graphql_digest"] != "" || line["graphql_digest_reason"] != "not_resolved" {
		t.Fatalf("unresolved line = %v", line)
	}
}

func TestGraphQLAccessLineNeverCarriesVariablesOrQueryText(t *testing.T) {
	const canary = "variable-canary-7f3a"
	body := `{"query": ` + jsonQuote(t, iaDocument) + `, "variables": {"who": "` + canary + `"}, "operationName": "` + canary + `"}`
	line, raw := graphQLAccessLine(t, edgeCell{}, body)
	if line["graphql_operation"] != "probe" {
		t.Fatalf("line = %v", line)
	}
	if strings.Contains(raw, canary) || strings.Contains(raw, iaDocument) {
		t.Fatalf("log leaked a variable or the document: %s", raw)
	}
}
