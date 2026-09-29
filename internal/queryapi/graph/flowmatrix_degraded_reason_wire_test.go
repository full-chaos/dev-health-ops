package graph

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// A real GraphQL selection of degradedReason, dispatched through the generated
// executable schema, reaches the field on the degraded flowMatrix path: the
// failing ClickHouse read answers empty arrays AND the disclosure string,
// while a selection without the field is byte-for-byte what it always was.
func TestFlowMatrixDegradedReasonThroughTheExecutableSchema(t *testing.T) {
	server := gqlhandler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: alwaysErrorsQueryClient{}}}))
	post := func(selection string) map[string]any {
		body, _ := json.Marshal(map[string]any{
			"query": `query Q($o: String!, $b: AnalyticsRequestInput!) { analytics(orgId: $o, batch: $b) { flowMatrix { ` + selection + ` } } }`,
			"variables": map[string]any{
				"o": "org-1",
				"b": map[string]any{"flowMatrix": map[string]any{
					"dimension": "TEAM", "measure": "COUNT", "maxNodes": 50, "maxEdges": 200,
					"dateRange": map[string]any{"startDate": "2026-08-01", "endDate": "2026-08-31"},
				}},
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		var out map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("response %q: %v", rec.Body.String(), err)
		}
		if out["errors"] != nil {
			t.Fatalf("errors in response: %v", out["errors"])
		}
		data, _ := out["data"].(map[string]any)
		analytics, _ := data["analytics"].(map[string]any)
		flow, _ := analytics["flowMatrix"].(map[string]any)
		return flow
	}

	with := post(`nodes { id } edges { source } degradedReason`)
	if with["degradedReason"] != "FLOW_MATRIX_EXECUTION_FAILED" {
		t.Errorf("degradedReason = %v, want FLOW_MATRIX_EXECUTION_FAILED; result %v", with["degradedReason"], with)
	}
	if nodes, _ := with["nodes"].([]any); nodes == nil || len(nodes) != 0 {
		t.Errorf("nodes = %v, want an empty list (the swallow-to-empty answer is unchanged)", with["nodes"])
	}
	without := post(`nodes { id } edges { source }`)
	if _, present := without["degradedReason"]; present {
		t.Errorf("a selection without degradedReason must not carry it: %v", without)
	}
}
