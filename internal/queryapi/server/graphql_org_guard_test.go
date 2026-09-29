package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// The server every registered operation runs on answers a QUERY that names
// another organization with the extension's refusal before any resolver runs
// (the resolver has no ClickHouse, so running it would fail differently), and
// answers an empty or padded orgId the same way.
func TestGraphQLServerRefusesAQueryNamingAnotherOrg(t *testing.T) {
	server := newGraphQLServer(&graph.Resolver{})
	post := func(orgID string) (int, []string, string) {
		body, _ := json.Marshal(map[string]any{
			"query":     `query Q($o: String!) { experiments(orgId: $o) { __typename } }`,
			"variables": map[string]any{"o": orgID},
		})
		req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-own"}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		var out struct {
			Data   json.RawMessage `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("response %q: %v", rec.Body.String(), err)
		}
		var messages []string
		for _, e := range out.Errors {
			messages = append(messages, e.Message)
		}
		return rec.Code, messages, string(out.Data)
	}
	for orgID, want := range map[string]string{
		"org-other": "Access denied: cannot query org 'org-other'",
		"":          "A valid organization ID is required",
		" org-own":  "A valid organization ID is required",
	} {
		code, messages, data := post(orgID)
		if code != http.StatusOK || len(messages) != 1 || messages[0] != want || (data != "null" && data != "") {
			t.Errorf("orgId %q: status %d errors %v data %s, want one error %q and null data", orgID, code, messages, data, want)
		}
	}
}
