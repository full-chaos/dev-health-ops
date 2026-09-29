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
		var out map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("response %q: %v", rec.Body.String(), err)
		}
		var messages []string
		if errs, ok := out["errors"].([]any); ok {
			for _, e := range errs {
				entry, _ := e.(map[string]any)
				if len(entry) != 1 {
					t.Errorf("orgId %q: wire error %v carries more than a message (Python's refusal is the message alone)", orgID, entry)
				}
				message, _ := entry["message"].(string)
				messages = append(messages, message)
			}
		}
		data, _ := json.Marshal(out["data"])
		return rec.Code, messages, string(data)
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
