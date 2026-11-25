package graph

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// The web code page sends teamIds in HotspotsInput (src/lib/api/code.ts); a
// team scope must reach the read, as repoIds does.
func TestWebhuntHotspotsTeamIdsReachTheRead(t *testing.T) {
	const query = `query($input: HotspotsInput!) { hotspots(input: $input) { rows { repoId } repos { repoId } } }`
	run := func(t *testing.T, scope map[string]any) *statementRecordingClient {
		t.Helper()
		ch := &statementRecordingClient{}
		server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))
		input := map[string]any{"orgId": "org-1", "sinceUtc": "2026-08-01T00:00:00Z", "untilUtc": "2026-08-31T00:00:00Z"}
		for k, v := range scope {
			input[k] = v
		}
		body, _ := json.Marshal(map[string]any{"query": query, "variables": map[string]any{"input": input}})
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if strings.Contains(rec.Body.String(), `"errors"`) {
			t.Fatalf("scope %v: %s", scope, rec.Body.String())
		}
		return ch
	}
	carries := func(c *statementRecordingClient, want string) bool {
		for i, s := range c.statements {
			if strings.Contains(s, want) {
				return true
			}
			for _, b := range c.bindings[i] {
				if strings.Contains(strings.ToLower(jsonOf(b.Value)), strings.ToLower(want)) {
					return true
				}
			}
		}
		return false
	}
	byRepo := run(t, map[string]any{"repoIds": []string{"repo-a"}})
	if !carries(byRepo, "repo-a") {
		t.Fatalf("control: repoIds [repo-a] is absent from every statement and binding: %v", byRepo.statements)
	}
	byTeam := run(t, map[string]any{"teamIds": []string{"team-a"}})
	if !carries(byTeam, "team-a") && !carries(byTeam, "team_repo_ownership") {
		t.Fatalf("teamIds [team-a] is absent from every statement and binding (%d statements): the team scope never reaches the read", len(byTeam.statements))
	}
}

func jsonOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
