package graph

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-14642: hotspots hands BOTH scope arguments of HotspotsInput to the read, through the generated schema. The web
// Code page sends teamIds for a team scope; a field that drops it answers for the whole org under the team's name.
func TestHotspots_PassesTheScopeArgumentsToTheRead(t *testing.T) {
	const query = `query($input: HotspotsInput!) { hotspots(input: $input) { rows { repoId } repos { repoId } } }`
	run := func(t *testing.T, repos, teams []string) *statementRecordingClient {
		t.Helper()
		ch := &statementRecordingClient{}
		server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))
		input := map[string]any{"orgId": "org-1", "sinceUtc": "2026-08-01T00:00:00Z", "untilUtc": "2026-08-31T00:00:00Z"}
		if repos != nil {
			input["repoIds"] = repos
		}
		if teams != nil {
			input["teamIds"] = teams
		}
		body, _ := json.Marshal(map[string]any{"query": query, "variables": map[string]any{"input": input}})
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if strings.Contains(rec.Body.String(), `"errors"`) {
			t.Fatalf("repos %v teams %v: %s", repos, teams, rec.Body.String())
		}
		if len(ch.statements) == 0 {
			t.Fatalf("repos %v teams %v: no read", repos, teams)
		}
		return ch
	}
	binding := func(c *statementRecordingClient, i int, name string) (any, bool) {
		for _, b := range c.bindings[i] {
			if b.Name == name {
				return b.Value, true
			}
		}
		return nil, false
	}
	for label, tc := range map[string]struct{ repos, teams []string }{
		"no scope":     {nil, nil},
		"repositories": {[]string{"repo-a"}, nil},
		"teams":        {nil, []string{"team-a"}},
		"both":         {[]string{"repo-a"}, []string{"team-a"}},
	} {
		t.Run(label, func(t *testing.T) {
			ch := run(t, tc.repos, tc.teams)
			reads := 0
			for i, statement := range ch.statements {
				if !strings.Contains(statement, "file_hotspot_daily") {
					continue
				}
				reads++
				got, has := binding(ch, i, "repo_ids")
				if has != (tc.repos != nil) || (has && !reflect.DeepEqual(got, tc.repos)) {
					t.Errorf("statement %d: repo_ids = %v (present %v), want %v", i, got, has, tc.repos)
				}
				got, has = binding(ch, i, teamscope.BindingTeamIDs)
				if has != (tc.teams != nil) || (has && !reflect.DeepEqual(got, tc.teams)) {
					t.Errorf("statement %d: %s = %v (present %v), want %v: the team scope never reaches the read", i, teamscope.BindingTeamIDs, got, has, tc.teams)
				}
				if strings.Contains(statement, teamscope.Marker) != (tc.teams != nil) {
					t.Errorf("statement %d: carries the team ownership condition = %v, want %v", i, tc.teams == nil, tc.teams != nil)
				}
				if _, has := binding(ch, i, "org_id"); !has {
					t.Errorf("statement %d: no org_id binding: a scope only narrows the org", i)
				}
			}
			if reads == 0 {
				t.Fatalf("no file_hotspot_daily read in %d statements", len(ch.statements))
			}
		})
	}
}
