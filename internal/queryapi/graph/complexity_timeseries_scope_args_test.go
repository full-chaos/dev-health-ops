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

const complexityTimeseriesScopeQuery = `query($input: ComplexityTimeseriesInput!) { complexityTimeseries(input: $input) { totalScope points { scopeId } } }`

// complexityTimeseriesScopeTables is the table each ComplexityScope reads.
var complexityTimeseriesScopeTables = map[string]string{"REPO": "repo_complexity_daily", "FILE": "file_complexity_snapshots"}

// complexityTimeseriesScopeRun sends one complexityTimeseries request through the generated schema and records the reads.
func complexityTimeseriesScopeRun(t *testing.T, scope string, extra map[string]any) *statementRecordingClient {
	t.Helper()
	ch := &statementRecordingClient{}
	server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))
	input := map[string]any{"orgId": "org-synthetic-1", "sinceUtc": "2026-08-01T00:00:00Z", "untilUtc": "2026-08-31T00:00:00Z",
		"granularity": "DAY", "scope": scope}
	for k, v := range extra {
		input[k] = v
	}
	body, _ := json.Marshal(map[string]any{"query": complexityTimeseriesScopeQuery, "variables": map[string]any{"input": input}})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-synthetic-1"}))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), `"errors"`) {
		t.Fatalf("scope %s input %v: %s", scope, extra, rec.Body.String())
	}
	if len(ch.statements) == 0 {
		t.Fatalf("scope %s input %v: no read", scope, extra)
	}
	return ch
}

func complexityTimeseriesScopeBinding(c *statementRecordingClient, i int, name string) (any, bool) {
	for _, b := range c.bindings[i] {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

// CHAOS-14674: complexityTimeseries hands BOTH scope arguments to the read, for both scopes, as hotspots does. The web
// complexity page sends teamIds for a team scope; a read that drops it answers the org's trend under the team's name.
func TestComplexityTimeseriesTeamIdsReachTheRead(t *testing.T) {
	for _, scope := range []string{"REPO", "FILE"} {
		for label, tc := range map[string]struct{ repos, teams []string }{
			"no scope":     {nil, nil},
			"repositories": {[]string{"repo-synthetic-a"}, nil},
			"teams":        {nil, []string{"team-synthetic-a"}},
			"both":         {[]string{"repo-synthetic-a"}, []string{"team-synthetic-a"}},
		} {
			t.Run(scope+"/"+label, func(t *testing.T) {
				extra := map[string]any{}
				if tc.repos != nil {
					extra["repoIds"] = tc.repos
				}
				if tc.teams != nil {
					extra["teamIds"] = tc.teams
				}
				ch := complexityTimeseriesScopeRun(t, scope, extra)
				reads := 0
				for i, statement := range ch.statements {
					if !strings.Contains(statement, complexityTimeseriesScopeTables[scope]) {
						continue
					}
					reads++
					got, has := complexityTimeseriesScopeBinding(ch, i, "repo_ids")
					if has != (tc.repos != nil) || (has && !reflect.DeepEqual(got, tc.repos)) {
						t.Errorf("statement %d: repo_ids = %v (present %v), want %v", i, got, has, tc.repos)
					}
					got, has = complexityTimeseriesScopeBinding(ch, i, teamscope.BindingTeamIDs)
					if has != (tc.teams != nil) || (has && !reflect.DeepEqual(got, tc.teams)) {
						t.Errorf("statement %d: %s = %v (present %v), want %v: the team scope never reaches the read", i, teamscope.BindingTeamIDs, got, has, tc.teams)
					}
					if strings.Contains(statement, teamscope.Marker) != (tc.teams != nil) {
						t.Errorf("statement %d: carries the team ownership condition = %v, want %v", i, !(tc.teams != nil), tc.teams != nil)
					}
					if _, has := complexityTimeseriesScopeBinding(ch, i, "org_id"); !has {
						t.Errorf("statement %d: no org_id binding: a scope only narrows the org", i)
					}
				}
				if reads == 0 {
					t.Fatalf("no %s read in %d statements", complexityTimeseriesScopeTables[scope], len(ch.statements))
				}
			})
		}
	}
}
