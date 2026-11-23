package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

type statementRecordingClient struct {
	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *statementRecordingClient) Query(_ context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, b)
	return emptyRows{}, nil
}

// CHAOS-13814: testopsRisk takes repoIds and teamIds in the form of testopsTestWindow. An absent field, null
// and [] are no scope and send the statements of the request with no scope field. repoIds [""] narrows (to no
// repository); teamIds [""] is the organization, as for testopsTestWindow (teamscope.RepoCondition drops a
// blank id). A scope only narrows: the org_id predicate of every site stays.
func TestTestopsRisk_ScopeArgumentsThroughTheGeneratedSchema(t *testing.T) {
	const query = `query($orgId: String!, $input: TestOpsRiskInput!) { testopsRisk(orgId: $orgId, input: $input) { releaseConfidence } }`
	run := func(t *testing.T, scope map[string]any) *statementRecordingClient {
		t.Helper()
		ch := &statementRecordingClient{}
		server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))
		input := map[string]any{"startDate": "2026-01-01", "endDate": "2026-01-31"}
		for k, v := range scope {
			input[k] = v
		}
		body, _ := json.Marshal(map[string]any{"query": query, "variables": map[string]any{"orgId": "org-1", "input": input}})
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if strings.Contains(rec.Body.String(), `"errors"`) {
			t.Fatalf("scope %v: %s", scope, rec.Body.String())
		}
		if len(ch.statements) != 2 {
			t.Fatalf("scope %v: %d statements, want 2", scope, len(ch.statements))
		}
		return ch
	}
	reference := run(t, nil)
	bindingNames := func(c *statementRecordingClient, i int) map[string]any {
		m := map[string]any{}
		for _, b := range c.bindings[i] {
			m[b.Name] = b.Value
		}
		return m
	}
	for _, field := range []string{"repoIds", "teamIds"} {
		for name, value := range map[string]any{"null": nil, "empty list": []string{}} {
			t.Run(field+" "+name, func(t *testing.T) {
				got := run(t, map[string]any{field: value})
				if !reflect.DeepEqual(got.statements, reference.statements) {
					t.Error("statements differ from the request with no scope field")
				}
				for i := range got.bindings {
					if !reflect.DeepEqual(bindingNames(got, i), bindingNames(reference, i)) {
						t.Errorf("statement %d bindings differ from the request with no scope field", i)
					}
				}
			})
		}
	}
	t.Run("teamIds blank-only is the organization", func(t *testing.T) {
		got := run(t, map[string]any{"teamIds": []string{""}})
		if !reflect.DeepEqual(got.statements, reference.statements) {
			t.Error("a blank team id changed a statement")
		}
	})
	t.Run("repoIds blank-only narrows, never widens", func(t *testing.T) {
		got := run(t, map[string]any{"repoIds": []string{""}})
		assertRepoScope(t, got, []string{""})
	})
	t.Run("repoIds", func(t *testing.T) {
		assertRepoScope(t, run(t, map[string]any{"repoIds": []string{"acme/a", "id-b"}}), []string{"acme/a", "id-b"})
	})
	t.Run("teamIds", func(t *testing.T) {
		got := run(t, map[string]any{"teamIds": []string{"team-a"}})
		for i, want := range []int{3, 1} {
			if n := strings.Count(got.statements[i], "owned_repo_id"); n != want {
				t.Errorf("statement %d: %d ownership clauses, want %d", i, n, want)
			}
			assertOrgPredicateKept(t, got.statements[i], reference.statements[i])
		}
	})
	t.Run("repoIds and teamIds both apply", func(t *testing.T) {
		got := run(t, map[string]any{"repoIds": []string{"acme/a"}, "teamIds": []string{"team-a"}})
		assertRepoScope(t, got, []string{"acme/a"})
		for i, want := range []int{3, 1} {
			if n := strings.Count(got.statements[i], "owned_repo_id"); n != want {
				t.Errorf("statement %d: %d ownership clauses, want %d", i, n, want)
			}
		}
	})
}

func assertRepoScope(t *testing.T, got *statementRecordingClient, ids []string) {
	t.Helper()
	for i, want := range []int{3, 1} {
		if n := strings.Count(got.statements[i], "{repo_ids:Array(String)}"); n != want*2 {
			t.Errorf("statement %d: %d repo_ids references, want %d (%d sites)", i, n, want*2, want)
		}
		found := false
		for _, b := range got.bindings[i] {
			if b.Name == "repo_ids" {
				found = reflect.DeepEqual(b.Value, ids)
			}
		}
		if !found {
			t.Errorf("statement %d: no repo_ids binding equal to %v", i, ids)
		}
	}
}

func assertOrgPredicateKept(t *testing.T, scoped, base string) {
	t.Helper()
	const predicate = "org_id = {org_id:String}"
	if strings.Count(scoped, predicate) < strings.Count(base, predicate) {
		t.Error("a scope removed an org_id predicate")
	}
}
