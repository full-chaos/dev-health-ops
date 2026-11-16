//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"
	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-13868 end to end: the four registered TestOps documents with filterEmptyReason, through the generated schema and the
// error presenter, on a real ClickHouse. A team that owns no repository gets team_owns_no_repository on the analytics answer,
// testopsTestWindow and testopsJobFailures; a team that owns one gets null; the legacy texts (without the field) still answer
// and carry no such key.
func TestTestOpsDocumentsServeFilterEmptyReason(t *testing.T) {
	ctx := context.Background()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const org = "org-13868-e2e"
	const repo = "13868000-0000-4000-8000-0000000040a1"
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/owned', 'github', ?, now(), now())`, repo, org); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-owner', ?, 'acme/owned', 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', NULL, '2025-01-01 00:00:00')`, org, repo); err != nil {
		t.Fatal(err)
	}

	server := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: client}}))
	server.SetErrorPresenter(safeGraphQLErrorPresenter("query", nil))
	post := func(document string, variables map[string]any) (map[string]any, string) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"query": document, "variables": variables})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Scope: pepgate.Legacy(org)}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		var out struct {
			Data   map[string]any   `json:"data"`
			Errors []map[string]any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		if len(out.Errors) != 0 || out.Data == nil {
			t.Fatalf("the document failed: %s", rec.Body.String())
		}
		return out.Data, rec.Body.String()
	}
	reasonOf := func(data map[string]any, root string) (any, bool) {
		answer, _ := data[root].(map[string]any)
		v, present := answer["filterEmptyReason"]
		return v, present
	}
	analyticsVars := func(team string) map[string]any {
		return map[string]any{"orgId": org, "batch": map[string]any{
			"timeseries": []map[string]any{{"dimension": "TEAM", "measure": "PIPELINE_SUCCESS_RATE", "interval": "DAY",
				"dateRange": map[string]any{"startDate": "2026-01-01", "endDate": "2026-01-06"}}},
			"breakdowns": []map[string]any{},
			"filters":    map[string]any{"scope": map[string]any{"level": "TEAM", "ids": []string{team}}},
		}}
	}
	windowVars := func(team string) map[string]any {
		return map[string]any{"orgId": org, "input": map[string]any{"sinceDate": "2026-01-01", "untilDate": "2026-01-06", "teamIds": []string{team}}}
	}
	jobVars := func(team string) map[string]any {
		return map[string]any{"orgId": org, "input": map[string]any{"sinceDate": "2026-01-01", "untilDate": "2026-01-06", "teamIds": []string{team}, "limit": 20}}
	}

	type op struct {
		name, root      string
		current, legacy string
		vars            func(string) map[string]any
	}
	for _, o := range []op{
		{"testOpsPipeline", "analytics", registeredTestOpsPipelineDocument, registeredTestOpsPipelineV2Document, analyticsVars},
		{"testOpsCoverage", "analytics", registeredTestOpsCoverageDocument, registeredTestOpsCoverageV2Document, analyticsVars},
		{"testopsTestWindow", "testopsTestWindow", registeredTestopsTestWindowDocument, registeredTestopsTestWindowV1Document, windowVars},
		{"testopsJobFailures", "testopsJobFailures", registeredTestopsJobFailuresDocument, registeredTestopsJobFailuresV1Document, jobVars},
	} {
		{
			data, raw := post(o.current, o.vars("team-nobody"))
			if v, present := reasonOf(data, o.root); !present || v != "team_owns_no_repository" {
				t.Errorf("%s, a team that owns no repository: filterEmptyReason = %v (present %v), want team_owns_no_repository: %s", o.name, v, present, raw)
			}
		}
		{
			data, raw := post(o.current, o.vars("team-owner"))
			if v, present := reasonOf(data, o.root); !present || v != nil {
				t.Errorf("%s, a team that owns a repository: filterEmptyReason = %v (present %v), want null: %s", o.name, v, present, raw)
			}
		}
		// The legacy text still answers and does not carry the key.
		data, raw := post(o.legacy, o.vars("team-nobody"))
		if _, present := reasonOf(data, o.root); present || strings.Contains(raw, "filterEmptyReason") {
			t.Errorf("%s legacy text answered a filterEmptyReason it did not select: %s", o.name, raw)
		}
	}
}
