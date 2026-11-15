//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// B2 end to end: the registered TestOpsPipeline text with the web's pipelines batch, through the generated schema and
// the query route's error presenter, on rows the real writer stored. Under the row bound the answer carries the pooled
// ORG P95 and queue series; past it the answer is null with one authored error that names the reason and the measure:
// unavailable, never a 0 and never a partial answer. The rates of the same batch go with it (the whole batch fails).
func TestTestOpsPipelinePooledReadPastTheBoundIsUnavailable(t *testing.T) {
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

	const org = "org-13858-e2e"
	const repoA, repoB = "13858000-0000-4000-8000-0000000030a1", "13858000-0000-4000-8000-0000000030b1"
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	exec := func(q string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, q, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, q)
		}
	}
	for repo, team := range map[string]string{repoA: "team-a", repoB: "team-b"} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, now(), now())`, repo, "acme/"+team, org)
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', NULL, '2025-01-01 00:00:00')`,
			org, team, repo, "acme/"+team)
	}
	// team-a [10,20,30] s, team-b [10..170] s, every run queued 60 s: ORG p95 160.5 (pooled), queue 60.
	seed := func(repo string, n int) {
		for i := 1; i <= n; i++ {
			started := day.Add(time.Duration(i) * time.Minute)
			exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id, duration_seconds, queue_seconds) VALUES (?, ?, 'success', ?, ?, ?, ?, 60)`,
				repo, fmt.Sprintf("e2e-%s-%d", repo[len(repo)-2:], i), started.Format("2006-01-02 15:04:05.000"), "2026-01-09 00:00:00.000", org, float64(10*i))
		}
	}
	seed(repoA, 3)
	seed(repoB, 17)
	pipeline, err := daily.NewTestopsPipelineExecutor(admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.ComputeFamily(ctx, daily.Run{ID: "run-e2e", OrganizationID: org, TargetDay: day},
		daily.Partition{ID: "part-e2e", RunID: "run-e2e", RepoIDs: []daily.RepositoryID{repoA, repoB}}); err != nil {
		t.Fatalf("writer: %v", err)
	}

	server := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: client}}))
	server.SetErrorPresenter(safeGraphQLErrorPresenter("query", nil))
	measures := []string{"PIPELINE_SUCCESS_RATE", "PIPELINE_FAILURE_RATE", "PIPELINE_DURATION_P95", "PIPELINE_QUEUE_TIME", "PIPELINE_RERUN_RATE"}
	var timeseries []map[string]any
	for _, m := range measures {
		timeseries = append(timeseries, map[string]any{"dimension": "TEAM", "measure": m, "interval": "DAY",
			"dateRange": map[string]any{"startDate": "2026-01-01", "endDate": "2026-01-06"}})
	}
	batch := map[string]any{"timeseries": timeseries, "breakdowns": []map[string]any{{"dimension": "TEAM", "measure": "PIPELINE_FAILURE_RATE",
		"dateRange": map[string]any{"startDate": "2026-01-01", "endDate": "2026-01-06"}, "topN": 10}}}
	run := func() (map[string]any, []map[string]any, string) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"query": registeredTestOpsPipelineDocument, "variables": map[string]any{"orgId": org, "batch": batch}})
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
		return out.Data, out.Errors, rec.Body.String()
	}

	// Under the bound: the ORG series of the pooled measures are served.
	data, errs, raw := run()
	if len(errs) != 0 || data == nil {
		t.Fatalf("under the bound the document failed: %s", raw)
	}
	orgValue := func(measure string) any {
		series, _ := data["analytics"].(map[string]any)["timeseries"].([]any)
		for _, s := range series {
			one := s.(map[string]any)
			if one["measure"] == measure && one["scope"] == "ORG" {
				buckets := one["buckets"].([]any)
				if len(buckets) != 1 {
					t.Fatalf("%s ORG buckets %v, want one", measure, buckets)
				}
				return buckets[0].(map[string]any)["value"]
			}
		}
		t.Fatalf("%s has no ORG series: %s", measure, raw)
		return nil
	}
	if v, _ := orgValue("PIPELINE_DURATION_P95").(float64); math.Abs(v-160.5) > 1e-9 {
		t.Fatalf("ORG P95 = %v, want 160.5 pooled: %s", v, raw)
	}
	if v, _ := orgValue("PIPELINE_QUEUE_TIME").(float64); math.Abs(v-60) > 1e-9 {
		t.Fatalf("ORG queue = %v, want 60: %s", v, raw)
	}

	// Past the bound: null answer, one authored UNAVAILABLE error with the reason and the measure.
	saved := analytics.TestopsPooledMaxRowsToRead
	t.Cleanup(func() { analytics.TestopsPooledMaxRowsToRead = saved })
	analytics.TestopsPooledMaxRowsToRead = 5
	data, errs, raw = run()
	if data != nil {
		t.Fatalf("past the bound the answer holds data, want null: %s", raw)
	}
	if len(errs) != 1 {
		t.Fatalf("past the bound: %d errors, want one: %s", len(errs), raw)
	}
	ext, _ := errs[0]["extensions"].(map[string]any)
	if ext["code"] != "UNAVAILABLE" || ext["reason"] != "testops_pooled_read_bound" {
		t.Fatalf("past the bound: extensions %v, want code UNAVAILABLE and reason testops_pooled_read_bound: %s", ext, raw)
	}
	if m := ext["measure"]; m != "PIPELINE_DURATION_P95" && m != "PIPELINE_QUEUE_TIME" {
		t.Fatalf("past the bound: measure %v, want a pooled measure: %s", m, raw)
	}
	if msg, _ := errs[0]["message"].(string); msg == graphQLInternalMessage || msg == "" {
		t.Fatalf("past the bound: message %q, want the authored unavailable text: %s", msg, raw)
	}
	t.Logf("B2 e2e past the bound: %s", raw)
}
