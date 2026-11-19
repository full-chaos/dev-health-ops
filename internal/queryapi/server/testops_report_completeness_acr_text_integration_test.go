//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// acrVendoredTestopsTestWindowDocument is the testopsTestWindow text the Context Fabric (dev-health-acr) catalogue vendors as
// the current text (contracts/mcp/ops-catalogue/registry.v1.json at acr dc2178b4eff2, byte for byte): the text before
// reportCompleteness.
const acrVendoredTestopsTestWindowDocument = `query TestOpsTestWindow($orgId: String!, $input: TestOpsTestWindowInput!) {
  testopsTestWindow(orgId: $orgId, input: $input) {
    sinceDate
    untilDate
    coverage {
      completedRuns
      runsWithTests
      sharePct
      __typename
    }
    cases {
      passed
      failed
      skipped
      quarantined
      total
      __typename
    }
    passRatePct
    failureRatePct
    flakeRatePct
    filterEmptyReason
    __typename
  }
}`

// The text acr vendors keeps routing to testopsTestWindow and answers in its own shape after reportCompleteness is added: the
// field is only served to a text that selects it. The current text answers the counts of the same rows.
func TestTestopsTestWindowAcrVendoredTextAnswersTheOldShape(t *testing.T) {
	if acrVendoredTestopsTestWindowDocument != registeredTestopsTestWindowV2Document {
		t.Fatal("the acr-vendored text is not the V2 legacy text")
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"testopsTestWindow": digestHex(registeredTestopsTestWindowDocument)},
		map[string][]string{"testopsTestWindow": legacyDigestsByOperation["testopsTestWindow"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	if op, ok := operationForDocument(acrVendoredTestopsTestWindowDocument, byDigest); !ok || op != "testopsTestWindow" {
		t.Fatalf("the acr-vendored text resolves to %q, %v; want testopsTestWindow, true", op, ok)
	}

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

	const org = "org-14098-acr"
	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count,
pass_rate, failure_rate, flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, computed_at, report_runs,
partial_report_runs, unmarked_report_runs) VALUES (?, '14098000-0000-4000-8000-0000000000a1', ?, 5, 4, 1, 0.8, 0.2, 0, 0, 2, 0, ?, 2, 1, 1)`,
		org, day, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	server := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: client}}))
	server.SetErrorPresenter(safeGraphQLErrorPresenter("query", nil))
	post := func(document string) (map[string]any, string) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"query": document, "variables": map[string]any{
			"orgId": org, "input": map[string]any{"sinceDate": "2026-07-22", "untilDate": "2026-07-22"}}})
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
		answer, _ := out.Data["testopsTestWindow"].(map[string]any)
		return answer, rec.Body.String()
	}

	old, raw := post(acrVendoredTestopsTestWindowDocument)
	t.Logf("acr-vendored text answer: %s", raw)
	keys := make([]string, 0, len(old))
	for key := range old {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{"__typename", "cases", "coverage", "failureRatePct", "filterEmptyReason", "flakeRatePct", "passRatePct", "sinceDate", "untilDate"}
	if strings.Join(keys, ",") != strings.Join(want, ",") || strings.Contains(raw, "reportCompleteness") {
		t.Errorf("the acr-vendored text answered keys %v, want exactly %v (no reportCompleteness)", keys, want)
	}
	if cases, _ := old["cases"].(map[string]any); cases["passed"] != float64(4) || cases["failed"] != float64(1) || cases["total"] != float64(5) {
		t.Errorf("the acr-vendored text answered cases %v, want passed 4, failed 1, total 5", old["cases"])
	}

	current, raw := post(registeredTestopsTestWindowDocument)
	t.Logf("current text answer: %s", raw)
	rc, _ := current["reportCompleteness"].(map[string]any)
	if rc["reportRuns"] != float64(2) || rc["partialRuns"] != float64(1) || rc["unmarkedRuns"] != float64(1) || rc["unmeasuredDays"] != float64(0) {
		t.Errorf("the current text answered reportCompleteness %v, want 2/1/1/0", current["reportCompleteness"])
	}
}
