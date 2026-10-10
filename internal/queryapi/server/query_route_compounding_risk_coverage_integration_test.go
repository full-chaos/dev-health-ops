//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Dual accept of the `compoundingRisk` document, through the real dispatch
// handler on a real ClickHouse (CHAOS-6545): the NEW registered text and the
// legacy text both reach the operation, and each is answered with exactly the
// fields it selects: the new one serves `coverage` beside the score, the old one
// serves the same score and no coverage key.
func TestCompoundingRiskDocumentsAreBothServedThroughTheDispatchHandler(t *testing.T) {
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

	const org = "org-risk-documents"
	day := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1).Format("2006-01-02")
	for _, row := range []struct{ id, score, churn, complexity string }{
		{"00000000-0000-4000-8000-0000000000a1", "0.5", "0.5", "0.5"},  // two inputs: coverage 0.6
		{"00000000-0000-4000-8000-0000000000a2", "0.4", "0.4", "NULL"}, // one input: coverage 0.3
	} {
		if err := admin.Exec(ctx, `INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm, ownership_norm, review_norm,
 w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
SELECT '`+org+`', toDate('`+day+`'), 'repo', '`+row.id+`', `+row.score+`, 'elevated', `+row.churn+`, `+row.complexity+`, NULL, NULL,
 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, now()`); err != nil {
			t.Fatal(err)
		}
	}

	gqlServer := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{ClickHouse: client, Postgres: noSyncRunPostgres{}},
	}))
	byDigest, err := buildOperationByDigest(
		map[string]string{"compoundingRisk": digestHex(registeredCompoundingRiskDocument)},
		map[string][]string{"compoundingRisk": legacyDigestsByOperation["compoundingRisk"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	served := 0
	documentRows := routeswitch.StaticSwitch{"compoundingRisk": true}
	mux := routeswitch.NewMux(documentRows)
	mux.Register("compoundingRisk", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served++
		gqlServer.ServeHTTP(w, r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: org})))
	}))
	dispatch := newDocumentDispatchHandler(func(string) string { return "" }, mux, byDigest, nil, nil, nil, "", nil)

	run := func(t *testing.T, document string) []map[string]any {
		t.Helper()
		body, err := json.Marshal(map[string]any{"query": document, "variables": map[string]any{"orgId": org}})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		*request = *request.WithContext(iaInternalCtx(request.Context()))
		request.Header.Set(internalidentity.HeaderOrgID, org)
		request.Header.Set(internalidentity.HeaderRole, "member")
		request.Header.Set(internalidentity.HeaderSuperuser, "false")
		request.Header.Set(internalidentity.HeaderImpersonationActive, "false")
		recorder := httptest.NewRecorder()
		dispatch(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("HTTP %d: %s", recorder.Code, recorder.Body.String())
		}
		var out struct {
			Data struct {
				CompoundingRisk struct {
					Rows []map[string]any `json:"rows"`
				} `json:"compoundingRisk"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil || len(out.Errors) != 0 {
			t.Fatalf("decode %s: %v", recorder.Body.String(), err)
		}
		return out.Data.CompoundingRisk.Rows
	}

	current := run(t, registeredCompoundingRiskDocument)
	legacy := run(t, registeredCompoundingRiskV1Document)
	if served != 2 {
		t.Fatalf("the operation was reached %d times, want 2 (one for each text)", served)
	}
	if len(current) != 2 || len(legacy) != 2 {
		t.Fatalf("rows: current %d, legacy %d, want 2 and 2", len(current), len(legacy))
	}
	wantCoverage := map[string]float64{
		"00000000-0000-4000-8000-0000000000a1": 0.6,
		"00000000-0000-4000-8000-0000000000a2": 0.3,
	}
	for _, row := range current {
		id, _ := row["scopeId"].(string)
		coverage, ok := row["coverage"].(float64)
		if !ok || coverage < wantCoverage[id]-1e-9 || coverage > wantCoverage[id]+1e-9 {
			t.Errorf("current text: row %s coverage = %v, want %v", id, row["coverage"], wantCoverage[id])
		}
	}
	for i, row := range legacy {
		if _, has := row["coverage"]; has {
			t.Errorf("legacy text: row %d has a coverage key it did not select: %v", i, row)
		}
		if row["score"] == nil {
			t.Errorf("legacy text: row %d has no score: %v", i, row)
		}
	}
}
