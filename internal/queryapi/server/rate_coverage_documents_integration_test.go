//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The registered texts of Home, run through the generated schema on rows the
// daily job's own compute and writer stored. The CURRENT text serves the
// coverage of the pull request rework ratio (rateCoverage) beside its state:
// the share of the merged pull requests the ratio speaks for, from 0 to 1. So
// "20 % of 5 reviewed, of 12 merged" and "20 % of every merged pull request"
// are two answers at the wire, and so are "no review data" (0) and "no merged
// pull request" (null). The text that was current before (V5) gets the same
// value and state and no rateCoverage key; the text the web sends today (V4)
// gets neither key.
//
// The current text is read from its fixture file, the bytes a client sends.
func TestRegisteredHomeDocumentsServeTheCoverageOfTheReworkRatio(t *testing.T) {
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
	fixture, err := os.ReadFile("testdata/wire_capture/home_captured.graphql")
	if err != nil {
		t.Fatal(err)
	}
	currentText := string(fixture)

	// Home reads a window that ends now.
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	unreviewed := prreworktest.PullRequest{}
	reviewed := prreworktest.PullRequest{Reviews: 2}
	rework := prreworktest.PullRequest{Reviews: 3, ChangesRequested: 1}
	open := prreworktest.PullRequest{Open: true}
	type repository struct {
		provider string
		groups   []any // a pull request, then how many of it
	}
	fraction := func(reviewedCount, merged float64) *float64 {
		value := reviewedCount / merged
		return &value
	}
	cases := []struct {
		org          string
		repositories []repository
		value        float64  // the ratio in percent, as Home serves it
		hasData      bool     // the ratio is measured
		state        any      // the JSON value of rateState
		coverage     *float64 // nil: the JSON value null
	}{
		// 12 merged, 5 with review data, 1 of them with changes requested.
		{"org-coverage-measured", []repository{{"github", []any{reviewed, 4, rework, 1, unreviewed, 7}}}, 20, true, "measured", fraction(5, 12)},
		// Every merged pull request has review data.
		{"org-coverage-whole", []repository{{"github", []any{reviewed, 1, rework, 1}}}, 50, true, "measured", fraction(2, 2)},
		// A provider with the signal and a provider without it: the pull
		// requests of the second are in the denominator only.
		{"org-coverage-mixed", []repository{{"github", []any{reviewed, 5, rework, 1}}, {"gitlab", []any{reviewed, 4}}}, 100.0 / 6, true, "measured", fraction(6, 10)},
		// Merged pull requests, none with review data: 0, not null.
		{"org-coverage-no-review-data", []repository{{"github", []any{unreviewed, 3}}}, 0, false, "unknown_no_review_evidence", fraction(0, 3)},
		// Only a provider that stores no changes-requested review: 0.
		{"org-coverage-no-signal", []repository{{"gitlab", []any{reviewed, 4}}}, 0, false, "not_applicable_no_rework_signal", fraction(0, 4)},
		// A stored day with no merged pull request: no coverage.
		{"org-coverage-none-merged", []repository{{"github", []any{open, 1}}}, 0, false, "not_applicable_no_merged_pull_requests", nil},
		// No stored row: no state and no coverage.
		{"org-coverage-no-row", nil, 0, false, nil, nil},
	}
	for _, tc := range cases {
		for _, repo := range tc.repositories {
			var pullRequests []prreworktest.PullRequest
			for index := 0; index < len(repo.groups); index += 2 {
				for count := 0; count < repo.groups[index+1].(int); count++ {
					pullRequests = append(pullRequests, repo.groups[index].(prreworktest.PullRequest))
				}
			}
			prreworktest.WriteDay(ctx, t, admin, tc.org, uuid.New(), repo.provider, yesterday, time.Now().UTC(), pullRequests)
		}
	}

	server := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{ClickHouse: client, Postgres: noSyncRunPostgres{}},
	}))
	deltas := func(t *testing.T, org, document string) map[string]map[string]any {
		t.Helper()
		body, err := json.Marshal(map[string]any{"query": document,
			"variables": map[string]any{"orgId": org, "window": map[string]any{"rangeDays": 7}}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		var out struct {
			Data struct {
				Home struct {
					Deltas []map[string]any `json:"deltas"`
				} `json:"home"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		if len(out.Errors) != 0 || len(out.Data.Home.Deltas) == 0 {
			t.Fatalf("the document did not run: %s", rec.Body.String())
		}
		byMetric := make(map[string]map[string]any, len(out.Data.Home.Deltas))
		for _, delta := range out.Data.Home.Deltas {
			metric, _ := delta["metric"].(string)
			byMetric[metric] = delta
		}
		return byMetric
	}

	for _, tc := range cases {
		t.Run(tc.org, func(t *testing.T) {
			current := deltas(t, tc.org, currentText)
			delta := current["pr_rework_ratio"]
			if delta == nil {
				t.Fatalf("no pr_rework_ratio delta in the response: %v", current)
			}
			coverage, selected := delta["rateCoverage"]
			switch {
			case !selected:
				t.Errorf("current text: the delta has no rateCoverage key: %v", delta)
			case tc.coverage == nil && coverage != nil:
				t.Errorf("current text: rateCoverage %v, want null", coverage)
			case tc.coverage != nil:
				got, isNumber := coverage.(float64)
				if !isNumber || math.Abs(got-*tc.coverage) > 1e-12 {
					t.Errorf("current text: rateCoverage %v, want %v", coverage, *tc.coverage)
				}
			}
			value, _ := delta["value"].(float64)
			if delta["rateState"] != tc.state || math.Abs(value-tc.value) > 1e-9 || delta["hasData"] != tc.hasData {
				t.Errorf("current text: rateState %v value %v hasData %v, want %v %v %v",
					delta["rateState"], delta["value"], delta["hasData"], tc.state, tc.value, tc.hasData)
			}
			// The coverage is of the rework ratio only: every other delta
			// serves the key with null, change failure rate too.
			if len(current) < 5 {
				t.Fatalf("the response has %d delta(s): the case is not set", len(current))
			}
			for metric, other := range current {
				if metric == "pr_rework_ratio" {
					continue
				}
				if otherCoverage, selected := other["rateCoverage"]; !selected || otherCoverage != nil {
					t.Errorf("current text: the delta of %s has rateCoverage %v (selected %v), want the key with null", metric, otherCoverage, selected)
				}
			}

			// The text that was current before: the same delta, no coverage key.
			earlier := deltas(t, tc.org, registeredHomeV5Document)["pr_rework_ratio"]
			if _, selected := earlier["rateCoverage"]; selected {
				t.Errorf("V5 text: the response has a rateCoverage key the text does not select")
			}
			if earlier["rateState"] != delta["rateState"] || earlier["value"] != delta["value"] || earlier["hasData"] != delta["hasData"] {
				t.Errorf("V5 text: rateState %v value %v hasData %v, the current text %v %v %v",
					earlier["rateState"], earlier["value"], earlier["hasData"], delta["rateState"], delta["value"], delta["hasData"])
			}
			// The text the web sends today: neither key, the same value.
			web := deltas(t, tc.org, registeredHomeV4Document)["pr_rework_ratio"]
			_, hasState := web["rateState"]
			_, hasCoverage := web["rateCoverage"]
			if hasState || hasCoverage || web["value"] != delta["value"] || web["hasData"] != delta["hasData"] {
				t.Errorf("V4 text: rateState key %v, rateCoverage key %v, value %v hasData %v; want no key and the current text's %v %v",
					hasState, hasCoverage, web["value"], web["hasData"], delta["value"], delta["hasData"])
			}
		})
	}
}
