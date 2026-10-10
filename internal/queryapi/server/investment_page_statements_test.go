package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// pageStatements is a ClickHouse client that answers the statements of the
// investment page and counts them by kind. Every other statement (a telemetry
// probe) is answered with no rows and not counted.
type pageStatements struct {
	mu     sync.Mutex
	counts map[string]int
}

const (
	statementBreakdown      = "breakdown"
	statementQualityStats   = "evidence quality stats"
	statementQualityByGroup = "evidence quality by group"
	statementCoverage       = "coverage sums"
	statementSankey         = "grouped sankey"
)

func (client *pageStatements) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	kind, row := "", []any(nil)
	switch {
	case strings.Contains(statement, "AS assigned_team"):
		// total, assigned to a team, repo total, assigned to a repo, direct, team fallback, fan-out
		kind, row = statementCoverage, []any{200.0, 150.0, 200.0, 120.0, 100.0, 20.0, 1.5}
	case strings.Contains(statement, "AS grouping_set,"):
		kind = statementSankey
	case strings.Contains(statement, "evidence_quality_group_key"):
		// key, mean, total, known
		kind, row = statementQualityByGroup, []any{"feature_delivery", 0.8, uint64(12), uint64(9)}
	case strings.Contains(statement, "quality_known_count"):
		// total, known, mean, stddev, high, moderate, low, very low, unknown
		kind, row = statementQualityStats, []any{uint64(40), uint64(30), 0.7, 0.1, uint64(10), uint64(12), uint64(6), uint64(2), uint64(10)}
	case strings.Contains(statement, "AS dimension_value"):
		// key, value
		kind, row = statementBreakdown, []any{"feature_delivery", 25.0}
	}
	if kind != "" {
		client.mu.Lock()
		client.counts[kind]++
		client.mu.Unlock()
	}
	if row == nil {
		return &oneRow{}, nil
	}
	return &oneRow{row: row}, nil
}

// oneRow gives at most one row, into the destinations the reads scan.
type oneRow struct {
	row  []any
	read bool
}

func (rows *oneRow) Next() bool {
	if rows.row == nil || rows.read {
		return false
	}
	rows.read = true
	return true
}

func (rows *oneRow) Scan(destinations ...any) error {
	for index, destination := range destinations {
		target := reflect.ValueOf(destination).Elem()
		value := reflect.ValueOf(rows.row[index])
		if target.Kind() == reflect.Pointer {
			held := reflect.New(target.Type().Elem())
			held.Elem().Set(value.Convert(target.Type().Elem()))
			target.Set(held)
			continue
		}
		target.Set(value.Convert(target.Type()))
	}
	return nil
}
func (*oneRow) Err() error   { return nil }
func (*oneRow) Close() error { return nil }

// investmentPage runs one operation of the investment page through the
// generated executable schema, as the query route serves it, and returns the
// bytes of data.analytics.
func investmentPage(t *testing.T, client *pageStatements, document string, batch map[string]any) json.RawMessage {
	t.Helper()
	server := gqlhandler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: client}}))
	body, _ := json.Marshal(map[string]any{"query": document, "variables": map[string]any{"orgId": "org-1", "batch": batch}})
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: "org-1"}))
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	var out struct {
		Data struct {
			Analytics json.RawMessage `json:"analytics"`
		} `json:"data"`
		Errors json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil || out.Errors != nil || out.Data.Analytics == nil {
		t.Fatalf("response %s (decode error %v)", recorder.Body.String(), err)
	}
	return out.Data.Analytics
}

func sankeyBatch(path []string, start, end string) map[string]any {
	return map[string]any{"useInvestment": true, "sankey": map[string]any{
		"path": path, "measure": "COUNT", "maxNodes": 100, "maxEdges": 500,
		"dateRange": map[string]any{"startDate": start, "endDate": end},
	}}
}

// everythingDocument selects every part of the analytics answer that has a
// statement of its own: an operation with this document makes the server
// compute what it computed for EVERY operation before the selection was read.
const everythingDocument = `query Everything($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    breakdowns { dimension measure items { key value } }
    sankey { nodes { id label dimension value } edges { source target value } coverage { teamCoverage repoCoverage } unit }
    evidenceQualityDistribution
    evidenceQualityStats { mean stddev total bandCounts }
    evidenceQualityByGroup { key label mean total }
  }
}`

// One load of the investment page, on its Evidence tab, is five analytics
// operations: the flow of the window (A), the flow of the prior window (B) and
// the repository-and-team flow of the window (C), all three with the
// registered InvestmentFull document; the mix (D) with the registered
// InvestmentBreakdown document; and the evidence table's groups (E) with the
// registered InvestmentEvidenceQuality document. E sends one breakdown only to
// give the evidence quality parts their window, and selects neither
// `breakdowns` nor the stats.
//
// Each operation sends only the statements of the parts its document selects.
// The same five batches with a document that selects everything show what the
// server sent for them before the selection was read: E's breakdown statement
// and E's evidence quality stats statement, for answers that no document
// selected. The page's six heavy statements (three coverage sums, three
// grouped sankeys) are all selected, and all sent, in both cases.
//
// And every field a document selects holds its value.
func TestTheInvestmentPageSendsOnlyTheStatementsItsDocumentsSelect(t *testing.T) {
	window, prior := [2]string{"2026-07-01", "2026-09-30"}, [2]string{"2026-04-01", "2026-06-30"}
	breakdown := func(dimension string, top int) map[string]any {
		return map[string]any{"dimension": dimension, "measure": "COUNT", "topN": top, "dateRange": map[string]any{"startDate": window[0], "endDate": window[1]}}
	}
	type operation struct {
		name, document string
		batch          map[string]any
	}
	page := []operation{
		{"A the flow", registeredInvestmentFullDocument, sankeyBatch([]string{"TEAM", "THEME", "REPO"}, window[0], window[1])},
		{"B the baseline flow", registeredInvestmentFullDocument, sankeyBatch([]string{"TEAM", "THEME", "REPO"}, prior[0], prior[1])},
		{"C the repository and team flow", registeredInvestmentFullDocument, sankeyBatch([]string{"SUBCATEGORY", "REPO", "TEAM"}, window[0], window[1])},
		{"D the mix", registeredInvestmentBreakdownDocument, map[string]any{"useInvestment": true, "breakdowns": []any{breakdown("THEME", 50), breakdown("SUBCATEGORY", 100)}}},
		{"E the evidence groups", registeredInvestmentEvidenceQualityDocument, map[string]any{"useInvestment": true, "breakdowns": []any{breakdown("THEME", 50)}, "evidenceQualityGroupBy": "THEME"}},
	}

	// What the server computes for these batches when everything is selected.
	everything := &pageStatements{counts: map[string]int{}}
	for _, call := range page {
		investmentPage(t, everything, everythingDocument, call.batch)
	}
	before := map[string]int{statementBreakdown: 3, statementQualityStats: 2, statementQualityByGroup: 1, statementCoverage: 3, statementSankey: 3}
	if !reflect.DeepEqual(everything.counts, before) {
		t.Fatalf("statements of the five batches with everything selected = %v, want %v: the case is not the one this test is for", everything.counts, before)
	}

	client := &pageStatements{counts: map[string]int{}}
	answers := map[string]json.RawMessage{}
	for _, call := range page {
		answers[call.name] = investmentPage(t, client, call.document, call.batch)
	}
	after := map[string]int{statementBreakdown: 2, statementQualityStats: 1, statementQualityByGroup: 1, statementCoverage: 3, statementSankey: 3}
	if !reflect.DeepEqual(client.counts, after) {
		t.Errorf("statements of one page load = %v, want %v (with everything selected: %v)", client.counts, after, before)
	}

	// Every selected field holds its value.
	const coverage = `"coverage":{"teamCoverage":0.75,"repoCoverage":0.6,"__typename":"SankeyCoverage"}`
	for _, name := range []string{"A the flow", "B the baseline flow", "C the repository and team flow"} {
		if answer := string(answers[name]); !strings.Contains(answer, coverage) || strings.Contains(answer, "evidenceQuality") {
			t.Errorf("%s selects sankey.coverage and no evidence quality field: %s", name, answer)
		}
	}
	mix := string(answers["D the mix"])
	for _, field := range []string{`"evidenceQualityStats":{"mean":0.7,"stddev":0.1,"total":40,`, `"evidenceQualityDistribution":{`, `"key":"feature_delivery","value":25`} {
		if !strings.Contains(mix, field) {
			t.Errorf("the mix selects the breakdowns, the evidence quality stats and its distribution and lacks %s: %s", field, mix)
		}
	}
	groups := string(answers["E the evidence groups"])
	if !strings.Contains(groups, `"evidenceQualityByGroup":[{"key":"feature_delivery"`) || strings.Contains(groups, "breakdowns") || strings.Contains(groups, "evidenceQualityStats") {
		t.Errorf("the evidence groups select evidenceQualityByGroup only: %s", groups)
	}
}

// What a document selects decides what is computed, part by part: each part
// with a statement of its own is sent exactly when its field is in the
// selection (for the stats, also when the distribution that is made of it
// is), whether the field is there plainly, under an alias, through a fragment,
// or kept by a directive. A grouped sankey is sent whenever the batch holds a
// sankey: it is one statement for the whole `sankey` field. And the answer of
// a field does not depend on what ELSE the document selects: its bytes are
// the bytes of the same field in the document that selects everything.
func TestAnAnalyticsPartIsComputedExactlyWhenItIsSelected(t *testing.T) {
	batch := sankeyBatch([]string{"TEAM", "REPO"}, "2026-07-01", "2026-09-30")
	batch["evidenceQualityGroupBy"] = "THEME"
	batch["breakdowns"] = []any{map[string]any{"dimension": "THEME", "measure": "COUNT", "topN": 50,
		"dateRange": map[string]any{"startDate": "2026-07-01", "endDate": "2026-09-30"}}}
	query := func(selection string) string {
		return `query Q($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { ` + selection + ` } }`
	}
	whole := map[string]json.RawMessage{}
	if err := json.Unmarshal(investmentPage(t, &pageStatements{counts: map[string]int{}}, everythingDocument, batch), &whole); err != nil {
		t.Fatal(err)
	}
	const sankey = statementSankey
	for _, test := range []struct {
		name     string
		document string
		want     map[string]int
		fields   map[string]string // field of the answer -> the field of the whole answer its bytes must equal
	}{
		{"the breakdowns", query(`breakdowns { dimension measure items { key value } }`),
			map[string]int{statementBreakdown: 1, sankey: 1}, map[string]string{"breakdowns": "breakdowns"}},
		{"the stats", query(`evidenceQualityStats { mean stddev total bandCounts }`),
			map[string]int{statementQualityStats: 1, sankey: 1}, map[string]string{"evidenceQualityStats": "evidenceQualityStats"}},
		{"the distribution only", query(`evidenceQualityDistribution`),
			map[string]int{statementQualityStats: 1, sankey: 1}, map[string]string{"evidenceQualityDistribution": "evidenceQualityDistribution"}},
		{"the stats under an alias", query(`q: evidenceQualityStats { mean stddev total bandCounts }`),
			map[string]int{statementQualityStats: 1, sankey: 1}, map[string]string{"q": "evidenceQualityStats"}},
		{"the stats through a fragment", query(`...F`) + ` fragment F on AnalyticsResult { evidenceQualityStats { mean stddev total bandCounts } }`,
			map[string]int{statementQualityStats: 1, sankey: 1}, map[string]string{"evidenceQualityStats": "evidenceQualityStats"}},
		{"the groups", query(`evidenceQualityByGroup { key label mean total }`),
			map[string]int{statementQualityByGroup: 1, sankey: 1}, map[string]string{"evidenceQualityByGroup": "evidenceQualityByGroup"}},
		{"the coverage through an inline fragment", query(`sankey { ... on SankeyResult { nodes { id label dimension value } edges { source target value } coverage { teamCoverage repoCoverage } unit } }`),
			map[string]int{statementCoverage: 1, sankey: 1}, map[string]string{"sankey": "sankey"}},
		{"a sankey with no coverage", query(`sankey { unit }`), map[string]int{sankey: 1}, nil},
		{"the stats kept by a directive", `query Q($orgId: String!, $batch: AnalyticsRequestInput!, $with: Boolean = true) { analytics(orgId: $orgId, batch: $batch) { evidenceQualityStats @include(if: $with) { mean stddev total bandCounts } } }`,
			map[string]int{statementQualityStats: 1, sankey: 1}, map[string]string{"evidenceQualityStats": "evidenceQualityStats"}},
		{"the stats left out by a directive", `query Q($orgId: String!, $batch: AnalyticsRequestInput!, $with: Boolean = false) { analytics(orgId: $orgId, batch: $batch) { evidenceQualityStats @include(if: $with) { mean } sankey { unit } } }`,
			map[string]int{sankey: 1}, nil},
		{"everything", everythingDocument,
			map[string]int{statementBreakdown: 1, statementQualityStats: 1, statementQualityByGroup: 1, statementCoverage: 1, sankey: 1},
			map[string]string{"breakdowns": "breakdowns", "evidenceQualityStats": "evidenceQualityStats", "evidenceQualityDistribution": "evidenceQualityDistribution", "evidenceQualityByGroup": "evidenceQualityByGroup", "sankey": "sankey"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &pageStatements{counts: map[string]int{}}
			answer := map[string]json.RawMessage{}
			if err := json.Unmarshal(investmentPage(t, client, test.document, batch), &answer); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(client.counts, test.want) {
				t.Errorf("statements = %v, want %v", client.counts, test.want)
			}
			for field, of := range test.fields {
				if got := string(answer[field]); got != string(whole[of]) || got == "null" || got == "" || got == "[]" {
					t.Errorf("%s = %s, want the bytes of %s in the document that selects everything: %s", field, got, of, whole[of])
				}
			}
		})
	}
}

// A batch that is wrong is an error whether or not the operation selects the
// part that is wrong: the selection decides which statements are sent, never
// which requests are valid. A group dimension that is not one of the three,
// and a breakdown whose date range is turned around, fail with a document
// that selects nothing of them, and send no statement.
func TestAWrongAnalyticsBatchIsAnErrorWhateverIsSelected(t *testing.T) {
	window := map[string]any{"startDate": "2026-07-01", "endDate": "2026-09-30"}
	for name, batch := range map[string]map[string]any{
		"a group dimension that is not allowed": {"useInvestment": true, "evidenceQualityGroupBy": "REPO",
			"breakdowns": []any{map[string]any{"dimension": "THEME", "measure": "COUNT", "topN": 10, "dateRange": window}}},
		"a breakdown whose range ends before it starts": {"useInvestment": true,
			"breakdowns": []any{map[string]any{"dimension": "THEME", "measure": "COUNT", "topN": 10, "dateRange": map[string]any{"startDate": "2026-09-30", "endDate": "2026-07-01"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			for _, document := range []string{
				`query Q($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { __typename } }`,
				everythingDocument,
			} {
				client := &pageStatements{counts: map[string]int{}}
				server := gqlhandler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: client}}))
				body, _ := json.Marshal(map[string]any{"query": document, "variables": map[string]any{"orgId": "org-1", "batch": batch}})
				request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
				request.Header.Set("Content-Type", "application/json")
				request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: "org-1"}))
				recorder := httptest.NewRecorder()
				server.ServeHTTP(recorder, request)
				var out struct {
					Errors []struct {
						Message string `json:"message"`
					} `json:"errors"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil {
					t.Fatal(err)
				}
				if len(out.Errors) == 0 {
					t.Errorf("a wrong batch was answered with no error by the document %.60q: %s", document, recorder.Body.String())
				}
				if client.counts[statementQualityByGroup] != 0 || (name == "a breakdown whose range ends before it starts" && client.counts[statementBreakdown] != 0) {
					t.Errorf("a wrong part sent its statement: %v", client.counts)
				}
			}
		})
	}
}
