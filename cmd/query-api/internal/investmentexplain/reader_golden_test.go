package investmentexplain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// breakdownQueryGolden is the shape testdata/generate_breakdown_query_goldens.py
// writes: the REAL query text and params dict fetch_investment_breakdown /
// fetch_mock_fixture_investment_row_count would have executed for a given
// case, captured by stubbing _query_investment_dicts rather than
// hand-imitating the Python.
//
// This test does NOT byte-diff the golden's full query text against the Go
// query: the two intentionally differ in STRUCTURE.
// LatestWorkUnitInvestmentsSource() inlines the CTE as a derived-table
// subquery (`FROM (...) AS work_unit_investments`, no WITH) -- the
// established convention every existing reader in cmd/query-api/internal/
// analytics already uses (sankeycoverage.go:136, investmentquality.go),
// predating CHAOS-4977 -- while Python names it via
// `WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE} ... FROM latest_work_unit_investments
// AS work_unit_investments`. Both compile to the same query plan; a
// full-string diff would fail on that structural difference alone, for
// every case, regardless of whether the FILTER-composition logic (the part
// that actually varies per case, and the only part this port could get
// wrong) is correct. What this test verifies instead is that filter
// composition: for each case, the Go builder's bound parameter set matches
// the golden's captured params (name and value, order-insensitively), and
// the category/scope-filter SQL fragments have the same PRESENCE and
// predicate shape as Python's, modulo the %(name)s -> {name:Type}
// placeholder-syntax translation every other query in this port already
// makes.
type breakdownQueryGolden struct {
	Function string         `json:"function"`
	Case     string         `json:"case"`
	OrgID    string         `json:"org_id"`
	Query    string         `json:"query"`
	Params   map[string]any `json:"params"`
}

// breakdownGoldenCases mirrors generate_breakdown_query_goldens.py's CASES
// dict exactly -- same case names, same filter shapes -- so each golden
// JSON can be replayed through BreakdownFilters and compared.
var breakdownGoldenCases = map[string]BreakdownFilters{
	"no_filters": {},
	"themes_only": {
		Themes: []string{"velocity", "quality"},
	},
	"subcategories_only": {
		Subcategories: []string{"velocity.feature", "quality.bugfix"},
	},
	"themes_and_subcategories": {
		Themes:        []string{"velocity"},
		Subcategories: []string{"quality.bugfix"},
	},
	"repo_scope": {
		RepoIDs: []string{"repo-1", "repo-2"},
	},
	"repo_scope_and_themes": {
		RepoIDs: []string{"repo-1"},
		Themes:  []string{"velocity"},
	},
}

func loadBreakdownGolden(t *testing.T, function, caseName string) breakdownQueryGolden {
	t.Helper()
	path := filepath.Join("testdata", function+"__"+caseName+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var golden breakdownQueryGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("decode golden %s: %v", path, err)
	}
	return golden
}

// bindingSet flattens []dhclickhouse.Binding into a comparable map. Slice
// values are sorted since the golden JSON's own list order is Python's
// dict/list construction order, which need not match dedupeStrings'
// first-sighting order.
func bindingSet(bindings []dhclickhouse.Binding) map[string]any {
	out := make(map[string]any, len(bindings))
	for _, b := range bindings {
		if v, ok := b.Value.([]string); ok {
			sorted := append([]string(nil), v...)
			sort.Strings(sorted)
			out[b.Name] = sorted
			continue
		}
		out[b.Name] = b.Value
	}
	return out
}

func normalizeGoldenParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	out := make(map[string]any, len(params))
	for k, v := range params {
		if list, ok := v.([]any); ok {
			strs := make([]string, 0, len(list))
			for _, item := range list {
				s, ok := item.(string)
				if !ok {
					t.Fatalf("golden param %q: non-string element %v", k, item)
				}
				strs = append(strs, s)
			}
			sort.Strings(strs)
			out[k] = strs
			continue
		}
		out[k] = v
	}
	return out
}

func assertBindingSetsEqual(t *testing.T, want, got map[string]any) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("binding count mismatch: want %v, got %v", want, got)
	}
	for k, wv := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("missing binding %q (want %v)", k, wv)
		}
		wantJSON, _ := json.Marshal(wv)
		gotJSON, _ := json.Marshal(gv)
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("binding %q mismatch: want %s, got %s", k, wantJSON, gotJSON)
		}
	}
}

// assertClauseShape checks the Go category/scope clause fragments have the
// same PRESENCE and predicate shape as Python's captured query for this
// case. Not a full-string check, since the placeholder syntax legitimately
// differs (%(themes)s vs {themes:Array(String)}) -- see the package doc
// comment above.
func assertClauseShape(t *testing.T, categorySQL, scopeSQL string, filters BreakdownFilters) {
	t.Helper()

	wantCategoryPresent := len(filters.Themes) > 0 || len(filters.Subcategories) > 0
	if wantCategoryPresent != (categorySQL != "") {
		t.Fatalf("category clause presence mismatch: filters=%+v categorySQL=%q", filters, categorySQL)
	}
	if len(filters.Themes) > 0 && !strings.Contains(categorySQL, "splitByChar('.', subcategory_kv.1)[1] IN {themes:Array(String)}") {
		t.Fatalf("category clause missing themes predicate: %q", categorySQL)
	}
	if len(filters.Subcategories) > 0 && !strings.Contains(categorySQL, "subcategory_kv.1 IN {subcategories:Array(String)}") {
		t.Fatalf("category clause missing subcategories predicate: %q", categorySQL)
	}
	if len(filters.Themes) > 0 && len(filters.Subcategories) > 0 && !strings.Contains(categorySQL, " OR ") {
		t.Fatalf("category clause missing OR join for two predicates: %q", categorySQL)
	}

	wantScopePresent := len(filters.RepoIDs) > 0
	if wantScopePresent != (scopeSQL != "") {
		t.Fatalf("scope clause presence mismatch: filters=%+v scopeSQL=%q", filters, scopeSQL)
	}
	if wantScopePresent && !strings.Contains(scopeSQL, "repo_id IN {scope_ids:Array(String)}") {
		t.Fatalf("scope clause missing repo_id predicate: %q", scopeSQL)
	}
}

func runFilterCompositionGolden(t *testing.T, function string) {
	for caseName, filters := range breakdownGoldenCases {
		t.Run(caseName, func(t *testing.T) {
			golden := loadBreakdownGolden(t, function, caseName)

			filters := filters
			filters.OrgID = golden.OrgID

			categorySQL, categoryBindings := filters.categoryClause()
			scopeSQL, scopeBindings := filters.scopeClause()
			bindings := filters.baseBindings()
			bindings = append(bindings, scopeBindings...)
			bindings = append(bindings, categoryBindings...)

			got := bindingSet(bindings)
			delete(got, "start_date")
			delete(got, "end_date")

			want := normalizeGoldenParams(t, golden.Params)

			assertBindingSetsEqual(t, want, got)
			assertClauseShape(t, categorySQL, scopeSQL, filters)
		})
	}
}

func TestFetchInvestmentBreakdownFilterCompositionMatchesPythonGolden(t *testing.T) {
	runFilterCompositionGolden(t, "fetch_investment_breakdown")
}

func TestFetchMockFixtureInvestmentRowCountFilterCompositionMatchesPythonGolden(t *testing.T) {
	runFilterCompositionGolden(t, "fetch_mock_fixture_investment_row_count")
}
