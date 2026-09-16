package filteroptions

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// fixtureRowScanner replays a fixed slice of single-column string rows --
// same shape as quadrant/golden_test.go's fixtureRowScanner, narrowed to
// the one-column "value" shape every query in this package returns.
type fixtureRowScanner struct {
	rows  []string
	index int
}

func (s *fixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *fixtureRowScanner) Scan(dest ...any) error {
	ptr, ok := dest[0].(*string)
	if !ok {
		return errors.New("fixtureRowScanner: expected *string dest")
	}
	*ptr = s.rows[s.index-1]
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// byQueryClient answers each query with a fixed set of rows, keyed by a
// distinguishing substring of the query text -- the same dispatch shape
// the golden-fixture capture script uses on the Python side, since none of
// this package's five queries share a param signature to switch on.
type byQueryClient struct {
	teams      []string
	repos      []string
	developers []string
	issueTypes []string
	flowStages []string
}

func (c byQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if len(bindings) == 0 || bindings[0].Name != "org_id" {
		return nil, errors.New("byQueryClient: expected org_id binding")
	}
	switch {
	case containsAll(query, "FROM teams FINAL"):
		return &fixtureRowScanner{rows: c.teams}, nil
	case containsAll(query, "FROM repos FINAL"):
		return &fixtureRowScanner{rows: c.repos}, nil
	case containsAll(query, "author_email AS value"):
		return &fixtureRowScanner{rows: c.developers}, nil
	case containsAll(query, "issue_type_norm AS value"):
		return &fixtureRowScanner{rows: c.issueTypes}, nil
	case containsAll(query, "status AS value"):
		return &fixtureRowScanner{rows: c.flowStages}, nil
	default:
		return nil, errors.New("byQueryClient: unrecognised query")
	}
}

func containsAll(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

type erroringClient struct{}

func (erroringClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errors.New("clickhouse unavailable")
}

func TestIsEmailValue(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{"dev@example.com", true},
		{"first.last+tag@sub.example.co", true},
		{"not-an-email", false},
		{"missing-domain@", false},
		{"@missing-local.com", false},
		{"has space@example.com", false},
		{"angle<br>@example.com", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isEmailValue(tc.value); got != tc.want {
			t.Errorf("isEmailValue(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestWorkCategoryIsThemesThenSubcategoriesSorted(t *testing.T) {
	got := workCategory()
	if len(got) != 20 {
		t.Fatalf("len(workCategory()) = %d, want 20 (5 themes + 15 subcategories)", len(got))
	}
	if got[0] != "feature_delivery" || got[4] != "risk" {
		t.Fatalf("themes block not in expected sorted position: %v", got[:5])
	}
	if got[5] != "feature_delivery.customer" || got[19] != "risk.vulnerability" {
		t.Fatalf("subcategories block not in expected sorted position: %v", got[5:])
	}
}

func TestBuildResponseFiltersNonEmailDevelopers(t *testing.T) {
	client := byQueryClient{
		teams:      []string{"team-a", "team-b"},
		repos:      []string{"api-gateway", "checkout-service"},
		developers: []string{"dev@example.com", "not-an-email", "second@example.com"},
		issueTypes: []string{"bug", "feature"},
		flowStages: []string{"done", "in_progress"},
	}

	resp, err := BuildResponse(context.Background(), client, "org-1")
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}

	wantDevelopers := []string{"dev@example.com", "second@example.com"}
	if !reflect.DeepEqual(resp.Developers, wantDevelopers) {
		t.Fatalf("Developers = %v, want %v", resp.Developers, wantDevelopers)
	}
	if !reflect.DeepEqual(resp.Teams, []string{"team-a", "team-b"}) {
		t.Fatalf("Teams = %v", resp.Teams)
	}
	if !reflect.DeepEqual(resp.Repos, []string{"api-gateway", "checkout-service"}) {
		t.Fatalf("Repos = %v", resp.Repos)
	}
	if !reflect.DeepEqual(resp.IssueType, []string{"bug", "feature"}) {
		t.Fatalf("IssueType = %v", resp.IssueType)
	}
	if !reflect.DeepEqual(resp.FlowStage, []string{"done", "in_progress"}) {
		t.Fatalf("FlowStage = %v", resp.FlowStage)
	}
	if resp.Services == nil || len(resp.Services) != 0 {
		t.Fatalf("Services = %v, want non-nil empty slice", resp.Services)
	}
}

func TestBuildResponseEmptyResultsAreEmptySlicesNotNil(t *testing.T) {
	client := byQueryClient{}
	resp, err := BuildResponse(context.Background(), client, "org-1")
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	for name, got := range map[string][]string{
		"Teams":      resp.Teams,
		"Repos":      resp.Repos,
		"Services":   resp.Services,
		"Developers": resp.Developers,
		"IssueType":  resp.IssueType,
		"FlowStage":  resp.FlowStage,
	} {
		if got == nil {
			t.Errorf("%s is nil, want non-nil empty slice", name)
		}
		if len(got) != 0 {
			t.Errorf("%s = %v, want empty", name, got)
		}
	}
	if len(resp.WorkCategory) != 20 {
		t.Errorf("WorkCategory len = %d, want 20 (static taxonomy, independent of ClickHouse rows)", len(resp.WorkCategory))
	}
}

func TestBuildResponsePropagatesQueryError(t *testing.T) {
	_, err := BuildResponse(context.Background(), erroringClient{}, "org-1")
	if err == nil {
		t.Fatal("expected an error when the ClickHouse client fails")
	}
}

// TestOrgIDScopesEveryDedupReadAtTheSameNestingDepthAsFinal pins the class
// ruling this package must hold: every ReplacingMergeTree read dedups with
// FINAL (the version column, computed_at, exists on every table this
// package reads FINAL against) and filters org_id at the SAME parenthesis
// nesting depth as that FINAL source -- never one level deeper, inside a
// separate dedup subquery whose org filter would only apply afterward.
// This endpoint is a page-load-frequency read; a dedup subquery with no
// org bound would scan every tenant's rows on every request.
//
// The check is depth-based (sqlshape.Depths, the same parenthesis-aware
// scan internal/providersync and internal/syncreconciler already share for
// their own structural SQL guards), not a bare substring/"(" search: a
// parenthesised function call between the FINAL marker and the org
// predicate (there is none here today, but a future edit could add one)
// would false-positive a naive "(" search without changing nesting depth,
// which this test would correctly ignore.
func TestOrgIDScopesEveryDedupReadAtTheSameNestingDepthAsFinal(t *testing.T) {
	const orgIDPredicate = "org_id = {org_id:String}"

	cases := []struct {
		name    string
		query   string
		markers []string
	}{
		{
			name:  "teamsQuery",
			query: teamsQuery,
			markers: []string{
				"FROM teams FINAL",
				"FROM user_metrics_daily FINAL",
				"FROM work_item_user_metrics_daily FINAL",
			},
		},
		{name: "reposQuery", query: reposQuery, markers: []string{"FROM repos FINAL"}},
		{name: "developersQuery", query: developersQuery, markers: []string{"FROM user_metrics_daily FINAL"}},
		{name: "flowStageQuery", query: flowStageQuery, markers: []string{"FROM work_item_state_durations_daily FINAL"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.query, "LIMIT 1 BY") || strings.Contains(tc.query, "argMax") {
				t.Fatalf("%s: contains a LIMIT-1-BY or argMax dedup subquery shape -- every dedup "+
					"read in this package must use FINAL with an inline org_id predicate instead "+
					"(the version column, computed_at, exists on every table read here)", tc.name)
			}

			depths := sqlshape.Depths(tc.query)
			for _, marker := range tc.markers {
				markerIdx := strings.Index(tc.query, marker)
				if markerIdx < 0 {
					t.Fatalf("%s: expected marker %q in query:\n%s", tc.name, marker, tc.query)
				}
				markerDepth := depths[markerIdx]

				rest := tc.query[markerIdx+len(marker):]
				orgIdx := strings.Index(rest, orgIDPredicate)
				if orgIdx < 0 {
					t.Fatalf("%s: expected %q after %q, got:\n%s", tc.name, orgIDPredicate, marker, rest)
				}
				absoluteOrgIdx := markerIdx + len(marker) + orgIdx
				orgDepth := depths[absoluteOrgIdx]

				if orgDepth != markerDepth {
					t.Fatalf("%s: %q's org_id predicate sits at nesting depth %d but FINAL sits at "+
						"depth %d -- org_id must scope the SAME statement as the dedup source, not a "+
						"subquery that dedups the whole table before the tenant filter narrows it",
						tc.name, marker, orgDepth, markerDepth)
				}
			}
		})
	}
}
