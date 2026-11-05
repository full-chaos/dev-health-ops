package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/aggflame"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/drilldown"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// CHAOS-13089 (PR-2a of CHAOS-10864): every REST handler reads its organization from claims.Scope, the scope the
// decision allowed. A request with no organization in its scope is a 503 before any ClickHouse statement. These
// cells drive the real handler of each of the 21 route classes over a recording ClickHouse client.

const (
	restTestScopeOrg   = "0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10"
	restScopeClaimsOrg = "claims-org-must-not-be-read"
)

type restOrgCase struct {
	class, method, target, body string
	pathValues                  map[string]string
	handler                     func(*testing.T, *teamScopeReachClient) http.HandlerFunc
}

func restNeed[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func restOrgCases() []restOrgCase {
	dates := "start_date=2026-01-01&end_date=2026-01-15"
	person := map[string]string{"person_id": "person-1"}
	return []restOrgCase{
		{"quadrant", http.MethodGet, "/api/v1/quadrant?type=wip_throughput&" + dates, "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newQuadrantWorkHandler(c) }},
		{"heatmap", http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=review_wait_density&range_days=30", "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newHeatmapWorkHandler(c) }},
		{"sankey", http.MethodGet, "/api/v1/sankey?" + dates, "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newSankeyGetHandler(c) }},
		{"home", http.MethodGet, "/api/v1/home?" + dates, "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newHomeGetHandler(c, nil) }},
		{"opportunities", http.MethodGet, "/api/v1/opportunities?" + dates, "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newOpportunitiesGetHandler(c) }},
		{"filteroptions", http.MethodGet, "/api/v1/filters/options", "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newFilterOptionsWorkHandler(c) }},
		{"investment", http.MethodGet, "/api/v1/investment?" + dates, "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newInvestmentGetHandler(restNeed(investment.NewReader(c)))
		}},
		{"investment_sunburst", http.MethodGet, "/api/v1/investment/sunburst?" + dates, "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newInvestmentSunburstGetHandler(restNeed(investment.NewReader(c)))
		}},
		{"investment_explain", http.MethodPost, "/api/v1/investment/explain?llm_provider=mock&" + dates, "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newInvestmentExplainWorkHandler(restNeed(investmentexplain.NewReader(c)), nil, nil, nil, testPlatformGate{})
		}},
		{"drilldown_prs", http.MethodGet, "/api/v1/drilldown/prs?" + dates, "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newDrilldownPRsGetHandler(restNeed(drilldown.NewReader(c)))
		}},
		{"drilldown_issues", http.MethodGet, "/api/v1/drilldown/issues?" + dates, "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newDrilldownIssuesGetHandler(restNeed(drilldown.NewReader(c)))
		}},
		{"work_units", http.MethodGet, "/api/v1/work-units?" + dates, "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newWorkUnitsGetHandler(restNeed(investmentexplain.NewReader(c)))
		}},
		{"work_unit_explain", http.MethodPost, "/api/v1/work-units/wu-1/explain?llm_provider=mock&" + dates, "", map[string]string{"work_unit_id": "wu-1"}, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newWorkUnitExplainHandler(restNeed(investmentexplain.NewReader(c)), nil, nil, nil, testPlatformGate{})
		}},
		{"people", http.MethodGet, "/api/v1/people?q=ali", "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newPeopleSearchHandler(restNeed(people.NewReader(c)))
		}},
		{"people_summary", http.MethodGet, "/api/v1/people/person-1/summary?" + dates, "", person, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newPeopleSummaryHandler(restNeed(people.NewReader(c)))
		}},
		{"people_metric", http.MethodGet, "/api/v1/people/person-1/metric?metric=churn&" + dates, "", person, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newPeopleMetricHandler(restNeed(people.NewReader(c)))
		}},
		{"people_drilldown_prs", http.MethodGet, "/api/v1/people/person-1/drilldown/prs?" + dates, "", person, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newPeopleDrilldownPRsHandler(restNeed(people.NewReader(c)))
		}},
		{"people_drilldown_issues", http.MethodGet, "/api/v1/people/person-1/drilldown/issues?" + dates, "", person, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newPeopleDrilldownIssuesHandler(restNeed(people.NewReader(c)))
		}},
		{"explain", http.MethodGet, "/api/v1/explain?metric=" + explain.MetricNames()[0] + "&" + dates, "", nil, func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newExplainGetHandler(restNeed(explain.NewReader(c)))
		}},
		{"flame", http.MethodGet, "/api/v1/flame?entity_type=issue&entity_id=abc", "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newFlameWorkHandler(c) }},
		{"flame_aggregated", http.MethodGet, "/api/v1/flame/aggregated?mode=throughput&" + dates, "", nil, func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			var client aggflame.QueryClient = c
			return newFlameAggregatedWorkHandler(client)
		}},
	}
}

type restOrgRun struct {
	status     int
	body       string
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (tc restOrgCase) run(t *testing.T, claims authctx.Claims) restOrgRun {
	t.Helper()
	quietStdLog(t)
	client := &teamScopeReachClient{}
	handler := tc.handler(t, client)
	request := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
	for name, value := range tc.pathValues {
		request.SetPathValue(name, value)
	}
	request = request.WithContext(authctx.WithClaims(request.Context(), claims))
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	client.mu.Lock()
	defer client.mu.Unlock()
	return restOrgRun{status: recorder.Code, body: recorder.Body.String(),
		statements: append([]string{}, client.statements...), bindings: append([][]dhclickhouse.Binding{}, client.bindings...)}
}

// fingerprint is the status, the body and every statement with its bindings, time values masked (a window that
// ends at "now" moves between two runs).
func (r restOrgRun) fingerprint() string {
	blocks := make([]string, 0, len(r.statements))
	for i, statement := range r.statements {
		var b strings.Builder
		fmt.Fprintf(&b, "%s\n", statement)
		for _, binding := range r.bindings[i] {
			if _, isTime := binding.Value.(time.Time); isTime {
				fmt.Fprintf(&b, "  %s=<time>\n", binding.Name)
				continue
			}
			fmt.Fprintf(&b, "  %s=%v\n", binding.Name, binding.Value)
		}
		blocks = append(blocks, b.String())
	}
	sort.Strings(blocks)
	return fmt.Sprintf("%d\n%s\n%s", r.status, r.body, strings.Join(blocks, "--\n"))
}

func TestRESTHandlersNoOrganizationInScopeIs503BeforeAnyStatement(t *testing.T) {
	for _, tc := range restOrgCases() {
		t.Run(tc.class, func(t *testing.T) {
			got := tc.run(t, authctx.Claims{OrgID: restScopeClaimsOrg})
			if got.status != http.StatusServiceUnavailable || !strings.Contains(got.body, "Authorization unavailable") {
				t.Fatalf("no scope answered %d %q, want 503 Authorization unavailable", got.status, got.body)
			}
			if len(got.statements) != 0 {
				t.Fatalf("no scope ran %d statement(s) before the refusal:\n%v", len(got.statements), got.statements)
			}
		})
	}
}

func TestRESTHandlersReadTheOrganizationOfTheScope(t *testing.T) {
	for _, tc := range restOrgCases() {
		for name, scope := range map[string]pepgate.Scope{
			"organization": pepgate.Organization(restTestScopeOrg),
			"legacy":       pepgate.Legacy(restTestScopeOrg),
		} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got := tc.run(t, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: scope})
				if len(got.statements) == 0 {
					t.Fatalf("the handler ran no statement (status %d, body %q): the case does not reach a read", got.status, got.body)
				}
				seenScopeOrg := false
				for i, statement := range got.statements {
					if strings.Contains(statement, restScopeClaimsOrg) {
						t.Fatalf("a statement carries the claims organization: %s", statement)
					}
					for _, binding := range got.bindings[i] {
						for _, value := range bindingStringValues(got.bindings[i], binding.Name) {
							if value == restScopeClaimsOrg {
								t.Fatalf("binding %s carries the claims organization, not the scope's", binding.Name)
							}
							if value == restTestScopeOrg {
								seenScopeOrg = true
							}
						}
					}
				}
				if !seenScopeOrg {
					t.Fatalf("no binding carries the scope organization:\n%s", got.fingerprint())
				}
			})
		}
	}
}

// R2: an Organization scope and the Legacy scope of the same organization read the same statements and answer the
// same bytes.
func TestRESTOrganizationScopeIsTodaysReadOnEveryClass(t *testing.T) {
	for _, tc := range restOrgCases() {
		t.Run(tc.class, func(t *testing.T) {
			// Some readers keep a process-wide probe cache: the first run pays its statements.
			tc.run(t, authctx.Claims{OrgID: restTestScopeOrg, Scope: pepgate.Legacy(restTestScopeOrg)})
			legacy := tc.run(t, authctx.Claims{OrgID: restTestScopeOrg, Scope: pepgate.Legacy(restTestScopeOrg)})
			enforced := tc.run(t, authctx.Claims{OrgID: restTestScopeOrg, Scope: pepgate.Organization(restTestScopeOrg)})
			if len(legacy.statements) == 0 || legacy.fingerprint() != enforced.fingerprint() {
				t.Fatalf("Organization scope differs from Legacy:\nlegacy:\n%s\nenforced:\n%s", legacy.fingerprint(), enforced.fingerprint())
			}
		})
	}
}

// restOrgPostCases are the POST handlers that sit beside the GET handlers above: each reads its organization in its
// own code, so each gets its own no-scope cell.
func restOrgPostCases() []restOrgCase {
	const filters = `{"filters":{}}`
	post := func(class, target string, handler func(*testing.T, *teamScopeReachClient) http.HandlerFunc) restOrgCase {
		body := filters
		switch class {
		case "sankey_post":
			body = `{"mode":"hotspot","filters":{}}`
		case "explain_post":
			body = `{"metric":"cycle_time","filters":{}}`
		}
		return restOrgCase{class, http.MethodPost, target, body, nil, handler}
	}
	return []restOrgCase{
		post("sankey_post", "/api/v1/sankey", func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newSankeyPostHandler(c) }),
		post("home_post", "/api/v1/home", func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newHomePostHandler(c, nil) }),
		post("opportunities_post", "/api/v1/opportunities", func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newOpportunitiesPostHandler(c) }),
		post("investment_post", "/api/v1/investment", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newInvestmentPostHandler(restNeed(investment.NewReader(c)))
		}),
		post("drilldown_prs_post", "/api/v1/drilldown/prs", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newDrilldownPRsPostHandler(restNeed(drilldown.NewReader(c)))
		}),
		post("drilldown_issues_post", "/api/v1/drilldown/issues", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newDrilldownIssuesPostHandler(restNeed(drilldown.NewReader(c)))
		}),
		post("work_units_post", "/api/v1/work-units", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newWorkUnitsPostHandler(restNeed(investmentexplain.NewReader(c)))
		}),
		post("explain_post", "/api/v1/explain", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newExplainPostHandler(restNeed(explain.NewReader(c)))
		}),
	}
}

func TestRESTPostHandlersNoOrganizationInScopeIs503BeforeAnyStatement(t *testing.T) {
	for _, tc := range restOrgPostCases() {
		t.Run(tc.class, func(t *testing.T) {
			// The body is valid: with a scope the handler reaches SQL, so the refusal below is the scope's.
			if ok := tc.run(t, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: pepgate.Organization(restTestScopeOrg)}); len(ok.statements) == 0 {
				t.Fatalf("a valid request ran no statement (status %d, body %q): the body does not reach a read", ok.status, ok.body)
			}
			got := tc.run(t, authctx.Claims{OrgID: restScopeClaimsOrg})
			if got.status != http.StatusServiceUnavailable || !strings.Contains(got.body, "Authorization unavailable") {
				t.Fatalf("no scope answered %d %q, want 503 Authorization unavailable", got.status, got.body)
			}
			if len(got.statements) != 0 {
				t.Fatalf("no scope ran %d statement(s) before the refusal:\n%v", len(got.statements), got.statements)
			}
		})
	}
}
