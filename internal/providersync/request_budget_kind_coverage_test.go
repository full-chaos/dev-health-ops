package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Two invariants, proved together by every test in this file:
//
// STRUCTURAL: for every route with a request plan, every request KIND the
// route sends has a term in its plan. Proved by running the route's REAL
// Collect against a fixture provider through a kindCountingDoer that
// classifies each request by kind (method + path template) and comparing
// the observed set against a hand-declared coverage table -- not the
// oracle's own case list. This is the check that would have caught the
// repo-metadata fetch, both list-pagination gaps, and the PR-detail fetch
// on the first round: none of them are a NEW dataset case, all of them are
// a kind the existing case's Collect always sent that no term ever counted.
//
// QUANTITATIVE: for every term, planned >= counted on the grid, given only
// the inputs the planner really has at plan time. Where Collect's own code
// bounds an input outright (github/deployments' maxDeployments truncation,
// gitlab/deployments' unconditional SinglePage list fetch), that bound IS
// the term's domain and is proven exactly. Where nothing in Collect bounds
// the input (github/prs' PR-per-window count), the term states a NAMED
// assumption constant with its domain, is proven to hold inside that
// domain, and one executed cell OUTSIDE it shows the real, undocumented
// consequence: an overshoot no ledger reconciles, because no production
// caller of ProviderRequestPlan exists today to enforce the reservation
// against real spend (confirmed by `rg -n "ProviderRequestPlan"` outside
// this package's own tests) -- so a cell outside a stated domain is a named
// limit, not a defect.
//
// Python parity is not the oracle for either invariant: github/prs' own
// live-Python-oracle term already under-reserves against a real Collect
// run (see request_plan_test.go and RISK-NOTES), which this file's
// executed cells catch and Python's own estimator does not.

var (
	numericPathSegment = regexp.MustCompile(`^[0-9]+$`)
	hexPathSegment     = regexp.MustCompile(`^[0-9a-f]{7,40}$`)
)

// requestKind classifies a request by method and path template: a numeric
// or SHA-shaped path segment collapses to a placeholder so every deployment
// or PR hit on the same endpoint counts as one kind, not one per ID.
func requestKind(method, rawPath string) string {
	path, err := url.QueryUnescape(rawPath)
	if err != nil {
		path = rawPath
	}
	segments := strings.Split(strings.Trim(path, "/"), "/")
	for i, segment := range segments {
		switch {
		case numericPathSegment.MatchString(segment):
			segments[i] = "{id}"
		case hexPathSegment.MatchString(segment):
			segments[i] = "{sha}"
		}
	}
	return method + " /" + strings.Join(segments, "/")
}

// kindCountingDoer wraps a fixture responder and records, per observed
// requestKind, how many real Do calls that kind received.
type kindCountingDoer struct {
	respond func(*http.Request) (*http.Response, error)
	kinds   map[string]int
	total   int
}

func (doer *kindCountingDoer) Do(request *http.Request) (*http.Response, error) {
	doer.total++
	doer.kinds[requestKind(request.Method, request.URL.Path)]++
	return doer.respond(request)
}

func jsonFixtureResponse(request *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

// planTerm names the (dimension, route family) a request kind must be
// reserved under.
type planTerm struct{ dimension, routeFamily string }

// assertKindsCoveredByPlan checks the structural invariant against the
// plan itself: the observed kind set equals the declared kind set exactly
// (a new or vanished kind fails), and every kind's declared term is present
// in ProviderRequestPlan's own output with positive units -- so a plan that
// loses or zeroes a term fails, including a plan that returns nothing.
func assertKindsCoveredByPlan(t *testing.T, provider, dataset string, observed map[string]int, declared map[string]planTerm) {
	t.Helper()
	route := provider + "/" + dataset
	observedKinds := make([]string, 0, len(observed))
	for kind := range observed {
		observedKinds = append(observedKinds, kind)
	}
	declaredKinds := make([]string, 0, len(declared))
	for kind := range declared {
		declaredKinds = append(declaredKinds, kind)
	}
	sort.Strings(observedKinds)
	sort.Strings(declaredKinds)
	if strings.Join(observedKinds, "\n") != strings.Join(declaredKinds, "\n") {
		t.Fatalf("%s: observed kinds %v, declared coverage %v -- a request kind with no declared term, or a declared term nothing requests any more", route, observedKinds, declaredKinds)
	}
	plan := ProviderRequestPlan(provider, dataset, 1, nil)
	for _, kind := range declaredKinds {
		term := declared[kind]
		units := 0
		for _, estimate := range plan {
			if estimate.Dimension == term.dimension && estimate.RouteFamily == term.routeFamily {
				units += estimate.Units
			}
		}
		if units <= 0 {
			t.Fatalf("%s: request kind %q is reserved under (%s, %s) but the plan has no such term with positive units: %+v", route, kind, term.dimension, term.routeFamily, plan)
		}
	}
}

func plannedUnits(provider, dataset string, spanDays int, dimension, routeFamily string) int {
	units := 0
	for _, estimate := range ProviderRequestPlan(provider, dataset, spanDays, nil) {
		if estimate.RouteFamily == routeFamily && estimate.Dimension == dimension {
			units += estimate.Units
		}
	}
	return units
}

func restCorePlanned(provider, dataset string, spanDays int, routeFamily string) int {
	return plannedUnits(provider, dataset, spanDays, BudgetRESTCore, routeFamily)
}

// --- github/deployments -----------------------------------------------

func githubDeploymentsFixture(deploymentCount int) func(*http.Request) (*http.Response, error) {
	return func(request *http.Request) (*http.Response, error) {
		switch {
		case request.URL.Path == "/repos/acme/api":
			return jsonFixtureResponse(request, gitHubRepositoryFixture), nil
		case request.URL.Path == "/repos/acme/api/releases":
			return jsonFixtureResponse(request, "[]"), nil
		case request.URL.Path == "/repos/acme/api/deployments":
			rows := make([]string, deploymentCount)
			for i := 0; i < deploymentCount; i++ {
				rows[i] = fmt.Sprintf(
					`{"id":%d,"environment":"production","created_at":"2026-07-22T10:%02d:00Z","ref":"v1","sha":"%07x"}`,
					100+i, i%60, 0xabc0000+i,
				)
			}
			return jsonFixtureResponse(request, "["+strings.Join(rows, ",")+"]"), nil
		case strings.HasSuffix(request.URL.Path, "/pulls"):
			return jsonFixtureResponse(request, "[]"), nil
		case strings.Contains(request.URL.Path, "/deployments/") && strings.HasSuffix(request.URL.Path, "/statuses"):
			return jsonFixtureResponse(request, `[{"id":1,"state":"success","created_at":"2026-07-22T10:05:00Z"}]`), nil
		}
		return jsonFixtureResponse(request, "{}"), nil
	}
}

func runGitHubDeployments(t *testing.T, deploymentCount, maxDeployments int) *kindCountingDoer {
	t.Helper()
	doer := &kindCountingDoer{respond: githubDeploymentsFixture(deploymentCount), kinds: map[string]int{}}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	if _, err := (GitHubDeploymentsRouteHandler{MaxDeployments: maxDeployments}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return doer
}

var githubDeploymentsDeclaredKinds = map[string]planTerm{
	"GET /repos/acme/api":                           {BudgetRESTCore, "deployments"},
	"GET /repos/acme/api/releases":                  {BudgetRESTCore, "deployments"},
	"GET /repos/acme/api/deployments":               {BudgetRESTCore, "deployments"},
	"GET /repos/acme/api/commits/{sha}/pulls":       {BudgetRESTCore, "deployments"},
	"GET /repos/acme/api/deployments/{id}/statuses": {BudgetRESTCore, "deployments"},
}

func TestGitHubDeploymentsRequestKindsAllCoveredByAPlanTerm(t *testing.T) {
	t.Parallel()
	doer := runGitHubDeployments(t, 3, 0)
	assertKindsCoveredByPlan(t, "github", "deployments", doer.kinds, githubDeploymentsDeclaredKinds)
}

// TestGitHubDeploymentsPlannedCoversCountedInsideStatedDomain runs at the
// domain the base, statuses, and pull-lookup terms all share:
// handler.MaxDeployments left at its zero value, so Collect itself derives
// maxDeployments = defaultGitHubDeploymentsMax and every one of its own
// internal caps (the [:maxDeployments] truncation, the statuses page cap,
// the pull-lookup's own MaxPages: 1) is exactly the bound each term
// assumes. The fixture returns each list endpoint in a single response (no
// Link-header pagination), so the observed per-kind request count is
// analytically rescaled by the SAME page/status-page multiplier the
// estimator itself uses -- the technique this file's own predecessor
// established for the statuses term, extended here to the list fetches:
// a shallow OBSERVED total would let a missing or undersized term pass
// exactly the way it did the first time (the dominant, ceiling-scaled
// statuses term alone already exceeds a shallow total).
func TestGitHubDeploymentsPlannedCoversCountedInsideStatedDomain(t *testing.T) {
	t.Parallel()
	const n = defaultGitHubDeploymentsMax
	doer := runGitHubDeployments(t, n, 0)
	if doer.kinds["GET /repos/acme/api/deployments/{id}/statuses"] != n {
		t.Fatalf("statuses requests = %d, want %d", doer.kinds["GET /repos/acme/api/deployments/{id}/statuses"], n)
	}
	if doer.kinds["GET /repos/acme/api/commits/{sha}/pulls"] != n {
		t.Fatalf("pull-lookup requests = %d, want %d", doer.kinds["GET /repos/acme/api/commits/{sha}/pulls"], n)
	}
	realWorstCase := doer.kinds["GET /repos/acme/api"] +
		doer.kinds["GET /repos/acme/api/releases"]*githubDeploymentsListPageBudget +
		doer.kinds["GET /repos/acme/api/deployments"]*githubDeploymentsListPageBudget +
		doer.kinds["GET /repos/acme/api/deployments/{id}/statuses"]*maxDeploymentStatusPages +
		doer.kinds["GET /repos/acme/api/commits/{sha}/pulls"]
	planned := restCorePlanned("github", "deployments", 1, "deployments")
	if planned < realWorstCase {
		t.Fatalf("planned REST_CORE units for deployments = %d, UNDER the true worst-case cost inside the stated domain (%d)", planned, realWorstCase)
	}
	const maxOverReserveFactor = 2
	if planned > realWorstCase*maxOverReserveFactor {
		t.Fatalf("planned REST_CORE units for deployments = %d, more than %dx the true worst-case cost (%d)", planned, maxOverReserveFactor, realWorstCase)
	}
}

// TestGitHubDeploymentsOutsideStatedDomainOvershootsWithNoReconciliation
// proves the domain boundary is real, not assumed: a handler constructed
// with MaxDeployments above defaultGitHubDeploymentsMax makes Collect
// derive a larger real `pages` value and a larger real per-deployment
// enrichment count than the plan -- which has no MaxDeployments parameter
// to see the override with -- ever reserves for. This is a named limit
// (RISK-NOTES), not fixed here: ProviderRequestPlan has no production
// caller today (confirmed by an executed `rg -n "ProviderRequestPlan"`
// caller sweep), so nothing reconciles the reservation against the real
// spend this cell measures -- the unit simply overshoots its plan.
func TestGitHubDeploymentsOutsideStatedDomainOvershootsWithNoReconciliation(t *testing.T) {
	t.Parallel()
	const outsideDomain = defaultGitHubDeploymentsMax + 200
	doer := runGitHubDeployments(t, outsideDomain, outsideDomain)
	realWorstCase := doer.kinds["GET /repos/acme/api"] +
		doer.kinds["GET /repos/acme/api/releases"]*githubDeploymentsListPageBudget +
		doer.kinds["GET /repos/acme/api/deployments"]*githubDeploymentsListPageBudget +
		doer.kinds["GET /repos/acme/api/deployments/{id}/statuses"]*maxDeploymentStatusPages +
		doer.kinds["GET /repos/acme/api/commits/{sha}/pulls"]
	planned := restCorePlanned("github", "deployments", 1, "deployments")
	if planned >= realWorstCase {
		t.Fatalf("expected this cell to fall OUTSIDE the stated domain (planned=%d, real=%d) -- raise outsideDomain further above defaultGitHubDeploymentsMax", planned, realWorstCase)
	}
	t.Logf("outside stated domain: planned=%d real=%d deficit=%d (named limit, not fixed -- see RISK-NOTES)", planned, realWorstCase, realWorstCase-planned)
}

// --- gitlab/deployments -------------------------------------------------

func gitLabDeploymentsFixture(deploymentCount int) func(*http.Request) (*http.Response, error) {
	return func(request *http.Request) (*http.Response, error) {
		path, err := url.QueryUnescape(request.URL.Path)
		if err != nil {
			path = request.URL.Path
		}
		switch {
		case strings.HasSuffix(path, "/projects/123"):
			return jsonFixtureResponse(request, `{"id":123,"path_with_namespace":"acme/api","last_activity_at":"2026-07-22T10:00:00Z"}`), nil
		case strings.HasSuffix(path, "/releases"):
			return jsonFixtureResponse(request, "[]"), nil
		case strings.HasSuffix(path, "/deployments"):
			rows := make([]string, deploymentCount)
			for i := 0; i < deploymentCount; i++ {
				rows[i] = fmt.Sprintf(
					`{"id":%d,"status":"success","created_at":"2026-07-22T10:%02d:00Z","sha":"%07x","ref":"v1"}`,
					100+i, i%60, 0xdef0000+i,
				)
			}
			return jsonFixtureResponse(request, "["+strings.Join(rows, ",")+"]"), nil
		case strings.Contains(path, "/merge_requests"):
			return jsonFixtureResponse(request, "[]"), nil
		}
		return jsonFixtureResponse(request, "{}"), nil
	}
}

func runGitLabDeployments(t *testing.T, deploymentCount int) *kindCountingDoer {
	t.Helper()
	doer := &kindCountingDoer{respond: gitLabDeploymentsFixture(deploymentCount), kinds: map[string]int{}}
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example.com")
	claim := nativeTestClaim("gitlab", "deployments")
	if _, err := (GitLabDeploymentsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return doer
}

var gitLabDeploymentsDeclaredKinds = map[string]planTerm{
	"GET /api/v4/projects/{id}":                                         {BudgetRESTCore, "pipelines"},
	"GET /api/v4/projects/{id}/releases":                                {BudgetRESTCore, "pipelines"},
	"GET /api/v4/projects/{id}/deployments":                             {BudgetRESTCore, "pipelines"},
	"GET /api/v4/projects/{id}/repository/commits/{sha}/merge_requests": {BudgetRESTCore, "pipelines"},
}

func TestGitLabDeploymentsRequestKindsAllCoveredByAPlanTerm(t *testing.T) {
	t.Parallel()
	doer := runGitLabDeployments(t, 3)
	assertKindsCoveredByPlan(t, "gitlab", "deployments", doer.kinds, gitLabDeploymentsDeclaredKinds)
}

// TestGitLabDeploymentsPlannedCoversCountedNoAssumptionDomain has no
// outside-domain sibling: fetchGitLabDeploymentObjects (the helper both the
// deployments-list fetch and the per-deployment merge-request lookup use)
// is unconditionally SinglePage, so the deployments list itself can never
// return more than perPage = min(maxDeployments, gitLabDeploymentsMaximumPerPage)
// records -- 100 at most -- however large a caller sets MaxDeployments.
// There is no assumption constant to name because there is no input this
// term could be wrong about: read from gitlab_deployments_route.go's own
// Collect, not assumed.
func TestGitLabDeploymentsPlannedCoversCountedNoAssumptionDomain(t *testing.T) {
	t.Parallel()
	const n = gitLabDeploymentsMaximumPerPage
	doer := runGitLabDeployments(t, n)
	if doer.kinds["GET /api/v4/projects/{id}/repository/commits/{sha}/merge_requests"] != n {
		t.Fatalf("merge-request lookups = %d, want %d", doer.kinds["GET /api/v4/projects/{id}/repository/commits/{sha}/merge_requests"], n)
	}
	real := doer.total
	planned := restCorePlanned("gitlab", "deployments", 1, "pipelines")
	if planned < real {
		t.Fatalf("planned REST_CORE units for pipelines = %d, UNDER the real cost (%d)", planned, real)
	}
	const maxOverReserveFactor = 2
	if planned > real*maxOverReserveFactor {
		t.Fatalf("planned REST_CORE units for pipelines = %d, more than %dx the real cost (%d)", planned, maxOverReserveFactor, real)
	}
}

// --- github/prs (and its pr-reviews/pr-comments aliases) ----------------

// githubPRsWorkload is the grid's three unbounded inputs for github/prs.
type githubPRsWorkload struct {
	inWindow     int // PRs updated inside the window (one detail fetch each)
	newerThanWin int // PRs updated after the window, listed before the in-window ones
	reviewsPerPR int // reviews on every in-window PR (GraphQL continuation pages)
}

func githubPRsFixture(w githubPRsWorkload) func(*http.Request) (*http.Response, error) {
	graphqlAlias := regexp.MustCompile(`pr(\d+): pullRequest\(number: (\d+)\) \{ number reviews\(first: (\d+), after: (null|"[^"]*")\)`)
	return func(request *http.Request) (*http.Response, error) {
		switch {
		case request.URL.Path == "/repos/acme/api":
			return jsonFixtureResponse(request, gitHubPullRequestRepoFixture), nil
		case request.URL.Path == "/repos/acme/api/pulls":
			items := make([]string, 0, w.newerThanWin+w.inWindow+1)
			for i := 0; i < w.newerThanWin; i++ {
				items = append(items, fmt.Sprintf(`{"number":%d,"updated_at":"2026-08-05T15:00:00Z"}`, 5000+i))
			}
			for i := 0; i < w.inWindow; i++ {
				items = append(items, fmt.Sprintf(`{"number":%d,"updated_at":"2026-07-21T15:00:00Z"}`, 100+i))
			}
			items = append(items, `{"number":1,"updated_at":"2026-06-01T00:00:00Z"}`)
			page := 1
			if value := request.URL.Query().Get("page"); value != "" {
				page, _ = strconv.Atoi(value)
			}
			start := min((page-1)*nativePerPage, len(items))
			end := min(start+nativePerPage, len(items))
			response := jsonFixtureResponse(request, "["+strings.Join(items[start:end], ",")+"]")
			if end < len(items) {
				response.Header.Set("Link", fmt.Sprintf(`<https://api.github.com/repos/acme/api/pulls?page=%d&per_page=100>; rel="next"`, page+1))
			}
			return response, nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/graphql"):
			raw, _ := io.ReadAll(request.Body)
			var envelope struct {
				Query string `json:"query"`
			}
			_ = json.Unmarshal(raw, &envelope)
			aliases := []string{}
			for _, match := range graphqlAlias.FindAllStringSubmatch(envelope.Query, -1) {
				offset := 0
				if match[4] != "null" {
					offset, _ = strconv.Atoi(strings.Trim(match[4], `"c`))
				}
				count := min(gitHubPullRequestReviewPageSize, w.reviewsPerPR-offset)
				nodes := make([]string, count)
				for k := range nodes {
					nodes[k] = fmt.Sprintf(`{"id":"rv-%s-%d","state":"APPROVED","submittedAt":"2026-07-22T10:30:00Z","author":{"login":"octocat"}}`, match[2], offset+k)
				}
				aliases = append(aliases, fmt.Sprintf(
					`"pr%s":{"number":%s,"reviews":{"nodes":[%s],"pageInfo":{"hasNextPage":%t,"endCursor":"c%d"}}}`,
					match[1], match[2], strings.Join(nodes, ","), offset+count < w.reviewsPerPR, offset+count,
				))
			}
			return jsonFixtureResponse(request, `{"data":{"repository":{`+strings.Join(aliases, ",")+`}}}`), nil
		case strings.HasPrefix(request.URL.Path, "/repos/acme/api/pulls/"):
			number := strings.TrimPrefix(request.URL.Path, "/repos/acme/api/pulls/")
			return jsonFixtureResponse(request, fmt.Sprintf(
				`{"id":9%s,"number":%s,"title":"t","state":"open","user":{"login":"octocat"},"created_at":"2026-07-21T09:00:00Z","updated_at":"2026-07-21T15:00:00Z","additions":1,"deletions":1,"changed_files":1,"comments":0}`,
				number, number,
			)), nil
		}
		return jsonFixtureResponse(request, "{}"), nil
	}
}

func runGitHubPRs(t *testing.T, w githubPRsWorkload) *kindCountingDoer {
	t.Helper()
	doer := &kindCountingDoer{respond: githubPRsFixture(w), kinds: map[string]int{}}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "prs")
	if _, err := (GitHubPullRequestSocialRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return doer
}

var githubPRsDeclaredKinds = map[string]planTerm{
	"GET /repos/acme/api":            {BudgetRESTCore, "prs"},
	"GET /repos/acme/api/pulls":      {BudgetRESTCore, "prs"},
	"GET /repos/acme/api/pulls/{id}": {BudgetRESTCore, "prs"},
	"POST /graphql":                  {BudgetGraphQLCost, "pr_social"},
}

func githubPRsInsideDomain() githubPRsWorkload {
	return githubPRsWorkload{
		inWindow:     githubPRsAssumedInWindow(1),
		newerThanWin: githubPRsAssumedNewerThanWindow,
		reviewsPerPR: githubPRsAssumedReviewsPerPR,
	}
}

// githubPRsCounted splits a run's requests into the two budgets the plan
// reserves separately.
func githubPRsCounted(doer *kindCountingDoer) (rest, graphql int) {
	graphql = doer.kinds["POST /graphql"]
	return doer.total - graphql, graphql
}

func TestGitHubPRsRequestKindsAllCoveredByAPlanTerm(t *testing.T) {
	t.Parallel()
	doer := runGitHubPRs(t, githubPRsWorkload{inWindow: 2, newerThanWin: 0, reviewsPerPR: 1})
	assertKindsCoveredByPlan(t, "github", "prs", doer.kinds, githubPRsDeclaredKinds)
}

// TestGitHubPRsPlannedCoversCountedInsideStatedDomain runs AT every stated
// range at once (in-window PRs, newer-than-window PRs, reviews per PR) and
// asserts the REST and GraphQL budgets separately.
func TestGitHubPRsPlannedCoversCountedInsideStatedDomain(t *testing.T) {
	t.Parallel()
	doer := runGitHubPRs(t, githubPRsInsideDomain())
	if doer.kinds["GET /repos/acme/api/pulls/{id}"] != githubPRsAssumedInWindow(1) {
		t.Fatalf("PR detail requests = %d, want %d", doer.kinds["GET /repos/acme/api/pulls/{id}"], githubPRsAssumedInWindow(1))
	}
	rest, graphql := githubPRsCounted(doer)
	plannedREST := plannedUnits("github", "prs", 1, BudgetRESTCore, "prs")
	plannedGraphQL := plannedUnits("github", "prs", 1, BudgetGraphQLCost, "pr_social")
	if plannedREST < rest {
		t.Fatalf("planned REST_CORE units for prs = %d, UNDER the real cost at the domain boundary (%d)", plannedREST, rest)
	}
	if plannedGraphQL < graphql {
		t.Fatalf("planned GRAPHQL_COST units for pr_social = %d, UNDER the real cost at the domain boundary (%d)", plannedGraphQL, graphql)
	}
	const maxOverReserveFactor = 2
	if plannedREST > rest*maxOverReserveFactor {
		t.Fatalf("planned REST_CORE units for prs = %d, more than %dx the real cost (%d)", plannedREST, maxOverReserveFactor, rest)
	}
}

// TestGitHubPRsOutsideStatedDomainOvershootsWithNoReconciliation executes
// one cell past each stated range in turn -- in-window PRs, newer-than-window
// PRs, reviews per PR -- and proves each range is a real boundary: the
// budget that input feeds is exceeded. Named limits, not fixed here:
// nothing in Collect bounds these inputs, no production caller of
// ProviderRequestPlan exists to reconcile spend against the reservation.
func TestGitHubPRsOutsideStatedDomainOvershootsWithNoReconciliation(t *testing.T) {
	t.Parallel()
	for _, cell := range []struct {
		name    string
		mutate  func(*githubPRsWorkload)
		graphql bool
	}{
		{"in-window PRs past githubPRsAssumedInWindow", func(w *githubPRsWorkload) { w.inWindow++ }, false},
		{"newer-than-window PRs past githubPRsAssumedNewerThanWindow", func(w *githubPRsWorkload) { w.newerThanWin += nativePerPage }, false},
		{"reviews per PR past githubPRsAssumedReviewsPerPR", func(w *githubPRsWorkload) { w.reviewsPerPR = 2*githubPRsAssumedReviewsPerPR + 1 }, true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			t.Parallel()
			w := githubPRsInsideDomain()
			cell.mutate(&w)
			rest, graphql := githubPRsCounted(runGitHubPRs(t, w))
			planned, counted := plannedUnits("github", "prs", 1, BudgetRESTCore, "prs"), rest
			if cell.graphql {
				planned, counted = plannedUnits("github", "prs", 1, BudgetGraphQLCost, "pr_social"), graphql
			}
			if planned >= counted {
				t.Fatalf("expected this cell to fall OUTSIDE the stated domain (planned=%d, counted=%d)", planned, counted)
			}
			t.Logf("outside stated domain (%s): planned=%d counted=%d deficit=%d (named limit, not fixed -- see RISK-NOTES)", cell.name, planned, counted, counted-planned)
		})
	}
}
