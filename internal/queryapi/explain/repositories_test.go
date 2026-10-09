package explain

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// explainFor runs BuildExplainResponse over dispatch for one metric and
// scope, in one fixed window.
func explainFor(t *testing.T, dispatch *explainQueryDispatch, metric, scopeLevel string, scopeIDs ...string) (*Response, error) {
	t.Helper()
	dispatch.t = t
	dispatch.currentStartDay = "2024-05-01"
	reader, err := NewReader(fakeQueryClient{t: t, handler: dispatch.handle})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric:       metric,
		StartDay:     day(2024, 5, 1),
		EndDay:       day(2024, 5, 15),
		CompareStart: day(2024, 4, 17),
		CompareEnd:   day(2024, 5, 1),
		ScopeLevel:   scopeLevel,
		ScopeIDs:     scopeIDs,
	})
}

// The four metrics stored per repository serve their per-repository
// aggregation; the four stored per team (and a metric that falls back to
// one of them) serve null. The list is the contributor rows: the same ids,
// the same order, the same transformed values.
func TestRepositoriesAreServedForTheMetricsStoredPerRepositoryOnly(t *testing.T) {
	for metric, config := range metricConfigs {
		t.Run(metric, func(t *testing.T) {
			dispatch := &explainQueryDispatch{
				displayNameRows: [][]any{{"repo-a", "webapp"}, {"team-a", "Team A"}},
				contributorRows: [][]any{{"repo-a", 0.25}, {"repo-b", 0.5}},
				sourceURLRows:   [][]any{{"repo-a", "https://github.com/acme/webapp"}},
			}
			got, err := explainFor(t, dispatch, metric, "org")
			if err != nil {
				t.Fatal(err)
			}
			if config.GroupBy != "repo_id" {
				if got.Repositories != nil || got.SourceURL != nil {
					t.Fatalf("a metric stored per team serves repositories %v and source_url %v, want null and null", got.Repositories, got.SourceURL)
				}
				if len(dispatch.sourceURLIDs) != 0 {
					t.Fatalf("a metric stored per team read the repository urls: %v", dispatch.sourceURLIDs)
				}
				return
			}
			if got.Repositories == nil {
				t.Fatal("a metric stored per repository serves null repositories")
			}
			name, sourceURL := "webapp", "https://github.com/acme/webapp"
			want := []Repository{
				{ID: "repo-a", Name: &name, Value: config.Transform(0.25), SourceURL: &sourceURL},
				{ID: "repo-b", Value: config.Transform(0.5)},
			}
			if !reflect.DeepEqual(*got.Repositories, want) {
				t.Fatalf("repositories = %s, want %s", mustMarshal(t, *got.Repositories), mustMarshal(t, want))
			}
			for index, contributor := range got.Contributors {
				repository := (*got.Repositories)[index]
				if repository.ID != contributor.ID || repository.Value != contributor.Value {
					t.Fatalf("repository %d = %+v, contributor %d = %+v: not the same aggregation", index, repository, index, contributor)
				}
			}
			if got.SourceURL != nil {
				t.Fatalf("an organization scope serves source_url %q, want null", *got.SourceURL)
			}
			if !reflect.DeepEqual(dispatch.sourceURLIDs, [][]string{{"repo-a", "repo-b"}}) {
				t.Fatalf("url reads = %v, want one read of the two contributor ids", dispatch.sourceURLIDs)
			}
		})
	}
	if _, fallback := metricConfigs["totally_bogus"]; fallback {
		t.Fatal("the fallback case needs a metric name that is not configured")
	}
	got, err := explainFor(t, &explainQueryDispatch{}, "totally_bogus", "org")
	if err != nil {
		t.Fatal(err)
	}
	if got.Repositories != nil {
		t.Fatal("an unknown metric (it borrows cycle_time, stored per team) serves repositories")
	}
}

// A metric stored per repository with no row in the window serves an empty
// list, not null: null means "not a repository metric".
func TestARepositoryMetricWithNoRowsServesAnEmptyList(t *testing.T) {
	dispatch := &explainQueryDispatch{}
	got, err := explainFor(t, dispatch, "churn", "org")
	if err != nil {
		t.Fatal(err)
	}
	if got.Repositories == nil || len(*got.Repositories) != 0 {
		t.Fatalf("repositories = %v, want an empty list", got.Repositories)
	}
	if body := mustMarshal(t, got); !strings.Contains(body, `"repositories":[]`) {
		t.Fatalf("the body has no empty list: %s", body)
	}
	if len(dispatch.sourceURLIDs) != 0 {
		t.Fatalf("no id to read, but the urls were read: %v", dispatch.sourceURLIDs)
	}
}

// A name that is not stored, or that looks like a UUID, is no name: the
// repository's id is never served as its name.
func TestARepositoryNameIsNeverItsID(t *testing.T) {
	const uuid = "44444444-4444-4444-4444-444444444444"
	dispatch := &explainQueryDispatch{
		displayNameRows: [][]any{{"repo-a", uuid}},
		contributorRows: [][]any{{"repo-a", 1.0}, {uuid, 2.0}},
	}
	got, err := explainFor(t, dispatch, "deploy_freq", "org")
	if err != nil {
		t.Fatal(err)
	}
	for _, repository := range *got.Repositories {
		if repository.Name != nil {
			t.Fatalf("repository %s has the name %q", repository.ID, *repository.Name)
		}
	}
	// The builder's own rule, whatever the name lookup returns: a stored
	// name that looks like a UUID is no name.
	built := buildRepositories([]metricRow{{ID: "repo-a", Value: 1}}, identityTransform, map[string]string{"repo-a": uuid}, nil)
	if built[0].Name != nil {
		t.Fatalf("a UUID-shaped stored name is served as the name %q", *built[0].Name)
	}
}

// The top-level source_url is served only when the scope is one repository,
// for every metric, and only when a URL is stored for it.
func TestTheTopLevelSourceURLIsServedOnlyForAScopeOfOneRepository(t *testing.T) {
	const scoped = "33333333-3333-3333-3333-333333333333"
	stored := [][]any{{scoped, "https://github.com/acme/webapp"}}
	for _, testCase := range []struct {
		name       string
		metric     string
		scopeLevel string
		scopeIDs   []string
		resolved   []any
		urls       [][]any
		want       string
	}{
		{"one repository, repository metric", "review_latency", "repo", []string{"acme/webapp"}, []any{scoped}, stored, "https://github.com/acme/webapp"},
		{"one repository, team metric", "throughput", "repo", []string{"acme/webapp"}, []any{scoped}, stored, "https://github.com/acme/webapp"},
		{"one repository, no url stored", "review_latency", "repo", []string{"acme/webapp"}, []any{scoped}, nil, ""},
		{"one repository that does not resolve", "throughput", "repo", []string{"acme/gone"}, nil, stored, ""},
		{"two repositories", "review_latency", "repo", []string{"acme/webapp", "acme/api"}, []any{scoped}, stored, ""},
		// The resolver would answer: a team id must not be read as a repository.
		{"a team scope", "throughput", "team", []string{"team-a"}, []any{scoped}, stored, ""},
		{"the organization", "review_latency", "org", nil, nil, stored, ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dispatch := &explainQueryDispatch{resolveRepoIDRow: testCase.resolved, resolveRepoIDMiss: testCase.resolved == nil, sourceURLRows: testCase.urls}
			got, err := explainFor(t, dispatch, testCase.metric, testCase.scopeLevel, testCase.scopeIDs...)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case testCase.want == "" && got.SourceURL != nil:
				t.Fatalf("source_url = %q, want null", *got.SourceURL)
			case testCase.want != "" && (got.SourceURL == nil || *got.SourceURL != testCase.want):
				t.Fatalf("source_url = %v, want %q", got.SourceURL, testCase.want)
			}
		})
	}
}

// Only an absolute http or https URL with a host is served as a link.
func TestOnlyAnAbsoluteWebURLIsServed(t *testing.T) {
	for stored, want := range map[string]string{
		"https://github.com/acme/webapp":       "https://github.com/acme/webapp",
		"http://gitlab.internal/acme/webapp":   "http://gitlab.internal/acme/webapp",
		"HTTPS://GitHub.com/acme/webapp":       "HTTPS://GitHub.com/acme/webapp",
		" https://github.com/acme/webapp ":     "https://github.com/acme/webapp",
		"":                                     "",
		"   ":                                  "",
		"github.com/acme/webapp":               "",
		"/acme/webapp":                         "",
		"javascript:alert(1)":                  "",
		"ftp://files.example.com/acme/webapp":  "",
		"https:///acme/webapp":                 "",
		"git@github.com:acme/webapp.git":       "",
		"ssh://git@github.com/acme/webapp.git": "",
		// User info is never served, and never stripped and served.
		"https://user:secret@github.com/acme/webapp": "",
		"https://token@github.com/acme/webapp":       "",
		"https://:secret@github.com/acme/webapp":     "",
		"http://user@gitlab.internal/acme/webapp":    "",
	} {
		got, ok := servableSourceURL(stored)
		if got != want || ok != (want != "") {
			t.Errorf("servableSourceURL(%q) = %q, %v; want %q", stored, got, ok, want)
		}
	}
}

// The URL read is bound to the organization, reads repos with FINAL, sends
// the ids as one bound array (never in the statement text), and a failed
// read fails the response: no URL is not the same as an unknown URL.
func TestTheSourceURLReadIsOrgBoundAndFailsLoudly(t *testing.T) {
	var statement string
	var bindings []dhclickhouse.Binding
	failure := errors.New("clickhouse: refused")
	dispatch := &explainQueryDispatch{contributorRows: [][]any{{"repo-a", 1.0}}}
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bound []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "AS source_url") {
			statement, bindings = query, bound
			return nil, failure
		}
		return dispatch.handle(t, query, bound)
	}}
	dispatch.t = t
	dispatch.currentStartDay = "2024-05-01"
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric: "churn", StartDay: day(2024, 5, 1), EndDay: day(2024, 5, 15),
		CompareStart: day(2024, 4, 17), CompareEnd: day(2024, 5, 1), ScopeLevel: "org",
	})
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want the read's failure", err)
	}
	for _, part := range []string{"FROM repos FINAL", "org_id = {org_id:String}", "toString(id) IN {repo_ids:Array(String)}", "JSONExtractString(settings, 'url')"} {
		if !strings.Contains(statement, part) {
			t.Fatalf("the url read has no %q:\n%s", part, statement)
		}
	}
	if strings.Contains(statement, "repo-a") {
		t.Fatalf("an id is in the statement text:\n%s", statement)
	}
	if org, _ := bindingValue(bindings, "org_id"); org != "org-acme" {
		t.Fatalf("org_id binding = %v", org)
	}
	if ids, _ := bindingValue(bindings, "repo_ids"); !reflect.DeepEqual(ids, []string{"repo-a"}) {
		t.Fatalf("repo_ids binding = %v", ids)
	}
}

// A stored URL that carries user info is served as no URL, for a repository
// of the list and for the scope's repository alike.
func TestAStoredURLWithUserInfoIsNeverServed(t *testing.T) {
	const scoped = "33333333-3333-3333-3333-333333333333"
	dispatch := &explainQueryDispatch{
		resolveRepoIDRow: []any{scoped},
		contributorRows:  [][]any{{scoped, 1.0}},
		sourceURLRows:    [][]any{{scoped, "https://user:secret@github.com/acme/webapp"}},
	}
	got, err := explainFor(t, dispatch, "churn", "repo", "acme/webapp")
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceURL != nil {
		t.Fatalf("top-level source_url = %q, want null", *got.SourceURL)
	}
	if got.Repositories == nil || len(*got.Repositories) != 1 || (*got.Repositories)[0].SourceURL != nil {
		t.Fatalf("repositories = %s, want one repository with no source_url", mustMarshal(t, got.Repositories))
	}
	if body := mustMarshal(t, got); strings.Contains(body, "secret") || strings.Contains(body, "user:") {
		t.Fatalf("the body holds the user info: %s", body)
	}
}

// CHAOS-8903: source is the sorted, joined distinct providers of the repositories behind a
// metric stored per repository; "unknown" and empty providers are no provider; a metric stored
// per team serves null and reads no provider.
func TestSourceIsTheStoredProvidersBehindARepositoryMetric(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		metric    string
		providers [][]any
		want      *string
	}{
		{"two providers sorted and joined", "churn", [][]any{{"repo-b", "gitlab"}, {"repo-a", "github"}}, ptr("github, gitlab")},
		{"one provider once", "churn", [][]any{{"repo-a", "github"}, {"repo-b", "github"}}, ptr("github")},
		{"unknown and empty are no provider", "churn", [][]any{{"repo-a", "unknown"}, {"repo-b", ""}}, nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dispatch := &explainQueryDispatch{
				contributorRows: [][]any{{"repo-a", 0.25}, {"repo-b", 0.5}},
				providerRows:    testCase.providers,
			}
			got, err := explainFor(t, dispatch, testCase.metric, "org")
			if err != nil {
				t.Fatal(err)
			}
			if (got.Source == nil) != (testCase.want == nil) || (got.Source != nil && *got.Source != *testCase.want) {
				t.Fatalf("source = %v, want %v", got.Source, testCase.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// CHAOS-8910: a metric stored per team takes the distinct providers of the work items behind it.
func TestSourceOfATeamMetricIsTheWorkItemProviders(t *testing.T) {
	for _, testCase := range []struct {
		name string
		rows [][]any
		want *string
	}{
		{"two providers sorted and joined", [][]any{{"jira"}, {"github"}, {"jira"}}, ptr("github, jira")},
		{"unknown and empty are no provider", [][]any{{"unknown"}, {""}}, nil},
		{"no rows", nil, nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dispatch := &explainQueryDispatch{
				contributorRows:      [][]any{{"team-a", 3.0}},
				workItemProviderRows: testCase.rows,
				// A repository table must never be read for a team metric.
				providerRows: [][]any{{"repo-a", "gitlab"}},
			}
			got, err := explainFor(t, dispatch, "throughput", "org")
			if err != nil {
				t.Fatal(err)
			}
			if (got.Source == nil) != (testCase.want == nil) || (got.Source != nil && *got.Source != *testCase.want) {
				t.Fatalf("source = %v, want %v", got.Source, testCase.want)
			}
		})
	}
}

// Change failure rate is served with its state: measured (with the weakest
// link tier behind it when a deployment failed), unknown, not applicable, or
// no state for a window with no stored counts. No other metric carries a state
// or a tier.
func TestChangeFailureRateIsServedWithItsStateAndLinkTier(t *testing.T) {
	// view: 10 deployments unless stated; the last value is the stored rows.
	view := func(deployments, native, heuristic, direct, via, stored uint64) [][]any {
		return [][]any{{deployments, native, heuristic, direct, via, stored}}
	}
	for _, tc := range []struct {
		name    string
		metric  string
		rows    [][]any
		value   float64
		hasData bool
		state   string
		tier    string
	}{
		{"heuristic only", "change_failure_rate", view(10, 0, 3, 1, 0, 4), 30, true, "measured", "heuristic"},
		{"native and heuristic", "change_failure_rate", view(10, 2, 1, 0, 1, 4), 30, true, "measured", "heuristic"},
		{"native only", "change_failure_rate", view(10, 2, 0, 1, 0, 4), 20, true, "measured", "native"},
		{"measured zero", "change_failure_rate", view(10, 0, 0, 1, 0, 4), 0, true, "measured", "null"},
		{"unknown", "change_failure_rate", view(10, 0, 0, 0, 0, 4), 0, false, "unknown_no_incident_evidence", "null"},
		{"not applicable", "change_failure_rate", view(0, 0, 0, 2, 0, 4), 0, false, "not_applicable_no_deployments", "null"},
		{"retraction rows only", "change_failure_rate", view(0, 0, 0, 0, 0, 4), 0, false, "not_applicable_no_deployments", "null"},
		{"no stored row", "change_failure_rate", view(0, 0, 0, 0, 0, 0), 0, false, "null", "null"},
		{"no row from the read", "change_failure_rate", nil, 0, false, "null", "null"},
		{"another metric", "revert_rate", view(10, 0, 3, 1, 0, 4), 0.5, true, "null", "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dispatch := &explainQueryDispatch{changeFailureViewRows: tc.rows}
			got, err := explainFor(t, dispatch, tc.metric, "org")
			if err != nil {
				t.Fatal(err)
			}
			served := func(v *string) string {
				if v == nil {
					return "null"
				}
				return *v
			}
			if served(got.RateState) != tc.state {
				t.Errorf("rate_state = %s, want %s", served(got.RateState), tc.state)
			}
			if served(got.LinkTier) != tc.tier {
				t.Errorf("link_tier = %s, want %s", served(got.LinkTier), tc.tier)
			}
			if got.HasData != tc.hasData {
				t.Errorf("has_data = %v, want %v", got.HasData, tc.hasData)
			}
			if tc.metric == "change_failure_rate" && got.Value != tc.value {
				t.Errorf("value = %v, want %v", got.Value, tc.value)
			}
		})
	}
}
