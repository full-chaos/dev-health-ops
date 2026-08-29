package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabTestsRouteDoer struct {
	t                *testing.T
	archive          []byte
	failJobs         bool
	capPipelines     bool
	emptyArtifacts   bool
	artifactJobCount int
	pipelineBody     string
	redirectStatus   int
	pipelineBodies   []string
	pipelineCalls    int
	requests         []string
}

func (doer *gitLabTestsRouteDoer) Do(request *http.Request) (*http.Response, error) {
	doer.requests = append(doer.requests, request.URL.String())
	status := http.StatusOK
	header := make(http.Header)
	body := ""
	path := request.URL.Path
	switch {
	case request.URL.Host == "blob.example":
		status = doer.redirectStatus
		body = `{"message":"blob failure"}`
	case path == "/api/v4/projects/123":
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		if doer.pipelineCalls < len(doer.pipelineBodies) {
			body = doer.pipelineBodies[doer.pipelineCalls]
		}
		doer.pipelineCalls++
		if body == "" {
			body = doer.pipelineBody
		}
		if body == "" {
			body = `[{"id":9001,"name":"CI","ref":"main","status":"success","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:05:00Z","source":"push","sha":"abc","web_url":"https://gitlab.example/acme/api/-/pipelines/9001"}]`
		}
		if doer.capPipelines {
			header.Set("X-Next-Page", "2")
		}
	case path == "/api/v4/projects/123/pipelines/9001/jobs":
		if doer.failJobs {
			status = http.StatusServiceUnavailable
			body = `{"message":"unavailable"}`
			break
		}
		if request.URL.Query().Get("include_retried") == "true" {
			body = `[{"id":11,"name":"unit","stage":"test","status":"success","started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:04:00Z","runner":{"runner_type":"instance_type"},"retried":true}]`
		} else if doer.artifactJobCount > 0 {
			body = "[" + strings.TrimSuffix(strings.Repeat(`{"id":11,"name":"unit","artifacts_file":{"filename":"reports.zip"}},`, doer.artifactJobCount), ",") + "]"
		} else if doer.emptyArtifacts {
			body = `[{"id":11,"name":"unit","artifacts_file":{},"artifacts":[]}]`
		} else {
			body = `[{"id":11,"name":"unit","artifacts_file":{"filename":"reports.zip"}}]`
		}
	case path == "/api/v4/projects/123/pipelines/9001/test_report":
		body = `{"test_suites":[{"name":"api","total_time":"1.25","test_cases":[{"name":"passes","classname":"tests.TestAPI","status":"success","execution_time":"1.25"}]}]}`
	case path == "/api/v4/projects/123/jobs/11/artifacts":
		if doer.redirectStatus != 0 {
			status = http.StatusFound
			header.Set("Location", "https://blob.example/reports.zip")
		} else {
			body = string(doer.archive)
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestGitLabCICDAndTestsAliasesEmitByteIdenticalSixEffects(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 30, 0, 456789000, time.UTC)
	collect := func(dataset string) CompleteRouteBatch {
		doer := &gitLabTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})}
		claim := nativeTestClaim("gitlab", dataset)
		batch, err := (GitLabTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), now)
		if err != nil {
			t.Fatal(err)
		}
		if len(doer.requests) != 7 {
			t.Fatalf("requests=%v", doer.requests)
		}
		return batch
	}
	cicd, tests := collect("cicd"), collect("tests")
	if !reflect.DeepEqual(cicd.Effects, tests.Effects) {
		t.Fatalf("alias effects diverged\ncicd=%+v\ntests=%+v", cicd.Effects, tests.Effects)
	}
	if cicd.Result["actual_route_family"] != "pipelines" || tests.Result["actual_route_family"] != "tests" {
		t.Fatalf("actual route families cicd=%v tests=%v", cicd.Result["actual_route_family"], tests.Result["actual_route_family"])
	}
	assertGitLabTestsUsageObservation(t, cicd, "pipelines")
	assertGitLabTestsUsageObservation(t, tests, "tests")
	wantCounts := map[string]int{"ci_pipeline_runs": 1, "ci_job_runs": 1, "ci_acceptance_checks": 2, "test_suite_results": 1, "test_case_results": 1, "coverage_snapshots": 1}
	if len(cicd.Effects) != len(wantCounts) {
		t.Fatalf("effects=%d want=%d", len(cicd.Effects), len(wantCounts))
	}
	for _, effect := range cicd.Effects {
		if got := len(effect.Rows); got != wantCounts[effect.Destination] {
			t.Fatalf("%s rows=%d want=%d", effect.Destination, got, wantCounts[effect.Destination])
		}
	}
}

func assertGitLabTestsUsageObservation(t *testing.T, batch CompleteRouteBatch, routeFamily string) {
	t.Helper()
	observations, ok := batch.Result["observations"].(map[string]any)
	if !ok {
		t.Fatalf("observations=%#v", batch.Result["observations"])
	}
	usage, ok := observations["provider_usage"].([]any)
	if !ok || len(usage) != 1 {
		t.Fatalf("provider usage=%#v", observations["provider_usage"])
	}
	observation, ok := usage[0].(map[string]any)
	if !ok || observation["transport"] != "rest" ||
		observation["route_family"] != routeFamily ||
		observation["dimension"] != "rest_core" ||
		observation["request_count"] != batch.Evidence.Requests {
		t.Fatalf("provider usage observation=%#v evidence=%+v", observation, batch.Evidence)
	}
}

func TestGitLabTestsRouteRejectsCrossScopeBeforeProviderRequests(t *testing.T) {
	githubClaim := nativeTestClaim("github", "tests")
	githubClaim.SourceExternalID = "123"
	for _, test := range []struct {
		name         string
		claim        Claim
		mutateClient func(*providerfoundation.HTTPClient)
	}{
		{name: "claim provider", claim: githubClaim},
		{name: "claim dataset", claim: nativeTestClaim("gitlab", "security")},
		{name: "client provider", claim: nativeTestClaim("gitlab", "tests"), mutateClient: func(client *providerfoundation.HTTPClient) { client.Provider = "github" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabTestsRouteDoer{t: t}
			client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
			if test.mutateClient != nil {
				test.mutateClient(client)
			}
			batch, err := (GitLabTestsRouteHandler{}).Collect(context.Background(), test.claim, providerfoundation.Credential{}, client, time.Now())
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v", err)
			}
			if len(doer.requests) != 0 || len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("requests=%v batch=%+v", doer.requests, batch)
			}
		})
	}
}

func TestGitLabTestsFailsClosedOnIncompleteAndTransientTraversal(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*gitLabTestsRouteDoer)
		want      error
		handler   GitLabTestsRouteHandler
	}{
		{name: "transient jobs", configure: func(d *gitLabTestsRouteDoer) { d.failJobs = true }},
		{name: "pipeline cap", configure: func(d *gitLabTestsRouteDoer) { d.capPipelines = true }, want: ErrPaginationCapExceeded, handler: GitLabTestsRouteHandler{MaxPages: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabTestsRouteDoer{t: t}
			test.configure(doer)
			claim := nativeTestClaim("gitlab", "tests")
			batch, err := test.handler.Collect(context.Background(), claim, providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now())
			if err == nil || (test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("partial batch=%+v", batch)
			}
		})
	}
}

func TestGitLabTestsFailsClosedOnMissingPipelineID(t *testing.T) {
	doer := &gitLabTestsRouteDoer{t: t, pipelineBodies: []string{
		`[{"ref":"main","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z"}]`,
		`[]`,
	}}
	batch, err := (GitLabTestsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "tests"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now(),
	)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("partial batch=%+v", batch)
	}
}

func TestGitLabTestsComparesSourcePrecisionBeforeStorageTruncation(t *testing.T) {
	doer := &gitLabTestsRouteDoer{t: t, pipelineBody: `[{"id":9001,"ref":"main","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-23T00:00:00.0009Z"}]`}
	claim := nativeTestClaim("gitlab", "tests")
	before := time.Date(2026, 7, 23, 0, 0, 0, 500000, time.UTC)
	claim.BeforeAt = &before
	batch, err := (GitLabTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if len(effect.Rows) != 0 {
			t.Fatalf("%s rows=%s", effect.Destination, effect.Rows)
		}
	}
}

func TestGitLabTestsArtifactTruthinessMatchesPython(t *testing.T) {
	doer := &gitLabTestsRouteDoer{t: t, emptyArtifacts: true}
	claim := nativeTestClaim("gitlab", "tests")
	batch, err := (GitLabTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range doer.requests {
		if strings.Contains(request, "/artifacts") {
			t.Fatalf("empty Python-false artifact metadata triggered download: %v", doer.requests)
		}
	}
	for _, effect := range batch.Effects {
		if effect.Destination == "coverage_snapshots" && len(effect.Rows) != 0 {
			t.Fatalf("coverage=%s", effect.Rows)
		}
	}
}

func TestGitLabTestsProjectPolicyDistinguishesMissingFromFalse(t *testing.T) {
	missing, missingProvenance := projectGitLabTestsRequirement(repositoryPayload{})
	if missing != nil || missingProvenance != "gitlab.project_merge_policy.missing_field" {
		t.Fatalf("missing policy required=%v provenance=%q", missing, missingProvenance)
	}
	flag := false
	present, presentProvenance := projectGitLabTestsRequirement(repositoryPayload{
		OnlyAllowMergeIfPipelineSucceeds: &flag,
	})
	if present == nil || len(present) != 0 || presentProvenance != "gitlab.project_merge_policy" {
		t.Fatalf("false policy required=%v provenance=%q", present, presentProvenance)
	}
}

func TestGitLabTestsPreservesSourcePrecisionInProviderQueries(t *testing.T) {
	doer := &gitLabTestsRouteDoer{t: t}
	claim := nativeTestClaim("gitlab", "tests")
	since := time.Date(2026, 7, 22, 0, 0, 0, 400000, time.UTC)
	before := time.Date(2026, 7, 23, 0, 0, 0, 500000, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	if _, err := (GitLabTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now(),
	); err != nil {
		t.Fatal(err)
	}
	var pipelineQueries []string
	for _, raw := range doer.requests {
		request, err := http.NewRequest(http.MethodGet, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		if request.URL.Path == "/api/v4/projects/123/pipelines" {
			pipelineQueries = append(pipelineQueries, request.URL.RawQuery)
		}
	}
	if len(pipelineQueries) != 2 {
		t.Fatalf("pipeline queries=%v", pipelineQueries)
	}
	wantSince, wantBefore := since.Format(time.RFC3339Nano), before.Format(time.RFC3339Nano)
	for index, raw := range pipelineQueries {
		query, err := url.ParseQuery(raw)
		if err != nil {
			t.Fatal(err)
		}
		if query.Get("updated_after") != wantSince {
			t.Fatalf("query %d updated_after=%q want=%q", index, query.Get("updated_after"), wantSince)
		}
		if index == 0 && query.Get("updated_before") != wantBefore {
			t.Fatalf("query %d updated_before=%q want=%q", index, query.Get("updated_before"), wantBefore)
		}
	}
}

func TestGitLabTestsAcceptsPythonSinglePageArtifactBoundary(t *testing.T) {
	doer := &gitLabTestsRouteDoer{t: t, artifactJobCount: nativePerPage}
	claim := nativeTestClaim("gitlab", "tests")
	batch, err := (GitLabTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Evidence.Requests != len(doer.requests) {
		t.Fatalf("evidence requests=%d physical=%d", batch.Evidence.Requests, len(doer.requests))
	}
	artifactRequests := 0
	for _, request := range doer.requests {
		if strings.HasSuffix(request, "/artifacts") {
			artifactRequests++
		}
	}
	if artifactRequests != gitLabTestsMaxArtifacts {
		t.Fatalf("artifact requests=%d want=%d", artifactRequests, gitLabTestsMaxArtifacts)
	}
}

func TestTestOpsAcceptanceNamesHaveDeterministicCaseCollisionOrder(t *testing.T) {
	started := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	pipeline := githubTestsPipelineRow{StartedAt: started}
	wantGitHub := []string{"Unit", "unit"}
	wantGitLab := []string{"pipeline", "Unit", "unit"}
	permutations := [][]githubTestsJobRow{
		{{JobName: "unit"}, {JobName: "Unit"}},
		{{JobName: "Unit"}, {JobName: "unit"}},
	}
	for iteration := 0; iteration < 32; iteration++ {
		for _, jobs := range permutations {
			githubRows := projectGitHubTestsChecks(Claim{}, "repo", pipeline, jobs, githubTestsPolicy{}, nil, nil, nil, started)
			if got := testOpsAcceptanceNames(githubRows); !reflect.DeepEqual(got, wantGitHub) {
				t.Fatalf("GitHub iteration %d names=%v want=%v", iteration, got, wantGitHub)
			}

			gitlabRows := projectGitLabTestsChecks(Claim{}, "repo", pipeline, gitLabTestsPipelinePayload{}, jobs, nil, "test", started)
			if got := testOpsAcceptanceNames(gitlabRows); !reflect.DeepEqual(got, wantGitLab) {
				t.Fatalf("GitLab iteration %d names=%v want=%v", iteration, got, wantGitLab)
			}
		}
	}
}

func testOpsAcceptanceNames(rows []githubTestsAcceptanceRow) []string {
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.CheckName)
	}
	return names
}

func TestGitLabTestsArtifactRedirectDoesNotForwardCredentialAndCountsPhysicalRequests(t *testing.T) {
	archive := githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})
	var blobHeaders http.Header
	blob := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		blobHeaders = request.Header.Clone()
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(archive)
	}))
	defer blob.Close()
	apiRequests := 0
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		apiRequests++
		if got := request.Header.Get("PRIVATE-TOKEN"); got != "gitlab-secret" {
			t.Errorf("API request %s PRIVATE-TOKEN=%q", request.URL.Path, got)
		}
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/api/v4/projects/123":
			_, _ = io.WriteString(writer, `{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`)
		case request.URL.Path == "/api/v4/projects/123/pipelines":
			_, _ = io.WriteString(writer, `[{"id":9001,"name":"CI","ref":"main","status":"success","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:05:00Z","source":"push","sha":"abc"}]`)
		case request.URL.Path == "/api/v4/projects/123/pipelines/9001/jobs" && request.URL.Query().Get("include_retried") == "true":
			_, _ = io.WriteString(writer, `[{"id":11,"name":"unit","stage":"test","status":"success"}]`)
		case request.URL.Path == "/api/v4/projects/123/pipelines/9001/jobs":
			_, _ = io.WriteString(writer, `[{"id":11,"name":"unit","artifacts_file":{"filename":"reports.zip"}}]`)
		case request.URL.Path == "/api/v4/projects/123/pipelines/9001/test_report":
			_, _ = io.WriteString(writer, `{"test_suites":[{"name":"api","test_cases":[{"name":"passes","status":"success"}]}]}`)
		case request.URL.Path == "/api/v4/projects/123/jobs/11/artifacts":
			writer.Header().Set("Location", blob.URL+"/reports.zip")
			writer.WriteHeader(http.StatusFound)
		default:
			t.Errorf("unexpected API request %s", request.URL.String())
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	credential, err := (providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"base_url": api.URL}}).
		WithEphemeralSecret("token", secrets.NewValue("gitlab-secret"))
	if err != nil {
		t.Fatal(err)
	}
	doer := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	client, err := providerfoundation.NewGitLabClient(
		credential, doer,
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (GitLabTestsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "tests"), credential, client,
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if apiRequests != 7 || batch.Evidence.Requests != 8 {
		t.Fatalf("api requests=%d physical requests=%d", apiRequests, batch.Evidence.Requests)
	}
	for _, header := range []string{"PRIVATE-TOKEN", "Authorization", "Cookie"} {
		if got := blobHeaders.Get(header); got != "" {
			t.Fatalf("blob %s=%q", header, got)
		}
	}
	for _, effect := range batch.Effects {
		if effect.Destination == "coverage_snapshots" && len(effect.Rows) != 1 {
			t.Fatalf("coverage rows=%d", len(effect.Rows))
		}
	}
}

func TestGitLabTestsRedirectTargetFailuresPreserveProviderClassification(t *testing.T) {
	for _, test := range []struct {
		status int
		class  providerfoundation.ErrorClass
	}{
		{status: http.StatusTooManyRequests, class: providerfoundation.ErrorRateLimited},
		{status: http.StatusServiceUnavailable, class: providerfoundation.ErrorTransient},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			doer := &gitLabTestsRouteDoer{t: t, redirectStatus: test.status}
			batch, err := (GitLabTestsRouteHandler{}).Collect(
				context.Background(), nativeTestClaim("gitlab", "tests"), providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now(),
			)
			var providerErr *providerfoundation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != test.class || providerErr.StatusCode != test.status {
				t.Fatalf("error=%v", err)
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("partial batch=%+v", batch)
			}
		})
	}
}

// TestGitLabNativeTestReportWithinSuiteDuplicateCaseNamesGetDistinctIDs is
// the GitLab-native twin of the JUnit fixture pinned in
// TestGitHubTestsWithinSuiteDuplicateCaseNamesGetDistinctIDsAndWriteSucceeds
// (CHAOS-4392): normalizeGitLabNativeTestReport hashed CaseID from
// (suiteID, caseName) alone, so two test_cases sharing a name in one
// test_suite collided exactly like the JUnit path did.
func TestGitLabNativeTestReportWithinSuiteDuplicateCaseNamesGetDistinctIDs(t *testing.T) {
	normalize := func(t *testing.T, payload []gitLabTestsCasePayload, wantCases int) []testCaseResultRow {
		t.Helper()
		claim := nativeTestClaim("gitlab", "cicd")
		report := gitLabTestsReportPayload{Suites: []gitLabTestsSuitePayload{{Name: "matrix", Cases: payload}}}
		_, cases, err := normalizeGitLabNativeTestReport(
			claim, "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", report, nil, nil,
			time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC),
		)
		if err != nil {
			t.Fatalf("normalize of a within-suite duplicate case name failed closed: %v", err)
		}
		if len(cases) != wantCases {
			t.Fatalf("cases=%+v, want %d rows retained", cases, wantCases)
		}
		ids := make(map[string]struct{}, len(cases))
		for _, row := range cases {
			if row.CaseID == "" {
				t.Fatalf("case=%+v has an empty CaseID", row)
			}
			if _, collided := ids[row.CaseID]; collided {
				t.Fatalf("cases=%+v, CaseID %q is not unique -- CHAOS-4392's collision", cases, row.CaseID)
			}
			ids[row.CaseID] = struct{}{}
		}
		return cases
	}

	t.Run("two identically named cases get distinct ids", func(t *testing.T) {
		cases := normalize(t, []gitLabTestsCasePayload{
			{Name: "flaky", ClassName: "pkg.TestA", Status: "success"},
			{Name: "flaky", ClassName: "pkg.TestB", Status: "failed"},
		}, 2)
		if cases[0].CaseName != "flaky" || cases[1].CaseName != "flaky" {
			t.Fatalf("cases=%+v, want CaseName preserved verbatim on both rows", cases)
		}
		if countDuplicateTestCases(cases) != 1 {
			t.Fatalf("countDuplicateTestCases=%d, want 1", countDuplicateTestCases(cases))
		}
	})

	// GitLab twin of the codex review finding on the JUnit path: an
	// ordinal-shaped real case name ("foo::1") must not collide with a
	// disambiguated duplicate of "foo" via hashTestIdentifier's unescaped
	// "::" join.
	t.Run("an ordinal-shaped case name does not collide with a real duplicate", func(t *testing.T) {
		normalize(t, []gitLabTestsCasePayload{
			{Name: "foo", ClassName: "pkg.TestA", Status: "success"},
			{Name: "foo", ClassName: "pkg.TestB", Status: "failed"},
			{Name: "foo::1", ClassName: "pkg.TestC", Status: "success"},
		}, 3)
	})
}

// TestGitLabNativeTestReportSameReportSiblingSuitesSameNameCollide is the
// GitLab-native twin of TestGitHubTestsSameArtifactSiblingSuitesSameNameCollide
// (CHAOS-4508): normalizeGitLabNativeTestReport hashed suiteID from
// (runID, name, "") alone, so two SIBLING test_suite objects sharing a name
// in the SAME test_report response collided exactly like the JUnit path did,
// and their first same-named case then also collided on caseID since
// caseOccurrence resets fresh per suite object.
func TestGitLabNativeTestReportSameReportSiblingSuitesSameNameCollide(t *testing.T) {
	claim := nativeTestClaim("gitlab", "cicd")
	report := gitLabTestsReportPayload{Suites: []gitLabTestsSuitePayload{
		{Name: "pytest", Cases: []gitLabTestsCasePayload{{Name: "test_health", ClassName: "tests.test_api", Status: "success"}}},
		{Name: "pytest", Cases: []gitLabTestsCasePayload{{Name: "test_health", ClassName: "tests.test_worker", Status: "success"}}},
	}}
	suites, cases, err := normalizeGitLabNativeTestReport(
		claim, "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", report, nil, nil,
		time.Date(2026, 8, 29, 11, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("normalize of two same-named sibling suites in one report failed closed: %v", err)
	}
	if len(suites) != 2 || len(cases) != 2 {
		t.Fatalf("suites=%+v cases=%+v, want 2 suite rows and 2 case rows retained", suites, cases)
	}
	if suites[0].SuiteID == suites[1].SuiteID {
		t.Fatalf("CHAOS-4508: sibling suites %q and %q in ONE report share SuiteID %q -- "+
			"hashTestIdentifier(runID, name, \"\") has no per-suite-object discriminator",
			suites[0].SuiteName, suites[1].SuiteName, suites[0].SuiteID)
	}
	if cases[0].CaseID == cases[1].CaseID {
		t.Fatalf("CHAOS-4508: cases named %q in two sibling same-named suites share CaseID %q",
			cases[0].CaseName, cases[0].CaseID)
	}
	if countDuplicateTestSuites(suites) != 1 {
		t.Fatalf("countDuplicateTestSuites=%d, want 1", countDuplicateTestSuites(suites))
	}
}
