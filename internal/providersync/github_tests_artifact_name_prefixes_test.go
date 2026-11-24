package providersync

import (
	"bytes"
	"io"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type githubTestsPrefixGuardArtifact struct {
	name    string
	expired bool
	members map[string]string
}

type githubTestsPrefixGuardDoer struct {
	t           *testing.T
	artifacts   []githubTestsPrefixGuardArtifact
	downloadIDs []string
}

func (doer *githubTestsPrefixGuardDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, 1)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		var body strings.Builder
		body.WriteString(`{"artifacts":[`)
		for index, artifact := range doer.artifacts {
			if index > 0 {
				body.WriteByte(',')
			}
			body.WriteString(`{"id":` + strconv.Itoa(index+1) + `,"name":"` + artifact.name + `","expired":` + strconv.FormatBool(artifact.expired) + `}`)
		}
		body.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, body.String()), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := githubTestsArtifactIDFromPath(doer.t, path)
		doer.downloadIDs = append(doer.downloadIDs, doer.artifacts[id-1].name)
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			Body: io.NopCloser(bytes.NewReader(githubTestsZip(doer.t, doer.artifacts[id-1].members))), Request: request,
		}, nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

// githubTestsPrefixGuardServed is what a run serves: the case rows' keys, the skip markers and the completeness flag.
type githubTestsPrefixGuardServed struct {
	caseKeys       []string
	skipped        []string
	reportsDone    any
	incomplete     int
	downloads      int
	excludedPrefix int
}

func (served githubTestsPrefixGuardServed) sameRows(other githubTestsPrefixGuardServed) bool {
	return slices.Equal(served.caseKeys, other.caseKeys) && slices.Equal(served.skipped, other.skipped) &&
		served.reportsDone == other.reportsDone && served.incomplete == other.incomplete
}

func (served githubTestsPrefixGuardServed) clean() bool {
	return len(served.skipped) == 0 && served.incomplete == 0 && served.reportsDone == true
}

func githubTestsPrefixGuardRun(t *testing.T, artifacts []githubTestsPrefixGuardArtifact, prefixes []string) githubTestsPrefixGuardServed {
	t.Helper()
	served, _ := githubTestsPrefixGuardRunWith(t, artifacts, prefixes, nil)
	return served
}

// githubTestsPrefixGuardRunWith runs the shape with the code list set to prefixes and the sync configuration's own list
// (the integration config the unit carries) set to configPrefixes; nil leaves the key out.
func githubTestsPrefixGuardRunWith(
	t *testing.T, artifacts []githubTestsPrefixGuardArtifact, prefixes, configPrefixes []string,
) (githubTestsPrefixGuardServed, githubTestsWalk) {
	t.Helper()
	saved := githubTestsNonReportArtifactPrefixes
	githubTestsNonReportArtifactPrefixes = prefixes
	defer func() { githubTestsNonReportArtifactPrefixes = saved }()
	claim := nativeTestClaim("github", "cicd")
	if configPrefixes != nil {
		items := make([]any, len(configPrefixes))
		for index, prefix := range configPrefixes {
			items[index] = prefix
		}
		claim.IntegrationConfig = map[string]any{"ci_artifact_exclude_prefixes": items}
	}
	doer := &githubTestsPrefixGuardDoer{t: t, artifacts: artifacts}
	final, effects := walkGitHubTestsChunksCapturingEffects(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, fakehttp.Client(doer)), 50)
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, fakehttp.Client(&githubTestsPrefixGuardDoer{t: t, artifacts: artifacts})), 50)
	served := githubTestsPrefixGuardServed{
		reportsDone: final.Result["reports_complete"], incomplete: len(walk.cursor.Incomplete),
		downloads: len(doer.downloadIDs), excludedPrefix: walk.cursor.ExcludedNonReportPrefix,
	}
	for _, marker := range walk.cursor.SkippedArtifacts {
		served.skipped = append(served.skipped, marker.Name+":"+marker.Cause)
	}
	if _, ok := findGitHubTestsEffect(effects, "test_case_results"); ok {
		for _, row := range decodeGitHubTestsEffectRows[testCaseResultRow](t, effects, "test_case_results") {
			served.caseKeys = append(served.caseKeys, row.SuiteID+"/"+row.CaseID+"/"+row.CaseName+"/"+strconv.FormatBool(row.Retracted))
		}
	}
	sort.Strings(served.caseKeys)
	return served, walk
}

func githubTestsPrefixGuardSuite(name string, cases ...string) string {
	var body strings.Builder
	body.WriteString(`<testsuite name="` + name + `" time="2.0" timestamp="2026-08-14T10:00:00">`)
	for _, c := range cases {
		body.WriteString(`<testcase classname="` + name + `" name="` + c + `" time="1.0"/>`)
	}
	body.WriteString(`</testsuite>`)
	return body.String()
}

// The content rules do not replace githubTestsNonReportArtifactPrefixes. Each shape runs the chunked route with the list as
// it is and with it emptied: an expired image artifact and JUnit parts beside their merged report serve a false incompleteness
// or moved case keys without the list; a live image tarball and a timings table serve the same rows and cost one download each.
func TestGitHubTestsArtifactNamePrefixesAreLoadBearing(t *testing.T) {
	suiteA := githubTestsPrefixGuardSuite("pkg/a", "TestA1", "TestA2")
	suiteB := githubTestsPrefixGuardSuite("pkg/b", "TestB1", "TestB2")
	merged := githubTestsPrefixGuardArtifact{name: "go-test-junit-integration", members: map[string]string{"junit.xml": "<testsuites>" + suiteA + suiteB + "</testsuites>"}}
	type shape struct {
		name       string
		artifacts  []githubTestsPrefixGuardArtifact
		loadedRows bool
	}
	shapes := []shape{
		{name: "image tarball", artifacts: []githubTestsPrefixGuardArtifact{{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}}, merged}},
		{name: "expired image tarball", loadedRows: true, artifacts: []githubTestsPrefixGuardArtifact{{name: "go-images-amd64-9-1", expired: true}, merged}},
		{name: "timings table", artifacts: []githubTestsPrefixGuardArtifact{{name: "go-test-timings-integration-packages-1", members: map[string]string{"timings.tsv.gz": "pkg\t1.0"}}, merged}},
		{name: "JUnit parts beside their merged report", loadedRows: true, artifacts: []githubTestsPrefixGuardArtifact{
			{name: "go-junit-part-packages-1", members: map[string]string{"a.xml": "<testsuites>" + suiteA + "</testsuites>"}},
			{name: "go-junit-part-packages-2", members: map[string]string{"b.xml": "<testsuites>" + suiteB + "</testsuites>"}},
			merged,
		}},
		{name: "expired JUnit parts beside their merged report", loadedRows: true, artifacts: []githubTestsPrefixGuardArtifact{
			{name: "go-junit-part-packages-1", expired: true}, {name: "go-junit-part-packages-2", expired: true}, merged,
		}},
	}
	for _, shape := range shapes {
		withList := githubTestsPrefixGuardRun(t, shape.artifacts, githubTestsNonReportArtifactPrefixes)
		emptied := githubTestsPrefixGuardRun(t, shape.artifacts, nil)
		if withList.excludedPrefix == 0 || !withList.clean() || len(withList.caseKeys) != 4 {
			t.Errorf("the prefix list is load-bearing: %s: with the list the run must exclude it by name and serve 4 clean case rows, got %+v",
				shape.name, withList)
			continue
		}
		// The configuration's own list alone, with the code list emptied, carries the same load (CHAOS-14559).
		byConfig, _ := githubTestsPrefixGuardRunWith(t, shape.artifacts, nil, []string{"go-images-", "go-test-timings-", "go-junit-part-"})
		if byConfig.excludedPrefix != withList.excludedPrefix || !byConfig.clean() || !byConfig.sameRows(withList) || byConfig.downloads != withList.downloads {
			t.Errorf("the prefix list is load-bearing: %s: the sync configuration's list must serve what the code list serves, with=%+v config=%+v",
				shape.name, withList, byConfig)
		}
		if shape.loadedRows {
			if emptied.clean() && emptied.sameRows(withList) {
				t.Errorf("the prefix list is load-bearing: %s: with the list emptied the run should show a false incompleteness or moved case keys, got the same rows %+v",
					shape.name, emptied)
			}
			continue
		}
		if !emptied.sameRows(withList) || emptied.downloads != withList.downloads+1 {
			t.Errorf("the prefix list is load-bearing: %s: with the list emptied the content rule should serve the same rows at one more download, with=%+v emptied=%+v",
				shape.name, withList, emptied)
		}
	}
}

// The seam joins the code list with the sync configuration's list: each excludes its own names, and a configuration never
// takes a code name away.
func TestGitHubTestsArtifactExcludeJoinsTheCodeListWithTheConfigList(t *testing.T) {
	suite := githubTestsPrefixGuardSuite("pkg/a", "TestA1", "TestA2")
	artifacts := []githubTestsPrefixGuardArtifact{
		{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}},
		{name: "ci-extra-1", members: map[string]string{"extra.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}},
		{name: "test-results", members: map[string]string{"junit.xml": "<testsuites>" + suite + "</testsuites>"}},
	}
	for _, cell := range []struct {
		name           string
		configPrefixes []string
		wantExcluded   int
		wantDownloads  int
		wantCases      int
	}{
		{name: "code list only", wantExcluded: 1, wantDownloads: 2, wantCases: 3},
		{name: "code list and a configured prefix", configPrefixes: []string{"ci-extra-"}, wantExcluded: 2, wantDownloads: 1, wantCases: 2},
		{name: "a configured list without the code names keeps them", configPrefixes: []string{"CI-EXTRA-"}, wantExcluded: 2, wantDownloads: 1, wantCases: 2},
		{name: "an invalid configured value excludes nothing more", configPrefixes: []string{""}, wantExcluded: 1, wantDownloads: 2, wantCases: 3},
	} {
		served, _ := githubTestsPrefixGuardRunWith(t, artifacts, githubTestsNonReportArtifactPrefixes, cell.configPrefixes)
		if served.excludedPrefix != cell.wantExcluded || served.downloads != cell.wantDownloads || len(served.caseKeys) != cell.wantCases {
			t.Errorf("%s: excluded=%d downloads=%d cases=%d, want %d, %d, %d (the code list and the configuration's list both apply)",
				cell.name, served.excludedPrefix, served.downloads, len(served.caseKeys), cell.wantExcluded, cell.wantDownloads, cell.wantCases)
		}
	}
}

// The prefix exclusion is counted and sampled under the fixed reason token only: no result key, counter name or sample
// reason carries a configured prefix.
func TestGitHubTestsConfiguredPrefixNeverLabelsTheExclusionCounter(t *testing.T) {
	const configured = "ci-zz9-"
	artifacts := []githubTestsPrefixGuardArtifact{
		{name: configured + "1", members: map[string]string{"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}},
		{name: "test-results", members: map[string]string{"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA") + "</testsuites>"}},
	}
	served, walk := githubTestsPrefixGuardRunWith(t, artifacts, githubTestsNonReportArtifactPrefixes, []string{configured})
	if served.excludedPrefix != 1 || walk.final.Result["excluded_non_report_prefix"] != 1 {
		t.Fatalf("excluded cursor=%d result=%v, want 1 and 1", served.excludedPrefix, walk.final.Result["excluded_non_report_prefix"])
	}
	for key := range walk.final.Result {
		if strings.Contains(key, configured) || strings.Contains(key, "zz9") {
			t.Errorf("result key %q carries the configured prefix", key)
		}
	}
	for _, entry := range walk.cursor.ExcludedArtifactSample {
		if !strings.HasSuffix(entry, " ("+githubTestsExclusionReasonPrefix+")") {
			t.Errorf("sample entry %q: the reason must be the fixed token %q", entry, githubTestsExclusionReasonPrefix)
		}
		if reason := entry[strings.LastIndex(entry, " ("):]; strings.Contains(reason, "zz9") {
			t.Errorf("sample entry %q: the reason carries the configured prefix", entry)
		}
	}
}
