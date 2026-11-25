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

// githubTestsPrefixGuardRunWith runs the shape with the sync configuration's own list (the integration config the unit
// carries) set to configPrefixes; nil leaves the key out.
func githubTestsPrefixGuardRunWith(
	t *testing.T, artifacts []githubTestsPrefixGuardArtifact, configPrefixes []string,
) (githubTestsPrefixGuardServed, githubTestsWalk) {
	t.Helper()
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

// githubTestsOwnCIPrefixes are this repository's own CI artifact names (go.yml), the one list the configured integration carries.
var githubTestsOwnCIPrefixes = []string{"go-images-", "go-test-timings-", "go-junit-part-"}

// The 5 shapes of CHAOS-14559. With the integration's list alone (the built-in list is gone) each shape serves what the
// built-in list served: the answer of the former built-in list at 6789819776167b684f1e585a3d8279ecc7666605 (excluded by name, 4 clean case rows);
// an integration with no list, or an invalid one, excludes no prefix.
func TestGitHubTestsConfigListAloneServesTheFiveShapes(t *testing.T) {
	suiteA := githubTestsPrefixGuardSuite("pkg/a", "TestA1", "TestA2")
	suiteB := githubTestsPrefixGuardSuite("pkg/b", "TestB1", "TestB2")
	merged := githubTestsPrefixGuardArtifact{name: "go-test-junit-integration", members: map[string]string{"junit.xml": "<testsuites>" + suiteA + suiteB + "</testsuites>"}}
	for _, shape := range []struct {
		name          string
		artifacts     []githubTestsPrefixGuardArtifact
		wantExcluded  int
		wantDownloads int
		loadedRows    bool
	}{
		{name: "image tarball", wantExcluded: 1, wantDownloads: 1, artifacts: []githubTestsPrefixGuardArtifact{{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}}, merged}},
		{name: "expired image tarball", loadedRows: true, wantExcluded: 1, wantDownloads: 1, artifacts: []githubTestsPrefixGuardArtifact{{name: "go-images-amd64-9-1", expired: true}, merged}},
		{name: "timings table", wantExcluded: 1, wantDownloads: 1, artifacts: []githubTestsPrefixGuardArtifact{{name: "go-test-timings-integration-packages-1", members: map[string]string{"timings.tsv.gz": "pkg\t1.0"}}, merged}},
		{name: "JUnit parts beside their merged report", loadedRows: true, wantExcluded: 2, wantDownloads: 1, artifacts: []githubTestsPrefixGuardArtifact{
			{name: "go-junit-part-packages-1", members: map[string]string{"a.xml": "<testsuites>" + suiteA + "</testsuites>"}},
			{name: "go-junit-part-packages-2", members: map[string]string{"b.xml": "<testsuites>" + suiteB + "</testsuites>"}},
			merged,
		}},
		{name: "expired JUnit parts beside their merged report", loadedRows: true, wantExcluded: 2, wantDownloads: 1, artifacts: []githubTestsPrefixGuardArtifact{
			{name: "go-junit-part-packages-1", expired: true}, {name: "go-junit-part-packages-2", expired: true}, merged,
		}},
	} {
		byConfig, _ := githubTestsPrefixGuardRunWith(t, shape.artifacts, githubTestsOwnCIPrefixes)
		if byConfig.excludedPrefix != shape.wantExcluded || byConfig.downloads != shape.wantDownloads || !byConfig.clean() || len(byConfig.caseKeys) != 4 {
			t.Errorf("%s: the integration's list alone must exclude %d by name, download %d and serve 4 clean case rows, got %+v",
				shape.name, shape.wantExcluded, shape.wantDownloads, byConfig)
		}
		for _, noList := range [][]string{nil, {""}} {
			bare, _ := githubTestsPrefixGuardRunWith(t, shape.artifacts, noList)
			if bare.excludedPrefix != 0 {
				t.Errorf("%s: an integration with list %q excluded %d by prefix, want 0", shape.name, noList, bare.excludedPrefix)
			}
			if shape.loadedRows && bare.clean() && bare.sameRows(byConfig) {
				t.Errorf("%s: without a list the run should show a false incompleteness or moved case keys, got the same rows %+v", shape.name, bare)
			}
			if !shape.loadedRows && (!bare.sameRows(byConfig) || bare.downloads != byConfig.downloads+shape.wantExcluded) {
				t.Errorf("%s: without a list the content rule should serve the same rows at %d more download, list=%+v bare=%+v", shape.name, shape.wantExcluded, byConfig, bare)
			}
		}
	}
}

// Each list excludes its own names: no list excludes none, a configured list excludes its names and matches without case.
func TestGitHubTestsArtifactExcludeUsesOnlyTheConfigList(t *testing.T) {
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
		{name: "no list excludes no prefix", wantExcluded: 0, wantDownloads: 3, wantCases: 3},
		{name: "a configured prefix", configPrefixes: []string{"ci-extra-"}, wantExcluded: 1, wantDownloads: 2, wantCases: 2},
		{name: "a configured prefix matches without case", configPrefixes: []string{"CI-EXTRA-"}, wantExcluded: 1, wantDownloads: 2, wantCases: 2},
		{name: "two configured prefixes", configPrefixes: []string{"ci-extra-", "go-images-"}, wantExcluded: 2, wantDownloads: 1, wantCases: 2},
		{name: "an invalid configured value excludes nothing", configPrefixes: []string{""}, wantExcluded: 0, wantDownloads: 3, wantCases: 3},
	} {
		served, _ := githubTestsPrefixGuardRunWith(t, artifacts, cell.configPrefixes)
		if served.excludedPrefix != cell.wantExcluded || served.downloads != cell.wantDownloads || len(served.caseKeys) != cell.wantCases {
			t.Errorf("%s: excluded=%d downloads=%d cases=%d, want %d, %d, %d",
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
	served, walk := githubTestsPrefixGuardRunWith(t, artifacts, []string{configured})
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
