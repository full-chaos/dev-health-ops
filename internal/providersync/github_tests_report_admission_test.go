package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsACRArtifact is one artifact of a real dev-health-acr main CI run
// (CHAOS-14000): its id, name, creation time and zip member names. The
// fixture zips keep those member names with small bodies.
type githubTestsACRArtifact struct {
	id      int64
	name    string
	created string
	members []string
}

// githubTestsAdmissionArtifact is one listed artifact the admission doer
// serves.
type githubTestsAdmissionArtifact struct {
	id      int64
	name    string
	created string
	archive []byte
	noRange bool // the blob store ignores Range for this artifact
}

// githubTestsAdmissionDoer serves one run whose artifacts redirect to a blob
// store that answers Range (http.ServeContent), and counts every request.
type githubTestsAdmissionDoer struct {
	t         *testing.T
	runID     string
	artifacts []githubTestsAdmissionArtifact
	api       map[int64]int
	whole     map[int64]int // blob GETs without Range
	ranged    map[int64]int
	requests  int
}

func newGitHubTestsAdmissionDoer(t *testing.T, runID string, artifacts []githubTestsAdmissionArtifact) *githubTestsAdmissionDoer {
	return &githubTestsAdmissionDoer{t: t, runID: runID, artifacts: artifacts, api: map[int64]int{}, whole: map[int64]int{}, ranged: map[int64]int{}}
}

func (doer *githubTestsAdmissionDoer) artifact(id int64) githubTestsAdmissionArtifact {
	for _, artifact := range doer.artifacts {
		if artifact.id == id {
			return artifact
		}
	}
	doer.t.Fatalf("unknown artifact %d", id)
	return githubTestsAdmissionArtifact{}
}

func (doer *githubTestsAdmissionDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests++
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case request.URL.Host == "blob.example":
		id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(path, "/"), ".zip"), 10, 64)
		if err != nil {
			doer.t.Fatalf("blob path %q", path)
		}
		artifact := doer.artifact(id)
		request = request.Clone(request.Context())
		if request.Header.Get("Range") != "" {
			doer.ranged[id]++
			if artifact.noRange {
				request.Header.Del("Range")
			}
		} else {
			doer.whole[id]++
		}
		recorder := httptest.NewRecorder()
		http.ServeContent(recorder, request, "artifact.zip", time.Time{}, bytes.NewReader(artifact.archive))
		response := recorder.Result()
		response.Request = request
		if artifact.noRange {
			response.Header.Del("Accept-Ranges")
		}
		return response, nil
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, `{"workflow_runs":[{"id":`+doer.runID+`,"name":"ci","status":"completed","conclusion":"success","created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z","updated_at":"2026-07-22T10:05:00Z","run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/`+doer.runID+`","pull_requests":[]}]}`), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		var list strings.Builder
		list.WriteString(`{"total_count":` + strconv.Itoa(len(doer.artifacts)) + `,"artifacts":[`)
		for index, artifact := range doer.artifacts {
			if index > 0 {
				list.WriteByte(',')
			}
			fmt.Fprintf(&list, `{"id":%d,"name":%q,"size_in_bytes":%d,"expired":false,"created_at":%q}`, artifact.id, artifact.name, len(artifact.archive), artifact.created)
		}
		list.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, list.String()), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id, err := strconv.ParseInt(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/artifacts/"), "/")[0], 10, 64)
		if err != nil {
			doer.t.Fatalf("artifact path %q", path)
		}
		doer.api[id]++
		return &http.Response{
			StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://blob.example/" + strconv.FormatInt(id, 10) + ".zip"}},
			Body: http.NoBody, Request: request,
		}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func (doer *githubTestsAdmissionDoer) rangeRequests() int {
	total := 0
	for _, count := range doer.ranged {
		total += count
	}
	return total
}

const githubTestsACRCoverage = `<?xml version="1.0" encoding="UTF-8"?>
<coverage line-rate="0.5" branch-rate="0" version="" timestamp="1794933870730" lines-covered="2" lines-valid="4" branches-covered="0" branches-valid="0" complexity="0">
<packages><package name="a" line-rate="0.5" branch-rate="0" complexity="0"><classes>
<class name="-" filename="a/a.go" line-rate="0.5" branch-rate="0" complexity="0"><methods></methods><lines>
<line number="1" hits="1"></line><line number="2" hits="0"></line></lines></class>
</classes></package></packages></coverage>`

func githubTestsACRShardSuite(name string) string {
	return `<testsuite name="` + name + `-pkg" tests="1" failures="0" time="1.0"><testcase name="Test` + strings.ReplaceAll(name, "-", "") + `" classname="` + name + `-pkg" time="1.0"/></testsuite>`
}

// githubTestsACRArchives builds the fixture zips of one real listing: each
// artifact keeps its real member names. A shard's junit.xml holds one suite,
// go-junit holds every shard's suite (the aggregate), go-coverage a
// Cobertura report, and every other member a non-report body.
func githubTestsACRArchives(t *testing.T, listing []githubTestsACRArtifact) []githubTestsAdmissionArtifact {
	t.Helper()
	var shards []string
	for _, artifact := range listing {
		if strings.HasPrefix(artifact.name, "go-unit-shard-") {
			shards = append(shards, artifact.name)
		}
	}
	sort.Strings(shards)
	out := make([]githubTestsAdmissionArtifact, 0, len(listing))
	for _, artifact := range listing {
		members := map[string]string{}
		for _, member := range artifact.members {
			body := `{"note":"not a report"}`
			switch {
			case member == "junit.xml" && artifact.name == "go-junit":
				var all strings.Builder
				all.WriteString("<testsuites>")
				for _, shard := range shards {
					all.WriteString(githubTestsACRShardSuite(shard))
				}
				all.WriteString("</testsuites>")
				body = all.String()
			case member == "junit.xml":
				body = "<testsuites>" + githubTestsACRShardSuite(artifact.name) + "</testsuites>"
			case member == "coverage.xml":
				body = githubTestsACRCoverage
			case strings.HasSuffix(member, ".out") || strings.HasSuffix(member, ".txt"):
				body = "mode: set\n"
			}
			members[member] = body
		}
		out = append(out, githubTestsAdmissionArtifact{id: artifact.id, name: artifact.name, created: artifact.created, archive: githubTestsOrderedZip(t, artifact.members, members)})
	}
	return out
}

func githubTestsOrderedZip(t *testing.T, order []string, bodies map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, name := range order {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(entry, bodies[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func githubTestsHasObservation(walk githubTestsWalk, component, cause string) bool {
	for _, observation := range walk.cursor.Incomplete {
		if observation.Component == component && observation.Cause == cause {
			return true
		}
	}
	return false
}

// The two real dev-health-acr main runs list 32 artifacts after the selection
// seam: 20 of them (race-json-*, container-reports) hold no report member. The
// directory check keeps them out of the 25 cap, so every go-unit-shard is
// read and no run_artifacts/per_run_cap is recorded.
func TestGitHubTestsACRRunsAdmitEveryShardUnderTheCap(t *testing.T) {
	for runID, listing := range githubTestsACRListings {
		t.Run(runID, func(t *testing.T) {
			artifacts := githubTestsACRArchives(t, listing)
			doer := newGitHubTestsAdmissionDoer(t, runID, artifacts)
			walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, nativeTestClaim("github", "cicd"), githubTestsClient(t, fakehttp.Client(doer)), 8)
			if githubTestsHasObservation(walk, githubTestsRunArtifactsComponent, githubTestsPerRunCapCause) {
				t.Fatalf("run_artifacts/per_run_cap recorded: %+v", walk.cursor.Incomplete)
			}
			shards, reportArtifacts, noReport := 0, 0, 0
			for _, artifact := range listing {
				reports := false
				for _, member := range artifact.members {
					reports = reports || strings.HasSuffix(member, ".xml")
				}
				switch {
				case reports:
					reportArtifacts++
					if doer.whole[artifact.id] != 1 {
						t.Fatalf("%s (%d) downloads=%d, want 1: a report artifact must be read", artifact.name, artifact.id, doer.whole[artifact.id])
					}
					if strings.HasPrefix(artifact.name, "go-unit-shard-") {
						shards++
					}
				default:
					noReport++
					if doer.whole[artifact.id] != 0 {
						t.Fatalf("%s (%d) downloads=%d, want 0: an artifact without a report member takes no slot", artifact.name, artifact.id, doer.whole[artifact.id])
					}
				}
			}
			if shards != 10 || reportArtifacts != 12 || noReport != 20 {
				t.Fatalf("shards=%d report_artifacts=%d no_report=%d, want 10/12/20", shards, reportArtifacts, noReport)
			}
			sample := 0
			for _, record := range walk.cursor.ExcludedArtifactSample {
				if strings.HasSuffix(record, "("+githubTestsExclusionReasonNoReportMember+")") {
					sample++
				}
			}
			if sample != githubTestsMaxExcludedArtifactSampleRecords {
				t.Fatalf("excluded sample %v, want %d no_report_member records", walk.cursor.ExcludedArtifactSample, githubTestsMaxExcludedArtifactSampleRecords)
			}
			apiChecks := 0
			for _, artifact := range listing {
				apiChecks += doer.api[artifact.id]
			}
			t.Logf("COST run=%s listed=%d directory_checks=%d range_requests=%d api_requests=%d (downloads %d) total_requests=%d observations=%+v",
				runID, len(listing), len(listing), doer.rangeRequests(), apiChecks, reportArtifacts, doer.requests, walk.cursor.Incomplete)
		})
	}
}

// The oracle Collect takes the same admission: the real runs no longer fail
// the per-run artifact cap.
func TestGitHubTestsACRRunsCollectUnderTheCap(t *testing.T) {
	for runID, listing := range githubTestsACRListings {
		t.Run(runID, func(t *testing.T) {
			doer := newGitHubTestsAdmissionDoer(t, runID, githubTestsACRArchives(t, listing))
			_, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "cicd"), providerfoundation.Credential{},
				githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
			if errors.Is(err, ErrPaginationCapExceeded) {
				t.Fatalf("err=%v, want the report artifacts admitted under the cap", err)
			}
		})
	}
}

func githubTestsAdmissionFixture(t *testing.T, names ...string) []githubTestsAdmissionArtifact {
	t.Helper()
	out := make([]githubTestsAdmissionArtifact, 0, len(names))
	for index, name := range names {
		body := map[string]string{"race.json": "{}"}
		order := []string{"race.json"}
		if !strings.HasPrefix(name, "race-json-") {
			body = map[string]string{"junit.xml": "<testsuites>" + githubTestsACRShardSuite(name) + "</testsuites>"}
			order = []string{"junit.xml"}
		}
		out = append(out, githubTestsAdmissionArtifact{
			id: int64(100 + index), name: name, created: fmt.Sprintf("2026-07-22T10:%02d:00Z", index),
			archive: githubTestsOrderedZip(t, order, body),
		})
	}
	return out
}

func githubTestsAdmissionWalk(t *testing.T, doer *githubTestsAdmissionDoer, maxArtifacts int) githubTestsWalk {
	t.Helper()
	return walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxArtifactsPerRun: maxArtifacts}, nativeTestClaim("github", "cicd"), githubTestsClient(t, fakehttp.Client(doer)), 8)
}

// An artifact with no report member takes no cap slot: both report artifacts
// are read, nothing is cut.
func TestGitHubTestsNoReportArtifactTakesNoCapSlot(t *testing.T) {
	doer := newGitHubTestsAdmissionDoer(t, "77", githubTestsAdmissionFixture(t, "race-json-a", "shard-a", "shard-b"))
	walk := githubTestsAdmissionWalk(t, doer, 2)
	if githubTestsHasObservation(walk, githubTestsRunArtifactsComponent, githubTestsPerRunCapCause) {
		t.Fatalf("per_run_cap recorded: %+v", walk.cursor.Incomplete)
	}
	if doer.whole[100] != 0 || doer.whole[101] != 1 || doer.whole[102] != 1 {
		t.Fatalf("downloads race=%d a=%d b=%d, want 0/1/1", doer.whole[100], doer.whole[101], doer.whole[102])
	}
	if len(walk.cursor.ExcludedArtifactSample) != 1 || walk.cursor.ExcludedArtifactSample[0] != "race-json-a (no_report_member)" {
		t.Fatalf("excluded sample %v, want the race artifact", walk.cursor.ExcludedArtifactSample)
	}
}

// A run at or under the cap reads no directory: no range request, one API and
// one blob request per artifact, as before.
func TestGitHubTestsRunUnderTheCapReadsNoDirectory(t *testing.T) {
	doer := newGitHubTestsAdmissionDoer(t, "77", githubTestsAdmissionFixture(t, "race-json-a", "shard-a", "shard-b"))
	githubTestsAdmissionWalk(t, doer, 3)
	if doer.rangeRequests() != 0 {
		t.Fatalf("range requests=%d, want 0", doer.rangeRequests())
	}
	for id := int64(100); id <= 102; id++ {
		if doer.api[id] != 1 || doer.whole[id] != 1 {
			t.Fatalf("artifact %d api=%d downloads=%d, want 1/1", id, doer.api[id], doer.whole[id])
		}
	}
}

// A directory that cannot be listed by range keeps today's behavior: the
// artifact takes a slot, and the report artifact past the cap is recorded as
// per_run_cap.
func TestGitHubTestsUnlistableDirectoryKeepsItsCapSlot(t *testing.T) {
	artifacts := githubTestsAdmissionFixture(t, "race-json-a", "shard-a", "shard-b")
	artifacts[0].noRange = true
	doer := newGitHubTestsAdmissionDoer(t, "77", artifacts)
	walk := githubTestsAdmissionWalk(t, doer, 2)
	if !githubTestsHasObservation(walk, githubTestsRunArtifactsComponent, githubTestsPerRunCapCause) {
		t.Fatalf("no per_run_cap: %+v", walk.cursor.Incomplete)
	}
	read := 0
	for id := int64(100); id <= 102; id++ {
		read += doer.whole[id]
	}
	if read != 2 {
		t.Fatalf("downloads=%d, want 2 (the cap)", read)
	}
}

// Report artifacts past the cap are still cut and recorded; the check stops
// at the first report artifact past the cap.
func TestGitHubTestsReportArtifactsPastTheCapAreCut(t *testing.T) {
	doer := newGitHubTestsAdmissionDoer(t, "77", githubTestsAdmissionFixture(t, "shard-a", "shard-b", "shard-c", "shard-d"))
	walk := githubTestsAdmissionWalk(t, doer, 2)
	if !githubTestsHasObservation(walk, githubTestsRunArtifactsComponent, githubTestsPerRunCapCause) {
		t.Fatalf("no per_run_cap: %+v", walk.cursor.Incomplete)
	}
	if doer.api[103] != 0 {
		t.Fatalf("api requests for the fourth artifact=%d, want 0: the check stops at the first report artifact past the cap", doer.api[103])
	}
}

var githubTestsACRListings = map[string][]githubTestsACRArtifact{
	"44125160244": {
		{14348897364, "go-coverage", "2026-11-17T16:44:34Z", []string{"coverage.xml"}},
		{14348790185, "race-json-race-shard-1", "2026-11-17T16:30:35Z", []string{"race.json"}},
		{14348690797, "race-json-race-shard-3", "2026-11-17T16:32:47Z", []string{"race.json"}},
		{14348515909, "race-json-race-devhealthsource", "2026-11-17T16:33:27Z", []string{"race.json"}},
		{14348445612, "race-json-race-contracts-v1", "2026-11-17T16:30:01Z", []string{"race.json"}},
		{14348430967, "race-json-race-devhealthsource-heavy", "2026-11-17T16:33:06Z", []string{"race.json"}},
		{14348395345, "race-json-race-mcp", "2026-11-17T16:29:22Z", []string{"race.json"}},
		{14348371053, "go-unit-shard-9", "2026-11-17T16:32:32Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14348275749, "race-json-race-shard-2", "2026-11-17T16:30:44Z", []string{"race.json"}},
		{14348192450, "go-unit-shard-10", "2026-11-17T16:41:21Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14348176047, "race-json-race-devhealthsource-mid", "2026-11-17T16:32:22Z", []string{"race.json"}},
		{14348155984, "go-unit-shard-2", "2026-11-17T16:31:50Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14348111332, "go-unit-shard-8", "2026-11-17T16:33:18Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14348098077, "go-unit-shard-7", "2026-11-17T16:43:52Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14347967249, "race-json-race-devhealthfacts-fourth", "2026-11-17T16:37:24Z", []string{"race.json"}},
		{14347953018, "race-json-race-devhealthfacts", "2026-11-17T16:39:59Z", []string{"race.json"}},
		{14347898325, "go-unit-shard-5", "2026-11-17T16:40:51Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14347757142, "go-unit-shard-4", "2026-11-17T16:33:10Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14347622149, "race-json-race-api-heavy", "2026-11-17T16:29:32Z", []string{"race.json"}},
		{14347603405, "race-json-race-devhealthfacts-heavy", "2026-11-17T16:38:49Z", []string{"race.json"}},
		{14347504678, "go-junit", "2026-11-17T16:44:35Z", []string{"junit.xml"}},
		{14347242679, "race-json-race-factoracle", "2026-11-17T16:28:58Z", []string{"race.json"}},
		{14347109753, "race-json-race-devhealthfacts-mid", "2026-11-17T16:38:26Z", []string{"race.json"}},
		{14347054298, "race-json-race-devhealthschema", "2026-11-17T16:35:54Z", []string{"race.json"}},
		{14347048747, "race-json-race-devhealthsource-fourth", "2026-11-17T16:32:42Z", []string{"race.json"}},
		{14346983982, "go-unit-shard-6", "2026-11-17T16:31:17Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14346983826, "go-unit-shard-3", "2026-11-17T16:30:22Z", []string{"junit.xml", "cover.out", "go-test.json"}},
		{14346649426, "go-unit-shard-1", "2026-11-17T16:27:21Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14346549368, "race-json-race-shard-4", "2026-11-17T16:29:20Z", []string{"race.json"}},
		{14346539362, "race-json-race-factoracle-heavy", "2026-11-17T16:29:02Z", []string{"race.json"}},
		{14346529434, "race-json-race-api", "2026-11-17T16:29:26Z", []string{"race.json"}},
		{14346404397, "container-reports", "2026-11-17T16:28:29Z", []string{"container-reports/acr-api-amd64-trivy.json", "container-reports/acr-api-arm64-trivy.json", "container-reports/acr-api-amd64.spdx.json", "container-reports/acr-api-arm64.spdx.json", "container-reports/acr-mcp-amd64-trivy.json", "container-reports/acr-mcp-arm64-trivy.json", "container-reports/acr-mcp-amd64.spdx.json", "container-reports/acr-mcp-arm64.spdx.json", "container-reports/trivy-db-metadata.json", "container-reports/trivy-db-snapshot.txt"}},
	},
	"44172787599": {
		{14370597119, "go-coverage", "2026-11-17T21:10:28Z", []string{"coverage.xml"}},
		{14370410051, "race-json-race-devhealthsource", "2026-11-17T20:56:13Z", []string{"race.json"}},
		{14370256065, "race-json-race-devhealthfacts-mid", "2026-11-17T21:01:23Z", []string{"race.json"}},
		{14370020207, "go-unit-shard-6", "2026-11-17T20:53:17Z", []string{"cover.out", "go-test.json", "junit.xml"}},
		{14369940083, "race-json-race-api", "2026-11-17T20:52:21Z", []string{"race.json"}},
		{14369800505, "race-json-race-devhealthschema", "2026-11-17T20:54:03Z", []string{"race.json"}},
		{14369610949, "go-unit-shard-4", "2026-11-17T20:54:39Z", []string{"junit.xml", "cover.out", "go-test.json"}},
		{14369540598, "race-json-race-api-heavy", "2026-11-17T20:51:27Z", []string{"race.json"}},
		{14369516086, "go-unit-shard-9", "2026-11-17T20:55:41Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14369452400, "race-json-race-devhealthfacts-fourth", "2026-11-17T21:03:20Z", []string{"race.json"}},
		{14369358394, "go-unit-shard-7", "2026-11-17T21:09:38Z", []string{"cover.out", "go-test.json", "junit.xml"}},
		{14369341446, "race-json-race-devhealthsource-heavy", "2026-11-17T20:56:23Z", []string{"race.json"}},
		{14369296228, "race-json-race-factoracle", "2026-11-17T20:54:18Z", []string{"race.json"}},
		{14369202473, "race-json-race-devhealthfacts-heavy", "2026-11-17T21:02:11Z", []string{"race.json"}},
		{14369044396, "go-junit", "2026-11-17T21:10:30Z", []string{"junit.xml"}},
		{14369001393, "race-json-race-contracts-v1", "2026-11-17T20:53:05Z", []string{"race.json"}},
		{14369001285, "race-json-race-shard-2", "2026-11-17T20:52:27Z", []string{"race.json"}},
		{14368932997, "go-unit-shard-10", "2026-11-17T21:02:42Z", []string{"junit.xml", "go-test.json", "cover.out"}},
		{14368848464, "race-json-race-devhealthfacts", "2026-11-17T21:04:54Z", []string{"race.json"}},
		{14368807108, "go-unit-shard-3", "2026-11-17T20:54:08Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14368757088, "go-unit-shard-1", "2026-11-17T20:50:58Z", []string{"junit.xml", "go-test.json", "cover.out"}},
		{14368722638, "race-json-race-devhealthsource-mid", "2026-11-17T20:55:38Z", []string{"race.json"}},
		{14368707602, "race-json-race-devhealthsource-fourth", "2026-11-17T20:54:41Z", []string{"race.json"}},
		{14368707273, "race-json-race-shard-1", "2026-11-17T20:52:20Z", []string{"race.json"}},
		{14368647495, "race-json-race-mcp", "2026-11-17T20:52:11Z", []string{"race.json"}},
		{14368534378, "go-unit-shard-5", "2026-11-17T21:02:53Z", []string{"junit.xml", "cover.out", "go-test.json"}},
		{14368354178, "race-json-race-shard-3", "2026-11-17T20:55:35Z", []string{"race.json"}},
		{14368349108, "go-unit-shard-8", "2026-11-17T20:55:57Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14368313908, "go-unit-shard-2", "2026-11-17T20:53:59Z", []string{"cover.out", "junit.xml", "go-test.json"}},
		{14368268753, "race-json-race-factoracle-heavy", "2026-11-17T20:54:58Z", []string{"race.json"}},
		{14368169296, "race-json-race-shard-4", "2026-11-17T20:53:35Z", []string{"race.json"}},
		{14367824693, "container-reports", "2026-11-17T20:51:54Z", []string{"container-reports/acr-api-amd64-trivy.json", "container-reports/acr-api-arm64-trivy.json", "container-reports/acr-api-amd64.spdx.json", "container-reports/acr-api-arm64.spdx.json", "container-reports/acr-mcp-amd64-trivy.json", "container-reports/acr-mcp-amd64.spdx.json", "container-reports/acr-mcp-arm64-trivy.json", "container-reports/acr-mcp-arm64.spdx.json", "container-reports/trivy-db-metadata.json", "container-reports/trivy-db-snapshot.txt"}},
	},
}
