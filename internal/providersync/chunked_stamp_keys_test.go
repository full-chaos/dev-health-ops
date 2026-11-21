package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A repeated natural key inside one write batch is refused by the TestOps sink. Canonical order puts the two rows of a repeated key next
// to each other, so main (100-row chunks) stores such an emission only when the pair falls across a 100-row boundary. An emission with a
// repeated key is split exactly as main splits it, so it is stored where main stores it and refused where main refuses it, with main's
// error (CHAOS-14411).

const keyRepoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"

var keyLastSynced = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

// keyRow is one valid sink row of the destination whose natural key varies with key.
func keyRow(t *testing.T, claim Claim, destination string, key int) json.RawMessage {
	t.Helper()
	provider := map[string]string{"github": "github_actions", "gitlab": "gitlab_ci"}[claim.Provider]
	id := fmt.Sprintf("%05d", key)
	var value any
	switch destination {
	case "ci_pipeline_runs":
		value = githubTestsPipelineRow{OrgID: claim.OrgID, RepoID: keyRepoID, RunID: id, Provider: provider, StartedAt: keyLastSynced, LastSynced: keyLastSynced}
	case "ci_job_runs":
		value = githubTestsJobRow{OrgID: claim.OrgID, RepoID: keyRepoID, RunID: "1", JobID: id, LastSynced: keyLastSynced}
	case "ci_acceptance_checks":
		value = githubTestsAcceptanceRow{OrgID: claim.OrgID, RepoID: keyRepoID, RunID: "1", CheckKey: id, Provider: provider, ObservedAt: keyLastSynced, LastSynced: keyLastSynced}
	case "test_suite_results":
		value = testSuiteResultRow{OrgID: claim.OrgID, RepoID: keyRepoID, RunID: "1", SuiteID: id, LastSynced: keyLastSynced}
	case "test_case_results":
		value = testCaseResultRow{OrgID: claim.OrgID, RepoID: keyRepoID, RunID: "1", SuiteID: "s", CaseID: id, Status: "passed", LastSynced: keyLastSynced}
	case "coverage_snapshots":
		value = coverageSnapshotRow{OrgID: claim.OrgID, RepoID: keyRepoID, RunID: "1", SnapshotID: id, LastSynced: keyLastSynced}
	case testOpsRunReportCompletenessDestination:
		value = testOpsRunReportCompletenessRow{OrgID: claim.OrgID, RepoID: keyRepoID, RunID: "1", RunAttempt: int64(key), Provider: provider,
			Complete: true, Reasons: []string{}, LastSynced: keyLastSynced, VerdictVersion: testOpsRunReportVerdictVersion(true, keyLastSynced)}
	default:
		t.Fatalf("no key row for %s", destination)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// keyEmission is a non-final emission of the 7 destinations: counts[d] distinct keys, plus one more row repeating the key at each index
// in repeats[d] (so the repeated pair sits at sorted positions (i, i+1) for one repeat).
func keyEmission(t *testing.T, claim Claim, counts map[string]int, repeats map[string][]int) ChunkRouteEmission {
	t.Helper()
	effects := make([]EffectBatch, 0, len(stampDestinations))
	for _, destination := range stampDestinations {
		rows := make([]json.RawMessage, 0, counts[destination]+len(repeats[destination]))
		for index := 0; index < counts[destination]; index++ {
			rows = append(rows, keyRow(t, claim, destination, 10_000+index))
		}
		for _, index := range repeats[destination] {
			rows = append(rows, keyRow(t, claim, destination, 10_000+index))
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, rows)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	return ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: effects}, CursorAfter: `{"e":1}`}
}

// keyConn accepts every batch and reads no stored pipeline finish day (the pipeline write reads them before its batch).
type keyConn struct{ githubTestsDuplicateKeyConn }

func (*keyConn) Query(context.Context, string, ...any) (driver.Rows, error) { return keyNoRows{}, nil }

type keyNoRows struct{ driver.Rows }

func (keyNoRows) Next() bool   { return false }
func (keyNoRows) Close() error { return nil }
func (keyNoRows) Err() error   { return nil }

// keyOutcome writes every destination slice of every chunk through the TestOps sink check: "" when all are stored, else the first error.
func keyOutcome(t *testing.T, claim Claim, chunks []PreparedProviderChunk) string {
	t.Helper()
	sink := TestOpsClickHouseEffects{Conn: &keyConn{}, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	for _, chunk := range chunks {
		for _, effect := range chunk.Effects {
			if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
				return err.Error()
			}
		}
	}
	return ""
}

// keySplits is MAIN's chunks (frozen split, default policy) and the head's chunks (stamped split, the route's policy) of one emission.
func keySplits(t *testing.T, claim Claim, emission ChunkRouteEmission) (mainChunks, headChunks []PreparedProviderChunk) {
	t.Helper()
	mainChunks, err := oracleMainSplit(emission.Batch, ShadowComparison{}, DefaultChunkPolicy())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, _ := Descriptor(claim.Provider, claim.Dataset)
	headChunks, err = splitStampedEmission(emission.Batch, chunkStamps{
		Claim: claim, CursorBefore: emission.CursorBefore, CursorAfter: emission.CursorAfter, StampAt: keyLastSynced,
	}, descriptor.ChunkPolicy)
	if err != nil {
		t.Fatal(err)
	}
	return mainChunks, headChunks
}

// The position sweep: one repeated ci_job_runs key at sorted (i, i+1) of n rows, at every position. The head stores exactly where MAIN
// stores (both ways), with MAIN's error where MAIN refuses and MAIN's chunk count where MAIN stores.
func TestChunkStampRepeatedKeySweepEqualsMain(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	for _, n := range []int{200, 1200} {
		const groups = 8
		var mu sync.Mutex
		mainStored, headStored := 0, 0
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			for group := range groups {
				t.Run(fmt.Sprintf("group=%d", group), func(t *testing.T) {
					t.Parallel()
					for i := group; i+1 < n; i += groups {
						emission := keyEmission(t, claim, map[string]int{"ci_job_runs": n - 1}, map[string][]int{"ci_job_runs": {i}})
						mainChunks, headChunks := keySplits(t, claim, emission)
						mainOut, headOut := keyOutcome(t, claim, mainChunks), keyOutcome(t, claim, headChunks)
						if mainOut != headOut || (mainOut == "" && len(headChunks) != len(mainChunks)) {
							t.Fatalf("n=%d pair at (%d,%d): head %d chunks %q, MAIN %d chunks %q", n, i, i+1, len(headChunks), headOut, len(mainChunks), mainOut)
						}
						mu.Lock()
						if mainOut == "" {
							mainStored++
						}
						if headOut == "" {
							headStored++
						}
						mu.Unlock()
					}
				})
			}
		})
		t.Logf("n=%d: stored at %d positions on MAIN and %d on the head, of %d", n, mainStored, headStored, n-1)
	}
}

// Every destination: one repeated key across a 100-row boundary (MAIN stores it) and one inside a 100-row slice (MAIN refuses it); the
// other destinations hold distinct keys and 600 case rows. The whole emission is split as MAIN splits it: same chunk count, same outcome.
func TestChunkStampRepeatedKeyEveryDestinationFallsBackToMain(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	for _, destination := range stampDestinations {
		for _, at := range []int{99, 49} {
			t.Run(fmt.Sprintf("%s/pair at %d", destination, at), func(t *testing.T) {
				t.Parallel()
				counts := map[string]int{"test_case_results": 600, "ci_job_runs": 30, "coverage_snapshots": 5}
				counts[destination] = max(counts[destination], 200)
				emission := keyEmission(t, claim, counts, map[string][]int{destination: {at}})
				mainChunks, headChunks := keySplits(t, claim, emission)
				mainOut, headOut := keyOutcome(t, claim, mainChunks), keyOutcome(t, claim, headChunks)
				if headOut != mainOut || len(headChunks) != len(mainChunks) {
					t.Fatalf("head %d chunks %q, MAIN %d chunks %q", len(headChunks), headOut, len(mainChunks), mainOut)
				}
				if (at == 99) != (mainOut == "") {
					t.Fatalf("MAIN outcome %q at %d: the shape is wrong", mainOut, at)
				}
			})
		}
	}
}

// Shapes: three repeats of one key; repeats in two destinations; a clean emission keeps the TestOps cap.
func TestChunkStampRepeatedKeyShapes(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	for _, shape := range []struct {
		name    string
		counts  map[string]int
		repeats map[string][]int
	}{
		{"three repeats of one key across boundaries", map[string]int{"ci_job_runs": 600}, map[string][]int{"ci_job_runs": {199, 199, 399}}},
		{"three repeats inside a slice", map[string]int{"ci_job_runs": 600}, map[string][]int{"ci_job_runs": {150, 150, 150}}},
		{"repeats in two destinations", map[string]int{"ci_job_runs": 600, "coverage_snapshots": 300}, map[string][]int{"ci_job_runs": {299}, "coverage_snapshots": {199}}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			emission := keyEmission(t, claim, shape.counts, shape.repeats)
			mainChunks, headChunks := keySplits(t, claim, emission)
			mainOut, headOut := keyOutcome(t, claim, mainChunks), keyOutcome(t, claim, headChunks)
			if headOut != mainOut || len(headChunks) != len(mainChunks) {
				t.Fatalf("head %d chunks %q, MAIN %d chunks %q", len(headChunks), headOut, len(mainChunks), mainOut)
			}
		})
	}
	t.Run("no repeat keeps the TestOps cap", func(t *testing.T) {
		t.Parallel()
		emission := keyEmission(t, claim, map[string]int{"ci_job_runs": 1200}, nil)
		mainChunks, headChunks := keySplits(t, claim, emission)
		if keyOutcome(t, claim, headChunks) != "" || len(headChunks) != 3 || len(mainChunks) != 12 {
			t.Fatalf("head %d chunks, MAIN %d: want 3 at 500 rows and MAIN's 12", len(headChunks), len(mainChunks))
		}
	})
}

// ---------------------------------------------------------------------------------------------------------------- the routes

// keyOverlapPages serves a job listing of two pages of 100 jobs; with overlap the first job of page 2 repeats the last of page 1 (a
// listing that shifted between the two requests).
type keyOverlapPages struct {
	provider string
	overlap  bool
	base     providerfoundation.HTTPDoer
}

func (d keyOverlapPages) Do(request *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(request.URL.Path, "/jobs") {
		return d.base.Do(request)
	}
	page, _ := strconv.Atoi(request.URL.Query().Get("page"))
	page = max(1, page)
	header := http.Header{"Content-Type": {"application/json"}}
	next := *request.URL
	query := next.Query()
	query.Set("page", "2")
	next.RawQuery = query.Encode()
	if page == 1 {
		header.Set("Link", "<"+next.String()+">; rel=\"next\"")
		header.Set("X-Next-Page", "2")
	}
	jobs := []string{}
	if page <= 2 {
		first := 1199
		if page == 2 {
			first = 1099
			if d.overlap {
				first++
			}
		}
		for i := range 100 {
			id := first - i
			jobs = append(jobs, fmt.Sprintf(`{"id":%d,"name":"job-%d","stage":"test","status":"completed","conclusion":"success","started_at":"2026-07-22T13:01:00Z","completed_at":"2026-07-22T13:04:00Z","finished_at":"2026-07-22T13:04:00Z"}`, id, id))
		}
	}
	body := "[" + strings.Join(jobs, ",") + "]"
	if d.provider == "github" {
		body = fmt.Sprintf(`{"total_count":200,"jobs":%s}`, body)
	}
	return githubTestsHTTPResponse(request, header, body), nil
}

type keyCoverageArchive struct {
	base    providerfoundation.HTTPDoer
	archive []byte
}

func (d keyCoverageArchive) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	if (strings.Contains(path, "/actions/artifacts/") && strings.HasSuffix(path, "/zip")) || (strings.Contains(path, "/jobs/") && strings.HasSuffix(path, "/artifacts")) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/zip"}}, Body: io.NopCloser(bytes.NewReader(d.archive)), Request: request}, nil
	}
	return d.base.Do(request)
}

// keyCoverageArchiveBytes is 100 lcov files whose snapshot keys sort in path order, plus a repeat of the last one.
func keyCoverageArchiveBytes(t *testing.T, runID, scope string) []byte {
	t.Helper()
	paths := make([]string, 100)
	for n := range paths {
		paths[n] = fmt.Sprintf("coverage-%03d.info", n)
	}
	sort.Slice(paths, func(i, j int) bool {
		return githubTestsSnapshotID(runID, scope, "lcov", paths[i]) < githubTestsSnapshotID(runID, scope, "lcov", paths[j])
	})
	paths = append(paths, paths[len(paths)-1])
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, path := range paths {
		part, err := writer.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.WriteString(part, "TN:\nSF:src/module.go\nDA:1,1\nLF:1\nLH:1\nend_of_record\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// keyRouteEmission runs the real route and returns its emission that holds rows of destination.
func keyRouteEmission(t *testing.T, claim Claim, handler ChunkedCompleteRouteHandler, client *providerfoundation.HTTPClient, destination string) ChunkRouteEmission {
	t.Helper()
	var target ChunkRouteEmission
	err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, keyLastSynced, "", func(emission ChunkRouteEmission) error {
		for _, effect := range emission.Batch.Effects {
			if effect.Destination == destination && len(effect.Rows) > 0 {
				target = emission
			}
		}
		return nil
	})
	if err != nil || len(target.Batch.Effects) == 0 {
		t.Fatalf("route: %v, effects=%d", err, len(target.Batch.Effects))
	}
	return target
}

// The real routes: a job listing that repeats its boundary job and a coverage archive that repeats a snapshot, on the 4 TestOps routes,
// plus the job controls without a repeat. The head stores exactly what MAIN stores.
func TestChunkStampRepeatedKeyThroughTheRoutes(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"github", "gitlab"} {
		for _, dataset := range []string{"cicd", "tests"} {
			for _, overlap := range []bool{true, false} {
				t.Run(fmt.Sprintf("jobs/%s/%s/overlap=%t", provider, dataset, overlap), func(t *testing.T) {
					t.Parallel()
					claim := nativeTestClaim(provider, dataset)
					var handler ChunkedCompleteRouteHandler
					var client *providerfoundation.HTTPClient
					if provider == "github" {
						handler = GitHubTestsRouteHandler{}
						client = githubTestsClient(t, fakehttp.Client(keyOverlapPages{provider, overlap, &githubTestsOversizedRunDoer{t: t}}))
					} else {
						handler = GitLabTestsRouteHandler{}
						pipeline := gitLabEdgePipeline{ID: "9101", Ref: "main", Status: "success", JobStatus: "success",
							Created: "2026-07-22T13:00:00Z", Started: "2026-07-22T13:00:01Z", Finished: "2026-07-22T13:04:00Z", Updated: "2026-07-22T13:04:00Z"}
						client = gitLabRepositoryClient(t, keyOverlapPages{provider, overlap, &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{pipeline}}}, "https://gitlab.example")
					}
					emission := keyRouteEmission(t, claim, handler, client, "ci_job_runs")
					mainChunks, headChunks := keySplits(t, claim, emission)
					mainOut, headOut := keyOutcome(t, claim, mainChunks), keyOutcome(t, claim, headChunks)
					if mainOut != "" {
						t.Fatalf("MAIN refuses (%s): the shape is wrong", mainOut)
					}
					if headOut != "" || (overlap && len(headChunks) != len(mainChunks)) {
						t.Fatalf("head %d chunks %q, MAIN %d chunks stored", len(headChunks), headOut, len(mainChunks))
					}
				})
			}
			t.Run(fmt.Sprintf("coverage/%s/%s", provider, dataset), func(t *testing.T) {
				t.Parallel()
				claim := nativeTestClaim(provider, dataset)
				var handler ChunkedCompleteRouteHandler
				var client *providerfoundation.HTTPClient
				if provider == "github" {
					handler = GitHubTestsRouteHandler{}
					slot := githubTestsReportSlot{Name: "test-results-1", Attempt: 1}
					client = githubTestsClient(t, fakehttp.Client(keyCoverageArchive{&githubTestsOversizedRunDoer{t: t, artifacts: 1}, keyCoverageArchiveBytes(t, "1", slot.scope())}))
				} else {
					handler = GitLabTestsRouteHandler{}
					client = gitLabRepositoryClient(t, &gitLabAllRefDoer{t: t, archive: keyCoverageArchiveBytes(t, "9101", "1100"), pipelines: []gitLabAllRefPipeline{{
						ID: "9101", Ref: "main", Created: "2026-07-22T13:00:00Z", Started: "2026-07-22T13:00:01Z", Finished: "2026-07-22T13:04:00Z", Updated: "2026-07-22T13:04:00Z",
						ArtifactJobs: []gitLabAllRefArtifactJob{{ID: "1100"}},
					}}}, "https://gitlab.example")
				}
				emission := keyRouteEmission(t, claim, handler, client, "coverage_snapshots")
				mainChunks, headChunks := keySplits(t, claim, emission)
				mainOut, headOut := keyOutcome(t, claim, mainChunks), keyOutcome(t, claim, headChunks)
				if mainOut != "" || headOut != "" || len(headChunks) != len(mainChunks) {
					t.Fatalf("head %d chunks %q, MAIN %d chunks %q", len(headChunks), headOut, len(mainChunks), mainOut)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------- the sink

// The sink's refusal of a repeated key, per destination: the same error text and the same structured detail as at d88f200db793.
func TestChunkStampSinkKeyRefusalUnchanged(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	want := map[string]string{
		"ci_pipeline_runs":                      "org_id=org-acme repo_id=" + keyRepoID + " run_id=10000",
		"ci_job_runs":                           "org_id=org-acme repo_id=" + keyRepoID + " run_id=1 job_id=10000",
		"ci_acceptance_checks":                  "org_id=org-acme repo_id=" + keyRepoID + " run_id=1 check_key=10000",
		"test_suite_results":                    "org_id=org-acme repo_id=" + keyRepoID + " run_id=1 suite_id=10000",
		"test_case_results":                     "org_id=org-acme repo_id=" + keyRepoID + " run_id=1 suite_id=s case_id=10000",
		"coverage_snapshots":                    "org_id=org-acme repo_id=" + keyRepoID + " run_id=1 snapshot_id=10000",
		testOpsRunReportCompletenessDestination: "org_id=org-acme repo_id=" + keyRepoID + " run_id=1 run_attempt=10000",
	}
	sink := TestOpsClickHouseEffects{Conn: &githubTestsDuplicateKeyConn{}, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	for _, destination := range stampDestinations {
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, []json.RawMessage{
			keyRow(t, claim, destination, 10_000), keyRow(t, claim, destination, 10_001), keyRow(t, claim, destination, 10_000),
		})
		if err != nil {
			t.Fatal(err)
		}
		err = sink.WriteEffect(context.Background(), claim, effect)
		text := "provider sync configuration is invalid: duplicate natural key in " + destination + " batch (" + want[destination] + ")"
		if err == nil || err.Error() != text {
			t.Fatalf("%s: err %v, want %q", destination, err, text)
		}
		table, fields, ok := DuplicateNaturalKeyDetailFrom(err)
		if !ok || table != destination || len(fields) != strings.Count(want[destination], "=") {
			t.Fatalf("%s: detail %s %+v ok=%t", destination, table, fields, ok)
		}
	}
}
