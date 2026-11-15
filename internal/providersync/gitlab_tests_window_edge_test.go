package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabEdgePipeline struct {
	ID, Ref, Status, JobStatus          string
	Created, Started, Finished, Updated string
	Report                              bool
}

// gitLabEdgeDoer serves a GitLab project and its pipelines. The listing honours updated_after and updated_before on
// each pipeline's Updated stamp, as GitLab does.
type gitLabEdgeDoer struct {
	t         *testing.T
	pipelines []gitLabEdgePipeline
}

func gitLabEdgeTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func (doer *gitLabEdgeDoer) find(id string) gitLabEdgePipeline {
	for _, pipeline := range doer.pipelines {
		if pipeline.ID == id {
			return pipeline
		}
	}
	doer.t.Fatalf("unknown pipeline %s", id)
	return gitLabEdgePipeline{}
}

func (doer *gitLabEdgeDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	status, body := http.StatusOK, "{}"
	path := request.URL.Path
	switch {
	case path == "/api/v4/projects/123":
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		query := request.URL.Query()
		items := []string{}
		for _, pipeline := range doer.pipelines {
			updated := gitLabEdgeTime(pipeline.Updated)
			if raw := query.Get("updated_after"); raw != "" && updated.Before(gitLabEdgeTime(raw)) {
				continue
			}
			if raw := query.Get("updated_before"); raw != "" && updated.After(gitLabEdgeTime(raw)) {
				continue
			}
			finished := "null"
			if pipeline.Finished != "" {
				finished = `"` + pipeline.Finished + `"`
			}
			items = append(items, `{"id":`+pipeline.ID+`,"name":"CI","ref":"`+pipeline.Ref+`","status":"`+pipeline.Status+`",`+
				`"created_at":"`+pipeline.Created+`","started_at":"`+pipeline.Started+`","finished_at":`+finished+`,`+
				`"source":"push","sha":"abc","updated_at":"`+pipeline.Updated+`","web_url":"https://gitlab.example/p/`+pipeline.ID+`"}`)
		}
		body = "[" + strings.Join(items, ",") + "]"
	case strings.HasSuffix(path, "/merge_requests"):
		body = "[]"
	case strings.HasSuffix(path, "/test_report"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/projects/123/pipelines/"), "/test_report")
		if doer.find(id).Report {
			body = `{"test_suites":[{"name":"api","total_time":"1.0","test_cases":[{"name":"case-` + id + `","classname":"t.T","status":"success","execution_time":"1.0"}]}]}`
		} else {
			status, body = http.StatusNotFound, `{"message":"404 Not found"}`
		}
	case strings.HasSuffix(path, "/jobs"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/projects/123/pipelines/"), "/jobs")
		pipeline := doer.find(id)
		finished := "null"
		if pipeline.Finished != "" {
			finished = `"` + pipeline.Finished + `"`
		}
		body = `[{"id":11,"name":"unit","stage":"test","status":"` + pipeline.JobStatus + `","started_at":"` + pipeline.Started +
			`","finished_at":` + finished + `,"runner":{"runner_type":"instance_type"}}]`
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

type gitLabEdgeRow struct {
	key, state string
	synced     time.Time
}

// gitLabEdgeLatest keeps, per key, the row with the latest last_synced: what the ReplacingMergeTree answers after FINAL.
func gitLabEdgeLatest(t *testing.T, batches []CompleteRouteBatch, destination, keyOf, stateOf string, extraKey string) map[string]gitLabEdgeRow {
	t.Helper()
	out := map[string]gitLabEdgeRow{}
	for _, batch := range batches {
		for _, effect := range batch.Effects {
			if effect.Destination != destination {
				continue
			}
			for _, raw := range effect.Rows {
				var row map[string]any
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				key, _ := row[keyOf].(string)
				if extraKey != "" {
					key += "/" + row[extraKey].(string)
				}
				synced, err := time.Parse(time.RFC3339Nano, row["last_synced"].(string))
				if err != nil {
					t.Fatal(err)
				}
				state, _ := row[stateOf].(string)
				if prior, seen := out[key]; !seen || synced.After(prior.synced) {
					out[key] = gitLabEdgeRow{key: key, state: state, synced: synced}
				}
			}
		}
	}
	return out
}

func gitLabEdgeKeys(rows map[string]gitLabEdgeRow) []string {
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

type gitLabEdgeRoute struct {
	name    string
	collect func(*testing.T, *gitLabEdgeDoer, Claim, time.Time) CompleteRouteBatch
}

var gitLabEdgeRoutes = []gitLabEdgeRoute{
	{"chunked", func(t *testing.T, doer *gitLabEdgeDoer, claim Claim, at time.Time) CompleteRouteBatch {
		client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
		var batch CompleteRouteBatch
		err := (GitLabTestsRouteHandler{}).CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, at, "",
			func(emission ChunkRouteEmission) error {
				batch.Effects = append(batch.Effects, emission.Batch.Effects...)
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}},
	{"non-chunked", func(t *testing.T, doer *gitLabEdgeDoer, claim Claim, at time.Time) CompleteRouteBatch {
		client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
		batch, err := (GitLabTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, at)
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}},
}

func gitLabEdgeWindow(since, before string) Claim {
	claim := nativeTestClaim("gitlab", "tests")
	sinceAt, beforeAt := gitLabEdgeTime(since), gitLabEdgeTime(before)
	claim.SinceAt, claim.BeforeAt = &sinceAt, &beforeAt
	return claim
}

func gitLabEdgeTables(t *testing.T, batches []CompleteRouteBatch) (pipelines, jobs, checks map[string]gitLabEdgeRow) {
	t.Helper()
	return gitLabEdgeLatest(t, batches, "ci_pipeline_runs", "run_id", "status", ""),
		gitLabEdgeLatest(t, batches, "ci_job_runs", "run_id", "status", "job_id"),
		gitLabEdgeLatest(t, batches, "ci_acceptance_checks", "run_id", "result", "check_key")
}

var gitLabEdgeCrossing = gitLabEdgePipeline{ID: "9101", Ref: "refs/merge-requests/12/head", Status: "success", JobStatus: "success",
	Created: "2026-08-01T23:58:00Z", Started: "2026-08-01T23:58:30Z", Finished: "2026-08-02T00:03:00Z", Updated: "2026-08-02T00:03:00Z"}

var (
	gitLabEdgeDayD   = [2]string{"2026-08-02T00:00:00Z", "2026-08-02T01:00:00Z"}
	gitLabEdgeDayD1  = [2]string{"2026-08-01T23:00:00Z", "2026-08-02T00:00:00Z"}
	gitLabEdgeDayDp1 = [2]string{"2026-08-02T01:00:00Z", "2026-08-02T02:00:00Z"}
	gitLabEdgeAt1    = time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	gitLabEdgeAt2    = time.Date(2026, 8, 14, 13, 0, 0, 0, time.UTC)
)

// C1: a pipeline that started before the window and was last updated inside it gets its pipeline row, its job rows and its
// acceptance rows in exactly the window that lists it, and no row in the window before.
func TestGitLabPipelineThatCrossesAWindowEdgeGetsItsRowsInTheWindowOfItsLastUpdate(t *testing.T) {
	for _, route := range gitLabEdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			doer := &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{gitLabEdgeCrossing}}
			pipelines, jobs, checks := gitLabEdgeTables(t, []CompleteRouteBatch{route.collect(t, doer, gitLabEdgeWindow(gitLabEdgeDayD[0], gitLabEdgeDayD[1]), gitLabEdgeAt1)})
			if got := gitLabEdgeKeys(pipelines); !slices.Equal(got, []string{"9101"}) {
				t.Fatalf("pipeline rows %v, want exactly the crossing pipeline", got)
			}
			if got := gitLabEdgeKeys(jobs); !slices.Equal(got, []string{"9101/11"}) {
				t.Fatalf("job rows %v, want its job", got)
			}
			if len(checks) == 0 {
				t.Fatal("no acceptance row for the crossing pipeline")
			}
			before := &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{gitLabEdgeCrossing}}
			earlier, earlierJobs, earlierChecks := gitLabEdgeTables(t, []CompleteRouteBatch{route.collect(t, before, gitLabEdgeWindow(gitLabEdgeDayD1[0], gitLabEdgeDayD1[1]), gitLabEdgeAt1)})
			if len(earlier)+len(earlierJobs)+len(earlierChecks) != 0 {
				t.Fatalf("the window before wrote rows for a pipeline last updated after it: %v %v %v", earlier, earlierJobs, earlierChecks)
			}
		})
	}
}

// C2 and C3: the same window replayed, and a pipeline updated again in the next window, write the same keys in all three
// tables.
func TestGitLabCrossingPipelineWritesTheSameKeysOnReplayAndInTheNextWindow(t *testing.T) {
	for _, route := range gitLabEdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			window := gitLabEdgeWindow(gitLabEdgeDayD[0], gitLabEdgeDayD[1])
			first := route.collect(t, &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{gitLabEdgeCrossing}}, window, gitLabEdgeAt1)
			again := route.collect(t, &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{gitLabEdgeCrossing}}, window, gitLabEdgeAt2)
			p1, j1, c1 := gitLabEdgeTables(t, []CompleteRouteBatch{first})
			p2, j2, c2 := gitLabEdgeTables(t, []CompleteRouteBatch{first, again})
			if !slices.Equal(gitLabEdgeKeys(p1), gitLabEdgeKeys(p2)) || !slices.Equal(gitLabEdgeKeys(j1), gitLabEdgeKeys(j2)) || !slices.Equal(gitLabEdgeKeys(c1), gitLabEdgeKeys(c2)) {
				t.Fatal("a replay of the same window wrote a new key")
			}
			rerun := gitLabEdgeCrossing
			rerun.Status, rerun.JobStatus, rerun.Finished, rerun.Updated = "success", "success", "2026-08-02T01:20:00Z", "2026-08-02T01:20:00Z"
			failed := gitLabEdgeCrossing
			failed.Status, failed.JobStatus = "failed", "failed"
			firstFailed := route.collect(t, &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{failed}}, window, gitLabEdgeAt1)
			next := route.collect(t, &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{rerun}}, gitLabEdgeWindow(gitLabEdgeDayDp1[0], gitLabEdgeDayDp1[1]), gitLabEdgeAt2)
			pn, jn, cn := gitLabEdgeTables(t, []CompleteRouteBatch{firstFailed, next})
			if !slices.Equal(gitLabEdgeKeys(pn), gitLabEdgeKeys(p1)) || !slices.Equal(gitLabEdgeKeys(jn), gitLabEdgeKeys(j1)) || !slices.Equal(gitLabEdgeKeys(cn), gitLabEdgeKeys(c1)) {
				t.Fatalf("the next window wrote other keys: %v %v %v", gitLabEdgeKeys(pn), gitLabEdgeKeys(jn), gitLabEdgeKeys(cn))
			}
			if pn["9101"].state != "success" || jn["9101/11"].state != "success" {
				t.Fatalf("latest status after the next window: pipeline %q job %q, want success", pn["9101"].state, jn["9101/11"].state)
			}
		})
	}
}

// C8: a pipeline listed while it runs and again, finished, in the next window ends with ONE row per key carrying the finished
// status in all three tables (its start is before the second window, which the old lower bound dropped for good).
func TestGitLabPipelineListedRunningThenFinishedEndsWithTheFinishedStatus(t *testing.T) {
	running := gitLabEdgePipeline{ID: "9201", Ref: "main", Status: "running", JobStatus: "running", Created: "2026-08-02T00:39:00Z",
		Started: "2026-08-02T00:40:00Z", Updated: "2026-08-02T00:50:00Z"}
	done := running
	done.Status, done.JobStatus, done.Finished, done.Updated = "success", "success", "2026-08-02T01:30:00Z", "2026-08-02T01:30:00Z"
	for _, route := range gitLabEdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			inD := route.collect(t, &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{running}}, gitLabEdgeWindow(gitLabEdgeDayD[0], gitLabEdgeDayD[1]), gitLabEdgeAt1)
			inDp1 := route.collect(t, &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{done}}, gitLabEdgeWindow(gitLabEdgeDayDp1[0], gitLabEdgeDayDp1[1]), gitLabEdgeAt2)
			pipelines, jobs, checks := gitLabEdgeTables(t, []CompleteRouteBatch{inD, inDp1})
			if len(pipelines) != 1 || pipelines["9201"].state != "success" {
				t.Fatalf("pipeline rows %v, want one row with the finished status", pipelines)
			}
			if len(jobs) != 1 || jobs["9201/11"].state != "success" {
				t.Fatalf("job rows %v, want one row with the finished status", jobs)
			}
			stateAfterD := gitLabEdgeLatest(t, []CompleteRouteBatch{inD}, "ci_acceptance_checks", "run_id", "result", "check_key")
			for key, row := range checks {
				if prior, ok := stateAfterD[key]; !ok || prior.synced.Equal(row.synced) {
					t.Fatalf("acceptance row %s was not rewritten by the next window", key)
				}
			}
			if len(checks) == 0 {
				t.Fatal("no acceptance row")
			}
		})
	}
}

// C4 and C5: a pipeline that started inside the window gets its row as before; a start after the upper bound stays outside;
// a start long before the window is written (no lower bound).
func TestGitLabPipelinePhaseKeepsTheUpperBoundAndTheInWindowRule(t *testing.T) {
	inside := gitLabEdgePipeline{ID: "9301", Ref: "main", Status: "success", JobStatus: "success", Created: "2026-08-02T00:10:00Z",
		Started: "2026-08-02T00:11:00Z", Finished: "2026-08-02T00:20:00Z", Updated: "2026-08-02T00:20:00Z"}
	late := gitLabEdgePipeline{ID: "9302", Ref: "main", Status: "success", JobStatus: "success", Created: "2026-08-02T01:20:00Z",
		Started: "2026-08-02T01:30:00Z", Finished: "2026-08-02T01:40:00Z", Updated: "2026-08-02T00:30:00Z"}
	old := gitLabEdgePipeline{ID: "9303", Ref: "main", Status: "success", JobStatus: "success", Created: "2026-07-01T10:00:00Z",
		Started: "2026-07-01T10:01:00Z", Finished: "2026-07-01T10:05:00Z", Updated: "2026-08-02T00:40:00Z"}
	for _, route := range gitLabEdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			doer := &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{inside, late, old}}
			pipelines, _, _ := gitLabEdgeTables(t, []CompleteRouteBatch{route.collect(t, doer, gitLabEdgeWindow(gitLabEdgeDayD[0], gitLabEdgeDayD[1]), gitLabEdgeAt1)})
			if got := gitLabEdgeKeys(pipelines); !slices.Equal(got, []string{"9301", "9303"}) {
				t.Fatalf("pipeline rows %v, want the in-window pipeline and the old re-listed one, not the late start", got)
			}
		})
	}
}

// C7: the report phase and the pipeline phase agree: every pipeline with suite rows has a pipeline row.
func TestGitLabEveryPipelineWithSuiteRowsHasAPipelineRow(t *testing.T) {
	withReport := gitLabEdgeCrossing
	// A default-branch ref: the report phase selects pipelines of the default branch only until the every-ref change lands.
	withReport.Ref, withReport.Report = "main", true
	for _, route := range gitLabEdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			doer := &gitLabEdgeDoer{t: t, pipelines: []gitLabEdgePipeline{withReport}}
			batch := route.collect(t, doer, gitLabEdgeWindow(gitLabEdgeDayD[0], gitLabEdgeDayD[1]), gitLabEdgeAt1)
			suites := gitLabEdgeLatest(t, []CompleteRouteBatch{batch}, "test_suite_results", "run_id", "suite_name", "suite_id")
			pipelines := gitLabEdgeLatest(t, []CompleteRouteBatch{batch}, "ci_pipeline_runs", "run_id", "status", "")
			if len(suites) == 0 {
				t.Fatal("the fixture wrote no suite row")
			}
			for key := range suites {
				run := strings.SplitN(key, "/", 2)[0]
				if _, ok := pipelines[run]; !ok {
					t.Fatalf("suite rows of run %s have no pipeline row", run)
				}
			}
		})
	}
}
