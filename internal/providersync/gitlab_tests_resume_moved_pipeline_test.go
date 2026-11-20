package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabResumePipeline is one pipeline of gitLabResumeFake.
type gitLabResumePipeline struct {
	id      int
	started time.Time
	updated time.Time
}

// gitLabResumeFake serves /projects/123/pipelines as GitLab does: updated_after / updated_before are inclusive
// (UpdatedAtFilterable), order_by is id or updated_at, sort is desc, and pages are offset pages with X-Next-Page. The
// pipeline list can change between two chunks, as it can on GitLab.
type gitLabResumeFake struct {
	t          *testing.T
	pipelines  []gitLabResumePipeline
	reportGets map[int]int
	// repoPath is the project's path_with_namespace ("acme/api" when empty); projectGets counts project reads.
	repoPath    string
	projectGets int
	// projectStatus, when set, is the status the project read answers with.
	projectStatus int
}

func (fake *gitLabResumeFake) Do(request *http.Request) (*http.Response, error) {
	fake.t.Helper()
	header := make(http.Header)
	body := ""
	path := request.URL.Path
	query := request.URL.Query()
	switch {
	case path == "/api/v4/projects/123":
		fake.projectGets++
		if fake.projectStatus != 0 {
			return &http.Response{StatusCode: fake.projectStatus, Header: header,
				Body: io.NopCloser(strings.NewReader(`{"message":"unavailable"}`)), Request: request}, nil
		}
		repoPath := fake.repoPath
		if repoPath == "" {
			repoPath = "acme/api"
		}
		body = `{"id":123,"path_with_namespace":"` + repoPath + `","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		listed := fake.list(query)
		page, perPage := 1, 20
		if raw := query.Get("page"); raw != "" {
			page, _ = strconv.Atoi(raw)
		}
		if raw := query.Get("per_page"); raw != "" {
			perPage, _ = strconv.Atoi(raw)
		}
		if page < 1 || perPage < 1 {
			fake.t.Fatalf("invalid page query %s", request.URL.RawQuery)
		}
		from := min((page-1)*perPage, len(listed))
		to := min(from+perPage, len(listed))
		if to < len(listed) {
			header.Set("X-Next-Page", strconv.Itoa(page+1))
		}
		items := make([]string, 0, to-from)
		for _, pipeline := range listed[from:to] {
			id := strconv.Itoa(pipeline.id)
			items = append(items, `{"id":`+id+`,"name":"CI","ref":"main","status":"success","source":"push","sha":"abc",`+
				`"created_at":"`+pipeline.started.Format(time.RFC3339)+`","started_at":"`+pipeline.started.Format(time.RFC3339)+`",`+
				`"finished_at":"`+pipeline.started.Add(4*time.Minute).Format(time.RFC3339)+`",`+
				`"updated_at":"`+pipeline.updated.Format(time.RFC3339)+`",`+
				`"web_url":"https://gitlab.example/acme/api/-/pipelines/`+id+`"}`)
		}
		body = "[" + strings.Join(items, ",") + "]"
	case strings.HasSuffix(path, "/jobs"):
		body = `[{"id":11,"name":"unit","stage":"test","status":"success",` +
			`"started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:04:00Z",` +
			`"runner":{"runner_type":"instance_type"},"artifacts_file":{},"artifacts":[]}]`
	case strings.HasSuffix(path, "/test_report"):
		id, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/projects/123/pipelines/"), "/test_report"))
		if err != nil {
			fake.t.Fatalf("test_report path %s: %v", path, err)
		}
		if fake.reportGets == nil {
			fake.reportGets = map[int]int{}
		}
		fake.reportGets[id]++
		body = `{"test_suites":[]}`
	default:
		fake.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func (fake *gitLabResumeFake) list(query map[string][]string) []gitLabResumePipeline {
	get := func(key string) string {
		if values := query[key]; len(values) > 0 {
			return values[0]
		}
		return ""
	}
	if get("sort") != "desc" {
		fake.t.Fatalf("sort=%q, want desc", get("sort"))
	}
	bound := func(key string) *time.Time {
		raw := get(key)
		if raw == "" {
			return nil
		}
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			fake.t.Fatalf("%s=%q: %v", key, raw, err)
		}
		return &parsed
	}
	after, before := bound("updated_after"), bound("updated_before")
	listed := make([]gitLabResumePipeline, 0, len(fake.pipelines))
	for _, pipeline := range fake.pipelines {
		if (after != nil && pipeline.updated.Before(*after)) || (before != nil && pipeline.updated.After(*before)) {
			continue
		}
		listed = append(listed, pipeline)
	}
	switch get("order_by") {
	case "id", "":
		sort.SliceStable(listed, func(i, j int) bool { return listed[i].id > listed[j].id })
	case "updated_at":
		// GitLab orders by the one column only (Ci::PipelinesFinder#sort_items); the fake breaks ties by id so it is
		// deterministic. The fixtures keep updated_at distinct.
		sort.SliceStable(listed, func(i, j int) bool {
			if !listed[i].updated.Equal(listed[j].updated) {
				return listed[i].updated.After(listed[j].updated)
			}
			return listed[i].id > listed[j].id
		})
	default:
		fake.t.Fatalf("order_by=%q is not a GitLab pipelines order", get("order_by"))
	}
	return listed
}

// gitLabResumeReads counts, per pipeline id, the reads of each phase: a ci_pipeline_runs row in the pipelines phase, a
// test_report request (one per pipeline read) in the reports phase.
type gitLabResumeReads struct {
	pipelines map[int]int
	reports   map[int]int
}

func (reads gitLabResumeReads) of(phase string) map[int]int {
	if phase == "reports" {
		return reads.reports
	}
	return reads.pipelines
}

// gitLabResumeWalkOptions shapes one gitLabResumeRun. The walk yields after the yields[i]-th emission of phase (counts
// are cumulative), calls mutate(i) (a change between two chunks), and resumes from that emission's after cursor, first
// passed through rewrite when it is set (a cursor an older binary wrote).
type gitLabResumeWalkOptions struct {
	handler GitLabTestsRouteHandler
	claim   *Claim
	phase   string
	yields  []int
	mutate  func(yield int)
	rewrite func(raw string) string
	observe func(cursor string)
}

type gitLabResumeResult struct {
	reads   gitLabResumeReads
	final   CompleteRouteBatch
	cursor  gitLabTestsChunkCursor
	metrics string
}

// gitLabResumeRun drives CollectChunks to its final emission.
func gitLabResumeRun(t *testing.T, fake *gitLabResumeFake, options gitLabResumeWalkOptions) (gitLabResumeResult, error) {
	t.Helper()
	claim := nativeTestClaim("gitlab", "tests")
	if options.claim != nil {
		claim = *options.claim
	}
	client := gitLabRepositoryClient(t, fake, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	fake.reportGets = map[int]int{}
	result := gitLabResumeResult{reads: gitLabResumeReads{pipelines: map[int]int{}, reports: fake.reportGets}}
	stop := errors.New("yield")
	resume, emitted, next := "", 0, 0
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for pass := 1; pass < 50; pass++ {
		last, finalSeen := resume, false
		err := options.handler.CollectChunks(ctx, claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if options.observe != nil {
					options.observe(emission.CursorAfter)
				}
				for _, effect := range emission.Batch.Effects {
					if effect.Destination != "ci_pipeline_runs" {
						continue
					}
					for _, raw := range effect.Rows {
						var row struct {
							RunID string `json:"run_id"`
						}
						if json.Unmarshal(raw, &row) != nil {
							t.Fatalf("undecodable %s row %s", effect.Destination, raw)
						}
						id, _ := strconv.Atoi(row.RunID)
						result.reads.pipelines[id]++
					}
				}
				if emission.Final {
					result.final, finalSeen = emission.Batch, true
					return nil
				}
				before, decodeErr := decodeGitLabTestsChunkCursor(emission.CursorBefore)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if before.Phase == options.phase {
					emitted++
				}
				if next < len(options.yields) && emitted == options.yields[next] {
					return stop
				}
				return nil
			})
		if finalSeen {
			var buffer bytes.Buffer
			if writeErr := client.Metrics.WritePrometheus(&buffer); writeErr != nil {
				t.Fatal(writeErr)
			}
			result.metrics = buffer.String()
			if err != nil {
				return result, err
			}
			if next < len(options.yields) {
				t.Fatalf("the walk reached %d of its %d yield points", next, len(options.yields))
			}
			cursor, decodeErr := decodeGitLabTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			result.cursor = cursor
			return result, nil
		}
		if !errors.Is(err, stop) {
			return result, err
		}
		if options.mutate != nil {
			options.mutate(next)
		}
		next++
		resume = last
		if options.rewrite != nil {
			resume = options.rewrite(resume)
		}
	}
	t.Fatal("the walk never reached a final emission")
	return result, nil
}

// gitLabResumeWalk is gitLabResumeRun with one yield point (0 = none) and the default handler and claim.
func gitLabResumeWalk(t *testing.T, fake *gitLabResumeFake, phase string, yield int, mutate func()) (gitLabResumeReads, error) {
	t.Helper()
	options := gitLabResumeWalkOptions{phase: phase}
	if yield > 0 {
		options.yields = []int{yield}
		options.mutate = func(int) { mutate() }
	}
	result, err := gitLabResumeRun(t, fake, options)
	return result.reads, err
}

// gitLabResumeFixture is 250 pipelines in the window of nativeTestClaim, ids 1001..1250, id order == updated_at order.
func gitLabResumeFixture() []gitLabResumePipeline {
	base := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	pipelines := make([]gitLabResumePipeline, 0, 250)
	for index := range 250 {
		started := base.Add(time.Duration(index) * time.Minute)
		pipelines = append(pipelines, gitLabResumePipeline{id: 1001 + index, started: started, updated: started.Add(5 * time.Minute)})
	}
	return pipelines
}

// gitLabResumeListed returns the window's ids in the updated_at listing order (newest updated first).
func gitLabResumeListed(fake *gitLabResumeFake) []int {
	ids := []int{}
	claim := nativeTestClaim("gitlab", "tests")
	for _, pipeline := range fake.list(map[string][]string{
		"order_by": {"updated_at"}, "sort": {"desc"},
		"updated_after": {claim.SinceAt.Format(time.RFC3339)}, "updated_before": {claim.BeforeAt.Format(time.RFC3339)},
	}) {
		ids = append(ids, pipeline.id)
	}
	return ids
}

func gitLabResumeDelete(fake *gitLabResumeFake, ids ...int) {
	gone := map[int]bool{}
	for _, id := range ids {
		gone[id] = true
	}
	kept := fake.pipelines[:0:0]
	for _, pipeline := range fake.pipelines {
		if !gone[pipeline.id] {
			kept = append(kept, pipeline)
		}
	}
	fake.pipelines = kept
}

func gitLabResumeUpdate(fake *gitLabResumeFake, id int, at time.Time) {
	for index := range fake.pipelines {
		if fake.pipelines[index].id == id {
			fake.pipelines[index].updated = at
		}
	}
}

// A pipeline that moves in the GitLab listing between two chunks must not move the resume point. Each cell yields
// inside one phase, changes the fake, resumes, and compares the phase's reads with a fresh walk over the changed fake:
// every listed pipeline is read, none twice. A deletion, or an update that takes a read pipeline out of the window
// (updated_at past BeforeAt, as any update after the plan does), shifts the offset pages forward (by more than one page
// when 120 go: the walk steps back, and no attempt is burned); under updated_at order an update inside the window also
// moved an unread pipeline in front of the resume point. The id-ordered listing with the LastID seek reads them all.
func TestGitLabTestsResumeAfterAPipelineMovedBeforeTheResumePointReadsEachPipelineOnce(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	outOfWindow := claim.BeforeAt.Add(time.Hour)
	newestInWindow := claim.BeforeAt.Add(-time.Second)
	type change func(fake *gitLabResumeFake, consumed int)
	none := func(*gitLabResumeFake, int) {}
	deleteNewest := func(n int) change {
		return func(fake *gitLabResumeFake, _ int) { gitLabResumeDelete(fake, gitLabResumeListed(fake)[:n]...) }
	}
	deleteAnchor := func(fake *gitLabResumeFake, consumed int) {
		gitLabResumeDelete(fake, gitLabResumeListed(fake)[consumed-1])
	}
	updateNewestOut := func(fake *gitLabResumeFake, _ int) {
		gitLabResumeUpdate(fake, gitLabResumeListed(fake)[0], outOfWindow)
	}
	updateReadIn := func(fake *gitLabResumeFake, _ int) {
		gitLabResumeUpdate(fake, gitLabResumeListed(fake)[10], newestInWindow)
	}
	updateUnreadIn := func(fake *gitLabResumeFake, _ int) {
		ids := gitLabResumeListed(fake)
		gitLabResumeUpdate(fake, ids[len(ids)-1], newestInWindow)
	}
	for _, phase := range []string{"pipelines", "reports"} {
		for _, tc := range []struct {
			name   string
			yield  int
			change change
		}{
			{"control: mid page, nothing changed", 50, none},
			{"control: page end, nothing changed", 100, none},
			{"control: mid page, a read pipeline updated inside the window", 50, updateReadIn},
			{"mid page, newest read pipeline deleted", 50, deleteNewest(1)},
			{"page end, newest read pipeline deleted", 100, deleteNewest(1)},
			{"mid page, the last read pipeline deleted", 50, deleteAnchor},
			{"second page, 120 read pipelines deleted", 150, deleteNewest(120)},
			{"mid page, newest read pipeline updated out of the window", 50, updateNewestOut},
			{"page end, newest read pipeline updated out of the window", 100, updateNewestOut},
			{"mid page, an unread pipeline updated inside the window", 50, updateUnreadIn},
			{"page end, an unread pipeline updated inside the window", 100, updateUnreadIn},
		} {
			t.Run(phase+": "+tc.name, func(t *testing.T) {
				t.Parallel()
				fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
				reads, err := gitLabResumeWalk(t, fake, phase, tc.yield, func() { tc.change(fake, tc.yield) })
				oracleFake := &gitLabResumeFake{t: t, pipelines: append([]gitLabResumePipeline(nil), fake.pipelines...)}
				oracle, oracleErr := gitLabResumeWalk(t, oracleFake, phase, 0, nil)
				if oracleErr != nil {
					t.Fatalf("oracle walk err=%v", oracleErr)
				}
				if len(oracle.of(phase)) == 0 {
					t.Fatal("the oracle read no pipeline: the cell proves nothing")
				}
				var skipped, twice []string
				for id := range oracle.of(phase) {
					if reads.of(phase)[id] == 0 {
						skipped = append(skipped, strconv.Itoa(id))
					}
				}
				for id, count := range reads.of(phase) {
					if count > 1 {
						twice = append(twice, strconv.Itoa(id)+"x"+strconv.Itoa(count))
					}
				}
				sort.Strings(skipped)
				sort.Strings(twice)
				t.Logf("phase=%s yield=%d err=%v oracle=%d read=%d skipped=%v read_twice=%v",
					phase, tc.yield, err, len(oracle.of(phase)), len(reads.of(phase)), skipped, twice)
				if err != nil {
					t.Fatalf("resumed walk err=%v", err)
				}
				if len(skipped) != 0 || len(twice) != 0 {
					t.Errorf("a pipeline moved between two chunks moved the resume: skipped=%v read_twice=%v", skipped, twice)
				}
			})
		}
	}
}

// A cursor written before LastID existed (no "li") names a position in the updated_at order: the phase starts again at
// page 1. The cursor yields on page 2, so a restart differs from a resume by position: the pipelines read before the
// yield are read twice (their rows replace the stored ones), none is skipped, the unit does not fail, the pages up to the
// legacy page cost no budget, and the restart is counted.
func TestGitLabTestsResumeLegacyCursorRestartsThePhase(t *testing.T) {
	for _, phase := range []string{"pipelines", "reports"} {
		for _, deleteNewest := range []bool{false, true} {
			t.Run(phase+" delete="+strconv.FormatBool(deleteNewest), func(t *testing.T) {
				t.Parallel()
				fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
				var readBefore []int
				result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
					phase: phase, yields: []int{150},
					mutate: func(int) {
						ids := gitLabResumeListed(fake)
						readBefore = append(readBefore, ids[:150]...)
						if deleteNewest {
							gitLabResumeDelete(fake, ids[0])
						}
					},
					rewrite: func(raw string) string {
						var fields map[string]any
						if err := json.Unmarshal([]byte(raw), &fields); err != nil {
							t.Fatal(err)
						}
						if fields["phase"] != phase || fields["index"] == nil {
							t.Fatalf("the yield cursor is not mid page in %s: %s", phase, raw)
						}
						delete(fields, "li")
						legacy, err := json.Marshal(fields)
						if err != nil {
							t.Fatal(err)
						}
						return string(legacy)
					},
				})
				if err != nil {
					t.Fatalf("a legacy cursor failed the unit: %v", err)
				}
				oracle, err := gitLabResumeWalk(t, &gitLabResumeFake{t: t, pipelines: append([]gitLabResumePipeline(nil), fake.pipelines...)}, phase, 0, nil)
				if err != nil {
					t.Fatal(err)
				}
				reads := result.reads.of(phase)
				for id := range oracle.of(phase) {
					if reads[id] == 0 {
						t.Errorf("pipeline %d skipped", id)
					}
				}
				again := map[int]bool{}
				for _, id := range readBefore {
					again[id] = true
				}
				for id, count := range reads {
					want := 1
					if again[id] && oracle.of(phase)[id] > 0 {
						want = 2 // read before the yield, read again by the restart
					}
					if count != want {
						t.Errorf("pipeline %d read %d times, want %d", id, count, want)
					}
				}
				pages := result.cursor.PipelinePages
				if phase == "reports" {
					pages = result.cursor.ReportPages
				}
				if pages != 3 || len(result.cursor.Truncated) != 0 || result.final.Watermark == nil {
					t.Errorf("pages=%d truncated=%v watermark=%v, want 3 pages charged once and a whole walk",
						pages, result.cursor.Truncated, result.final.Watermark != nil)
				}
				want := `dev_health_provider_resume_reanchor_total{provider="gitlab",dataset="tests",phase="` + phase + `"} 1`
				if !strings.Contains(result.metrics, want) {
					t.Errorf("the restart is not counted: want %s", want)
				}
			})
		}
	}
}

// A resume on a page end re-reads the finished page (the page that holds LastID); that read is free in the phase's
// page budget, so a walk that needs exactly its budget still ends whole when it resumes on every page end.
func TestGitLabTestsResumeOnEachPageEndCostsNoBudgetPage(t *testing.T) {
	for _, phase := range []string{"pipelines", "reports"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
			result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
				handler: GitLabTestsRouteHandler{MaxPages: 3}, phase: phase, yields: []int{100, 200},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.cursor.Truncated) != 0 || result.final.Watermark == nil {
				t.Fatalf("a walk of exactly its page budget resumed on each page end: truncated=%v watermark=%v",
					result.cursor.Truncated, result.final.Watermark != nil)
			}
			for _, reads := range []map[int]int{result.reads.pipelines, result.reads.reports} {
				if len(reads) != 250 {
					t.Fatalf("read %d pipelines, want 250", len(reads))
				}
				for id, count := range reads {
					if count != 1 {
						t.Fatalf("pipeline %d read %d times", id, count)
					}
				}
			}
		})
	}
}

// A step back (a shift of more than one page under a resume) is counted on the GitLab phase's own label; a resume whose
// page did not move counts nothing.
func TestGitLabTestsResumeStepBackIsCounted(t *testing.T) {
	for _, tc := range []struct {
		name    string
		deleted int
		want    bool
	}{{"120 read pipelines deleted", 120, true}, {"nothing deleted", 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
			result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
				phase: "pipelines", yields: []int{150},
				mutate: func(int) { gitLabResumeDelete(fake, gitLabResumeListed(fake)[:tc.deleted]...) },
			})
			if err != nil {
				t.Fatal(err)
			}
			line := `dev_health_provider_resume_reanchor_total{provider="gitlab",dataset="tests",phase="pipelines"}`
			if strings.Contains(result.metrics, line) != tc.want {
				t.Fatalf("re-seek counted=%v, want %v:\n%s", strings.Contains(result.metrics, line), tc.want, result.metrics)
			}
		})
	}
}

// A pipeline that is NOT in the window when the walk starts (id > LastID, updated before SinceAt) and is updated between
// two chunks: (a) updated past BeforeAt (any update after the plan): this window does not list it, the next window
// lists and reads it. (b) updated INTO this window (GitLab's clock behind ours, or an explicit future before_at such as
// a backfill): its id is above LastID, so this walk passes it, and the next window (updated_after = this BeforeAt) does
// not list it either. (b) is the named residual, NOT closed (ticket 14383); a NEW pipeline listed inside the window after
// the walk passed its id is the same class. The updated_at-ordered binary missed it the same way.
func TestGitLabTestsResumeAPipelineUpdatedIntoAWindowAfterTheWalkPassedItsID(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	next := claim
	nextSince, nextBefore := claim.BeforeAt.UTC(), claim.BeforeAt.Add(24*time.Hour).UTC()
	next.SinceAt, next.BeforeAt = &nextSince, &nextBefore
	const late = 1300
	for _, tc := range []struct {
		name      string
		updatedAt time.Time
		thisWalk  int
		nextWalk  int
	}{
		{"updated past BeforeAt: read by the next window", claim.BeforeAt.Add(time.Hour), 0, 1},
		{"updated into this window: missed by both (residual)", claim.BeforeAt.Add(-time.Second), 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pipelines := gitLabResumeFixture()
			pipelines = append(pipelines, gitLabResumePipeline{
				id: late, started: claim.SinceAt.Add(-2 * time.Hour), updated: claim.SinceAt.Add(-time.Hour),
			})
			fake := &gitLabResumeFake{t: t, pipelines: pipelines}
			result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
				phase: "pipelines", yields: []int{50},
				mutate: func(int) { gitLabResumeUpdate(fake, late, tc.updatedAt) },
			})
			if err != nil {
				t.Fatal(err)
			}
			following, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{claim: &next})
			if err != nil {
				t.Fatal(err)
			}
			if result.reads.pipelines[late] != tc.thisWalk || following.reads.pipelines[late] != tc.nextWalk {
				t.Fatalf("pipeline %d read %d times by this walk and %d by the next window, want %d and %d",
					late, result.reads.pipelines[late], following.reads.pipelines[late], tc.thisWalk, tc.nextWalk)
			}
			if len(result.reads.pipelines) != 250 {
				t.Fatalf("this walk read %d pipelines, want the 250 of the window", len(result.reads.pipelines))
			}
		})
	}
}

// Pipelines finish in another order than they start, so the updated_at order is not the id order. The resume key is the
// id, so the listing must be the id order: a resume over an updated_at-ordered listing would skip the unread pipelines
// with a larger id than LastID. Every listed pipeline is read once, with or without a deletion before the resume point.
func TestGitLabTestsResumeWhenTheUpdatedOrderIsNotTheIDOrder(t *testing.T) {
	permuted := func() []gitLabResumePipeline {
		pipelines := gitLabResumeFixture()
		base := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
		for index := range pipelines {
			pipelines[index].updated = base.Add(time.Duration(index*37%250+5) * time.Minute)
		}
		return pipelines
	}
	for _, phase := range []string{"pipelines", "reports"} {
		for _, deleteNewest := range []bool{false, true} {
			t.Run(phase+" delete="+strconv.FormatBool(deleteNewest), func(t *testing.T) {
				t.Parallel()
				fake := &gitLabResumeFake{t: t, pipelines: permuted()}
				reads, err := gitLabResumeWalk(t, fake, phase, 50, func() {
					if deleteNewest {
						gitLabResumeDelete(fake, 1250) // the largest id: read first in the id order
					}
				})
				if err != nil {
					t.Fatal(err)
				}
				oracle, err := gitLabResumeWalk(t, &gitLabResumeFake{t: t, pipelines: append([]gitLabResumePipeline(nil), fake.pipelines...)}, phase, 0, nil)
				if err != nil {
					t.Fatal(err)
				}
				for id := range oracle.of(phase) {
					if reads.of(phase)[id] != 1 {
						t.Errorf("pipeline %d read %d times, want 1", id, reads.of(phase)[id])
					}
				}
			})
		}
	}
}

// gitLabResumeFixtureN is gitLabResumeFixture with n pipelines.
func gitLabResumeFixtureN(n int) []gitLabResumePipeline {
	pipelines := gitLabResumeFixture()
	base := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	for index := len(pipelines); index < n; index++ {
		started := base.Add(time.Duration(index) * time.Minute)
		pipelines = append(pipelines, gitLabResumePipeline{id: 1001 + index, started: started, updated: started.Add(5 * time.Minute)})
	}
	return pipelines[:n]
}

// A phase that has charged its whole page budget still re-reads its already-charged pages for free: a yield inside the
// last budgeted page, and a page-end yield after which deletions move the last unread pipeline onto that page, both end
// whole (no truncation, every pipeline read once). Only a page the phase never charged needs budget.
func TestGitLabTestsResumeWithTheWholeBudgetChargedStillReadsChargedPages(t *testing.T) {
	for _, phase := range []string{"pipelines", "reports"} {
		for _, tc := range []struct {
			name      string
			pipelines int
			maxPages  int
			yield     int
			deleted   int
		}{
			{"mid page of the last budgeted page, steady", 250, 3, 225, 0},
			// Two deletions leave page 2 short (99), so the listing provably ends on it: a full last page would make the
			// paginator ask for page 3, which needs budget.
			{"page end of the last budgeted page, two read pipelines deleted", 201, 2, 200, 2},
		} {
			t.Run(phase+": "+tc.name, func(t *testing.T) {
				t.Parallel()
				fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixtureN(tc.pipelines)}
				result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
					handler: GitLabTestsRouteHandler{MaxPages: tc.maxPages}, phase: phase, yields: []int{tc.yield},
					mutate: func(int) { gitLabResumeDelete(fake, gitLabResumeListed(fake)[:tc.deleted]...) },
				})
				if err != nil {
					t.Fatal(err)
				}
				// The other phase walks the listing fresh and may need a page past the budget (201 pipelines, 2 pages): only
				// the resumed phase's own truncation is the defect.
				component := gitLabPipelineInventoryComponent
				if phase == "reports" {
					component = gitLabReportInventoryComponent
				}
				for _, truncated := range result.cursor.Truncated {
					if truncated == component {
						t.Fatalf("truncated=%v, want the resumed phase whole: the re-read pages were charged already", result.cursor.Truncated)
					}
				}
				reads := result.reads.of(phase)
				if len(reads) != tc.pipelines {
					t.Fatalf("read %d pipelines, want %d", len(reads), tc.pipelines)
				}
				for id, count := range reads {
					if count != 1 {
						t.Fatalf("pipeline %d read %d times", id, count)
					}
				}
			})
		}
	}
}

// Two yields around a step back (or around a legacy restart) charge no page twice: after the first resume the cursor
// sits on a lower page than the phase has charged, and the second resume still knows the charged pages.
func TestGitLabTestsResumeTwiceAfterAStepBackChargesNoPageTwice(t *testing.T) {
	for _, phase := range []string{"pipelines", "reports"} {
		for _, legacy := range []bool{false, true} {
			t.Run(phase+" legacy="+strconv.FormatBool(legacy), func(t *testing.T) {
				t.Parallel()
				fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
				options := gitLabResumeWalkOptions{
					phase: phase, yields: []int{150, 151},
					mutate: func(yield int) {
						if yield == 0 && !legacy {
							gitLabResumeDelete(fake, gitLabResumeListed(fake)[:120]...)
						}
					},
				}
				rewrites := 0
				if legacy {
					options.rewrite = func(raw string) string {
						rewrites++
						if rewrites > 1 {
							return raw
						}
						var fields map[string]any
						if err := json.Unmarshal([]byte(raw), &fields); err != nil {
							t.Fatal(err)
						}
						delete(fields, "li")
						legacyRaw, err := json.Marshal(fields)
						if err != nil {
							t.Fatal(err)
						}
						return string(legacyRaw)
					}
				}
				result, err := gitLabResumeRun(t, fake, options)
				if err != nil {
					t.Fatal(err)
				}
				pages, want := result.cursor.PipelinePages, 3
				if phase == "reports" {
					pages = result.cursor.ReportPages
				}
				if !legacy {
					want = 2 // 130 pipelines left after the deletion: pages 1 and 2
				}
				if pages != want {
					t.Fatalf("the phase charged %d pages, want %d (no page charged twice)", pages, want)
				}
			})
		}
	}
}

// The listed pipeline id is read from the exact "id" key as a positive JSON integer; a quoted id, a case variant of the
// key and every non-integer form are refused.
func TestGitLabTestsListedPipelineIDIsAnExactPositiveInteger(t *testing.T) {
	for raw, want := range map[string]int64{
		`{"id":1250}`:               1250,
		`{"id":1250,"ID":7}`:        1250,
		`{"id":"1250"}`:             0,
		`{"ID":1250}`:               0,
		`{"id":0,"Id":1250}`:        0,
		`{"Id":1250}`:               0,
		`{"id":null}`:               0,
		`{"id":-3}`:                 0,
		`{"id":12.5}`:               0,
		`{"id":1e3}`:                0,
		`{}`:                        0,
		`{"id":[1250]}`:             0,
		`{"id":true}`:               0,
		`{"id":"x","name":"other"}`: 0,
	} {
		id, err := gitLabTestsListedPipelineID(json.RawMessage(raw))
		if want > 0 && (err != nil || id != want) {
			t.Errorf("%s: id=%d err=%v, want %d", raw, id, err, want)
		}
		if want == 0 && err == nil {
			t.Errorf("%s: accepted as %d, want a refusal", raw, id)
		}
	}
}
