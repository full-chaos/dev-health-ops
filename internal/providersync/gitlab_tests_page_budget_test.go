package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabTestsPagedDoer serves a fixed number of pipeline pages, each holding
// perPage pipelines with no artifacts to download. Both chunked phases walk
// the same /pipelines listing, so pipelineListRequests counts the re-visits
// that CHAOS-4130 is about.
type gitLabTestsPagedDoer struct {
	t                    *testing.T
	pages                int
	perPage              int
	pipelineListRequests int
}

func (doer *gitLabTestsPagedDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := make(http.Header)
	body := ""
	path := request.URL.Path
	switch {
	case path == "/api/v4/projects/123":
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		page := 1
		if raw := request.URL.Query().Get("page"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				doer.t.Fatalf("invalid page query %q: %v", raw, err)
			}
			page = parsed
		}
		if page < 1 || page > doer.pages {
			doer.t.Fatalf("unexpected pipelines page %d", page)
		}
		doer.pipelineListRequests++
		if page < doer.pages {
			header.Set("X-Next-Page", strconv.Itoa(page+1))
		}
		items := make([]string, 0, doer.perPage)
		for offset := 0; offset < doer.perPage; offset++ {
			id := strconv.Itoa(9000 + (page-1)*doer.perPage + offset)
			items = append(items, `{"id":`+id+`,"name":"CI","ref":"main","status":"success",`+
				`"created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z",`+
				`"finished_at":"2026-07-22T10:05:00Z","source":"push","sha":"abc",`+
				`"web_url":"https://gitlab.example/acme/api/-/pipelines/`+id+`"}`)
		}
		body = "[" + strings.Join(items, ",") + "]"
	case strings.HasSuffix(path, "/jobs"):
		body = `[{"id":11,"name":"unit","stage":"test","status":"success",` +
			`"started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:04:00Z",` +
			`"runner":{"runner_type":"instance_type"},"artifacts_file":{},"artifacts":[]}]`
	case strings.HasSuffix(path, "/test_report"):
		body = `{"test_suites":[]}`
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

// The GitLab chunked route carries the identical cumulative page counters and
// the identical re-visit shape as the GitHub route CHAOS-4130 was measured on.
// Nothing has cancelled a GitLab project yet only because none has been busy
// enough; the counters must charge a page once, on first entry, here too.
func TestGitLabTestsChunkRouteCountsEachPipelinePageOnce(t *testing.T) {
	doer := &gitLabTestsPagedDoer{t: t, pages: 2, perPage: 3}
	claim := nativeTestClaim("gitlab", "tests")
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	stop := errors.New("test continuation yield")

	resume := ""
	var cursor gitLabTestsChunkCursor
	for pass := 1; ; pass++ {
		if pass > 500 {
			t.Fatal("continuation walk never reached a final emission")
		}
		emitted := 0
		last := resume
		finalSeen := false
		err := (GitLabTestsRouteHandler{}).CollectChunks(
			context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if emission.Final {
					finalSeen = true
					return nil
				}
				emitted++
				if emitted >= 2 {
					return stop
				}
				return nil
			},
		)
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			decoded, decodeErr := decodeGitLabTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatalf("decode terminal cursor: %v", decodeErr)
			}
			cursor = decoded
			break
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v, want a continuation yield", pass, err)
		}
		resume = last
	}

	if cursor.PipelinePages != 2 {
		t.Fatalf("PipelinePages=%d, want 2 (one charge per real page, not per visit)", cursor.PipelinePages)
	}
	if cursor.ReportPages != 2 {
		t.Fatalf("ReportPages=%d, want 2 (the reports twin counts the same way)", cursor.ReportPages)
	}
	// Anti-vacuity: both phases together walk 4 real pages, so anything at or
	// below that means the walk never re-entered a page and the assertions
	// above would hold under the defect too.
	if doer.pipelineListRequests <= 4 {
		t.Fatalf("only %d pipeline listing request(s); the walk never re-entered a page", doer.pipelineListRequests)
	}
}

// GitLab's budget stop must finalize with recorded lower-bound coverage, not
// cancel. Returning ErrPaginationCapExceeded here reaches the same
// deterministic-terminal category that destroyed the GitHub units in
// CHAOS-4130 -- the project just has to be busy enough to get there.
func TestGitLabTestsChunkRouteFinalizesTruncatedInventoryInsteadOfCancelling(t *testing.T) {
	doer := &gitLabTestsPagedDoer{t: t, pages: 3, perPage: 2}
	claim := nativeTestClaim("gitlab", "tests")
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)

	// MaxPages=1 is one page of budget per phase against three real pages.
	var final CompleteRouteBatch
	terminal := ""
	emissions := 0
	err := (GitLabTestsRouteHandler{MaxPages: 1}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, "",
		func(emission ChunkRouteEmission) error {
			terminal = emission.CursorAfter
			if emission.Final {
				final = emission.Batch
				return nil
			}
			emissions++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("CollectChunks err=%v, want a finalized unit", err)
	}
	if emissions == 0 {
		t.Fatal("truncated unit committed nothing; the rows before the cap must still land")
	}
	cursor, decodeErr := decodeGitLabTestsChunkCursor(terminal)
	if decodeErr != nil {
		t.Fatalf("decode terminal cursor: %v", decodeErr)
	}
	want := map[string]bool{
		gitLabPipelineInventoryComponent: false,
		gitLabReportInventoryComponent:   false,
	}
	for _, component := range cursor.Truncated {
		if _, tracked := want[component]; tracked {
			want[component] = true
		}
	}
	for component, seen := range want {
		if !seen {
			t.Fatalf("no %s truncation recorded in %v", component, cursor.Truncated)
		}
	}
	if final.Watermark != nil {
		t.Fatalf("truncated unit advanced its watermark to %v", final.Watermark)
	}
	if complete, ok := final.Result["coverage_complete"].(bool); !ok || complete {
		t.Fatalf("coverage_complete=%v, want false", final.Result["coverage_complete"])
	}
	// The cursor's closed vocabulary must survive a round trip; an unknown
	// component has to fail closed rather than reach a coverage reader.
	poisoned, encodeErr := encodeGitLabTestsChunkCursor(gitLabTestsChunkCursor{
		Phase: "done", Truncated: []string{"whatever_inventory"},
	})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if _, err := decodeGitLabTestsChunkCursor(poisoned); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("unknown truncation component decoded with err=%v", err)
	}
}

// The cumulative-budget refusal path (remainingPageBudget returning the error
// on entry, before the paginator's own per-invocation cap can fire) is a
// DIFFERENT branch from collection.CapReached, and only a resume reaches it.
// It is the branch the CHAOS-4130 units actually died on: they never got to
// spend a request.
func TestGitLabTestsChunkRouteResumeWithExhaustedBudgetFinalizes(t *testing.T) {
	doer := &gitLabTestsPagedDoer{t: t, pages: 3, perPage: 2}
	claim := nativeTestClaim("gitlab", "tests")
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	spent, err := encodeGitLabTestsChunkCursor(gitLabTestsChunkCursor{
		Phase: "pipelines", PipelinePages: 1, ReportPages: 1, Repo: "acme/api", ProjectID: 123,
	})
	if err != nil {
		t.Fatal(err)
	}
	var final CompleteRouteBatch
	terminal := ""
	err = (GitLabTestsRouteHandler{MaxPages: 1}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), spent,
		func(emission ChunkRouteEmission) error {
			terminal = emission.CursorAfter
			if emission.Final {
				final = emission.Batch
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("resume with an exhausted cumulative budget err=%v, want a finalized unit", err)
	}
	// CHAOS-3822's invariant: an exhausted phase may not spend one more request.
	if doer.pipelineListRequests != 0 {
		t.Fatalf("exhausted budget still fetched %d listing page(s)", doer.pipelineListRequests)
	}
	cursor, decodeErr := decodeGitLabTestsChunkCursor(terminal)
	if decodeErr != nil {
		t.Fatalf("decode terminal cursor: %v", decodeErr)
	}
	if cursor.PipelinePages != 1 || cursor.ReportPages != 1 {
		t.Fatalf(
			"cumulative spend was not carried forward: PipelinePages=%d ReportPages=%d",
			cursor.PipelinePages, cursor.ReportPages,
		)
	}
	if len(cursor.Truncated) != 2 {
		t.Fatalf("both phases must record their truncation, got %v", cursor.Truncated)
	}
	if final.Watermark != nil {
		t.Fatalf("a budget-truncated unit advanced its watermark to %v", final.Watermark)
	}
}
