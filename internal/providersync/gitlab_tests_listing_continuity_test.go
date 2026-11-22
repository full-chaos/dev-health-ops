package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabContinuityFake is gitLabResumeFake that changes the listing just before a listing request (pending, then every), and
// records each listing request (page, per_page) and the ids it listed. emptyNextPage answers the last page with an empty
// X-Next-Page header, as GitLab does; maxListings fails the test past that many listing requests (no hang).
type gitLabContinuityFake struct {
	*gitLabResumeFake
	claim         Claim
	pending       []func()
	every         func(request, page, perPage int)
	requests      [][2]int
	ends          []int
	snapshots     [][]int
	emptyNextPage bool
	maxListings   int
	// shortPages answers every listing request with at most that many items and an X-Next-Page (a misbehaving listing).
	shortPages int
	// total counts every request the fake answered; answer, when it returns a response, answers a request in place of the
	// fake (listing is the 0-based listing request index, -1 for any other request).
	total  int
	answer func(request *http.Request, listing int) *http.Response
}

func (fake *gitLabContinuityFake) Do(request *http.Request) (*http.Response, error) {
	fake.total++
	if request.URL.Path != "/api/v4/projects/123/pipelines" {
		if fake.answer != nil {
			if response := fake.answer(request, -1); response != nil {
				return response, nil
			}
		}
		return fake.gitLabResumeFake.Do(request)
	}
	if fake.answer != nil {
		if response := fake.answer(request, len(fake.requests)); response != nil {
			page, _ := strconv.Atoi(request.URL.Query().Get("page"))
			perPage, _ := strconv.Atoi(request.URL.Query().Get("per_page"))
			fake.requests = append(fake.requests, [2]int{page, perPage})
			return response, nil
		}
	}
	if fake.maxListings > 0 && len(fake.requests) >= fake.maxListings {
		fake.t.Fatalf("more than %d listing requests: the walk does not end", fake.maxListings)
	}
	for _, change := range fake.pending {
		change()
	}
	fake.pending = nil
	page, _ := strconv.Atoi(request.URL.Query().Get("page"))
	perPage, _ := strconv.Atoi(request.URL.Query().Get("per_page"))
	if fake.every != nil {
		fake.every(len(fake.requests), page, perPage)
	}
	fake.requests = append(fake.requests, [2]int{page, perPage})
	snapshot := fake.listed()
	fake.snapshots = append(fake.snapshots, snapshot)
	fake.ends = append(fake.ends, min((page-1)*perPage+perPage, len(snapshot)))
	response, err := fake.gitLabResumeFake.Do(request)
	if response != nil && fake.emptyNextPage && response.Header.Get("X-Next-Page") == "" {
		response.Header["X-Next-Page"] = []string{""}
	}
	if response != nil && fake.shortPages > 0 {
		var items []json.RawMessage
		if err := json.NewDecoder(response.Body).Decode(&items); err != nil {
			fake.t.Fatal(err)
		}
		body, _ := json.Marshal(items[:min(len(items), fake.shortPages)])
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.Header.Set("X-Next-Page", strconv.Itoa(page+1))
	}
	return response, err
}

// listed is the window listing now, newest id first.
func (fake *gitLabContinuityFake) listed() []int {
	ids := []int{}
	for _, pipeline := range fake.list(map[string][]string{
		"order_by": {"id"}, "sort": {"desc"},
		"updated_after":  {fake.claim.SinceAt.UTC().Format(time.RFC3339Nano)},
		"updated_before": {fake.claim.BeforeAt.UTC().Format(time.RFC3339Nano)},
	}) {
		ids = append(ids, pipeline.id)
	}
	return ids
}

func (fake *gitLabContinuityFake) remove(ids ...int) {
	gitLabResumeDelete(fake.gitLabResumeFake, ids...)
}

// updatePast moves pipelines past the window's BeforeAt (a running pipeline that finishes).
func (fake *gitLabContinuityFake) updatePast(ids ...int) {
	for _, id := range ids {
		gitLabResumeUpdate(fake.gitLabResumeFake, id, fake.claim.BeforeAt.Add(time.Hour))
	}
}

// leave takes pipelines out of the window listing by kind: "delete" or "update_past_before_at".
func (fake *gitLabContinuityFake) leave(kind string, ids ...int) {
	if kind == "delete" {
		fake.remove(ids...)
		return
	}
	fake.updatePast(ids...)
}

func (fake *gitLabContinuityFake) add(ids ...int) {
	at := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	for _, id := range ids {
		fake.pipelines = append(fake.pipelines, gitLabResumePipeline{id: id, started: at, updated: at})
	}
}

// gitLabContinuityFixture is n pipelines in the window of nativeTestClaim, ids first, first+step, ...
func gitLabContinuityFixture(n, first, step int) []gitLabResumePipeline {
	base := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	pipelines := make([]gitLabResumePipeline, 0, n)
	for index := range n {
		started := base.Add(time.Duration(index) * time.Minute)
		pipelines = append(pipelines, gitLabResumePipeline{id: first + index*step, started: started, updated: started.Add(5 * time.Minute)})
	}
	return pipelines
}

// gitLabContinuityArm changes the listing just before the listing request that follows the emission-th emission of phase in
// the invocation-th invocation (1-based): the gap after the response that holds that emission.
type gitLabContinuityArm struct {
	phase      string
	invocation int
	emission   int
	change     func()
}

type gitLabContinuityOptions struct {
	handler GitLabTestsRouteHandler
	gate    providerfoundation.BackoffGate
	retry   *providerfoundation.RetryPolicy
	// allowError ends the walk at the first invocation error and returns it instead of failing the test.
	allowError bool
	// yieldPhase, yieldAt: the first invocation stops after its yieldAt-th emission of yieldPhase; the next one resumes.
	yieldPhase string
	yieldAt    int
	arms       []gitLabContinuityArm
	// now runs its changes at the emission itself, before any later request.
	now []gitLabContinuityArm
}

type gitLabContinuityResult struct {
	pipelines map[int]int
	reports   map[int]int
	rows      map[int]string
	final     CompleteRouteBatch
	done      gitLabTestsChunkCursor
	cursors   []gitLabTestsChunkCursor
	metrics   string
	err       error
}

func gitLabContinuityWalk(t *testing.T, fake *gitLabContinuityFake, options gitLabContinuityOptions) gitLabContinuityResult {
	t.Helper()
	client := gitLabRepositoryClient(t, fake, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	if options.gate != nil {
		client.Gate = options.gate
	}
	if options.retry != nil {
		client.Retry = *options.retry
	}
	fake.reportGets = map[int]int{}
	result := gitLabContinuityResult{pipelines: map[int]int{}, rows: map[int]string{}}
	stop := errors.New("yield")
	resume, finalSeen := "", false
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for invocation := 1; invocation <= 2 && !finalSeen; invocation++ {
		emitted := map[string]int{}
		err := options.handler.CollectChunks(ctx, fake.claim, providerfoundation.Credential{}, client,
			time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume,
			func(emission ChunkRouteEmission) error {
				resume = emission.CursorAfter
				after, err := decodeGitLabTestsChunkCursor(emission.CursorAfter)
				if err != nil {
					t.Fatal(err)
				}
				result.cursors = append(result.cursors, after)
				if emission.Final {
					result.final, result.done, finalSeen = emission.Batch, after, true
					return nil
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
							t.Fatalf("undecodable row %s", raw)
						}
						id, _ := strconv.Atoi(row.RunID)
						result.pipelines[id]++
						result.rows[id] = string(raw)
					}
				}
				before, err := decodeGitLabTestsChunkCursor(emission.CursorBefore)
				if err != nil {
					t.Fatal(err)
				}
				emitted[before.Phase]++
				for _, arm := range options.arms {
					if arm.phase == before.Phase && arm.invocation == invocation && arm.emission == emitted[before.Phase] {
						fake.pending = append(fake.pending, arm.change)
					}
				}
				for _, arm := range options.now {
					if arm.phase == before.Phase && arm.invocation == invocation && arm.emission == emitted[before.Phase] {
						arm.change()
					}
				}
				if invocation == 1 && before.Phase == options.yieldPhase && emitted[before.Phase] == options.yieldAt {
					return stop
				}
				return nil
			})
		if err != nil && !errors.Is(err, stop) {
			if options.allowError {
				result.err, result.reports = err, fake.reportGets
				return result
			}
			t.Fatal(err)
		}
	}
	if !finalSeen {
		t.Fatal("the walk did not reach its final emission")
	}
	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatal(err)
	}
	result.metrics = buffer.String()
	result.reports = fake.reportGets
	return result
}

// gitLabContinuityStable is every pipeline listed by every listing request of the walk.
func gitLabContinuityStable(fake *gitLabContinuityFake) []int {
	if len(fake.snapshots) == 0 {
		return nil
	}
	stable := slices.Clone(fake.snapshots[0])
	for _, snapshot := range fake.snapshots[1:] {
		stable = slices.DeleteFunc(stable, func(id int) bool { return !slices.Contains(snapshot, id) })
	}
	return stable
}

// gitLabContinuityUnread is every stable pipeline that a phase did not read.
func gitLabContinuityUnread(fake *gitLabContinuityFake, result gitLabContinuityResult) (pipelines, reports []int) {
	for _, id := range gitLabContinuityStable(fake) {
		if result.pipelines[id] == 0 {
			pipelines = append(pipelines, id)
		}
		if result.reports[id] == 0 {
			reports = append(reports, id)
		}
	}
	return pipelines, reports
}

// gitLabContinuityStepBacks is the walk's step-back count, both phases, from dev_health_provider_resume_reanchor_total.
func gitLabContinuityStepBacks(t *testing.T, metrics string) int {
	t.Helper()
	total := 0
	for _, line := range strings.Split(metrics, "\n") {
		if !strings.HasPrefix(line, `dev_health_provider_resume_reanchor_total{provider="gitlab"`) {
			continue
		}
		value, err := strconv.Atoi(line[strings.LastIndex(line, " ")+1:])
		if err != nil {
			t.Fatalf("metric line %q: %v", line, err)
		}
		total += value
	}
	return total
}

func gitLabContinuityComplete(result gitLabContinuityResult) bool {
	return result.final.Result["coverage_complete"] == true && result.final.Watermark != nil && len(result.done.Truncated) == 0
}

func newGitLabContinuityFake(t *testing.T, pipelines []gitLabResumePipeline) *gitLabContinuityFake {
	return &gitLabContinuityFake{
		gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: pipelines},
		claim:            nativeTestClaim("gitlab", "tests"), maxListings: 400,
	}
}

// A pipeline that leaves the window listing between two listing requests of one invocation (deleted, or updated past BeforeAt,
// as a running pipeline does when it finishes) moves every later pipeline up by one. The walk still reads every pipeline that
// stays listed, in both phases, on a fresh walk and on a resumed one, and claims its coverage. Shapes: the newest pipeline
// leaves (the CHAOS-14438 probe), the last read pipeline itself leaves (the boundary sits exactly at a page end), two and 101
// pipelines above the boundary leave (101 at the first and at the second gap: two step backs, after which the walk goes on
// from the step-back response, not from the older end), and a pipeline enters before one gap and another leaves before the next.
func TestGitLabTestsListingThatShrinksBetweenTwoRequestsSkipsNoPipeline(t *testing.T) {
	shapes := []struct {
		name   string
		resume []bool
		arms   func(fake *gitLabContinuityFake, phase string, invocation int, kind string) []gitLabContinuityArm
	}{
		{"newest leaves", []bool{false, true}, func(fake *gitLabContinuityFake, phase string, invocation int, kind string) []gitLabContinuityArm {
			return []gitLabContinuityArm{{phase, invocation, 1, func() { fake.leave(kind, 1250) }}}
		}},
		{"last read leaves at the page end", []bool{false, true}, func(fake *gitLabContinuityFake, phase string, invocation int, kind string) []gitLabContinuityArm {
			return []gitLabContinuityArm{{phase, invocation, 1, func() { fake.leave(kind, 1151) }}}
		}},
		{"two above the boundary leave", []bool{false}, func(fake *gitLabContinuityFake, phase string, invocation int, kind string) []gitLabContinuityArm {
			return []gitLabContinuityArm{{phase, invocation, 1, func() { fake.leave(kind, 1250, 1249) }}}
		}},
		{"101 above the boundary leave", []bool{false}, func(fake *gitLabContinuityFake, phase string, invocation int, kind string) []gitLabContinuityArm {
			ids := []int{}
			for id := 1250; id >= 1150; id-- {
				ids = append(ids, id)
			}
			return []gitLabContinuityArm{{phase, invocation, 1, func() { fake.leave(kind, ids...) }}}
		}},
		{"101 above the boundary leave at the second gap", []bool{false}, func(fake *gitLabContinuityFake, phase string, invocation int, kind string) []gitLabContinuityArm {
			ids := []int{}
			for id := 1250; id >= 1150; id-- {
				ids = append(ids, id)
			}
			return []gitLabContinuityArm{{phase, invocation, 101, func() { fake.leave(kind, ids...) }}}
		}},
		{"one enters then one leaves", []bool{false}, func(fake *gitLabContinuityFake, phase string, invocation int, kind string) []gitLabContinuityArm {
			return []gitLabContinuityArm{
				{phase, invocation, 1, func() { fake.add(1251) }},
				{phase, invocation, 101, func() { fake.leave(kind, 1250) }},
			}
		}},
	}
	for _, shape := range shapes {
		for _, phase := range []string{"pipelines", "reports"} {
			for _, resume := range shape.resume {
				for _, kind := range []string{"delete", "update_past_before_at"} {
					t.Run(fmt.Sprintf("%s/%s/resume=%t/%s", shape.name, phase, resume, kind), func(t *testing.T) {
						t.Parallel()
						fake := newGitLabContinuityFake(t, gitLabResumeFixture())
						options := gitLabContinuityOptions{arms: shape.arms(fake, phase, 1, kind)}
						if resume {
							options = gitLabContinuityOptions{yieldPhase: phase, yieldAt: 50, arms: shape.arms(fake, phase, 2, kind)}
						}
						result := gitLabContinuityWalk(t, fake, options)
						if len(fake.pending) != 0 {
							t.Fatal("a listing change never ran: the cell proves nothing")
						}
						pipelines, reports := gitLabContinuityUnread(fake, result)
						if len(pipelines)+len(reports) != 0 {
							t.Fatalf("listed through the walk and never read: pipelines phase %v, reports phase %v", pipelines, reports)
						}
						if !gitLabContinuityComplete(result) {
							t.Fatalf("coverage not claimed: complete=%v watermark=%v truncated=%v",
								result.final.Result["coverage_complete"], result.final.Watermark, result.done.Truncated)
						}
					})
				}
			}
		}
	}
}

// Controls: a stable listing, a pipeline below the boundary that leaves, and a pipeline above it that leaves while another enters
// in the same gap (the boundary does not move) are walked whole, with each listed pipeline read once per phase.
func TestGitLabTestsListingContinuityControls(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(fake *gitLabContinuityFake)
	}{
		{"stable", nil},
		{"below the boundary leaves", func(fake *gitLabContinuityFake) { fake.remove(1100) }},
		{"one leaves and one enters in one gap", func(fake *gitLabContinuityFake) { fake.remove(1250); fake.add(1251) }},
	} {
		for _, phase := range []string{"pipelines", "reports"} {
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				fake := newGitLabContinuityFake(t, gitLabResumeFixture())
				options := gitLabContinuityOptions{}
				if tc.change != nil {
					options.arms = []gitLabContinuityArm{{phase, 1, 1, func() { tc.change(fake) }}}
				}
				result := gitLabContinuityWalk(t, fake, options)
				pipelines, reports := gitLabContinuityUnread(fake, result)
				if len(pipelines)+len(reports) != 0 || !gitLabContinuityComplete(result) {
					t.Fatalf("unread pipelines %v reports %v, complete=%v", pipelines, reports, gitLabContinuityComplete(result))
				}
				if tc.change == nil {
					for _, id := range gitLabContinuityStable(fake) {
						if result.pipelines[id] != 1 || result.reports[id] != 1 {
							t.Fatalf("pipeline %d read %d/%d times, want once per phase", id, result.pipelines[id], result.reports[id])
						}
					}
				}
			})
		}
	}
}

// Seeded interleavings: at random listing requests, deletes, updates past BeforeAt and pipelines that enter (new ones, and old
// ones updated into the window), at random offsets above and below the boundary, both phases, fresh and resumed. Every pipeline
// listed by every listing request of the walk is read by both phases; otherwise the unit claims no coverage. Bursts of 101 to
// 120 removals move the boundary by more than a page. The step backs stay within what the moves explain.
func TestGitLabTestsListingContinuitySeededInterleavings(t *testing.T) {
	// Seed 29 holds a burst above the boundary that needs two step backs in one gap.
	for _, seed := range []uint64{1, 2, 3, 4, 5, 29} {
		for _, resume := range []string{"", "pipelines", "reports"} {
			t.Run(fmt.Sprintf("seed=%d/resume=%q", seed, resume), func(t *testing.T) {
				t.Parallel()
				random := rand.New(rand.NewPCG(seed, 14438))
				pipelines := gitLabContinuityFixture(320, 2, 2)
				outside := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
				for id := 3; id < 640; id += 2 {
					if random.IntN(6) == 0 {
						pipelines = append(pipelines, gitLabResumePipeline{id: id, started: outside, updated: outside})
					}
				}
				fake := newGitLabContinuityFake(t, pipelines)
				next, changes, bursts, allowed := 1000, 0, 0, 0
				fake.every = func(request, _, _ int) {
					if request == 0 || changes >= 40 || random.IntN(3) == 0 {
						return
					}
					removed := 0
					defer func() {
						if removed > 0 {
							allowed += 2 + removed/nativePerPage
						}
					}()
					for range 1 + random.IntN(3) {
						listed := fake.listed()
						if len(listed) == 0 {
							return
						}
						changes++
						switch random.IntN(6) {
						case 5:
							if bursts >= 2 || len(listed) < 150 {
								continue
							}
							bursts++
							from, count := random.IntN(len(listed)-120), 101+random.IntN(20)
							removed += count
							fake.remove(listed[from : from+count]...)
						case 0, 1:
							removed++
							fake.remove(listed[random.IntN(len(listed))])
						case 2:
							removed++
							fake.updatePast(listed[random.IntN(len(listed))])
						case 3:
							fake.add(next)
							next++
						default:
							for index := range fake.pipelines {
								if fake.pipelines[index].updated.Equal(outside) && random.IntN(4) == 0 {
									fake.pipelines[index].updated = time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
									break
								}
							}
						}
					}
				}
				result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{yieldPhase: resume, yieldAt: 150})
				unreadPipelines, unreadReports := gitLabContinuityUnread(fake, result)
				if len(unreadPipelines)+len(unreadReports) != 0 && gitLabContinuityComplete(result) {
					t.Fatalf("coverage claimed with stable pipelines unread: pipelines phase %v, reports phase %v (%d changes)",
						unreadPipelines, unreadReports, changes)
				}
				// A gap that removes r pipelines moves the boundary back by at most r: at most 2 + r/100 step backs. More means
				// the walk lost its place.
				if stepBacks := gitLabContinuityStepBacks(t, result.metrics); stepBacks > allowed {
					t.Fatalf("%d step backs for listing moves that allow %d (%d changes)", stepBacks, allowed, changes)
				}
			})
		}
	}
}

// The request shape and the page charge on a stable listing: after page 1 every request holds the last listed item and the items
// past it, per_page picked per request; the grid page charge is ceil(N/100) per phase as with 100-item pages. With no
// X-Next-Page a full last response is followed by one more request (the paginator's full-page rule): 12 for N=1000.
func TestGitLabTestsListingRequestShapeAndPageCharge(t *testing.T) {
	for _, tc := range []struct {
		n     int
		want  [][2]int
		count int
		pages int
	}{
		{80, [][2]int{{1, 100}}, 1, 1},
		{250, [][2]int{{1, 100}, {2, 99}, {3, 98}}, 3, 3},
		{1000, nil, 12, 10},
	} {
		t.Run(strconv.Itoa(tc.n), func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabContinuityFixture(tc.n, 1001, 1))
			result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
			if len(fake.requests) != 2*tc.count {
				t.Fatalf("listing requests %v, want %d per phase", fake.requests, tc.count)
			}
			if tc.want != nil && (!slices.Equal(fake.requests[:tc.count], tc.want) || !slices.Equal(fake.requests[tc.count:], tc.want)) {
				t.Fatalf("listing requests %v, want %v per phase", fake.requests, tc.want)
			}
			if result.done.PipelinePages != tc.pages || result.done.ReportPages != tc.pages {
				t.Fatalf("charged pages %d/%d, want %d", result.done.PipelinePages, result.done.ReportPages, tc.pages)
			}
			for _, id := range gitLabContinuityStable(fake) {
				if result.pipelines[id] != 1 || result.reports[id] != 1 {
					t.Fatalf("pipeline %d read %d/%d times, want once per phase", id, result.pipelines[id], result.reports[id])
				}
			}
		})
	}
}

// The step-back bound: a listing that shrinks above the boundary by the request's overlap before every forward request makes
// the walk step back again and again. The phase stops after gitLabTestsMaxListingStepBacks (30) step backs and records its
// inventory component: coverage_complete false, no watermark. A listing that moves 5 times is walked whole. Neither can hold the
// walk in a loop.
func TestGitLabTestsListingStepBackBound(t *testing.T) {
	if gitLabTestsMaxListingStepBacks != 30 {
		t.Fatalf("gitLabTestsMaxListingStepBacks = %d, want 30", gitLabTestsMaxListingStepBacks)
	}
	for _, tc := range []struct {
		name   string
		moves  int
		capped bool
	}{{"moves past the bound", 1000, true}, {"moves 5 times", 5, false}} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabContinuityFixture(600, 1, 1))
			moves := 0
			fake.every = func(request, page, perPage int) {
				if request == 0 || perPage == nativePerPage || moves >= tc.moves {
					return
				}
				overlap := fake.ends[request-1] - (page-1)*perPage
				listed := fake.listed()
				if overlap < 1 || len(listed) < overlap {
					return
				}
				moves++
				fake.remove(listed[:overlap]...)
			}
			result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
			stepBacks := strings.Count(result.metrics, `dev_health_provider_resume_reanchor_total{provider="gitlab",dataset="tests",phase="pipelines"} 30`)
			if !tc.capped {
				pipelines, reports := gitLabContinuityUnread(fake, result)
				if !gitLabContinuityComplete(result) || len(pipelines)+len(reports) != 0 {
					t.Fatalf("complete=%v unread %v %v", gitLabContinuityComplete(result), pipelines, reports)
				}
				return
			}
			if result.final.Result["coverage_complete"] != false || result.final.Watermark != nil ||
				!slices.Contains(result.done.Truncated, gitLabPipelineInventoryComponent) || stepBacks != 1 {
				t.Fatalf("complete=%v watermark=%v truncated=%v pipelines step backs 30 counted=%v:\n%s",
					result.final.Result["coverage_complete"], result.final.Watermark, result.done.Truncated, stepBacks == 1, result.metrics)
			}
		})
	}
}

// Growth at every request (a backfill whose BeforeAt is ahead of GitLab's clock): new pipelines keep entering above the boundary,
// offsets keep growing, and the page budget ends the phase with its inventory component and no watermark.
func TestGitLabTestsListingThatKeepsGrowingEndsAtThePageBudget(t *testing.T) {
	fake := newGitLabContinuityFake(t, gitLabContinuityFixture(400, 1, 1))
	next := 100000
	fake.every = func(request, _, _ int) {
		if request == 0 {
			return
		}
		for range 150 {
			fake.add(next)
			next++
		}
	}
	result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{handler: GitLabTestsRouteHandler{MaxPages: 3}})
	if result.final.Watermark != nil || !slices.Contains(result.done.Truncated, gitLabPipelineInventoryComponent) ||
		result.done.PipelinePages != 3 {
		t.Fatalf("watermark=%v truncated=%v pipeline pages=%d", result.final.Watermark, result.done.Truncated, result.done.PipelinePages)
	}
}

// The request rule for every end offset the budget allows: the request holds end-1, per_page is 1..100, the page is the
// start's page on that per_page grid, it stays within the limit and reads at least 49 offsets past end (65 from end 100 on).
func TestGitLabTestsListingRequestHoldsTheLastListedOffset(t *testing.T) {
	const limit = 3000
	for end := 1; end <= limit; end++ {
		page, perPage, start, ok := gitLabTestsListingRequest(end, limit)
		if end == limit {
			if ok {
				t.Fatalf("end=%d: a request past the limit (page %d per_page %d)", end, page, perPage)
			}
			continue
		}
		gain, floor := start+perPage-end, 49
		if end >= 100 {
			floor = 65
		}
		if !ok || perPage < 1 || perPage > nativePerPage || start%perPage != 0 || page != start/perPage+1 ||
			start > end-1 || start+perPage <= end-1 || start+perPage > limit || (gain < floor && start+perPage < limit) {
			t.Fatalf("end=%d: page=%d per_page=%d start=%d ok=%v gain=%d", end, page, perPage, start, ok, gain)
		}
	}
	if page, perPage, start, ok := gitLabTestsListingRequest(0, limit); !ok || page != 1 || perPage != 100 || start != 0 {
		t.Fatalf("first request page=%d per_page=%d start=%d ok=%v", page, perPage, start, ok)
	}
}

// A cursor main wrote before this change (100-item pages: Page, Index, li, ws; and a legacy one without li) resumes with no
// conflict and every remaining pipeline read; a cursor this binary writes keeps that form.
func TestGitLabTestsListingResumesACursorAnOlderBinaryWrote(t *testing.T) {
	ws := time.Date(2026, 8, 14, 11, 59, 0, 0, time.UTC).Unix()
	for _, tc := range []struct {
		name   string
		cursor string
		from   int
	}{
		{"page end", fmt.Sprintf(`{"phase":"pipelines","page":2,"pipelines":100,"jobs":100,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":101,"pages":1,"pipeline_pages":1,"report_pages":0,"li":1151,"ws":%d}`, ws), 1150},
		{"mid page", fmt.Sprintf(`{"phase":"pipelines","page":2,"index":30,"pipelines":130,"jobs":130,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":132,"pages":2,"pipeline_pages":2,"report_pages":0,"li":1121,"ws":%d}`, ws), 1120},
		{"legacy", fmt.Sprintf(`{"phase":"pipelines","page":2,"index":30,"pipelines":130,"jobs":130,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":132,"pages":2,"pipeline_pages":2,"report_pages":0,"ws":%d}`, ws), 1250},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabResumeFixture())
			client := gitLabRepositoryClient(t, fake, "https://gitlab.example")
			reads := map[int]int{}
			var final ChunkRouteEmission
			err := GitLabTestsRouteHandler{}.CollectChunks(context.Background(), fake.claim, providerfoundation.Credential{}, client,
				time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), tc.cursor, func(emission ChunkRouteEmission) error {
					if emission.Final {
						final = emission
					}
					for _, effect := range emission.Batch.Effects {
						if effect.Destination != "ci_pipeline_runs" {
							continue
						}
						for _, raw := range effect.Rows {
							var row struct {
								RunID string `json:"run_id"`
							}
							_ = json.Unmarshal(raw, &row)
							id, _ := strconv.Atoi(row.RunID)
							reads[id]++
						}
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			for id := tc.from; id >= 1001; id-- {
				if reads[id] == 0 {
					t.Fatalf("pipeline %d not read after the resume", id)
				}
			}
			if final.Batch.Result["coverage_complete"] != true || final.Batch.Watermark == nil {
				t.Fatalf("complete=%v watermark=%v", final.Batch.Result["coverage_complete"], final.Batch.Watermark)
			}
		})
	}
	fake := newGitLabContinuityFake(t, gitLabResumeFixture())
	result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
	for _, cursor := range result.cursors {
		if cursor.Phase == "pipelines" && cursor.Pipelines == 100 && (cursor.Page != 2 || cursor.Index != 0 || cursor.LastID != 1151) {
			t.Fatalf("cursor after 100 pipelines: page=%d index=%d li=%d, want 2/0/1151", cursor.Page, cursor.Index, cursor.LastID)
		}
		if cursor.Phase == "pipelines" && cursor.Pipelines == 130 && (cursor.Page != 2 || cursor.Index != 30 || cursor.LastID != 1121) {
			t.Fatalf("cursor after 130 pipelines: page=%d index=%d li=%d, want 2/30/1121", cursor.Page, cursor.Index, cursor.LastID)
		}
	}
}

// The end of the listing at an odd per_page: the last response is partial at per_page 98 and GitLab sends an empty X-Next-Page,
// which ends the walk: no request after it. A full last response with an empty X-Next-Page costs one more request, which holds
// only the last listed pipeline and ends the walk.
func TestGitLabTestsListingEndsOnAnEmptyNextPageAtAnOddPerPage(t *testing.T) {
	for _, tc := range []struct {
		n    int
		want [][2]int
	}{
		{205, [][2]int{{1, 100}, {2, 99}, {3, 98}}},
		{294, [][2]int{{1, 100}, {2, 99}, {3, 98}, {4, 97}}},
	} {
		t.Run(strconv.Itoa(tc.n), func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabContinuityFixture(tc.n, 1001, 1))
			fake.emptyNextPage = true
			result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
			if !slices.Equal(fake.requests[:len(tc.want)], tc.want) || len(fake.requests) != 2*len(tc.want) {
				t.Fatalf("listing requests %v, want %v per phase", fake.requests, tc.want)
			}
			pipelines, reports := gitLabContinuityUnread(fake, result)
			if len(pipelines)+len(reports) != 0 || !gitLabContinuityComplete(result) {
				t.Fatalf("unread %v %v complete=%v", pipelines, reports, gitLabContinuityComplete(result))
			}
		})
	}
}

// A pipeline that enters the listing above the boundary inside one invocation and lands past every listed offset is read by
// THIS walk (the CHAOS-14383 rule), not deferred to the next window.
func TestGitLabTestsListingReadsAPipelineThatEntersPastTheListedOffsets(t *testing.T) {
	for _, phase := range []string{"pipelines", "reports"} {
		t.Run(phase, func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabContinuityFixture(150, 2, 2))
			fake.add(103)
			fake.pipelines[len(fake.pipelines)-1].updated = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
			result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{arms: []gitLabContinuityArm{{phase, 1, 1, func() {
				fake.pipelines[len(fake.pipelines)-1].updated = time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
				fake.add(1001, 1003)
			}}}})
			reads := result.pipelines
			if phase == "reports" {
				reads = result.reports
			}
			if reads[103] == 0 {
				t.Fatal("the entering pipeline 103 was not read by this walk")
			}
		})
	}
}

// A listing that answers every request with a short page and an X-Next-Page (GitLab says more, but the offsets never move
// forward) cannot hold the walk in a loop: the stalled requests count against the step-back bound and the phase ends without
// coverage.
func TestGitLabTestsListingThatNeverMovesForwardEnds(t *testing.T) {
	fake := newGitLabContinuityFake(t, gitLabResumeFixture())
	fake.shortPages = 2
	result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
	if result.final.Watermark != nil || result.final.Result["coverage_complete"] != false ||
		!slices.Contains(result.done.Truncated, gitLabPipelineInventoryComponent) {
		t.Fatalf("watermark=%v complete=%v truncated=%v", result.final.Watermark, result.final.Result["coverage_complete"], result.done.Truncated)
	}
}

// A resume after pipelines entered above the resume point (the listing grew between two invocations) reads every pipeline once:
// what the growth pushes past the resume offset was read before the yield (ids at or above the resume-time id are not read).
func TestGitLabTestsListingResumeAfterGrowthReadsEachPipelineOnce(t *testing.T) {
	for _, phase := range []string{"pipelines", "reports"} {
		t.Run(phase, func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabResumeFixture())
			growth := func() {
				for id := 1300; id < 1330; id++ {
					fake.add(id)
				}
			}
			result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{yieldPhase: phase, yieldAt: 150,
				arms: []gitLabContinuityArm{{phase, 1, 150, growth}}})
			if len(fake.pending) != 0 || len(fake.pipelines) != 280 {
				t.Fatal("the listing never grew: the cell proves nothing")
			}
			reads := result.pipelines
			if phase == "reports" {
				reads = result.reports
			}
			for id := 1001; id <= 1250; id++ {
				if reads[id] != 1 {
					t.Fatalf("pipeline %d read %d times in the %s phase, want once", id, reads[id], phase)
				}
			}
		})
	}
}

// gitLabContinuityRequests is the request count the unit reports, three ways: the cursor, the fetch evidence and the
// provider_usage observation.
func gitLabContinuityRequests(t *testing.T, result gitLabContinuityResult) int {
	t.Helper()
	usage, _ := result.final.Result["observations"].(map[string]any)["provider_usage"].([]any)
	if len(usage) != 1 {
		t.Fatalf("provider_usage %v", result.final.Result["observations"])
	}
	observed, _ := usage[0].(map[string]any)["request_count"].(int)
	if observed != result.done.Requests || result.final.Evidence.Requests != result.done.Requests {
		t.Fatalf("request counts differ: cursor %d evidence %d observation %d", result.done.Requests, result.final.Evidence.Requests, observed)
	}
	return result.done.Requests
}

// An empty response holds no offset, so it charges no page of the budget; it is still a request and is counted. Cells: an
// empty project; an empty reports phase after a non-empty pipelines phase; and a listing that becomes empty at a later offset
// (the last response is full and says more, the listing shrinks, the next response is empty): ceil(N/100) pages for the N
// pipelines it holds.
func TestGitLabTestsListingEmptyResponseChargesNoPageButCountsAsARequest(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		pipelines                []gitLabResumePipeline
		change                   func(fake *gitLabContinuityFake, request int)
		pipelinePages, reports   int
		pipelineList, reportList int
	}{
		{"empty project", nil, nil, 0, 0, 1, 1},
		{"empty reports phase", gitLabContinuityFixture(150, 1001, 1), func(fake *gitLabContinuityFake, request int) {
			if request == 2 {
				for id := 1001; id <= 1150; id++ {
					fake.updatePast(id)
				}
			}
		}, 2, 0, 2, 1},
		{"empty at a later offset", gitLabContinuityFixture(200, 1001, 1), func(fake *gitLabContinuityFake, request int) {
			if request == 2 {
				fake.remove(1001, 1002, 1003, 1004, 1005)
			}
		}, 2, 2, 4, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGitLabContinuityFake(t, tc.pipelines)
			if tc.change != nil {
				fake.every = func(request, _, _ int) { tc.change(fake, request) }
			}
			result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
			requests := gitLabContinuityRequests(t, result)
			if result.done.PipelinePages != tc.pipelinePages || result.done.ReportPages != tc.reports {
				t.Fatalf("page charge pipelines/reports = %d/%d, want %d/%d", result.done.PipelinePages, result.done.ReportPages, tc.pipelinePages, tc.reports)
			}
			if len(fake.requests) != tc.pipelineList+tc.reportList {
				t.Fatalf("listing requests %v, want %d + %d", fake.requests, tc.pipelineList, tc.reportList)
			}
			if requests != fake.total {
				t.Fatalf("requests counted %d, the provider answered %d (every listing request, the empty ones too)", requests, fake.total)
			}
			if !gitLabContinuityComplete(result) {
				t.Fatalf("complete=%v", gitLabContinuityComplete(result))
			}
		})
	}
}

// At the page budget the walk sends at most ONE request past it per phase per invocation: the charged grid page that holds the
// last listed offset, to find a shrink or the end. It is counted like every request. With a listing that goes on, the phase
// ends at the budget (inventory component, no watermark). A listing that shrinks under that request makes the walk step back
// and read on, and the next time no request fits it stops without a second one (the reports phase starts next: (1,100)).
func TestGitLabTestsListingAtThePageBudgetSendsOneCountedRequest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		n        int
		maxPages int
		change   func(fake *gitLabContinuityFake, request int)
		want     [][2]int
	}{
		{"steady", 300, 1, nil, [][2]int{{1, 100}, {1, 100}}},
		{"shrinks under the request past the budget", 400, 2, func(fake *gitLabContinuityFake, request int) {
			if request == 3 {
				ids := []int{}
				for id := 1400; id > 1299; id-- {
					ids = append(ids, id)
				}
				fake.remove(ids...)
			}
		}, [][2]int{{1, 100}, {2, 99}, {2, 100}, {2, 100}, {1, 100}, {2, 99}, {2, 100}, {1, 100}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabContinuityFixture(tc.n, 1001, 1))
			if tc.change != nil {
				fake.every = func(request, _, _ int) { tc.change(fake, request) }
			}
			result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{handler: GitLabTestsRouteHandler{MaxPages: tc.maxPages}})
			if len(fake.requests) < len(tc.want) || !slices.Equal(fake.requests[:len(tc.want)], tc.want) {
				t.Fatalf("pipelines phase listing requests %v, want %v first", fake.requests, tc.want)
			}
			if tc.change == nil && !slices.Equal(fake.requests[len(tc.want):], tc.want) {
				t.Fatalf("reports phase listing requests %v, want %v", fake.requests[len(tc.want):], tc.want)
			}
			if result.final.Watermark != nil || !slices.Contains(result.done.Truncated, gitLabPipelineInventoryComponent) ||
				result.done.PipelinePages != tc.maxPages {
				t.Fatalf("watermark=%v truncated=%v pipeline pages=%d", result.final.Watermark, result.done.Truncated, result.done.PipelinePages)
			}
			if requests := gitLabContinuityRequests(t, result); requests != fake.total {
				t.Fatalf("requests counted %d, the provider answered %d", requests, fake.total)
			}
		})
	}
}

// gitLabContinuityGate is a backoff gate: armed, it tells every request to wait an hour (past GateWaitMax).
type gitLabContinuityGate struct{ armed, penalized bool }

func (gate *gitLabContinuityGate) Wait(context.Context) (time.Duration, error) {
	if gate.armed {
		return time.Hour, nil
	}
	return 0, nil
}

func (gate *gitLabContinuityGate) Penalize(context.Context, time.Duration) error {
	gate.armed, gate.penalized = true, true
	return nil
}

// The request past the page budget takes the client's rate-limit path like every request: with the backoff gate armed (another
// request saw a 429 or RateLimit-Remaining 0), or with the last request before it answered 403 + RateLimit-Remaining 0, it is
// not sent and the invocation ends rate limited; when it is answered 429 itself, the gate is armed and nothing follows it.
func TestGitLabTestsListingAtThePageBudgetRequestObeysTheRateLimitPath(t *testing.T) {
	limited := func(status int, header string) *http.Response {
		response := &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"message":"rate limited"}`))}
		response.Header.Set(header, "0")
		return response
	}
	for _, tc := range []struct {
		name   string
		arm    bool
		answer func(request *http.Request, listing int) *http.Response
		want   int
	}{
		{"gate armed before it", true, nil, 1},
		{"403 with RateLimit-Remaining 0 before it", false, func(request *http.Request, listing int) *http.Response {
			if request.URL.Path == "/api/v4/projects/123/pipelines/1201/jobs" {
				return limited(http.StatusForbidden, "RateLimit-Remaining")
			}
			return nil
		}, 1},
		{"429 on it", false, func(_ *http.Request, listing int) *http.Response {
			if listing == 1 {
				return limited(http.StatusTooManyRequests, "RateLimit-Remaining")
			}
			return nil
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGitLabContinuityFake(t, gitLabContinuityFixture(300, 1001, 1))
			fake.answer = tc.answer
			gate := &gitLabContinuityGate{}
			options := gitLabContinuityOptions{handler: GitLabTestsRouteHandler{MaxPages: 1}, gate: gate, allowError: true}
			if tc.arm {
				// The 100th pipeline is the last of the budgeted page: the next request is the one past the budget.
				options.now = []gitLabContinuityArm{{"pipelines", 1, 100, func() { gate.armed = true }}}
			}
			result := gitLabContinuityWalk(t, fake, options)
			var providerErr *providerfoundation.ProviderError
			if !errors.As(result.err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
				t.Fatalf("err=%v, want a rate-limited end", result.err)
			}
			if len(fake.requests) != tc.want {
				t.Fatalf("listing requests %v, want %d: no request past the budget once rate limited", fake.requests, tc.want)
			}
			if tc.name == "429 on it" && !gate.penalized {
				t.Fatal("the 429 did not arm the gate")
			}
		})
	}
}

// A pipeline that enters the listing past the listed offsets in a response the walk discards (a shrink above the boundary in the
// same gap), and that updates past BeforeAt before the next request, is not read by this walk: its updated_at is now after the
// window, so the next window (since = the stored watermark) lists it and reads it in its newer state. Across both windows it is
// read exactly once per phase.
func TestGitLabTestsListingAPipelineDroppedWithAStepBackIsReadByTheNextWindow(t *testing.T) {
	const entrant = 1281
	fake := newGitLabContinuityFake(t, gitLabContinuityFixture(250, 1002, 2))
	outside := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	fake.pipelines = append(fake.pipelines, gitLabResumePipeline{id: entrant, started: outside, updated: outside})
	newer := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	fake.every = func(request, _, _ int) {
		switch request {
		case 1:
			// Two newer pipelines leave (the response is discarded) and the entrant enters at offset 108, past the 100 listed.
			gitLabResumeUpdate(fake.gitLabResumeFake, entrant, time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC))
			fake.remove(1500, 1498)
		case 2:
			for index := range fake.pipelines {
				if fake.pipelines[index].id == entrant {
					fake.pipelines[index].started, fake.pipelines[index].updated = newer, newer
				}
			}
		}
	}
	first := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
	if fake.requests[1] != [2]int{2, 99} || fake.requests[2] != [2]int{1, 100} || first.final.Watermark == nil {
		t.Fatalf("first window did not step back after (2,99): requests %v watermark %v", fake.requests, first.final.Watermark)
	}
	if offset := slices.Index(fake.snapshots[1], entrant); offset < 100 || offset >= 198 || slices.Contains(fake.snapshots[2], entrant) {
		t.Fatalf("the entrant was not at an offset past the 100 listed in the discarded response (offset %d), or was still listed after", offset)
	}
	nextBefore := time.Date(2026, 8, 14, 11, 0, 0, 0, time.UTC)
	watermark := gitLabEntryStored(fake.claim, first.final, nextBefore)
	next := fake.claim
	next.Mode, next.SinceAt, next.BeforeAt = "incremental", &watermark, &nextBefore
	fake.claim, fake.every, fake.requests, fake.snapshots, fake.ends = next, nil, nil, nil, nil
	second := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{})
	if got := first.pipelines[entrant] + second.pipelines[entrant]; got != 1 {
		t.Fatalf("entrant read %d times in the pipelines phase across both windows (first %d, next %d), want 1", got, first.pipelines[entrant], second.pipelines[entrant])
	}
	if got := first.reports[entrant] + second.reports[entrant]; got != 1 {
		t.Fatalf("entrant read %d times in the reports phase across both windows, want 1", got)
	}
	if !strings.Contains(second.rows[entrant], newer.Format("2006-01-02T15:04:05")) {
		t.Fatalf("the next window did not read the newer state: %s", second.rows[entrant])
	}
}

// The request past the page budget is ONE logical request under the client's shared retry policy (providerfoundation
// DefaultRetryPolicy, 3 attempts): a 503 then a 200 is two wire sends, both counted, and the walk sends no other request
// past the budget.
func TestGitLabTestsListingAtThePageBudgetRequestRetriesUnderTheSharedPolicy(t *testing.T) {
	fake := newGitLabContinuityFake(t, gitLabContinuityFixture(300, 1001, 1))
	failed := false
	fake.answer = func(_ *http.Request, listing int) *http.Response {
		if listing != 1 || failed {
			return nil
		}
		failed = true
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"message":"unavailable"}`))}
	}
	retry := providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}
	result := gitLabContinuityWalk(t, fake, gitLabContinuityOptions{handler: GitLabTestsRouteHandler{MaxPages: 1}, retry: &retry})
	// Pipelines phase: (1,100), then the one request past the budget sent twice (503, 200). Reports phase: (1,100) and its one.
	want := [][2]int{{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}}
	if !failed || !slices.Equal(fake.requests, want) {
		t.Fatalf("listing sends %v, want %v", fake.requests, want)
	}
	if requests := gitLabContinuityRequests(t, result); requests != fake.total {
		t.Fatalf("requests counted %d, the provider answered %d (both sends of the retried request)", requests, fake.total)
	}
	if result.final.Watermark != nil || !slices.Contains(result.done.Truncated, gitLabPipelineInventoryComponent) || result.done.PipelinePages != 1 {
		t.Fatalf("watermark=%v truncated=%v pipeline pages=%d", result.final.Watermark, result.done.Truncated, result.done.PipelinePages)
	}
}
