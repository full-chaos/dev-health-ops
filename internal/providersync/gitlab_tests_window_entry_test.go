package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabEntryFake is gitLabResumeFake with GitLab's clock: every response carries a Date header read from date (none when
// date is zero; rawDate, when set, is sent as is). step moves the clock on after each response, and a cell moves it
// forward when it changes the fake: GitLab's clock does not stand still during a walk.
type gitLabEntryFake struct {
	*gitLabResumeFake
	date    time.Time
	step    time.Duration
	rawDate string
	first   time.Time
	// insert, when armed, joins the pipelines just before the next listing request for page 2: inside one invocation,
	// a pipeline that enters the listing above the page boundary shifts the offset pages by one.
	insert *gitLabResumePipeline
	armed  bool
	// withReport answers each test_report with one suite of one case, so the reports phase writes rows.
	withReport bool
}

func (fake *gitLabEntryFake) Do(request *http.Request) (*http.Response, error) {
	if fake.armed && fake.insert != nil && strings.HasSuffix(request.URL.Path, "/pipelines") && request.URL.Query().Get("page") == "2" {
		fake.pipelines = append(fake.pipelines, *fake.insert)
		fake.armed, fake.insert = false, nil
	}
	response, err := fake.gitLabResumeFake.Do(request)
	if err != nil {
		return response, err
	}
	if fake.withReport && strings.HasSuffix(request.URL.Path, "/test_report") {
		id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/api/v4/projects/123/pipelines/"), "/test_report")
		response.Body = io.NopCloser(strings.NewReader(`{"test_suites":[{"name":"api","total_time":"1.0","test_cases":[{"name":"case-` + id +
			`","classname":"t.T","status":"success","execution_time":"1.0"}]}]}`))
	}
	switch {
	case fake.rawDate != "":
		response.Header.Set("Date", fake.rawDate)
	case !fake.date.IsZero():
		sent := fake.date.UTC().Truncate(time.Second)
		if fake.first.IsZero() {
			fake.first = sent
		}
		response.Header.Set("Date", sent.Format(http.TimeFormat))
		fake.date = fake.date.Add(fake.step)
	}
	return response, err
}

// gitLabEntryOptions shapes one gitLabEntryRun. The walk stops after the yields[i]-th emission of phase (cumulative),
// calls change(i), and resumes from that emission's after cursor, first passed through rewrite when it is set. start is
// the resume cursor of the first invocation.
type gitLabEntryOptions struct {
	claim        Claim
	normalizedAt time.Time
	phase        string
	yields       []int
	change       func(yield int)
	rewrite      func(raw string) string
	start        string
	// armAt, when > 0, arms fake's insert at the armAt-th emission of phase (cumulative).
	armAt int
	fake  *gitLabEntryFake
}

// gitLabEntryResult counts, per phase and pipeline id, the reads before and after the first change.
type gitLabEntryResult struct {
	before, after gitLabResumeReads
	final         CompleteRouteBatch
	last          string
	invocations   int
	// rows holds every written ci_pipeline_runs and testops_run_report_completeness row, by destination and run id.
	rows map[string]map[string][]string
}

func gitLabEntryRun(t *testing.T, fake *gitLabEntryFake, options gitLabEntryOptions) gitLabEntryResult {
	t.Helper()
	client := gitLabRepositoryClient(t, fake, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	fake.reportGets = map[int]int{}
	current := gitLabResumeReads{pipelines: map[int]int{}, reports: fake.reportGets}
	result := gitLabEntryResult{rows: map[string]map[string][]string{}}
	stop := errors.New("yield")
	resume, emitted, next := options.start, 0, 0
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for pass := 1; pass < 50; pass++ {
		result.invocations++
		last, finalSeen := resume, false
		err := GitLabTestsRouteHandler{}.CollectChunks(ctx, options.claim, providerfoundation.Credential{}, client, options.normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				for _, effect := range emission.Batch.Effects {
					if effect.Destination != "ci_pipeline_runs" && effect.Destination != testOpsRunReportCompletenessDestination {
						continue
					}
					for _, raw := range effect.Rows {
						var row struct {
							RunID string `json:"run_id"`
						}
						if json.Unmarshal(raw, &row) != nil {
							t.Fatalf("undecodable %s row %s", effect.Destination, raw)
						}
						if result.rows[effect.Destination] == nil {
							result.rows[effect.Destination] = map[string][]string{}
						}
						result.rows[effect.Destination][row.RunID] = append(result.rows[effect.Destination][row.RunID], string(raw))
						if effect.Destination == "ci_pipeline_runs" {
							id, _ := strconv.Atoi(row.RunID)
							current.pipelines[id]++
						}
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
					if options.armAt > 0 && emitted == options.armAt {
						options.fake.armed = true
					}
				}
				if next < len(options.yields) && emitted == options.yields[next] {
					return stop
				}
				return nil
			})
		if finalSeen {
			if err != nil {
				t.Fatalf("walk err=%v", err)
			}
			if next < len(options.yields) {
				t.Fatalf("the walk reached %d of its %d yield points", next, len(options.yields))
			}
			result.last = last
			if len(options.yields) > 0 {
				result.after = current
			} else {
				result.before = current
			}
			return result
		}
		if !errors.Is(err, stop) {
			t.Fatalf("walk err=%v", err)
		}
		if next == 0 {
			result.before = gitLabResumeReads{pipelines: maps.Clone(current.pipelines), reports: maps.Clone(current.reports)}
			clear(current.pipelines)
			clear(current.reports)
		}
		if options.change != nil {
			options.change(next)
		}
		next++
		resume = last
		if options.rewrite != nil {
			resume = options.rewrite(resume)
		}
	}
	t.Fatal("the walk never reached a final emission")
	return result
}

// gitLabEntryStored is where the next window starts: the route's final watermark through the write boundary
// (normalizeWatermarkWrite: at most BeforeAt and the completion time), or the old SinceAt when the route withholds it.
func gitLabEntryStored(claim Claim, final CompleteRouteBatch, completedAt time.Time) time.Time {
	if final.Watermark == nil {
		return *claim.SinceAt
	}
	normalized, clampLog := normalizeWatermarkWrite(*final.Watermark, claim.BeforeAt, completedAt, claim.OrgID, claim.SourceExternalID, claim.Dataset)
	clampLog()
	return normalized
}

func gitLabEntryClaim(beforeAt time.Time) Claim {
	claim := nativeTestClaim("gitlab", "tests")
	claim.BeforeAt = &beforeAt
	return claim
}

// A pipeline that enters the window listing during a walk, with an id above the resume point (LastID) and an updated_at
// at or before BeforeAt, is read by this walk or by the next window, never by neither. The same holds for a pipeline
// the walk already read (id above LastID) that is updated again inside the window: its later state is read. Two clocks
// make such an updated_at possible after the walk started: GitLab's clock behind ours (BeforeAt in the past), and an
// explicit future BeforeAt (a backfill, `--before` tomorrow). The next window starts where this unit's watermark is
// stored: the route's final watermark through the write boundary (normalizeWatermarkWrite: at most BeforeAt and the
// completion time), or the old SinceAt when the route withholds it. Within one walk no pipeline is read twice per phase.
func TestGitLabTestsAPipelineThatEntersTheWindowDuringTheWalkIsReadByThisWalkOrTheNext(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	completedAt := normalizedAt.Add(30 * time.Minute)
	type clock struct {
		name     string
		beforeAt time.Time
		// gitLab is GitLab's clock when the walk starts; changedAt is GitLab's updated_at of the change.
		gitLab, changedAt time.Time
	}
	clocks := []clock{
		{"GitLab clock 5 min behind, BeforeAt in the past", normalizedAt.Add(-time.Minute), normalizedAt.Add(-5 * time.Minute), normalizedAt.Add(-3 * time.Minute)},
		{"explicit future BeforeAt", normalizedAt.Add(24 * time.Hour), normalizedAt, normalizedAt.Add(5 * time.Minute)},
	}
	const arrival, readFirst = 1300, 1250
	type change struct {
		name   string
		target int
		apply  func(fake *gitLabEntryFake, at time.Time)
	}
	changes := []change{
		{"control: nothing changes", 0, func(*gitLabEntryFake, time.Time) {}},
		{"a new pipeline with an id above LastID", arrival, func(fake *gitLabEntryFake, at time.Time) {
			fake.pipelines = append(fake.pipelines, gitLabResumePipeline{id: arrival, started: at.Add(-time.Minute), updated: at})
		}},
		{"a read pipeline with an id above LastID updated again", readFirst, func(fake *gitLabEntryFake, at time.Time) {
			gitLabResumeUpdate(fake.gitLabResumeFake, readFirst, at)
		}},
	}
	for _, clk := range clocks {
		for _, chg := range changes {
			for _, phase := range []string{"pipelines", "reports"} {
				t.Run(clk.name+": "+chg.name+": change in the "+phase+" phase", func(t *testing.T) {
					t.Parallel()
					claim := gitLabEntryClaim(clk.beforeAt)
					fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, date: clk.gitLab}
					walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: claim, normalizedAt: normalizedAt, phase: phase, yields: []int{50},
						change: func(int) {
							chg.apply(fake, clk.changedAt)
							fake.date = clk.changedAt.Add(5 * time.Minute)
						}})
					stored := gitLabEntryStored(claim, walk.final, completedAt)
					next := claim
					nextBefore := completedAt.Add(time.Hour)
					next.SinceAt, next.BeforeAt = &stored, &nextBefore
					fake.date = completedAt
					following := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: next, normalizedAt: completedAt})
					for _, dataset := range []string{"pipelines", "reports"} {
						for id, count := range walk.before.of(dataset) {
							if count+walk.after.of(dataset)[id] > 1 {
								t.Errorf("%s: pipeline %d read %d times by one walk", dataset, id, count+walk.after.of(dataset)[id])
							}
						}
						if chg.target == 0 {
							continue
						}
						inWalk, inNext := walk.after.of(dataset)[chg.target], following.before.of(dataset)[chg.target]
						t.Logf("%s: pipeline %d after the change: this walk %d, next window %d (stored watermark %s, BeforeAt %s)",
							dataset, chg.target, inWalk, inNext, stored.Format(time.RFC3339), clk.beforeAt.Format(time.RFC3339))
						if inWalk+inNext == 0 {
							t.Errorf("%s: pipeline %d (updated_at %s) after the change is read by neither this walk nor the next window (since %s)",
								dataset, chg.target, clk.changedAt.Format(time.RFC3339), stored.Format(time.RFC3339))
						}
					}
				})
			}
		}
	}
}

// The watermark of a whole walk is min(BeforeAt, GitLab's clock at the walk start - 60 s): the Date of the FIRST response
// (the project read, before any listing), kept in the cursor across resumes. GitLab's clock moves on (1 s per response),
// so a Date read later, or read again on a resume, gives another value.
func TestGitLabTestsWatermarkStopsAtTheWalkStartOnGitLabsClock(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	future := normalizedAt.Add(24 * time.Hour)
	past := *nativeTestClaim("gitlab", "tests").BeforeAt
	for _, tc := range []struct {
		name     string
		beforeAt time.Time
		gitLab   time.Time
		phase    string
		yields   []int
		want     func(first time.Time) time.Time
	}{
		{"future BeforeAt, one invocation", future, normalizedAt.Add(10 * time.Minute), "", nil, func(first time.Time) time.Time { return first.Add(-time.Minute) }},
		{"future BeforeAt, two resumes in the pipelines phase", future, normalizedAt.Add(10 * time.Minute), "pipelines", []int{50, 150}, func(first time.Time) time.Time { return first.Add(-time.Minute) }},
		{"future BeforeAt, a resume in the reports phase", future, normalizedAt.Add(10 * time.Minute), "reports", []int{50}, func(first time.Time) time.Time { return first.Add(-time.Minute) }},
		{"GitLab clock behind, BeforeAt just past", normalizedAt.Add(-time.Minute), normalizedAt.Add(-5 * time.Minute), "pipelines", []int{50}, func(first time.Time) time.Time { return first.Add(-time.Minute) }},
		{"historic window: BeforeAt before the walk start", past, normalizedAt, "pipelines", []int{50}, func(time.Time) time.Time { return past }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, date: tc.gitLab, step: time.Second}
			walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: gitLabEntryClaim(tc.beforeAt), normalizedAt: normalizedAt, phase: tc.phase, yields: tc.yields})
			want := tc.want(fake.first)
			if walk.final.Watermark == nil || !walk.final.Watermark.Equal(want) {
				t.Fatalf("watermark=%v, want %s (first Date %s, %d invocations)", walk.final.Watermark, want.Format(time.RFC3339), fake.first.Format(time.RFC3339), walk.invocations)
			}
			if len(walk.final.Result) == 0 || walk.final.Result["coverage_complete"] != true {
				t.Fatalf("coverage_complete=%v, want a whole walk", walk.final.Result["coverage_complete"])
			}
		})
	}
}

// The margin edge (60 s, the clock skew the Date header may carry): a pipeline that enters the listing after the walk
// passed its id with an updated_at exactly 60 s before GitLab's walk-start clock is read by the next window
// (updated_after is inclusive). One second earlier is outside the margin: the route assumes nothing GitLab stamps that
// early is still unlisted when the walk reads page 1, so neither window reads it (the documented limit).
func TestGitLabTestsWindowStartMarginEdge(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	completedAt := normalizedAt.Add(30 * time.Minute)
	gitLab := normalizedAt.Add(2 * time.Minute)
	for _, tc := range []struct {
		name      string
		updatedAt time.Time
		want      int
	}{
		{"at the edge: read by the next window", gitLab.Add(-time.Minute), 1},
		{"1 s outside the margin: read by neither", gitLab.Add(-time.Minute - time.Second), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			claim := gitLabEntryClaim(normalizedAt.Add(24 * time.Hour))
			fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, date: gitLab}
			walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: claim, normalizedAt: normalizedAt, phase: "pipelines", yields: []int{50},
				change: func(int) {
					fake.pipelines = append(fake.pipelines, gitLabResumePipeline{id: 1300, started: tc.updatedAt.Add(-time.Minute), updated: tc.updatedAt})
				}})
			stored := gitLabEntryStored(claim, walk.final, completedAt)
			next := claim
			nextBefore := completedAt.Add(time.Hour)
			next.SinceAt, next.BeforeAt = &stored, &nextBefore
			following := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: next, normalizedAt: completedAt})
			if got := walk.after.pipelines[1300] + following.before.pipelines[1300]; got != tc.want {
				t.Fatalf("pipeline 1300 (updated_at %s) read %d times, want %d (stored watermark %s)",
					tc.updatedAt.Format(time.RFC3339), got, tc.want, stored.Format(time.RFC3339))
			}
		})
	}
}

// With no usable Date header on the project read (none, one that does not parse, or one so close to the epoch that the
// derived window start is below 1 s) the walk start is our clock: normalizedAt (the attempt start, before any request)
// - 60 s. The unit still advances, across a resume; it does not withhold for ever on a Date-less instance.
func TestGitLabTestsWindowStartWithoutADateUsesOurClock(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		rawDate string
	}{
		{"no Date header", ""},
		{"a Date that does not parse", "yesterday-ish"},
		{"a Date 30 s after the epoch (derived window start < 0)", time.Unix(30, 0).UTC().Format(http.TimeFormat)},
		{"a Date 60 s after the epoch (derived window start 0)", time.Unix(60, 0).UTC().Format(http.TimeFormat)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, rawDate: tc.rawDate}
			walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: gitLabEntryClaim(normalizedAt.Add(24 * time.Hour)), normalizedAt: normalizedAt,
				phase: "reports", yields: []int{50}})
			if want := normalizedAt.Add(-time.Minute); walk.final.Watermark == nil || !walk.final.Watermark.Equal(want) {
				t.Fatalf("watermark=%v, want %s", walk.final.Watermark, want.Format(time.RFC3339))
			}
		})
	}
}

// A cursor an older binary wrote has no window start ("ws"): the walk resumes as before (no error, each listed pipeline
// read once by a LastID resume), and the watermark is withheld, so the next window walks the range again rather than
// skip a pipeline that entered the listing before the new binary took over. The same holds for a done cursor.
func TestGitLabTestsCursorWithoutAWindowStartWithholdsTheWatermark(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	drop := func(t *testing.T, keys ...string) func(string) string {
		return func(raw string) string {
			var fields map[string]any
			if err := json.Unmarshal([]byte(raw), &fields); err != nil {
				t.Fatal(err)
			}
			if fields["ws"] == nil {
				t.Fatalf("the cursor has no window start to drop: %s", raw)
			}
			for _, key := range keys {
				delete(fields, key)
			}
			out, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			return string(out)
		}
	}
	for _, phase := range []string{"pipelines", "reports"} {
		for _, keys := range [][]string{{"ws"}, {"ws", "li"}} {
			t.Run(phase+" without "+strconv.Itoa(len(keys))+" keys", func(t *testing.T) {
				t.Parallel()
				fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, date: normalizedAt}
				walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: gitLabEntryClaim(normalizedAt.Add(24 * time.Hour)), normalizedAt: normalizedAt,
					phase: phase, yields: []int{50}, rewrite: drop(t, keys...)})
				if walk.final.Watermark != nil {
					t.Fatalf("watermark=%v, want withheld for a cursor without a window start", walk.final.Watermark)
				}
				if len(keys) == 1 {
					for id, count := range walk.before.of(phase) {
						if count+walk.after.of(phase)[id] != 1 {
							t.Fatalf("%s: pipeline %d read %d times", phase, id, count+walk.after.of(phase)[id])
						}
					}
				}
			})
		}
	}
	t.Run("done cursor", func(t *testing.T) {
		t.Parallel()
		fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, date: normalizedAt}
		claim := gitLabEntryClaim(normalizedAt.Add(24 * time.Hour))
		walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: claim, normalizedAt: normalizedAt})
		again := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: claim, normalizedAt: normalizedAt, start: drop(t, "ws")(walk.last)})
		if walk.final.Watermark == nil || again.final.Watermark != nil {
			t.Fatalf("watermark %v, after a done resume without a window start %v; want set, then withheld", walk.final.Watermark, again.final.Watermark)
		}
	})
}

// The done resume re-publishes the watermark of the walk that wrote the done cursor, without a provider request.
func TestGitLabTestsDoneResumeRepublishesTheWatermark(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, date: normalizedAt.Add(3 * time.Minute)}
	claim := gitLabEntryClaim(normalizedAt.Add(24 * time.Hour))
	walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: claim, normalizedAt: normalizedAt})
	fake.date = normalizedAt.Add(time.Hour)
	again := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: claim, normalizedAt: normalizedAt.Add(time.Hour), start: walk.last})
	if walk.final.Watermark == nil || again.final.Watermark == nil || !again.final.Watermark.Equal(*walk.final.Watermark) ||
		!walk.final.Watermark.Equal(normalizedAt.Add(2*time.Minute)) {
		t.Fatalf("watermark %v, after the done resume %v; want %s both times", walk.final.Watermark, again.final.Watermark,
			normalizedAt.Add(2*time.Minute).Format(time.RFC3339))
	}
}

// The first usable epoch second is accepted as GitLab's clock: a Date 61 s after the epoch gives a window start of 1 s.
func TestGitLabTestsWindowStartAtTheFirstEpochSecond(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, rawDate: time.Unix(61, 0).UTC().Format(http.TimeFormat)}
	walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: gitLabEntryClaim(normalizedAt.Add(24 * time.Hour)), normalizedAt: normalizedAt,
		phase: "pipelines", yields: []int{50}})
	if want := time.Unix(1, 0).UTC(); walk.final.Watermark == nil || !walk.final.Watermark.Equal(want) {
		t.Fatalf("watermark=%v, want %s", walk.final.Watermark, want.Format(time.RFC3339))
	}
}

// Inside one invocation the listing moves under the walk: a pipeline that enters the window above the page boundary
// shifts the offset pages by one, so the last pipeline of page 1 is listed again first on page 2 and may be read twice
// by the walk (both phases, fresh and resumed). The stored state is what counts: every destination is a
// ReplacingMergeTree on its natural key, so a second read must write the same row. After the walk each listed pipeline
// has exactly one stored row per destination (the rows written for one key are byte-equal), and the entering pipeline is
// read by this walk or by the next window.
func TestGitLabTestsAPipelineAboveThePageBoundaryLeavesOneStoredRowPerKey(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	completedAt := normalizedAt.Add(30 * time.Minute)
	for _, phase := range []string{"pipelines", "reports"} {
		for _, tc := range []struct {
			name   string
			yields []int
			armAt  int
		}{{"fresh walk", nil, 1}, {"resumed walk", []int{50}, 51}} {
			t.Run(phase+": "+tc.name, func(t *testing.T) {
				t.Parallel()
				claim := gitLabEntryClaim(normalizedAt.Add(24 * time.Hour))
				enters := gitLabResumePipeline{id: 1300, started: normalizedAt.Add(time.Minute), updated: normalizedAt.Add(2 * time.Minute)}
				fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, date: normalizedAt, insert: &enters,
					withReport: true}
				walk := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: claim, normalizedAt: normalizedAt, phase: phase, yields: tc.yields,
					armAt: tc.armAt, fake: fake})
				if fake.insert != nil {
					t.Fatal("the pipeline never entered the listing: the cell proves nothing")
				}
				destination := map[string]string{"pipelines": "ci_pipeline_runs", "reports": testOpsRunReportCompletenessDestination}[phase]
				stored := walk.rows[destination]
				twice := 0
				for runID, rows := range stored {
					for _, row := range rows[1:] {
						if row != rows[0] {
							t.Errorf("%s: run %s written twice with different rows:\n%s\n%s", destination, runID, rows[0], row)
						}
					}
					if len(rows) > 1 {
						twice++
					}
				}
				for _, pipeline := range gitLabResumeFixture() {
					if len(stored[strconv.Itoa(pipeline.id)]) == 0 {
						t.Errorf("%s: pipeline %d has no stored row", destination, pipeline.id)
					}
				}
				following := gitLabEntryRun(t, fake, gitLabEntryOptions{claim: func() Claim {
					next := claim
					since, before := gitLabEntryStored(claim, walk.final, completedAt), completedAt.Add(time.Hour)
					next.SinceAt, next.BeforeAt = &since, &before
					return next
				}(), normalizedAt: completedAt})
				if len(stored["1300"])+len(following.rows[destination]["1300"]) == 0 {
					t.Errorf("%s: the entering pipeline is read by neither this walk nor the next window", destination)
				}
				t.Logf("%s: %d keys stored, %d written twice by the walk (rows replace)", destination, len(stored), twice)
			})
		}
	}
}
