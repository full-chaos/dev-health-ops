package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
)

// The deletion-shift classes of CHAOS-14385 (Codex r1 on the re-seek credit): a run deleted before the resume point can
// shift what an uninterrupted walk of the surviving runs reads at a budget edge in ways the re-seek credit does not see.
//   - charged: the records already charged to the deleted run stay charged, so a walk at the record bound reads fewer
//     survivors than the uninterrupted walk of the survivors.
//   - peel: a deletion in the overlap of a peeled range moves where the uninterrupted walk of the survivors peels.
// The contract of this change is that a re-seek never reads LESS than main and closes the two CHAOS-14316 shapes; these
// cells measure each shape and assert this binary leaves at most as many survivors unread as main does (deletionShiftMain:
// the same cells run on the stacked base). They are the measured repro of the follow-up tickets.

// deletionShiftListing is one listing the classes reach: the runs range, the deep range, the artifacts range, the relist.
type deletionShiftListing struct {
	name    string
	claim   Claim
	in      func(githubTestsChunkCursor) bool
	reports bool // the listing reads artifacts (its records are report rows)
	deep    bool
}

// deletionShiftRaceListings is the listings a measured cell walks: all four, or under -race (the walks of 1,120 runs cost
// minutes there; the full matrix runs in the plain test legs) the runs listing only, as cutDifferentialRace does.
func deletionShiftRaceListings() []deletionShiftListing {
	if cutDifferentialRace {
		return deletionShiftListings()[:1]
	}
	return deletionShiftListings()
}

// deletionShiftFlags is the CoalesceRuns values a measured cell runs with: off (the shipped default) and on, or under
// -race off only.
func deletionShiftFlags() []bool {
	if cutDifferentialRace {
		return []bool{false}
	}
	return []bool{false, true}
}

// deletionShiftRaceSkip skips the executor modes and the first-range deletions under -race (they cost minutes there; the
// plain test legs run them).
func deletionShiftRaceSkip(t *testing.T, mode string) {
	if cutDifferentialRace && (strings.HasPrefix(mode, "executor") || strings.HasSuffix(mode, "first_range")) {
		t.Skip("run in the plain test legs; -race keeps the route modes")
	}
}

func deletionShiftListings() []deletionShiftListing {
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	deep := lateRerunMidnightClaim("incremental")
	return []deletionShiftListing{
		{"runs", window, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && !c.LateRuns }, false, false},
		{"deep_runs", deep, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && c.LateRuns }, false, true},
		{"artifacts", window, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsArtifactsPhase && c.Relist == 0 }, true, false},
		{"deep_reports", deep, func(c githubTestsChunkCursor) bool {
			return c.Phase == githubTestsArtifactsPhase && c.Relist == githubTestsRelistWalking
		}, true, true},
	}
}

// runs builds count runs, one per created second (deep: in the deep range), ids first.., created ascending with the id.
func (listing deletionShiftListing) runs(first, count int) []listingRun {
	if listing.deep {
		since := listing.claim.SinceAt.UTC()
		runs := lateRerunRuns(first, count, since)
		for index := range runs {
			runs[index].created = since.Add(-72*time.Hour + time.Duration(index)*time.Second)
		}
		return runs
	}
	return makeListingRuns(first, count, listingSince.Add(time.Minute), time.Second)
}

func (listing deletionShiftListing) reads(fake *listingFake) map[int]int {
	if listing.reports {
		return fake.artifactGets
	}
	return fake.jobGets
}

// deletionShiftDoer serves a listingFake, with one JUnit report artifact per run when withReports.
func deletionShiftDoer(t *testing.T, fake *listingFake, withReports bool) providerfoundation.HTTPDoer {
	if withReports {
		return resumeReportsDoer{fake, githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})}
	}
	return fake
}

// deletionShiftPlainWalk walks to the final emission without a yield and returns the final batch.
func deletionShiftPlainWalk(t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer) CompleteRouteBatch {
	t.Helper()
	var final CompleteRouteBatch
	err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, fakehttp.Client(doer)),
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(emission ChunkRouteEmission) error {
			if emission.Final {
				final = emission.Batch
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return final
}

// deletionShiftWalk drives CollectChunks to its final emission: the first emission whose after cursor satisfies yieldAt ends
// the pass, mutate changes the fake, and the walk resumes from that cursor (through rewrite when set). The executor's flush
// requests (Barrier, CoalesceRuns) carry no cursor and are passed over.
func deletionShiftWalk(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer,
	yieldAt func(githubTestsChunkCursor) bool, mutate func(githubTestsChunkCursor), rewrite func(string) string,
) CompleteRouteBatch {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	resume, yielded, stop := "", false, errors.New("yield")
	for pass := 1; pass < 1000; pass++ {
		var final *CompleteRouteBatch
		var at githubTestsChunkCursor
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
			time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
				if emission.Barrier {
					return nil
				}
				resume = emission.CursorAfter
				if emission.Final {
					batch := emission.Batch
					final = &batch
					return nil
				}
				after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if !yielded && yieldAt(after) {
					yielded, at = true, after
					return stop
				}
				return nil
			})
		if final != nil {
			if err != nil || !yielded {
				t.Fatalf("final emission err=%v yielded=%v", err, yielded)
			}
			return *final
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v, want the yield", pass, err)
		}
		mutate(at)
		if rewrite != nil {
			resume = rewrite(resume)
		}
	}
	t.Fatal("the walk never reached a final emission")
	return CompleteRouteBatch{}
}

// deletionShiftStrip is the cursor a binary without re-seek state writes.
func deletionShiftStrip(t *testing.T) func(string) string {
	return func(raw string) string {
		fields := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, "rp")
		delete(fields, "rc")
		out, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
}

// deletionShiftExecutor runs the chunked executor (groups of up to 8 runs when the handler's CoalesceRuns is on, 16 chunks
// per attempt) and calls mutate once, at the first attempt end whose stored cursor satisfies at.
func deletionShiftExecutor(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer,
	at func(githubTestsChunkCursor) bool, mutate func(),
) CompleteRouteExecutionResult {
	t.Helper()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	unit := nativeTestClaim("github", "cicd").Unit
	unit.SinceAt, unit.BeforeAt, unit.Mode = claim.SinceAt, claim.BeforeAt, claim.Mode
	leases := newMemoryLeaseRepository(unit, "dispatching")
	leased, err := leases.Claim(context.Background(), ClaimRequest{UnitID: unit.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: 15 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{Repository: leases, Claim: leased, LeaseDuration: 15 * time.Minute,
		Deadline: now.Add(48 * time.Hour), Now: func() time.Time { return now }}
	descriptor, ok := Descriptor("github", unit.Dataset)
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	descriptor.ChunkPolicy.MaxEffectRows, descriptor.ChunkPolicy.MaxChunksPerAttempt = 8, 16
	sink, store := newRunsGroupSink(), newChunkMemoryStore()
	executor := completeRouteExecutor(now, handler, store, sink)
	executor.Committer.Readback = sink
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.Doer = fakehttp.Client(doer)
	executor.BudgetLimits = map[CostClass]int{CostMedium: 100_000, CostHeavy: 100_000, CostLight: 100_000}
	mutated := false
	for attempt := 1; attempt <= 2000; attempt++ {
		result, execErr := executor.Execute(context.Background(), session, descriptor)
		if execErr == nil {
			if !mutated {
				t.Fatal("premise: the walk never reached the mutation point")
			}
			return result
		}
		if !errors.As(execErr, new(ChunkContinuationError)) {
			t.Fatalf("attempt %d: %v", attempt, execErr)
		}
		checkpoint, loadErr := store.LoadChunkCheckpoint(context.Background(), leased, now)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		cursor, decodeErr := decodeGitHubTestsChunkCursor(checkpoint.NextCursor)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if !mutated && at(cursor) {
			mutated = true
			mutate()
		}
	}
	t.Fatal("the unit does not complete in 2000 attempts")
	return CompleteRouteExecutionResult{}
}

// deletionShiftOutcome is one measured cell: the survivors the uninterrupted walk of the survivors reads and the resumed
// walk does not, and whether the resumed walk advanced the watermark.
type deletionShiftOutcome struct {
	unread     []int
	watermark  bool
	incomplete []string
}

// deletionShiftIncomplete is the component/cause of each incomplete observation of a final result.
func deletionShiftIncomplete(t *testing.T, result map[string]any) []string {
	t.Helper()
	raw, err := json.Marshal(result["incomplete"])
	if err != nil {
		t.Fatal(err)
	}
	var observations []GitHubTestsIncomplete
	if err := json.Unmarshal(raw, &observations); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, observation := range observations {
		out = append(out, observation.Component+"/"+observation.Cause)
	}
	return out
}

func (outcome deletionShiftOutcome) String() string {
	return fmt.Sprintf("unread=%d first=%v watermark=%v incomplete=%v", len(outcome.unread), outcome.unread[:min(3, len(outcome.unread))],
		outcome.watermark, outcome.incomplete)
}

// deletionShiftMeasure runs one shape in one mode ("route", "previous_cursor", "executor") with run groups off or on: yield
// at the first cursor of the listing that satisfies at, delete the ids, resume; then walk the survivors uninterrupted
// under the same handler.
func deletionShiftMeasure(
	t *testing.T, listing deletionShiftListing, handler GitHubTestsRouteHandler, coalesce bool, runs []listingRun, mode string,
	at func(githubTestsChunkCursor) bool, deleted ...int,
) deletionShiftOutcome {
	t.Helper()
	handler.CoalesceRuns = coalesce
	fake := newListingFake(t, slices.Clone(runs))
	doer := deletionShiftDoer(t, fake, listing.reports)
	var watermark bool
	var incomplete []string
	in := func(c githubTestsChunkCursor) bool { return listing.in(c) && at(c) }
	switch mode {
	case "executor":
		result := deletionShiftExecutor(t, handler, listing.claim, doer, in, func() { resumeDeletionDelete(fake, deleted...) })
		watermark = result.Watermark != nil
		incomplete = deletionShiftIncomplete(t, result.Result)
	default:
		var rewrite func(string) string
		if mode == "previous_cursor" {
			rewrite = deletionShiftStrip(t)
		}
		final := deletionShiftWalk(t, handler, listing.claim, doer, in, func(githubTestsChunkCursor) { resumeDeletionDelete(fake, deleted...) }, rewrite)
		watermark = final.Watermark != nil
		incomplete = deletionShiftIncomplete(t, final.Result)
	}
	survivors := newListingFake(t, slices.Clone(fake.runs))
	deletionShiftPlainWalk(t, handler, listing.claim, deletionShiftDoer(t, survivors, listing.reports))
	var unread []int
	for id := range listing.reads(survivors) {
		if listing.reads(fake)[id] == 0 {
			unread = append(unread, id)
		}
	}
	sort.Ints(unread)
	return deletionShiftOutcome{unread: unread, watermark: watermark, incomplete: incomplete}
}

// deletionShiftReportRecords is the report rows one run's JUnit artifact writes.
func deletionShiftReportRecords(t *testing.T, listing deletionShiftListing) int {
	t.Helper()
	fake := newListingFake(t, listing.runs(1, 1))
	var cursor githubTestsChunkCursor
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), listing.claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(deletionShiftDoer(t, fake, true))), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			decoded, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
			if decodeErr == nil {
				cursor = decoded
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	records := cursor.Suites + cursor.Cases + cursor.Coverage
	if records < 1 {
		t.Fatal("premise: a run's report writes no rows")
	}
	return records
}

func deletionShiftFlag(coalesce bool) string {
	if coalesce {
		return "coalesce_on"
	}
	return "coalesce_off"
}

// deletionShiftMain is each cell's unread count on the stacked base (main's route, no re-seek state; measured by running
// these cells there, log in the PR). This binary must not leave more unread.
var deletionShiftMain = map[string]int{
	"charged/runs/coalesce_off/route":                           1,
	"charged/runs/coalesce_off/previous_cursor":                 1,
	"charged/runs/coalesce_off/executor":                        2,
	"charged/runs/coalesce_on/route":                            1,
	"charged/runs/coalesce_on/previous_cursor":                  1,
	"charged/runs/coalesce_on/executor":                         1,
	"charged/deep_runs/coalesce_off/route":                      1,
	"charged/deep_runs/coalesce_off/previous_cursor":            1,
	"charged/deep_runs/coalesce_off/executor":                   2,
	"charged/deep_runs/coalesce_on/route":                       1,
	"charged/deep_runs/coalesce_on/previous_cursor":             1,
	"charged/deep_runs/coalesce_on/executor":                    1,
	"charged/artifacts/coalesce_off/route":                      2,
	"charged/artifacts/coalesce_off/previous_cursor":            2,
	"charged/artifacts/coalesce_off/executor":                   3,
	"charged/artifacts/coalesce_on/route":                       2,
	"charged/artifacts/coalesce_on/previous_cursor":             2,
	"charged/artifacts/coalesce_on/executor":                    3,
	"charged/deep_reports/coalesce_off/route":                   1,
	"charged/deep_reports/coalesce_off/previous_cursor":         1,
	"charged/deep_reports/coalesce_off/executor":                2,
	"charged/deep_reports/coalesce_on/route":                    1,
	"charged/deep_reports/coalesce_on/previous_cursor":          1,
	"charged/deep_reports/coalesce_on/executor":                 2,
	"peel/runs/coalesce_off/midpage":                            2,
	"peel/runs/coalesce_on/midpage":                             2,
	"peel/runs/coalesce_off/spent_page_end":                     0,
	"peel/runs/coalesce_on/spent_page_end":                      0,
	"peel/runs/coalesce_off/midpage_first_range":                3,
	"peel/runs/coalesce_on/midpage_first_range":                 3,
	"peel/runs/coalesce_off/spent_page_end_first_range":         3,
	"peel/runs/coalesce_on/spent_page_end_first_range":          3,
	"peel/deep_runs/coalesce_off/midpage":                       2,
	"peel/deep_runs/coalesce_on/midpage":                        2,
	"peel/deep_runs/coalesce_off/spent_page_end":                0,
	"peel/deep_runs/coalesce_on/spent_page_end":                 0,
	"peel/deep_runs/coalesce_off/midpage_first_range":           3,
	"peel/deep_runs/coalesce_on/midpage_first_range":            3,
	"peel/deep_runs/coalesce_off/spent_page_end_first_range":    3,
	"peel/deep_runs/coalesce_on/spent_page_end_first_range":     3,
	"peel/artifacts/coalesce_off/midpage":                       2,
	"peel/artifacts/coalesce_on/midpage":                        2,
	"peel/artifacts/coalesce_off/spent_page_end":                0,
	"peel/artifacts/coalesce_on/spent_page_end":                 0,
	"peel/artifacts/coalesce_off/midpage_first_range":           3,
	"peel/artifacts/coalesce_on/midpage_first_range":            3,
	"peel/artifacts/coalesce_off/spent_page_end_first_range":    3,
	"peel/artifacts/coalesce_on/spent_page_end_first_range":     3,
	"peel/deep_reports/coalesce_off/midpage":                    2,
	"peel/deep_reports/coalesce_on/midpage":                     2,
	"peel/deep_reports/coalesce_off/spent_page_end":             0,
	"peel/deep_reports/coalesce_on/spent_page_end":              0,
	"peel/deep_reports/coalesce_off/midpage_first_range":        3,
	"peel/deep_reports/coalesce_on/midpage_first_range":         3,
	"peel/deep_reports/coalesce_off/spent_page_end_first_range": 3,
	"peel/deep_reports/coalesce_on/spent_page_end_first_range":  3,
	"peel/runs/coalesce_off/executor":                           0,
	"peel/runs/coalesce_on/executor":                            0,
	"peel/runs/coalesce_off/executor_first_range":               3,
	"peel/runs/coalesce_on/executor_first_range":                3,
	"peel/deep_runs/coalesce_off/executor":                      0,
	"peel/deep_runs/coalesce_on/executor":                       0,
	"peel/deep_runs/coalesce_off/executor_first_range":          3,
	"peel/deep_runs/coalesce_on/executor_first_range":           3,
	"peel/artifacts/coalesce_off/executor":                      0,
	"peel/artifacts/coalesce_on/executor":                       0,
	"peel/artifacts/coalesce_off/executor_first_range":          3,
	"peel/artifacts/coalesce_on/executor_first_range":           3,
	"peel/deep_reports/coalesce_off/executor":                   0,
	"peel/deep_reports/coalesce_on/executor":                    0,
	"peel/deep_reports/coalesce_off/executor_first_range":       3,
	"peel/deep_reports/coalesce_on/executor_first_range":        3,
	"dense/runs/coalesce_off":                                   1050,
	"dense/runs/coalesce_on":                                    1050,
	"dense/deep_runs/coalesce_off":                              1000,
	"dense/deep_runs/coalesce_on":                               1000,
}

func deletionShiftCheck(t *testing.T, name string, outcome deletionShiftOutcome) {
	t.Helper()
	t.Logf("CELL %s %s", name, outcome)
	want, ok := deletionShiftMain[name]
	if !ok {
		t.Errorf("no main count for %s", name)
		return
	}
	if len(outcome.unread) > want {
		t.Errorf("%s: %d survivors unread, main leaves %d: this binary reads less than main", name, len(outcome.unread), want)
	}
	// Class charged: a deletion before the anchor is credited, so the walk reads every survivor the uninterrupted walk of
	// the survivors reads. The artifacts listing's average (report rows over every run the runs phase read) under-credits
	// mid-phase: its walk leaves one survivor unread, and must stop on a recorded truncation with the watermark held, so
	// the next walk reads it (never a silent skip).
	switch {
	case strings.HasPrefix(name, "charged/artifacts/"):
		if len(outcome.unread) != 1 || outcome.watermark || !slices.Contains(outcome.incomplete, githubTestsArtifactInventoryComponent+"/"+githubTestsPageBudgetCause) {
			t.Errorf("%s: unread=%d watermark advanced=%v incomplete=%v, want 1 unread on a recorded %s stop with the watermark held",
				name, len(outcome.unread), outcome.watermark, outcome.incomplete, githubTestsArtifactInventoryComponent)
		}
	case strings.HasPrefix(name, "charged/") && len(outcome.unread) != 0:
		t.Errorf("%s: %d survivors unread, want 0 (watermark advanced=%v)", name, len(outcome.unread), outcome.watermark)
	}
}

// Class "charged": a run already read (and charged to the record bound) is deleted before the resume point; the walk at
// the record bound then reads one survivor fewer than the uninterrupted walk of the survivors, as main does.
func TestGitHubTestsResumeDeletedRunStaysChargedNoWorseThanMain(t *testing.T) {
	for _, listing := range deletionShiftRaceListings() {
		runs := listing.runs(1, 250)
		bound := 230
		if listing.reports {
			per := deletionShiftReportRecords(t, listing)
			bound = per * 130
			if !listing.deep {
				bound += 250 // the runs phase's pipeline rows count in the window records too
			}
		}
		handler := GitHubTestsRouteHandler{MaxWalkRecords: bound}
		for _, coalesce := range deletionShiftFlags() {
			for _, mode := range []string{"route", "previous_cursor", "executor"} {
				name := "charged/" + listing.name + "/" + deletionShiftFlag(coalesce) + "/" + mode
				t.Run(listing.name+"/"+deletionShiftFlag(coalesce)+"/"+mode, func(t *testing.T) {
					deletionShiftRaceSkip(t, mode)
					outcome := deletionShiftMeasure(t, listing, handler, coalesce, runs, mode,
						func(c githubTestsChunkCursor) bool { return c.RangeItems >= 50 }, 250)
					deletionShiftCheck(t, name, outcome)
				})
			}
		}
	}
}

// Class "peel": 1,120 runs, ids 121..123 in one created second; the first range ends at that second (GitHub's 1,000 cap) and
// the second range lists it again. Run 123 (read in the first range) is deleted while the second range runs: the
// uninterrupted walk of the survivors peels one run lower, so at the page budget edge it reads runs the resumed walk does
// not, as main does. Modes: a mid-page resume, a page-end resume at a spent page budget, and the executor's run groups.
func TestGitHubTestsResumeDeletionShiftingAPeelNoWorseThanMain(t *testing.T) {
	deletionShiftPeel(t, false)
}

// The same peel cells through the chunked executor (its attempts are the yields), run groups off and on.
func TestGitHubTestsResumeDeletionShiftingAPeelThroughTheExecutorNoWorseThanMain(t *testing.T) {
	deletionShiftPeel(t, true)
}

func deletionShiftPeel(t *testing.T, executor bool) {
	for _, listing := range deletionShiftRaceListings() {
		runs := listing.runs(1, 1120)
		for index := 120; index < 123; index++ {
			runs[index].created = runs[122].created
			runs[index].updated = runs[122].updated
		}
		handler := GitHubTestsRouteHandler{MaxRuns: 1100}
		if listing.deep {
			handler = GitHubTestsRouteHandler{LateRerunMaxPages: 11}
		}
		secondRange := func(c githubTestsChunkCursor) bool { return c.RangeHi != "" }
		midpage := func(c githubTestsChunkCursor) bool { return secondRange(c) && c.RangeItems == 2 && c.Index > 0 }
		pageEnd := func(c githubTestsChunkCursor) bool { return secondRange(c) && c.RangeItems == 100 && c.Index == 0 }
		grouped := func(c githubTestsChunkCursor) bool { return secondRange(c) && c.RangeItems >= 2 }
		// Run 123 is in the overlap (it moves the second range's anchor); run 500 is in the first range only (nothing in
		// the second range moves, but the survivors' first range still ends one run lower).
		for _, cell := range []struct {
			mode    string
			at      func(githubTestsChunkCursor) bool
			deleted int
		}{
			{"midpage", midpage, 123}, {"spent_page_end", pageEnd, 123}, {"executor", grouped, 123},
			{"midpage_first_range", midpage, 500}, {"spent_page_end_first_range", pageEnd, 500}, {"executor_first_range", grouped, 500},
		} {
			if strings.HasPrefix(cell.mode, "executor") != executor {
				continue
			}
			for _, coalesce := range deletionShiftFlags() {
				name := "peel/" + listing.name + "/" + deletionShiftFlag(coalesce) + "/" + cell.mode
				t.Run(listing.name+"/"+deletionShiftFlag(coalesce)+"/"+cell.mode, func(t *testing.T) {
					deletionShiftRaceSkip(t, cell.mode)
					mode := "route"
					if strings.HasPrefix(cell.mode, "executor") {
						mode = "executor"
					}
					outcome := deletionShiftMeasure(t, listing, handler, coalesce, runs, mode, cell.at, cell.deleted)
					deletionShiftCheck(t, name, outcome)
				})
			}
		}
	}
}

// A dense cut second (73 runs of 11-digit ids, an 875-byte cut list) at the first range's end, then the record bound inside
// the second range. Whatever the runs cut-list cap keeps or drops, the walk must not read fewer runs than main, and its stop
// is named: the cell logs the walk's incomplete observations and the watermark.
func TestGitHubTestsDenseCutSecondAtTheRecordBoundNoWorseThanMain(t *testing.T) {
	for _, cell := range []struct {
		listing  deletionShiftListing
		coalesce bool
	}{{deletionShiftListings()[0], false}, {deletionShiftListings()[0], true}, {deletionShiftListings()[1], false}, {deletionShiftListings()[1], true}} {
		listing, coalesce := cell.listing, cell.coalesce
		name := "dense/" + listing.name + "/" + deletionShiftFlag(coalesce)
		t.Run(listing.name+"/"+deletionShiftFlag(coalesce), func(t *testing.T) {
			runs := listing.runs(0, 1100)
			for index := range runs {
				runs[index].id = 12_345_678_000 + index
			}
			for index := 95; index <= 172; index++ { // newest-first positions 928..1,005: 73 in the first range
				runs[index].created = runs[172].created
				runs[index].updated = runs[172].updated
			}
			handler := GitHubTestsRouteHandler{MaxWalkRecords: 1050, CoalesceRuns: coalesce}
			if listing.deep {
				handler.LateRerunMaxPages = 20 // the record bound, not the deep page budget, ends the walk
			}
			fake := newListingFake(t, runs)
			walk := walkListing(t, handler, listing.claim, fake, 0)
			read := len(fake.jobGets)
			t.Logf("CELL %s read=%d incomplete=%v watermark=%v", name, read, walk.incompleteComponents(), walk.final.Watermark != nil)
			want, ok := deletionShiftMain[name]
			if !ok {
				t.Fatalf("no main count for %s", name)
			}
			if read < want {
				t.Errorf("%s: read %d runs, main reads %d", name, read, want)
			}
		})
	}
}
