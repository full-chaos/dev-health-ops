package providersync

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The bounds pinned by value: a backfill claim walks up to 40,000,000 records and 25,000 listed runs (250 pages); an
// incremental or full_resync claim keeps 4,100,000 records and 10,000 runs (100 pages).
const (
	backfillWalkRecordsPin  = 40_000_000
	backfillRunsPin         = 25_000
	scheduledWalkRecordsPin = 4_100_000
	scheduledRunsPin        = 10_000
)

var scheduledModes = []string{"incremental", "full_resync"}

// boundsClaim is a claim of dataset cicd, the dataset every backfill wave unit of the test-ops family carries (the
// planner folds tests into cicd, scheduler/sync/backfill_planner.go:104-107, :165-166).
func boundsClaim(mode string) Claim { return boundsDatasetClaim(mode, "cicd") }

// boundsDatasetClaim sets the cost class from the dataset's capability, as a planned unit has it (empty for an unknown
// dataset, which Claim.Validate refuses).
func boundsDatasetClaim(mode, dataset string) Claim {
	claim := nativeTestClaim("github", dataset)
	claim.Mode = mode
	return claim
}

// boundsStoredCursor is the stalled cursor shape of githubTestsStalledBackfillCursor with the given total records (the rest in Cases); mutate edits it.
func boundsStoredCursor(t *testing.T, phase string, records int, mutate func(*githubTestsChunkCursor)) string {
	t.Helper()
	stored, err := decodeGitHubTestsChunkCursor(githubTestsStalledBackfillCursor(t, phase, "api.github.com"))
	if err != nil {
		t.Fatal(err)
	}
	stored.Pipelines, stored.Jobs, stored.Acceptance, stored.Suites, stored.Coverage = 3090, 30_000, 9_000, 120_000, 0
	stored.Cases = records - (3090 + 30_000 + 9_000 + 120_000)
	if mutate != nil {
		mutate(&stored)
	}
	if githubTestsWalkRecords(stored) != records && mutate == nil {
		t.Fatalf("premise broken: records=%d want %d", githubTestsWalkRecords(stored), records)
	}
	raw, err := encodeGitHubTestsChunkCursor(stored)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type boundsAttempt struct {
	runLists, artifactLists int
	final                   *CompleteRouteBatch
	cursor                  githubTestsChunkCursor
}

// boundsCollect runs ONE attempt from the stored cursor: it yields at the first run emission or takes the final one.
func boundsCollect(t *testing.T, handler GitHubTestsRouteHandler, mode, raw string) boundsAttempt {
	t.Helper()
	return boundsCollectClaim(t, handler, boundsClaim(mode), raw)
}

func boundsCollectClaim(t *testing.T, handler GitHubTestsRouteHandler, claim Claim, raw string) boundsAttempt {
	t.Helper()
	mode := claim.Mode + "/" + claim.Dataset
	doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 50}
	last, attempt := raw, boundsAttempt{}
	err := handler.CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), raw,
		func(emission ChunkRouteEmission) error {
			last = emission.CursorAfter
			if emission.Final {
				batch := emission.Batch
				attempt.final = &batch
				return nil
			}
			return errGitHubTestsWalkContinuation
		},
	)
	if err != nil && !errors.Is(err, errGitHubTestsWalkContinuation) {
		t.Fatalf("mode %s: CollectChunks err=%v", mode, err)
	}
	cursor, err := decodeGitHubTestsChunkCursor(last)
	if err != nil {
		t.Fatal(err)
	}
	attempt.runLists, attempt.artifactLists, attempt.cursor = doer.runListRequests, doer.artifactListRequests, cursor
	return attempt
}

func (attempt boundsAttempt) listed() bool { return attempt.runLists+attempt.artifactLists > 0 }

// (i) A backfill claim walks past the scheduled record bound and past the scheduled 100 listing pages.
func TestGitHubTestsBackfillWalksPastTheScheduledBounds(t *testing.T) {
	for _, phase := range []string{githubTestsArtifactsPhase, githubTestsRunsPhase} {
		for _, records := range []int{scheduledWalkRecordsPin, scheduledWalkRecordsPin + 1, backfillWalkRecordsPin - 1} {
			attempt := boundsCollect(t, GitHubTestsRouteHandler{}, "backfill", boundsStoredCursor(t, phase, records, nil))
			t.Logf("CELL i-records/%s/%d listed=%v final=%v incomplete=%+v", phase, records, attempt.listed(), attempt.final != nil, attempt.cursor.Incomplete)
			if attempt.final != nil || !attempt.listed() || len(attempt.cursor.Incomplete) != 0 {
				t.Errorf("backfill %s at %d records: final=%v listed=%v incomplete=%+v; want it to list on with no truncation",
					phase, records, attempt.final != nil, attempt.listed(), attempt.cursor.Incomplete)
			}
		}
	}
	window := boundsCollect(t, GitHubTestsRouteHandler{}, "backfill", boundsWindowCursor(t))
	t.Logf("CELL i-window listed=%v final=%v incomplete=%+v", window.listed(), window.final != nil, window.cursor.Incomplete)
	if window.final != nil || window.artifactLists == 0 || len(window.cursor.Incomplete) != 0 {
		t.Errorf("backfill artifacts at %d window records: final=%v artifactLists=%d incomplete=%+v; want it to list on",
			scheduledWalkRecordsPin, window.final != nil, window.artifactLists, window.cursor.Incomplete)
	}
	for _, phase := range []string{githubTestsArtifactsPhase, githubTestsRunsPhase} {
		for _, pages := range []int{scheduledRunsPin / nativePerPage, backfillRunsPin/nativePerPage - 1} {
			attempt := boundsCollect(t, GitHubTestsRouteHandler{}, "backfill", boundsSpentPagesCursor(t, phase, pages))
			t.Logf("CELL i-pages/%s/%d listed=%v final=%v incomplete=%+v", phase, pages, attempt.listed(), attempt.final != nil, attempt.cursor.Incomplete)
			lists, component := attempt.artifactLists, githubTestsArtifactInventoryComponent
			if phase == githubTestsRunsPhase {
				lists, component = attempt.runLists, githubTestsRunInventoryComponent
			}
			if lists == 0 || githubTestsHasPageBudgetObservation(attempt.cursor, component) {
				t.Errorf("backfill %s with %d pages spent: %d listing requests of the phase, incomplete=%+v; want the next page listed",
					phase, pages, lists, attempt.cursor.Incomplete)
			}
		}
	}
}

// boundsWindowCursor is an artifacts-phase cursor whose window records (the walk's records less the deep range's) are at
// the scheduled bound, 1,000,000 of them deep range records: the artifacts phase checks the bound on the window.
func boundsWindowCursor(t *testing.T) string {
	return boundsStoredCursor(t, githubTestsArtifactsPhase, scheduledWalkRecordsPin+1_000_000, func(cursor *githubTestsChunkCursor) {
		cursor.LateRecords = 1_000_000
	})
}

// boundsSpentPagesCursor is a cursor at a page end of its phase's listing with the given pages spent, under the record bound.
func boundsSpentPagesCursor(t *testing.T, phase string, pages int) string {
	return boundsStoredCursor(t, phase, 2_002_076, func(cursor *githubTestsChunkCursor) {
		cursor.NextURL, cursor.Index = "", 0
		if phase == githubTestsRunsPhase {
			cursor.RunPages = pages
		} else {
			cursor.ArtifactPages = pages
		}
	})
}

// (ii) incremental and full_resync keep the CHAOS-14212 bounds: they stop at 4,100,000 records and at 100 pages, list just
// under them, and refuse a configured bound above them. These arms are green on the base and must stay green.
func TestGitHubTestsScheduledModesKeepTheWalkBounds(t *testing.T) {
	for _, mode := range scheduledModes {
		for _, phase := range []string{githubTestsArtifactsPhase, githubTestsRunsPhase} {
			for _, records := range []int{scheduledWalkRecordsPin, scheduledWalkRecordsPin + 1} {
				attempt := boundsCollect(t, GitHubTestsRouteHandler{}, mode, boundsStoredCursor(t, phase, records, nil))
				t.Logf("CELL ii-records/%s/%s/%d listed=%v final=%v", mode, phase, records, attempt.listed(), attempt.final != nil)
				if attempt.final == nil || attempt.listed() || !githubTestsHasPageBudgetObservation(attempt.cursor, githubTestsArtifactInventoryComponent) {
					t.Errorf("%s %s at %d records: final=%v listed=%v incomplete=%+v; want a stop without listing",
						mode, phase, records, attempt.final != nil, attempt.listed(), attempt.cursor.Incomplete)
				}
			}
			under := boundsCollect(t, GitHubTestsRouteHandler{}, mode, boundsStoredCursor(t, phase, scheduledWalkRecordsPin-1, nil))
			if under.final != nil || !under.listed() {
				t.Errorf("%s %s one record under the bound: final=%v listed=%v, want it to list", mode, phase, under.final != nil, under.listed())
			}
			if phase == githubTestsArtifactsPhase {
				window := boundsCollect(t, GitHubTestsRouteHandler{}, mode, boundsWindowCursor(t))
				if window.final == nil || window.listed() || !githubTestsHasPageBudgetObservation(window.cursor, githubTestsArtifactInventoryComponent) {
					t.Errorf("%s artifacts at %d window records: final=%v listed=%v incomplete=%+v; want a stop without listing",
						mode, scheduledWalkRecordsPin, window.final != nil, window.listed(), window.cursor.Incomplete)
				}
			}
			spent := boundsCollect(t, GitHubTestsRouteHandler{}, mode, boundsSpentPagesCursor(t, phase, scheduledRunsPin/nativePerPage))
			component := githubTestsArtifactInventoryComponent
			if phase == githubTestsRunsPhase {
				component = githubTestsRunInventoryComponent
			}
			t.Logf("CELL ii-pages/%s/%s runLists=%d artifactLists=%d incomplete=%+v", mode, phase, spent.runLists, spent.artifactLists, spent.cursor.Incomplete)
			if !githubTestsHasPageBudgetObservation(spent.cursor, component) {
				t.Errorf("%s %s with 100 pages spent: incomplete=%+v, want %s page_budget_exhausted", mode, phase, spent.cursor.Incomplete, component)
			}
		}
	}
}

func boundsConfigErr(t *testing.T, handler GitHubTestsRouteHandler, mode string) error {
	t.Helper()
	return boundsConfigErrClaim(t, handler, boundsClaim(mode))
}

func boundsConfigErrClaim(t *testing.T, handler GitHubTestsRouteHandler, claim Claim) error {
	t.Helper()
	doer := &githubTestsPagedDoer{t: t, pages: 1, perPage: 1}
	err := handler.CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				return nil
			}
			return errGitHubTestsWalkContinuation
		},
	)
	if errors.Is(err, errGitHubTestsWalkContinuation) {
		return nil
	}
	return err
}

// (ii) config: the handler may lower a bound inside the claim mode's ceiling only.
func TestGitHubTestsWalkBoundCeilingFollowsTheClaimMode(t *testing.T) {
	over := []GitHubTestsRouteHandler{{MaxWalkRecords: scheduledWalkRecordsPin + 1}, {MaxRuns: scheduledRunsPin + 1}}
	for _, mode := range scheduledModes {
		for _, handler := range over {
			if err := boundsConfigErr(t, handler, mode); !errors.Is(err, ErrInvalidConfiguration) {
				t.Errorf("%s MaxWalkRecords=%d MaxRuns=%d: err=%v, want ErrInvalidConfiguration", mode, handler.MaxWalkRecords, handler.MaxRuns, err)
			}
		}
	}
	for _, handler := range append(over, GitHubTestsRouteHandler{MaxWalkRecords: backfillWalkRecordsPin}, GitHubTestsRouteHandler{MaxRuns: backfillRunsPin}) {
		if err := boundsConfigErr(t, handler, "backfill"); err != nil {
			t.Errorf("backfill MaxWalkRecords=%d MaxRuns=%d: err=%v, want accepted", handler.MaxWalkRecords, handler.MaxRuns, err)
		}
	}
	for _, handler := range []GitHubTestsRouteHandler{{MaxWalkRecords: backfillWalkRecordsPin + 1}, {MaxRuns: backfillRunsPin + 1}} {
		if err := boundsConfigErr(t, handler, "backfill"); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("backfill MaxWalkRecords=%d MaxRuns=%d: err=%v, want ErrInvalidConfiguration", handler.MaxWalkRecords, handler.MaxRuns, err)
		}
	}
}

// An empty or unknown mode never reaches the bounds: Claim.Validate (lease.go:114-118) refuses it before the first
// provider call, and the backfill bounds key on mode == "backfill" only, so a mode added to Validate later keeps the
// scheduled bounds unless it is named there.
func TestGitHubTestsUnknownModesNeverReachTheBounds(t *testing.T) {
	for _, mode := range []string{"", "hourly", "Backfill"} {
		for _, handler := range []GitHubTestsRouteHandler{{}, {MaxWalkRecords: backfillWalkRecordsPin}} {
			if err := boundsConfigErr(t, handler, mode); !errors.Is(err, ErrInvalidConfiguration) {
				t.Errorf("mode %q MaxWalkRecords=%d: err=%v, want ErrInvalidConfiguration", mode, handler.MaxWalkRecords, err)
			}
		}
	}
}

// (iii) The backfill stop at its bound (and one past it) writes the same truncation as the incremental stop at 4,100,000:
// the same incomplete entries, reports_complete=false, no watermark, accepted by the production comparator.
func TestGitHubTestsBackfillWalkBoundWritesTheSameIncomplete(t *testing.T) {
	for _, phase := range []string{githubTestsArtifactsPhase, githubTestsRunsPhase} {
		want := boundsCollect(t, GitHubTestsRouteHandler{}, "incremental", boundsStoredCursor(t, phase, scheduledWalkRecordsPin, nil))
		if want.final == nil {
			t.Fatalf("premise broken: incremental %s at the bound did not finalize", phase)
		}
		for _, records := range []int{backfillWalkRecordsPin, backfillWalkRecordsPin + 1} {
			got := boundsCollect(t, GitHubTestsRouteHandler{}, "backfill", boundsStoredCursor(t, phase, records, nil))
			t.Logf("CELL iii/%s/%d final=%v listed=%v incomplete=%+v", phase, records, got.final != nil, got.listed(), got.cursor.Incomplete)
			if got.final == nil || got.listed() {
				t.Errorf("backfill %s at %d records: final=%v listed=%v, want a stop without listing", phase, records, got.final != nil, got.listed())
				continue
			}
			if !reflect.DeepEqual(got.cursor.Incomplete, want.cursor.Incomplete) {
				t.Errorf("backfill %s incomplete=%+v, want the incremental stop's %+v", phase, got.cursor.Incomplete, want.cursor.Incomplete)
			}
			if complete, ok := got.final.Result[GitHubTestsResultKeyReportsComplete].(bool); !ok || complete || got.final.Watermark != nil {
				t.Errorf("backfill %s stop: reports_complete=%v watermark=%v, want false and none", phase, got.final.Result[GitHubTestsResultKeyReportsComplete], got.final.Watermark)
			}
			if _, err := (ProductionContractComparator{}).CompareCompleteRoute(context.Background(), boundsClaim("backfill"), *got.final); err != nil {
				t.Errorf("production comparator rejected the backfill stop: %v", err)
			}
		}
		under := boundsCollect(t, GitHubTestsRouteHandler{}, "backfill", boundsStoredCursor(t, phase, backfillWalkRecordsPin-1, nil))
		if under.final != nil || !under.listed() {
			t.Errorf("backfill %s one record under its bound: final=%v listed=%v, want it to list", phase, under.final != nil, under.listed())
		}
	}
}

// restartWalk walks the window claim of mode through the listing fake in attempts. Each attempt is a new CollectChunks
// call from the stored cursor of the one before (a worker restart between chunks); before an attempt, inflate may raise
// the stored records (the cheap stand-in for the rows of the runs before). An attempt yields after `runs` more runs read.
type restartWalk struct {
	t      *testing.T
	fake   *listingFake
	claim  Claim
	stored string
}

func newRestartWalk(t *testing.T, mode, dataset string) *restartWalk {
	claim := boundsDatasetClaim(mode, dataset)
	since, before := listingSince, listingSince.Add(6*time.Hour)
	claim.SinceAt, claim.BeforeAt = &since, &before
	return &restartWalk{t: t, fake: newListingFake(t, makeListingRuns(1, 400, listingSince.Add(time.Minute), 30*time.Second)), claim: claim}
}

func (walk *restartWalk) inflateTo(records int) {
	walk.t.Helper()
	cursor, err := decodeGitHubTestsChunkCursor(walk.stored)
	if err != nil {
		walk.t.Fatal(err)
	}
	cursor.Jobs += records - githubTestsWalkRecords(cursor)
	if walk.stored, err = encodeGitHubTestsChunkCursor(cursor); err != nil {
		walk.t.Fatal(err)
	}
}

// attempt returns the final batch when the attempt finalized, and the runs whose jobs it read.
func (walk *restartWalk) attempt(runs int) (*CompleteRouteBatch, []int) {
	walk.t.Helper()
	before := map[int]int{}
	for id, count := range walk.fake.jobGets {
		before[id] = count
	}
	var final *CompleteRouteBatch
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), walk.claim, providerfoundation.Credential{},
		githubTestsClient(walk.t, fakehttp.Client(walk.fake)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), walk.stored,
		func(emission ChunkRouteEmission) error {
			if emission.Barrier {
				return nil
			}
			walk.stored = emission.CursorAfter
			if emission.Final {
				batch := emission.Batch
				final = &batch
				return nil
			}
			read := 0
			for id, count := range walk.fake.jobGets {
				read += count - before[id]
			}
			if runs > 0 && read >= runs {
				return errGitHubTestsWalkContinuation
			}
			return nil
		})
	if final == nil && !errors.Is(err, errGitHubTestsWalkContinuation) {
		walk.t.Fatalf("attempt err=%v", err)
	}
	var read []int
	for id, count := range walk.fake.jobGets {
		if count > before[id] {
			read = append(read, id)
		}
	}
	return final, read
}

func (walk *restartWalk) cursor() githubTestsChunkCursor {
	cursor, err := decodeGitHubTestsChunkCursor(walk.stored)
	if err != nil {
		walk.t.Fatal(err)
	}
	return cursor
}

// (iii) restart arm: a backfill walk crosses 4,100,000 records across a worker restart (the second attempt resumes from the
// stored cursor of the first and reads no run again), and later ends at its own bound with the incremental stop's entry.
func TestGitHubTestsBackfillWalkCrossesTheScheduledBoundAcrossARestart(t *testing.T) {
	scheduled := newRestartWalk(t, "incremental", "cicd")
	if final, _ := scheduled.attempt(10); final != nil {
		t.Fatal("premise broken: the incremental walk ended in its first attempt")
	}
	scheduled.inflateTo(scheduledWalkRecordsPin - 5)
	scheduledFinal, _ := scheduled.attempt(0)
	if scheduledFinal == nil || !githubTestsHasPageBudgetObservation(scheduled.cursor(), githubTestsRunInventoryComponent) {
		t.Fatalf("premise broken: the incremental walk did not stop at its bound: incomplete=%+v", scheduled.cursor().Incomplete)
	}

	walk := newRestartWalk(t, "backfill", "cicd")
	first, firstRead := walk.attempt(10)
	if first != nil || len(firstRead) < 10 {
		t.Fatalf("first attempt: final=%v read=%d, want a yield after 10 runs", first != nil, len(firstRead))
	}
	walk.inflateTo(scheduledWalkRecordsPin - 5)
	second, secondRead := walk.attempt(20)
	crossed := githubTestsWalkRecords(walk.cursor())
	t.Logf("CELL iii-restart second attempt final=%v read=%d records=%d incomplete=%+v", second != nil, len(secondRead), crossed, walk.cursor().Incomplete)
	if second != nil || crossed <= scheduledWalkRecordsPin || len(walk.cursor().Incomplete) != 0 {
		t.Fatalf("backfill after the restart: final=%v records=%d incomplete=%+v; want it past %d with no truncation",
			second != nil, crossed, walk.cursor().Incomplete, scheduledWalkRecordsPin)
	}
	for _, id := range secondRead {
		if walk.fake.jobGets[id] != 1 {
			t.Fatalf("run %d read %d times: the resumed attempt re-read done work", id, walk.fake.jobGets[id])
		}
	}
	walk.inflateTo(backfillWalkRecordsPin - 5)
	final, _ := walk.attempt(0)
	end := walk.cursor()
	t.Logf("CELL iii-restart end final=%v records=%d incomplete=%+v", final != nil, githubTestsWalkRecords(end), end.Incomplete)
	if final == nil || githubTestsWalkRecords(end) < backfillWalkRecordsPin || githubTestsWalkRecords(end) > backfillWalkRecordsPin+5 {
		t.Fatalf("backfill end: final=%v records=%d, want a stop just past %d", final != nil, githubTestsWalkRecords(end), backfillWalkRecordsPin)
	}
	if !reflect.DeepEqual(end.Incomplete, scheduled.cursor().Incomplete) {
		t.Fatalf("backfill end incomplete=%+v, want the incremental stop's %+v", end.Incomplete, scheduled.cursor().Incomplete)
	}
}

// (iv) The deletion credit ceiling is a twentieth of the claim mode's walk bound (CHAOS-14434 path): a walk that already
// holds 205,000 records of credit (the scheduled ceiling) earns more for a proven deletion only under backfill.
func TestGitHubTestsBackfillDeletionCreditCeilingScales(t *testing.T) {
	const heldCredit = scheduledWalkRecordsPin / 20
	for arm, wantMore := range map[[2]string]bool{
		{"incremental", "cicd"}: false, {"full_resync", "cicd"}: false, {"backfill", "cicd"}: true, {"backfill", "tests"}: false,
	} {
		mode := arm[0] + "/" + arm[1]
		walk := newRestartWalk(t, arm[0], arm[1])
		if final, _ := walk.attempt(20); final != nil {
			t.Fatalf("%s: premise broken: the walk ended in its first attempt", mode)
		}
		cursor := walk.cursor()
		anchor, err := strconv.Atoi(cursor.AnchorID)
		if err != nil || cursor.Pipelines <= 0 {
			t.Fatalf("%s: premise broken: anchor %q pipelines %d", mode, cursor.AnchorID, cursor.Pipelines)
		}
		cursor.Jobs += 1_000_000
		cursor.RecordCredit = heldCredit
		if walk.stored, err = encodeGitHubTestsChunkCursor(cursor); err != nil {
			t.Fatal(err)
		}
		resumeDeletionDelete(walk.fake, anchor+1)
		walk.attempt(1)
		got := walk.cursor()
		t.Logf("CELL iv/%s credit %d -> %d proven=%d", mode, heldCredit, got.RecordCredit, got.DeletionProven)
		if got.DeletionProven != 1 {
			t.Fatalf("%s: premise broken: no deletion proven", mode)
		}
		// The re-seek re-read credit (uncapped, a few records) may add to either mode; the deletion credit of a run here is
		// about 1,000,000/20 records.
		if more := got.RecordCredit > heldCredit+100; more != wantMore {
			t.Errorf("%s: credit %d after a proven deletion at held credit %d; want more=%v (ceiling = bound/20)", mode, got.RecordCredit, heldCredit, wantMore)
		}
	}
}

// boundsWidestBackfillCounters sets the record counters a backfill walk can carry at their widest: four of the five
// row counters at 10,000,000 sum to the 40,000,000 bound (pipelines count runs, at most 25,000), and the deep range's
// record count at the bound.
func boundsWidestBackfillCounters(cursor *githubTestsChunkCursor) {
	cursor.Jobs, cursor.Acceptance, cursor.Suites, cursor.Cases = 10_000_000, 10_000_000, 10_000_000, 10_000_000
	if cursor.LateRecords != 0 {
		cursor.LateRecords = backfillWalkRecordsPin
	}
}

// Backfill floors of the per-phase cursor margin: a backfill walk's record counters reach 8 digits (4.1M: 7), so its
// worst-case cursor is up to 9 bytes wider than a scheduled one. Each is the measured backfill margin (runs peel 297,
// deep range peel 241 bytes) rounded down to a multiple of 5; the scheduled arms keep 300 and 250. The real limit is the
// hard 4 KiB cap (maxChunkCursorBytes), which the encoder enforces.
const (
	backfillRunsPeelCursorFloor      = 295
	backfillDeepRangePeelCursorFloor = 240
)

func backfillCursorFloor(name string, floor int) int {
	switch name {
	case "runs peel":
		return backfillRunsPeelCursorFloor
	case "runs deep range peel":
		return backfillDeepRangePeelCursorFloor
	}
	return floor
}

// Every (mode, dataset) arm of the shared claim fields: ONLY (backfill, cicd) gets 25,000 runs / 40,000,000 records. The
// other valid arms keep main's 10,000 / 4,100,000 (accept the bound, refuse bound+1, stop at the bound, list one under
// it); an empty or unknown mode or dataset is refused by Claim.Validate before any bound, as on main.
func TestGitHubTestsWalkBoundsPerModeAndDataset(t *testing.T) {
	for _, mode := range []string{"backfill", "incremental", "full_resync", "", "hourly"} {
		for _, dataset := range []string{"cicd", "tests", "", "builds"} {
			claim := boundsDatasetClaim(mode, dataset)
			valid := claim.Validate() == nil
			runs, records := scheduledRunsPin, scheduledWalkRecordsPin
			if mode == "backfill" && dataset == "cicd" {
				runs, records = backfillRunsPin, backfillWalkRecordsPin
			}
			arm := mode + "/" + dataset
			if !valid {
				for _, handler := range []GitHubTestsRouteHandler{{}, {MaxWalkRecords: backfillWalkRecordsPin}, {MaxRuns: backfillRunsPin}} {
					if err := boundsConfigErrClaim(t, handler, claim); !errors.Is(err, ErrInvalidConfiguration) {
						t.Errorf("%s: err=%v, want ErrInvalidConfiguration (refused before any bound)", arm, err)
					}
				}
				continue
			}
			for _, cell := range []struct {
				handler GitHubTestsRouteHandler
				refused bool
			}{
				{GitHubTestsRouteHandler{MaxWalkRecords: records}, false}, {GitHubTestsRouteHandler{MaxWalkRecords: records + 1}, true},
				{GitHubTestsRouteHandler{MaxRuns: runs}, false}, {GitHubTestsRouteHandler{MaxRuns: runs + 1}, true},
			} {
				err := boundsConfigErrClaim(t, cell.handler, claim)
				if refused := errors.Is(err, ErrInvalidConfiguration); refused != cell.refused || (!refused && err != nil) {
					t.Errorf("%s MaxWalkRecords=%d MaxRuns=%d: err=%v, want refused=%v", arm, cell.handler.MaxWalkRecords, cell.handler.MaxRuns, err, cell.refused)
				}
			}
			at := boundsCollectClaim(t, GitHubTestsRouteHandler{}, claim, boundsStoredCursor(t, githubTestsArtifactsPhase, records, nil))
			under := boundsCollectClaim(t, GitHubTestsRouteHandler{}, claim, boundsStoredCursor(t, githubTestsArtifactsPhase, records-1, nil))
			pages := boundsCollectClaim(t, GitHubTestsRouteHandler{}, claim, boundsSpentPagesCursor(t, githubTestsRunsPhase, runs/nativePerPage))
			t.Logf("CELL arms/%s bound=%d/%d stop=%v under-lists=%v pages-stop=%v", arm, runs, records, at.final != nil && !at.listed(),
				under.final == nil && under.listed(), githubTestsHasPageBudgetObservation(pages.cursor, githubTestsRunInventoryComponent))
			if at.final == nil || at.listed() || under.final != nil || !under.listed() ||
				!githubTestsHasPageBudgetObservation(pages.cursor, githubTestsRunInventoryComponent) {
				t.Errorf("%s: stop at %d records final=%v listed=%v; at %d final=%v listed=%v; %d pages spent incomplete=%+v; want the stop at the arm's bound",
					arm, records, at.final != nil, at.listed(), records-1, under.final != nil, under.listed(), runs/nativePerPage, pages.cursor.Incomplete)
			}
		}
	}
}
