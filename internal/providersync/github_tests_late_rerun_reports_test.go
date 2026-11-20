package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func lateRerunS0() time.Time { return time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) }

func lateRerunClaim(since, before time.Time) Claim {
	claim := listingClaim(since, before)
	claim.Mode = "incremental"
	return claim
}

// lateRerunDeepRanges is the deep created ranges a fake was asked for: their end is one second before a UTC midnight, never a
// claim edge in these cells.
func lateRerunDeepRanges(fake *listingFake) map[string]bool {
	out := map[string]bool{}
	for _, created := range fake.listRanges {
		if strings.HasSuffix(created, "T23:59:59Z") {
			out[created] = true
		}
	}
	return out
}

func lateRerunReads(t *testing.T, claim Claim, runs []listingRun, id int) (int, *listingFake, listingWalk) {
	t.Helper()
	fake := newListingFake(t, runs)
	walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
	return fake.artifactGets[id], fake, walk
}

func lateRerunCollectReads(t *testing.T, claim Claim, runs []listingRun, id int) int {
	t.Helper()
	fake := newListingFake(t, runs)
	client := githubTestsClient(t, fakehttp.Client(fake))
	if _, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return fake.artifactGets[id]
}

// A chain of contiguous incremental claims (since = previous before) and one run created in claim 1, re-run later. Each shape
// reads the re-run's reports the pinned number of times, on the chunked route and on the non-chunked oracle alike. The chain
// runs past the first UTC midnight after the run's last update, where the deep range reads it.
func TestGitHubTestsLateRerunChainReadsPinnedCounts(t *testing.T) {
	for _, cell := range []struct {
		name    string
		width   time.Duration
		created time.Duration // offset from claim 1's before (negative = inside claim 1)
		started time.Duration // offset from claim 1's before
		want    int
	}{
		{"loss 1h rerun 72h later", time.Hour, -10 * time.Minute, 72 * time.Hour, 1},
		{"loss 1d rerun 3d later", 24 * time.Hour, -10 * time.Minute, 72 * time.Hour, 3},
		{"loss 7d rerun in claim 3", 7 * 24 * time.Hour, -24 * time.Hour, 8 * 24 * time.Hour, 1}, // claim 4's deep range starts 14 d before its since, after the run's creation
		{"loss 1h rerun 6d later", time.Hour, -10 * time.Minute, 6 * 24 * time.Hour, 1},
		{"control 1h queued across the boundary", time.Hour, -10 * time.Minute, 10 * time.Minute, 3},
		{"control 1h rerun 6h later", time.Hour, -10 * time.Minute, 6 * time.Hour, 4},
		{"control 1h rerun 30h later", time.Hour, -10 * time.Minute, 30 * time.Hour, 4},
		{"control 1d queued across the boundary", 24 * time.Hour, -10 * time.Minute, 10 * time.Minute, 2},
	} {
		t.Run(cell.name, func(t *testing.T) {
			b1 := lateRerunS0().Add(cell.width)
			run := listingRun{id: 1, created: b1.Add(cell.created), startedAt: b1.Add(cell.started), status: "completed", conclusion: "success"}
			run.updated = run.startedAt.Add(time.Hour)
			nextMidnight := run.updated.Truncate(24 * time.Hour).Add(24 * time.Hour)
			end := run.updated.Add(3 * cell.width)
			if alt := nextMidnight.Add(cell.width); alt.After(end) {
				end = alt
			}
			claims := int((end.Sub(lateRerunS0()) + cell.width - 1) / cell.width)
			chunked, collected := 0, 0
			var readIn []int
			for index := range claims {
				claim := lateRerunClaim(lateRerunS0().Add(time.Duration(index)*cell.width), lateRerunS0().Add(time.Duration(index+1)*cell.width))
				reads, _, _ := lateRerunReads(t, claim, []listingRun{run}, run.id)
				if reads > 0 {
					chunked += reads
					readIn = append(readIn, index+1)
				}
				collected += lateRerunCollectReads(t, claim, []listingRun{run}, run.id)
			}
			t.Logf("CELL %s: claims=%d reads=%d in claims %v; oracle reads=%d", cell.name, claims, chunked, readIn, collected)
			if chunked != cell.want {
				t.Errorf("chunked reads=%d, want %d", chunked, cell.want)
			}
			if collected != chunked {
				t.Errorf("non-chunked oracle reads=%d, chunked %d: the two routes must not grade apart", collected, chunked)
			}
		})
	}
}

// The midnight test is since < M <= before: a claim that starts at a midnight does not own it, the claim that ends at it does.
func TestGitHubTestsLateRerunMidnightEdges(t *testing.T) {
	m := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	ending := lateRerunClaim(m.Add(-time.Hour), m)
	starting := lateRerunClaim(m, m.Add(time.Hour))
	_, fake, _ := lateRerunReads(t, ending, nil, 0)
	if got := lateRerunDeepRanges(fake); len(got) != 1 || !got["2026-06-23..2026-06-29T23:59:59Z"] {
		t.Errorf("M == before: deep ranges %v, want exactly 2026-06-23..2026-06-29T23:59:59Z", got)
	}
	_, fake, _ = lateRerunReads(t, starting, nil, 0)
	if got := lateRerunDeepRanges(fake); len(got) != 0 {
		t.Errorf("M == since: deep ranges %v, want none (the previous claim owns M)", got)
	}
	open := lateRerunClaim(m.Add(-time.Hour), m)
	open.SinceAt = nil
	backfill := lateRerunClaim(m.Add(-time.Hour), m)
	backfill.Mode = "backfill"
	resync := lateRerunClaim(m.Add(-time.Hour), m)
	resync.Mode = "full_resync"
	for name, claim := range map[string]Claim{"open start": open, "backfill": backfill, "full_resync": resync} {
		if githubTestsRelistOwned(claim) {
			t.Errorf("%s claim owns the deep range, want only incremental claims with both edges", name)
		}
	}
	_, fake, _ = lateRerunReads(t, backfill, nil, 0)
	if got := lateRerunDeepRanges(fake); len(got) != 0 {
		t.Errorf("backfill: deep ranges %v, want none", got)
	}
}

// Over three days of hourly claims only the three claims that hold a midnight list the deep range, a deep run that did not
// change in the window costs no artifact read, and a run on the floor date is listed (and read) once.
func TestGitHubTestsLateRerunDeepRangeCost(t *testing.T) {
	stale := listingRun{id: 7, created: time.Date(2026, 6, 28, 3, 0, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	stale.updated = stale.created.Add(time.Hour)
	owners := 0
	for index := range 72 {
		claim := lateRerunClaim(lateRerunS0().Add(time.Duration(index)*time.Hour), lateRerunS0().Add(time.Duration(index+1)*time.Hour))
		reads, fake, _ := lateRerunReads(t, claim, []listingRun{stale}, stale.id)
		if len(lateRerunDeepRanges(fake)) > 0 {
			owners++
		}
		if reads != 0 {
			t.Errorf("claim %d read the unchanged deep run %d times, want 0", index+1, reads)
		}
	}
	if owners != 3 {
		t.Errorf("claims listing the deep range=%d over 72 hourly claims, want 3 (one per midnight)", owners)
	}
	claim := lateRerunClaim(time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	onFloor := listingRun{id: 8, created: time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC), startedAt: time.Date(2026, 7, 4, 22, 0, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	onFloor.updated = onFloor.startedAt.Add(30 * time.Minute)
	if reads, _, _ := lateRerunReads(t, claim, []listingRun{onFloor}, onFloor.id); reads != 1 {
		t.Errorf("a run created on the floor date was read %d times in one claim, want 1 (the ranges are disjoint)", reads)
	}
}

// A midnight claim held by its regular artifacts budget lists no deep range and holds its watermark; its retry keeps the same
// window, owns the same midnight and reads the late re-run.
func TestGitHubTestsLateRerunHeldClaimReownsMidnight(t *testing.T) {
	since, before := time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	late := listingRun{id: 1, created: time.Date(2026, 7, 1, 0, 50, 0, 0, time.UTC), startedAt: time.Date(2026, 7, 4, 1, 0, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	late.updated = late.startedAt.Add(time.Hour)
	runs := append([]listingRun{late}, makeListingRuns(1000, 150, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), time.Minute)...)
	fake := newListingFake(t, runs)
	held := walkListing(t, GitHubTestsRouteHandler{MaxRuns: 100}, lateRerunClaim(since, before), fake, 0)
	if held.final.Watermark != nil {
		t.Fatalf("held claim watermark=%v, want nil (regular artifacts budget exhausted)", held.final.Watermark)
	}
	if got := lateRerunDeepRanges(fake); len(got) != 0 || fake.artifactGets[late.id] != 0 {
		t.Fatalf("held claim: deep ranges %v, late reads %d; want none", got, fake.artifactGets[late.id])
	}
	reads, _, retry := lateRerunReads(t, lateRerunClaim(since, before), runs, late.id)
	if reads != 1 || retry.final.Watermark == nil || !retry.final.Watermark.Equal(before) {
		t.Fatalf("retry: late reads=%d watermark=%v, want 1 and %s", reads, retry.final.Watermark, before)
	}
}

// The deep range stops at its own page budget, or at a second it cannot peel past: one late_rerun_reports observation,
// reports_complete=false, the watermark advances, and the comparator accepts the completion.
func TestGitHubTestsLateRerunDeepStopAdvances(t *testing.T) {
	since, before := time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	claim := lateRerunClaim(since, before)
	late := listingRun{id: 1, created: time.Date(2026, 7, 1, 0, 50, 0, 0, time.UTC), startedAt: time.Date(2026, 7, 4, 1, 0, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	late.updated = late.startedAt.Add(time.Hour)
	second := time.Date(2026, 6, 28, 5, 0, 0, 0, time.UTC)
	saturating := make([]listingRun, 0, 1001)
	for index := range 1001 {
		saturating = append(saturating, listingRun{id: 5000 + index, created: second, updated: second.Add(time.Minute), status: "completed", conclusion: "success"})
	}
	for _, cell := range []struct {
		name    string
		handler GitHubTestsRouteHandler
		runs    []listingRun
		cause   string
	}{
		{"page budget", GitHubTestsRouteHandler{LateRerunMaxPages: 1}, append([]listingRun{late}, makeListingRuns(2000, 150, time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC), time.Minute)...), githubTestsPageBudgetCause},
		{"saturated second", GitHubTestsRouteHandler{}, append([]listingRun{late}, saturating...), githubTestsListingSaturatedCause},
		// 1,000 runs fill the first deep range and a 10-page budget exactly; the peeled next range finds the budget spent.
		{"page budget at a peel", GitHubTestsRouteHandler{LateRerunMaxPages: 10}, append([]listingRun{late}, makeListingRuns(3000, 1000, time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC), time.Minute)...), githubTestsPageBudgetCause},
	} {
		t.Run(cell.name, func(t *testing.T) {
			fake := newListingFake(t, cell.runs)
			walk := walkListing(t, cell.handler, claim, fake, 0)
			if fake.artifactGets[late.id] != 1 {
				t.Errorf("late re-run reads=%d, want 1 (newest first: listed before the stop)", fake.artifactGets[late.id])
			}
			want := []string{githubTestsLateRerunReportsComponent + "/" + cell.cause}
			var got []string
			var incomplete []GitHubTestsIncomplete
			if decodeCompletionValue(walk.final.Result, "incomplete", &incomplete) != nil {
				t.Fatal("result incomplete does not decode")
			}
			for _, observation := range incomplete {
				got = append(got, observation.Component+"/"+observation.Cause)
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("incomplete=%v, want %v", got, want)
			}
			if complete, _ := walk.final.Result[GitHubTestsResultKeyReportsComplete].(bool); complete {
				t.Error("reports_complete=true, want false")
			}
			if walk.final.Watermark == nil || !walk.final.Watermark.Equal(before) {
				t.Errorf("watermark=%v, want %s (an advancing pair)", walk.final.Watermark, before)
			}
			if err := validateGitHubTestsCompletion(claim, walk.final); err != nil {
				t.Errorf("comparator refused the completion: %v", err)
			}
			if cell.name != "page budget" {
				return
			}
			oracleFake := newListingFake(t, cell.runs)
			batch, err := cell.handler.Collect(context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, fakehttp.Client(oracleFake)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			if decodeCompletionValue(batch.Result, "incomplete", &incomplete) != nil || len(incomplete) != 1 || incomplete[0].Component != githubTestsLateRerunReportsComponent ||
				batch.Watermark == nil || oracleFake.artifactGets[late.id] != 1 {
				t.Errorf("oracle: incomplete=%+v watermark=%v late reads=%d, want the deep-range stop, an advanced watermark and 1 read", incomplete, batch.Watermark, oracleFake.artifactGets[late.id])
			}
			if err := validateGitHubTestsCompletion(claim, batch); err != nil {
				t.Errorf("comparator refused the oracle completion: %v", err)
			}
		})
	}
}

// The cursor carries the re-list state in one small field: walking, or one of the two stops. Any other value is corrupt, and a
// clean deep walk leaves it at zero.
func TestGitHubTestsLateRerunCursorState(t *testing.T) {
	for _, raw := range []string{`{"phase":"artifacts","rl":-1}`, `{"phase":"artifacts","rl":4}`} {
		if _, err := decodeGitHubTestsChunkCursor(raw); err == nil {
			t.Errorf("cursor %s decoded, want a checkpoint conflict", raw)
		}
	}
	for state := githubTestsRelistWalking; state <= githubTestsRelistStoppedSaturated; state++ {
		raw := fmt.Sprintf(`{"phase":"artifacts","rl":%d}`, state)
		if _, err := decodeGitHubTestsChunkCursor(raw); err != nil {
			t.Errorf("cursor %s: %v, want it accepted", raw, err)
		}
	}
	claim := lateRerunClaim(time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	_, fake, walk := lateRerunReads(t, claim, nil, 0)
	if len(lateRerunDeepRanges(fake)) != 1 || walk.cursor.Relist != 0 || walk.cursor.Phase != "done" {
		t.Errorf("deep ranges=%v final relist=%d phase=%q, want one deep range walked and the state back at 0", lateRerunDeepRanges(fake), walk.cursor.Relist, walk.cursor.Phase)
	}
}

// The default deep budget is 1,000 pages. A deep range of 102 pages (over the regular range's 100) is walked in many chunks:
// each continuation resumes the deep listing where the last one stopped, and the walk ends with no observation.
func TestGitHubTestsLateRerunDeepRangeResumesAcrossChunks(t *testing.T) {
	since, before := time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	late := listingRun{id: 1, created: time.Date(2026, 7, 1, 0, 50, 0, 0, time.UTC), startedAt: time.Date(2026, 7, 4, 1, 0, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	late.updated = late.startedAt.Add(time.Hour)
	old := makeListingRuns(100000, 10100, time.Date(2026, 6, 27, 0, 0, 0, 0, time.UTC), 30*time.Second)
	fake := newListingFake(t, append([]listingRun{late}, old...))
	walk := walkListing(t, GitHubTestsRouteHandler{}, lateRerunClaim(since, before), fake, 2000)
	if walk.passes < 3 {
		t.Fatalf("passes=%d, want a deep range that needs several chunks", walk.passes)
	}
	if fake.artifactGets[late.id] != 1 {
		t.Errorf("late re-run reads=%d, want 1", fake.artifactGets[late.id])
	}
	if got := append(walk.cursor.Incomplete, githubTestsSaturatedObservations(walk.cursor)...); len(got) != 0 {
		t.Errorf("incomplete=%+v, want none: the deep range was walked whole", got)
	}
	if walk.final.Watermark == nil || !walk.final.Watermark.Equal(before) || walk.cursor.ArtifactPages <= 100 {
		t.Errorf("watermark=%v deep pages=%d, want %s and more than the regular range's 100-page budget", walk.final.Watermark, walk.cursor.ArtifactPages, before)
	}
}

// The deep range's settings are refused outside their bounds by both routes, before any request.
func TestGitHubTestsLateRerunRefusesBadSettings(t *testing.T) {
	claim := lateRerunClaim(time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for name, handler := range map[string]GitHubTestsRouteHandler{
		"pages over 1000": {LateRerunMaxPages: githubTestsLateRerunMaxPagesDefault + 1},
		"negative pages":  {LateRerunMaxPages: -1},
		"negative L":      {LateRerunLookback: -time.Hour},
	} {
		client := githubTestsClient(t, fakehttp.Client(newListingFake(t, nil)))
		if err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, "", func(ChunkRouteEmission) error { return nil }); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: CollectChunks err=%v, want ErrInvalidConfiguration", name, err)
		}
		if _, err := handler.Collect(context.Background(), claim, providerfoundation.Credential{}, client, now); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: Collect err=%v, want ErrInvalidConfiguration", name, err)
		}
	}
}

// lateRerunLooseDoer is a GitHub whose created filter is loose on the deep range: a deep listing answers every run. Each route
// must still read a run created at or after the regular floor only once, in the regular range.
// With every set, every listing is loose, not only the deep one. A run created at the zero time is served with created_at null.
type lateRerunLooseDoer struct {
	*listingFake
	deep  *int
	every bool
}

func (doer lateRerunLooseDoer) Do(request *http.Request) (*http.Response, error) {
	if created := request.URL.Query().Get("created"); request.URL.Path == "/repos/acme/api/actions/runs" && created != "" &&
		(doer.every || strings.HasSuffix(created, "T23:59:59Z")) {
		if strings.HasSuffix(created, "T23:59:59Z") {
			*doer.deep++
		}
		loose := *request.URL
		query := loose.Query()
		query.Del("created")
		loose.RawQuery = query.Encode()
		request = request.Clone(request.Context())
		request.URL = &loose
	}
	response, err := doer.listingFake.Do(request)
	if err != nil || request.URL.Path != "/repos/acme/api/actions/runs" {
		return response, err
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(strings.NewReader(strings.ReplaceAll(string(body), `"created_at":"0001-01-01T00:00:00Z"`, `"created_at":null`)))
	return response, nil
}

func TestGitHubTestsLateRerunDeepRangeSkipsRunsAtOrAfterTheFloor(t *testing.T) {
	claim := lateRerunClaim(time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	inWindow := listingRun{id: 9, created: time.Date(2026, 7, 4, 23, 10, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	inWindow.updated = inWindow.created.Add(20 * time.Minute)
	t.Run("chunked", func(t *testing.T) {
		fake, deep := newListingFake(t, []listingRun{inWindow}), 0
		client := githubTestsClient(t, fakehttp.Client(lateRerunLooseDoer{listingFake: fake, deep: &deep}))
		if err := (GitHubTestsRouteHandler{}).CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
			func(ChunkRouteEmission) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if deep != 1 || fake.artifactGets[inWindow.id] != 1 {
			t.Errorf("deep listings=%d reads=%d, want one deep listing that answered the run and one read (regular range only)", deep, fake.artifactGets[inWindow.id])
		}
	})
	t.Run("oracle", func(t *testing.T) {
		fake, deep := newListingFake(t, []listingRun{inWindow}), 0
		client := githubTestsClient(t, fakehttp.Client(lateRerunLooseDoer{listingFake: fake, deep: &deep}))
		if _, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
		if deep != 1 || fake.artifactGets[inWindow.id] != 1 {
			t.Errorf("deep listings=%d reads=%d, want one deep listing that answered the run and one read (regular range only)", deep, fake.artifactGets[inWindow.id])
		}
	})
}

// The deep range starts the page count again only once the regular range is walked whole. A walk that yields and resumes many
// times inside the regular range keeps its count: the regular budget (2 pages) still stops a 3-page regular range (a re-entered
// page takes its paginator allowance, so it stops after the first page here), the watermark is held and no deep range is
// listed.
func TestGitHubTestsLateRerunRegularRangeResumeKeepsItsPageCount(t *testing.T) {
	claim := lateRerunClaim(time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	fake := newListingFake(t, makeListingRuns(500, 300, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), time.Minute))
	walk := walkListing(t, GitHubTestsRouteHandler{MaxRuns: 200}, claim, fake, 40)
	if walk.passes < 3 {
		t.Fatalf("passes=%d, want resumes inside the regular range", walk.passes)
	}
	truncated := false
	for _, observation := range walk.cursor.Incomplete {
		truncated = truncated || observation.Component == githubTestsArtifactInventoryComponent && observation.Cause == githubTestsPageBudgetCause
	}
	if !truncated || walk.final.Watermark != nil || len(lateRerunDeepRanges(fake)) != 0 || walk.cursor.ArtifactPages < 1 || walk.cursor.ArtifactPages > 2 {
		t.Errorf("incomplete=%+v watermark=%v deep ranges=%v artifact pages=%d, want the regular budget to stop at 2 pages, no watermark, no deep range",
			walk.cursor.Incomplete, walk.final.Watermark, lateRerunDeepRanges(fake), walk.cursor.ArtifactPages)
	}
}

// A run with no created time is never read by the deep range (GitHub cannot list it by created), on either route; the regular
// range still reads it (its rule never excludes a run it cannot date), on either route.
func TestGitHubTestsLateRerunNilCreatedIsNotReadByTheDeepRange(t *testing.T) {
	undated := listingRun{id: 11, startedAt: time.Date(2026, 7, 4, 22, 10, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	undated.updated = undated.startedAt.Add(20 * time.Minute)
	owning := lateRerunClaim(time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	regular := lateRerunClaim(time.Date(2026, 7, 4, 22, 0, 0, 0, time.UTC), time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC))
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, cell := range []struct {
		name  string
		claim Claim
		every bool
		want  int
	}{
		{"deep range", owning, false, 0},
		{"regular range unchanged", regular, true, 1},
	} {
		for _, route := range []string{"chunked", "oracle"} {
			t.Run(cell.name+"/"+route, func(t *testing.T) {
				fake, deep := newListingFake(t, []listingRun{undated}), 0
				client := githubTestsClient(t, fakehttp.Client(lateRerunLooseDoer{listingFake: fake, deep: &deep, every: cell.every}))
				var err error
				if route == "chunked" {
					err = (GitHubTestsRouteHandler{}).CollectChunks(context.Background(), cell.claim, providerfoundation.Credential{}, client, now, "",
						func(ChunkRouteEmission) error { return nil })
				} else {
					_, err = (GitHubTestsRouteHandler{}).Collect(context.Background(), cell.claim, providerfoundation.Credential{}, client, now)
				}
				if err != nil {
					t.Fatal(err)
				}
				if cell.want == 0 && deep != 1 {
					t.Fatalf("deep listings=%d, want the deep listing to answer the undated run", deep)
				}
				if fake.artifactGets[undated.id] != cell.want {
					t.Errorf("reads=%d, want %d", fake.artifactGets[undated.id], cell.want)
				}
			})
		}
	}
}
