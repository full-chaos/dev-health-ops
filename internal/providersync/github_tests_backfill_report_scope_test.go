package providersync

import (
	"context"
	"maps"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsCreatedInRange applies a runs listing "created" qualifier (lo..hi,
// >=lo or *..hi; each edge a date or an RFC 3339 time, both inclusive) to a
// run's created_at, the way GitHub filters the listing.
func githubTestsCreatedInRange(t *testing.T, qualifier, created string) bool {
	t.Helper()
	if qualifier == "" {
		return true
	}
	at, err := time.Parse(time.RFC3339, created)
	if err != nil {
		t.Fatalf("fixture created_at %q: %v", created, err)
	}
	lo, hi := "*", "*"
	if rest, ok := strings.CutPrefix(qualifier, ">="); ok {
		lo = rest
	} else if before, after, ok := strings.Cut(qualifier, ".."); ok {
		lo, hi = before, after
	} else {
		t.Fatalf("unknown created qualifier %q", qualifier)
	}
	edge := func(value string, end bool) time.Time {
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			return parsed
		}
		parsed, err := time.Parse(time.DateOnly, value)
		if err != nil {
			t.Fatalf("created qualifier edge %q: %v", value, err)
		}
		if end {
			return parsed.Add(24*time.Hour - time.Nanosecond)
		}
		return parsed
	}
	if lo != "*" && at.Before(edge(lo, false)) {
		return false
	}
	return hi == "*" || !at.After(edge(hi, true))
}

// githubTestsReportReads counts the report reads of fixture run 9001: its
// artifact listings and its artifact downloads.
func githubTestsReportReads(doer *githubTestsReportWindowDoer) (lists, zips int) {
	for _, path := range doer.requests {
		switch path {
		case "/repos/acme/api/actions/runs/9001/artifacts":
			lists++
		case "/repos/acme/api/actions/artifacts/77/zip":
			zips++
		}
	}
	return lists, zips
}

type githubTestsReportScopeCell struct {
	name, mode, finished string
	since, before        time.Time
	wantRead             bool
}

func runGitHubTestsReportScopeCell(t *testing.T, cell githubTestsReportScopeCell) {
	t.Helper()
	doer := githubTestsReportWindowDoerFor(t, cell.finished)
	claim := githubTestsWindowClaim(t, cell.since, cell.before)
	claim.Mode = cell.mode
	_, cases := collectGitHubTestsReportRows(t, doer, claim, cell.before.Add(5*time.Minute))
	lists, zips := githubTestsReportReads(doer)
	if read := lists+zips > 0; read != cell.wantRead {
		t.Errorf("%s: %s window [%s, %s) run finished %s: artifact lists=%d downloads=%d cases=%d, want read=%v",
			cell.name, cell.mode, cell.since.Format(time.RFC3339), cell.before.Format(time.RFC3339), cell.finished,
			lists, zips, cases, cell.wantRead)
	}
	if cell.wantRead && (lists != 1 || zips != 1 || cases == 0) {
		t.Errorf("%s: artifact lists=%d downloads=%d cases=%d, want one read that projects cases",
			cell.name, lists, zips, cases)
	}
}

// CHAOS-14213. An incremental unit offers a run to its own window AND the one
// after it (two looks), because a run's artifacts can become listable late. A
// backfill unit reads only the runs that finished inside its own window: its
// chunks run in unit-id order, not time order, so the next chunk's look is no
// later look, and it only read the previous chunk's reports again.
func TestGitHubTestsBackfillReportScopeReadsOnlyRunsFinishedInItsWindow(t *testing.T) {
	t.Parallel()
	hourSince := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	hourBefore := hourSince.Add(time.Hour)
	weekSince := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	weekBefore := weekSince.AddDate(0, 0, 7)
	for _, cell := range []githubTestsReportScopeCell{
		{"previous hourly window", "incremental", "2026-07-22T11:30:00Z", hourSince, hourBefore, true},
		{"previous hourly window", "backfill", "2026-07-22T11:30:00Z", hourSince, hourBefore, false},
		{"previous 7-day chunk", "incremental", "2026-07-22T10:30:00Z", weekSince, weekBefore, true},
		{"previous 7-day chunk", "backfill", "2026-07-22T10:30:00Z", weekSince, weekBefore, false},
		{"one window before since", "incremental", "2026-07-22T11:00:00Z", hourSince, hourBefore, true},
		{"one second under since", "backfill", "2026-07-22T11:59:59Z", hourSince, hourBefore, false},
		{"at since", "backfill", "2026-07-22T12:00:00Z", hourSince, hourBefore, true},
		{"inside the window", "backfill", "2026-07-22T12:30:00Z", hourSince, hourBefore, true},
		{"at before", "backfill", "2026-07-22T13:00:00Z", hourSince, hourBefore, false},
		{"at before", "incremental", "2026-07-22T13:00:00Z", hourSince, hourBefore, true},
		{"over one window before since", "incremental", "2026-07-22T10:59:59Z", hourSince, hourBefore, false},
		{"previous hourly window", "full_resync", "2026-07-22T11:30:00Z", hourSince, hourBefore, true},
	} {
		runGitHubTestsReportScopeCell(t, cell)
	}
}

// CHAOS-14213 (acp condition 1). One look still reads a run CREATED before the
// window that finished inside it: the listing keeps its created-from lookback
// (the run is created on the day before since), and the run is read once, by
// the window it finished in, not by the previous backfill chunk it started in.
func TestGitHubTestsBackfillReadsARunCreatedBeforeItsWindowOnceInTheWindowItFinished(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	before := since.Add(24 * time.Hour)
	previousSince := since.Add(-24 * time.Hour)
	for _, cell := range []struct {
		name          string
		since, before time.Time
		wantReads     int
	}{
		{"this window", since, before, 1},
		{"previous window", previousSince, since, 0},
	} {
		doer := githubTestsReportWindowDoerFor(t, "2026-07-22T02:00:00Z")
		doer.createdAt, doer.startedAt, doer.filterCreated = "2026-07-21T23:00:00Z", "2026-07-21T23:01:00Z", true
		claim := githubTestsWindowClaim(t, cell.since, cell.before)
		claim.Mode = "backfill"
		_, cases := collectGitHubTestsReportRows(t, doer, claim, before.Add(5*time.Minute))
		lists, zips := githubTestsReportReads(doer)
		if lists != cell.wantReads || zips != cell.wantReads || (cell.wantReads > 0) != (cases > 0) {
			t.Errorf("%s [%s, %s): artifact lists=%d downloads=%d cases=%d, want %d read(s); run queries=%v",
				cell.name, cell.since.Format(time.RFC3339), cell.before.Format(time.RFC3339), lists, zips, cases,
				cell.wantReads, doer.runQueries)
		}
	}
}

// CHAOS-14213 (acp condition 2). With one look, a report lost in a backfill
// unit's own window gets no second read from the next chunk, so the loss must
// stay loud on its own unit: reports_complete=false, the counted cause in
// `incomplete` (a gap cause for sync coverage, not a closed-with-reason one,
// synccoverage closedWithReasonCauses) and the per-artifact marker a targeted
// backfill uses.
func TestGitHubTestsBackfillTransientReportLossStaysALoudGap(t *testing.T) {
	t.Parallel()
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 2, noLocation: map[int]bool{1: true}}
	claim := nativeTestClaim("github", "cicd")
	since := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	before := since.Add(24 * time.Hour)
	claim.Mode, claim.SinceAt, claim.BeforeAt = "backfill", &since, &before
	var final *CompleteRouteBatch
	err := GitHubTestsRouteHandler{}.CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), before.Add(5*time.Minute), "",
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				batch := emission.Batch
				final = &batch
			}
			return nil
		},
	)
	if err != nil || final == nil {
		t.Fatalf("CollectChunks err=%v final=%v: the unit must finish with the loss recorded", err, final != nil)
	}
	if doer.archiveRequests != 2 {
		t.Fatalf("archive requests=%d, want 2: the run finished in the window must be read", doer.archiveRequests)
	}
	if complete, ok := final.Result["reports_complete"].(bool); !ok || complete {
		t.Fatalf("reports_complete=%v, want false", final.Result["reports_complete"])
	}
	incomplete, ok := final.Result["incomplete"].([]GitHubTestsIncomplete)
	if !ok {
		t.Fatalf("incomplete=%#v, want the typed loss list", final.Result["incomplete"])
	}
	observation := githubTestsSkipObservation(t, incomplete, githubTestsReportMemberComponent)
	if observation.Cause != githubTestsArtifactUnavailableCause || observation.Count != 1 {
		t.Fatalf("observation=%+v, want cause=%s count=1", observation, githubTestsArtifactUnavailableCause)
	}
	markers, ok := final.Result["skipped_artifacts"].([]GitHubTestsSkippedArtifact)
	if !ok || len(markers) != 1 || markers[0].Cause != githubTestsArtifactUnavailableCause {
		t.Fatalf("skipped_artifacts=%#v, want one %s marker", final.Result["skipped_artifacts"], githubTestsArtifactUnavailableCause)
	}
}

// githubTestsBackfillChunkReads runs one backfill chunk over fixture run 9001
// (listed only when its created_at is inside the chunk's created qualifier) and
// returns how many times the chunk read the run's reports. next is the claim's
// BackfillNext (nil = tiling not known).
func githubTestsBackfillChunkReads(t *testing.T, since, before time.Time, next *BackfillNextWindow, created, updated time.Time) int {
	t.Helper()
	lists, _ := githubTestsBackfillChunkReadsAt(t, since, before, next, created, updated, updated.Add(time.Hour))
	return lists
}

// githubTestsBackfillChunkReadsAt is githubTestsBackfillChunkReads at a given
// normalize time; it also returns the runs listings' raw queries.
func githubTestsBackfillChunkReadsAt(
	t *testing.T, since, before time.Time, next *BackfillNextWindow, created, updated, normalizedAt time.Time,
) (int, []string) {
	t.Helper()
	return githubTestsChunkReadsOfRun(t, "backfill", since, before, next, created, created.Add(time.Minute), updated, normalizedAt)
}

// githubTestsChunkReadsOfRun runs one claim of the mode over fixture run 9001
// with the given created_at, run_started_at (its latest attempt) and
// updated_at (zero = the payload has no updated_at). It returns how many times
// the claim read the run's reports and the runs listings' raw queries.
func githubTestsChunkReadsOfRun(
	t *testing.T, mode string, since, before time.Time, next *BackfillNextWindow, created, started, updated, normalizedAt time.Time,
) (int, []string) {
	t.Helper()
	doer := githubTestsReportWindowDoerFor(t, updated.UTC().Format(time.RFC3339))
	doer.omitUpdatedAt = updated.IsZero()
	doer.createdAt = created.UTC().Format(time.RFC3339)
	doer.startedAt = started.UTC().Format(time.RFC3339)
	doer.filterCreated = true
	claim := githubTestsWindowClaim(t, since, before)
	claim.Mode = mode
	claim.BackfillNext = next
	_, cases := collectGitHubTestsReportRows(t, doer, claim, normalizedAt)
	lists, zips := githubTestsReportReads(doer)
	if lists != zips || (lists > 0) != (cases > 0) {
		t.Fatalf("chunk [%s, %s): artifact lists=%d downloads=%d cases=%d, want a whole read or none",
			since.Format(time.RFC3339), before.Format(time.RFC3339), lists, zips, cases)
	}
	return lists, doer.runQueries
}

// githubTestsBackfillNextOf is the BackfillNext the claim SQL gives chunk index
// of the tiling edges: the next chunk's before, or none for the last chunk.
func githubTestsBackfillNextOf(edges []time.Time, index int) *BackfillNextWindow {
	if index+2 >= len(edges) {
		return &BackfillNextWindow{}
	}
	nextBefore := edges[index+2]
	return &BackfillNextWindow{BeforeAt: &nextBefore}
}

// githubTestsBackfillTotalReads sums the reads of one run over contiguous
// backfill chunks of one width starting at start, each claim knowing the tiling.
func githubTestsBackfillTotalReads(t *testing.T, start time.Time, width time.Duration, chunks int, created, updated time.Time) (int, []int) {
	t.Helper()
	edges := make([]time.Time, chunks+1)
	for index := range edges {
		edges[index] = start.Add(time.Duration(index) * width)
	}
	perChunk := githubTestsBackfillReadsOverWindows(t, edges, created, updated)
	total := 0
	for _, reads := range perChunk {
		total += reads
	}
	return total, perChunk
}

// CHAOS-14213 r1 P2 (round repro TestReviewBackfillLongRunListingGap). A run
// created before the finish chunk's listing floor and updated after the chunk
// it was created in is read by the chunk that still lists it; one read in all.
func TestGitHubTestsBackfillLongRunListingGap(t *testing.T) {
	t.Parallel()
	day := func(d, h, m int) time.Time { return time.Date(2026, 7, d, h, m, 0, 0, time.UTC) }
	for _, cell := range []struct {
		name             string
		start            time.Time
		width            time.Duration
		created, updated time.Time
		want             []int
	}{
		{"seven-day chunks", day(22, 0, 0), 7 * 24 * time.Hour, day(21, 23, 0), day(30, 0, 30), []int{1, 0}},
		{"seven-day chunks, run at the finish chunk's listing floor", day(22, 0, 0), 7 * 24 * time.Hour, day(22, 0, 0), day(30, 0, 30), []int{0, 1}},
		{"one-day chunks", day(22, 0, 0), 24 * time.Hour, day(21, 23, 0), day(23, 0, 30), []int{1, 0}},
		{"one-day chunks, run at the finish chunk's listing floor", day(22, 0, 0), 24 * time.Hour, day(22, 0, 0), day(23, 0, 30), []int{0, 1}},
	} {
		total, perChunk := githubTestsBackfillTotalReads(t, cell.start, cell.width, len(cell.want), cell.created, cell.updated)
		t.Logf("CELL %s: created=%s updated=%s reads per chunk=%v total=%d", cell.name,
			cell.created.Format(time.RFC3339), cell.updated.Format(time.RFC3339), perChunk, total)
		if total != 1 || !slices.Equal(perChunk, cell.want) {
			t.Errorf("%s: reads per chunk=%v, want %v (one read in all)", cell.name, perChunk, cell.want)
		}
	}
	doer := githubTestsReportWindowDoerFor(t, day(30, 0, 30).Format(time.RFC3339))
	doer.createdAt, doer.startedAt, doer.filterCreated = day(21, 23, 0).Format(time.RFC3339), day(21, 23, 1).Format(time.RFC3339), true
	claim := githubTestsWindowClaim(t, day(22, 0, 0), day(29, 0, 0))
	if collectGitHubTestsReportRows(t, doer, claim, day(30, 1, 0)); !doer.fetchedArtifacts() {
		t.Errorf("incremental control: the run was not read: %v", doer.requests)
	}
}

// CHAOS-14213 (acp condition 2 of the r1 ruling). Over contiguous backfill
// chunks of one width, every run updated at or after the first chunk's since is
// read exactly once. Chunk 1 [b0, b1) is the reference: created in it, at the
// next chunk's listing floor, one second under that floor, or before the first
// chunk; updated at since, mid, at before, in the next chunk, or past the last.
func TestGitHubTestsBackfillReadsEachRunOnceAcrossContiguousChunks(t *testing.T) {
	t.Parallel()
	const chunks = 4
	for _, shape := range []struct {
		name  string
		start time.Time
		width time.Duration
	}{
		{"1-day", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), 24 * time.Hour},
		{"7-day", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), 7 * 24 * time.Hour},
		{"30-hour off midnight", time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC), 30 * time.Hour},
	} {
		b0 := shape.start.Add(shape.width)
		b1 := b0.Add(shape.width)
		nextBefore := b1.Add(shape.width)
		nextFloor, err := time.Parse(time.DateOnly, githubTestsArtifactRunsCreatedFrom(Claim{Unit: Unit{SinceAt: &b1, BeforeAt: &nextBefore}}))
		if err != nil {
			t.Fatal(err)
		}
		createdAt := map[string]time.Time{
			"in chunk": b0.Add(shape.width / 2), "== next_floor": nextFloor,
			"next_floor - 1s": nextFloor.Add(-time.Second), "before the first chunk": shape.start.Add(-time.Hour),
		}
		updatedAt := map[string]time.Time{
			"== since": b0, "mid": b0.Add(shape.width / 2), "== before": b1,
			"next chunk": b1.Add(shape.width / 2), "past the last chunk": shape.start.Add(chunks*shape.width + time.Hour),
		}
		for _, createdName := range []string{"in chunk", "== next_floor", "next_floor - 1s", "before the first chunk"} {
			for _, updatedName := range []string{"== since", "mid", "== before", "next chunk", "past the last chunk"} {
				created, updated := createdAt[createdName], updatedAt[updatedName]
				if created.Add(time.Minute).After(updated) {
					continue
				}
				total, perChunk := githubTestsBackfillTotalReads(t, shape.start, shape.width, chunks, created, updated)
				t.Logf("CELL %s created=%s (%s) updated=%s (%s): reads per chunk=%v total=%d want=1", shape.name,
					created.Format(time.RFC3339), createdName, updated.Format(time.RFC3339), updatedName, perChunk, total)
				if total != 1 {
					t.Errorf("%s created %s updated %s: reads per chunk=%v, want exactly 1 read", shape.name, createdName, updatedName, perChunk)
				}
			}
		}
		// CHAOS-14222: the last chunk knows it is last, so a run it lists and
		// that is updated past the last chunk is read once, by the last chunk.
		created, updated := shape.start.Add(3*shape.width+shape.width/2), shape.start.Add(chunks*shape.width+time.Hour)
		total, perChunk := githubTestsBackfillTotalReads(t, shape.start, shape.width, chunks, created, updated)
		t.Logf("CELL %s past the last chunk created=%s updated=%s: reads per chunk=%v total=%d", shape.name,
			created.Format(time.RFC3339), updated.Format(time.RFC3339), perChunk, total)
		if !slices.Equal(perChunk, []int{0, 0, 0, 1}) {
			t.Errorf("%s past the last chunk: reads per chunk=%v, want [0 0 0 1] (the last chunk reads it)", shape.name, perChunk)
		}
	}
}

// CHAOS-14213 (acp condition 5). Chunks [a, b) and [b, c); a run updated at
// exactly b is read once, by [b, c).
func TestGitHubTestsBackfillRunUpdatedAtTheBoundaryIsReadOnceByTheLaterChunk(t *testing.T) {
	t.Parallel()
	a := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	width := 7 * 24 * time.Hour
	b := a.Add(width)
	total, perChunk := githubTestsBackfillTotalReads(t, a, width, 2, a.Add(width/2), b)
	t.Logf("CELL boundary: reads per chunk=%v total=%d", perChunk, total)
	if !slices.Equal(perChunk, []int{0, 1}) {
		t.Errorf("reads per chunk=%v, want [0 1]: read once, by [b, c)", perChunk)
	}
}

// githubTestsBackfillReadsOverWindows sums one run's reads over contiguous
// backfill chunks of any widths (edges[i], edges[i+1]); each claim carries the
// tiling the claim SQL gives it (the real next chunk, or last).
func githubTestsBackfillReadsOverWindows(t *testing.T, edges []time.Time, created, updated time.Time) []int {
	t.Helper()
	return githubTestsReadsOfRunOverWindows(t, "backfill", true, edges, created, created.Add(time.Minute), updated)
}

// githubTestsReadsOfRunOverWindows is one run's reads over contiguous claims of
// the mode (edges[i], edges[i+1]); tiled = each claim carries the tiling the
// claim SQL gives a backfill chunk, else none. A zero updated = no updated_at.
func githubTestsReadsOfRunOverWindows(t *testing.T, mode string, tiled bool, edges []time.Time, created, started, updated time.Time) []int {
	t.Helper()
	normalizedAt := updated.Add(time.Hour)
	if updated.IsZero() {
		normalizedAt = edges[len(edges)-1].Add(time.Hour)
	}
	perChunk := make([]int, len(edges)-1)
	for index := range perChunk {
		var next *BackfillNextWindow
		if tiled {
			next = githubTestsBackfillNextOf(edges, index)
		}
		perChunk[index], _ = githubTestsChunkReadsOfRun(t, mode, edges[index], edges[index+1], next, created, started, updated, normalizedAt)
	}
	return perChunk
}

// githubTestsBackfillReadsOverWindowsUnknownTiling is the same walk with no
// tiling on any claim (BackfillNext nil: an in-process or old claim).
func githubTestsBackfillReadsOverWindowsUnknownTiling(t *testing.T, edges []time.Time, created, updated time.Time) []int {
	t.Helper()
	perChunk := make([]int, len(edges)-1)
	for index := range perChunk {
		perChunk[index] = githubTestsBackfillChunkReads(t, edges[index], edges[index+1], nil, created, updated)
	}
	return perChunk
}

// CHAOS-14222: the planner's last chunk can be shorter (a range [D, D+8d)
// gives [D, D+7d) and [D+7d, D+8d)), so its own listing floor is later than a
// same-width guess. A run created between the two floors and updated inside the
// last chunk is not listed by the last chunk; the first chunk knows the real
// next floor and reads it. Was RESIDUAL...ShorterLastChunkReadsNothing ([0 0]).
func TestGitHubTestsBackfillShorterLastChunkReadsOnce(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	edges := []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 8)}
	created, updated := d.AddDate(0, 0, 3), d.AddDate(0, 0, 7).Add(12*time.Hour)
	perChunk := githubTestsBackfillReadsOverWindows(t, edges, created, updated)
	t.Logf("CELL shorter last chunk: chunks [%s,%s) [%s,%s) created=%s updated=%s reads per chunk=%v",
		edges[0].Format(time.RFC3339), edges[1].Format(time.RFC3339), edges[1].Format(time.RFC3339), edges[2].Format(time.RFC3339),
		created.Format(time.RFC3339), updated.Format(time.RFC3339), perChunk)
	if !slices.Equal(perChunk, []int{1, 0}) {
		t.Errorf("reads per chunk=%v, want [1 0]: the first chunk is the last one that lists the run", perChunk)
	}
}

// CHAOS-14222: a chunk a whole day or more shorter than the chunk after it
// takes the real next floor, so a run the next chunk lists is left to it: one
// read. Was RESIDUAL...ShorterFirstChunkReadsTwice ([1 1]). The planner's own
// first chunk (less than one day short) reads once either way.
func TestGitHubTestsBackfillShorterFirstChunkReadsOnce(t *testing.T) {
	t.Parallel()
	a := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	for _, cell := range []struct {
		name             string
		edges            []time.Time
		created, updated time.Time
		want             []int
	}{
		{"1-day chunk before a 7-day chunk", []time.Time{a, a.AddDate(0, 0, 1), a.AddDate(0, 0, 8)},
			a.Add(-12 * time.Hour), a.AddDate(0, 0, 3), []int{0, 1}},
		{"planner first chunk from 12:00 before a 7-day chunk", []time.Time{a.Add(12 * time.Hour), a.AddDate(0, 0, 7), a.AddDate(0, 0, 14)},
			a.Add(-12 * time.Hour), a.AddDate(0, 0, 10), []int{1, 0}},
	} {
		perChunk := githubTestsBackfillReadsOverWindows(t, cell.edges, cell.created, cell.updated)
		t.Logf("CELL %s: created=%s updated=%s reads per chunk=%v", cell.name,
			cell.created.Format(time.RFC3339), cell.updated.Format(time.RFC3339), perChunk)
		if !slices.Equal(perChunk, cell.want) {
			t.Errorf("%s: reads per chunk=%v, want %v", cell.name, perChunk, cell.want)
		}
	}
}

// CHAOS-14222 (acp condition 4): a claim with no tiling (BackfillNext nil: an
// in-process or old claim) keeps the same-width rule of ops #6587, so the two
// shapes above read as before: [0 0] and [1 1].
func TestGitHubTestsBackfillUnknownTilingKeepsTheSameWidthRule(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	a := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	for _, cell := range []struct {
		name             string
		edges            []time.Time
		created, updated time.Time
		want             []int
	}{
		{"shorter last chunk", []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 8)}, d.AddDate(0, 0, 3), d.AddDate(0, 0, 7).Add(12 * time.Hour), []int{0, 0}},
		{"1-day chunk before a 7-day chunk", []time.Time{a, a.AddDate(0, 0, 1), a.AddDate(0, 0, 8)}, a.Add(-12 * time.Hour), a.AddDate(0, 0, 3), []int{1, 1}},
		{"past the last chunk", []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 8)}, d.AddDate(0, 0, 7).Add(6 * time.Hour), d.AddDate(0, 0, 8).Add(time.Hour), []int{0, 0}},
	} {
		perChunk := githubTestsBackfillReadsOverWindowsUnknownTiling(t, cell.edges, cell.created, cell.updated)
		t.Logf("CELL nil tiling %s: reads per chunk=%v", cell.name, perChunk)
		if !slices.Equal(perChunk, cell.want) {
			t.Errorf("nil tiling %s: reads per chunk=%v, want the same-width rule %v", cell.name, perChunk, cell.want)
		}
	}
}

// CHAOS-14222: over the planner's real tilings (resolveBackfillWindows for
// github/tests: a short first chunk, 7-day interior chunks, a short last chunk,
// a lone short chunk), every run some chunk lists (created from the first
// chunk's floor to the range's before) and updated at or after the range's
// since is read exactly once.
func TestGitHubTestsBackfillMixedWidthTilingReadsEachRunOnce(t *testing.T) {
	t.Parallel()
	githubTestsBackfillPlannerTilingsReadEachRunOnce(t, func(created, _ time.Time) time.Time { return created.Add(time.Minute) })
}

// CHAOS-14249: the same planner tilings with run_started_at one minute before
// updated_at (a run re-run or queued late: its latest attempt starts after the
// chunk it was created in). Each run is still read exactly once: no loss, and
// no second chunk reads it.
func TestGitHubTestsBackfillLateStartMixedWidthTilingReadsEachRunOnce(t *testing.T) {
	t.Parallel()
	githubTestsBackfillPlannerTilingsReadEachRunOnce(t, func(_, updated time.Time) time.Time { return updated.Add(-time.Minute) })
}

func githubTestsBackfillPlannerTilingsReadEachRunOnce(t *testing.T, startedOf func(created, updated time.Time) time.Time) {
	t.Helper()
	at := func(day, hour int) time.Time { return time.Date(2026, 7, day, hour, 0, 0, 0, time.UTC) }
	for _, tiling := range []struct {
		name  string
		edges []time.Time
	}{
		{"[07-01, 07-09)", []time.Time{at(1, 0), at(8, 0), at(9, 0)}},
		{"[07-01 12:00, 07-17 06:00)", []time.Time{at(1, 12), at(8, 0), at(15, 0), at(17, 6)}},
		{"[07-01, 07-15)", []time.Time{at(1, 0), at(8, 0), at(15, 0)}},
		{"[07-01 06:00, 07-01 18:00)", []time.Time{at(1, 6), at(1, 18)}},
	} {
		edges := tiling.edges
		first, last := edges[0], edges[len(edges)-1]
		floorOf := func(index int) time.Time {
			floor, err := time.Parse(time.DateOnly, githubTestsArtifactRunsCreatedFrom(Claim{Unit: Unit{SinceAt: &edges[index], BeforeAt: &edges[index+1]}}))
			if err != nil {
				t.Fatal(err)
			}
			return floor
		}
		firstFloor := floorOf(0)
		created := []time.Time{firstFloor, firstFloor.Add(time.Hour), first.Add(-12 * time.Hour)}
		var updated []time.Time
		for index := 0; index+1 < len(edges); index++ {
			since, before := edges[index], edges[index+1]
			floor, mid := floorOf(index), since.Add(before.Sub(since)/2)
			created = append(created, floor, floor.Add(-time.Second), mid, before.Add(-2*time.Hour), since)
			updated = append(updated, since, mid, before.Add(-time.Second), before.AddDate(0, 0, 3))
		}
		updated = append(updated, last, last.Add(time.Hour), last.AddDate(0, 0, 9))
		cells := 0
		for _, c := range created {
			if c.Before(firstFloor) || c.After(last) {
				continue
			}
			for _, u := range updated {
				if started := startedOf(c, u); u.Before(first) || started.Before(c) || started.After(u) {
					continue
				}
				perChunk := githubTestsReadsOfRunOverWindows(t, "backfill", true, edges, c, startedOf(c, u), u)
				total := 0
				for _, reads := range perChunk {
					total += reads
				}
				cells++
				if total != 1 {
					t.Errorf("%s created=%s updated=%s: reads per chunk=%v, want exactly 1", tiling.name,
						c.Format(time.RFC3339), u.Format(time.RFC3339), perChunk)
				}
			}
		}
		t.Logf("CELL %s: %d runs, each read once", tiling.name, cells)
	}
}

// CHAOS-14222 (acp condition 1). The last chunk reads with no upper bound on
// updated_at, but only runs its runs listing returns, and that listing is
// created from its floor to its own before (githubTestsArtifactRunQuery). A
// backfill that ended weeks ago reads a run created inside it and updated
// today, and does not read a run created after it or before its floor.
func TestGitHubTestsBackfillLastChunkReadsOnlyRunsCreatedInItsListing(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	since, before := d, d.AddDate(0, 0, 1)
	now := d.AddDate(0, 0, 40)
	last := &BackfillNextWindow{}
	for _, cell := range []struct {
		name    string
		created time.Time
		want    int
	}{
		{"created inside the chunk", d.Add(12 * time.Hour), 1},
		{"created at the floor", d.AddDate(0, 0, -1), 1},
		{"created before the floor", d.AddDate(0, 0, -1).Add(-time.Hour), 0},
		{"created after the backfill", before.Add(time.Hour), 0},
		{"created weeks after the backfill", d.AddDate(0, 0, 30), 0},
	} {
		reads, queries := githubTestsBackfillChunkReadsAt(t, since, before, last, cell.created, now.Add(-time.Hour), now)
		t.Logf("CELL last chunk bound %s: reads=%d runs queries=%v", cell.name, reads, queries)
		if reads != cell.want {
			t.Errorf("%s: reads=%d, want %d", cell.name, reads, cell.want)
		}
		if len(queries) == 0 {
			t.Fatalf("%s: no runs listing", cell.name)
		}
		for _, query := range queries {
			values, err := url.ParseQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			if created := values.Get("created"); !strings.HasSuffix(created, ".."+before.Format(time.RFC3339)) {
				t.Errorf("%s: runs listing created=%q, want it to end at the chunk's before %s", cell.name, created, before.Format(time.RFC3339))
			}
		}
	}
}

// githubTestsLateStartShape is a run created in one chunk whose latest attempt
// (run_started_at) starts at or after that chunk's before.
type githubTestsLateStartShape struct {
	name                      string
	edges                     []time.Time
	created, started, updated time.Time
}

func githubTestsLateStartShapes() []githubTestsLateStartShape {
	day := func(d, h int) time.Time { return time.Date(2026, 7, d, h, 0, 0, 0, time.UTC) }
	d := day(1, 0)
	return []githubTestsLateStartShape{
		{"lone short chunk", []time.Time{day(1, 6), day(1, 18)}, day(1, 17), day(1, 19), day(1, 20)},
		{"lone short chunk, start at before", []time.Time{day(1, 6), day(1, 18)}, day(1, 17), day(1, 18), day(1, 20)},
		{"short last chunk", []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 8)},
			d.AddDate(0, 0, 7).Add(12 * time.Hour), d.AddDate(0, 0, 8).Add(time.Hour), d.AddDate(0, 0, 8).Add(2 * time.Hour)},
		{"full chunk before a short last chunk", []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 8)},
			d.AddDate(0, 0, 3), d.AddDate(0, 0, 7).Add(12 * time.Hour), d.AddDate(0, 0, 7).Add(13 * time.Hour)},
		{"full chunk, re-run in the next full chunk", []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 14), d.AddDate(0, 0, 15)},
			d.AddDate(0, 0, -1), d.AddDate(0, 0, 8), d.AddDate(0, 0, 8).Add(time.Hour)},
		{"next chunk lists it", []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 14)},
			d.AddDate(0, 0, 6).Add(23 * time.Hour), d.AddDate(0, 0, 7).Add(time.Hour), d.AddDate(0, 0, 7).Add(2 * time.Hour)},
	}
}

// CHAOS-14249. A backfill chunk lists runs by created_at, but run_started_at is
// the latest attempt. A chunk that knows the tiling no longer drops a run that
// started after its before: the run is read once, by the chunk that owns it by
// updated_at (or the last chunk that lists it). Was [0] [1] [0 0] [0 0] [0 0 0] [0 1].
func TestGitHubTestsBackfillLateStartRunIsReadOnce(t *testing.T) {
	t.Parallel()
	want := map[string][]int{
		"lone short chunk":                          {1},
		"lone short chunk, start at before":         {1},
		"short last chunk":                          {0, 1},
		"full chunk before a short last chunk":      {1, 0},
		"full chunk, re-run in the next full chunk": {1, 0, 0},
		"next chunk lists it":                       {0, 1},
	}
	for _, shape := range githubTestsLateStartShapes() {
		perChunk := githubTestsReadsOfRunOverWindows(t, "backfill", true, shape.edges, shape.created, shape.started, shape.updated)
		t.Logf("CELL late start %s: created=%s started=%s updated=%s reads per chunk=%v", shape.name,
			shape.created.Format(time.RFC3339), shape.started.Format(time.RFC3339), shape.updated.Format(time.RFC3339), perChunk)
		if !slices.Equal(perChunk, want[shape.name]) {
			t.Errorf("%s: reads per chunk=%v, want %v", shape.name, perChunk, want[shape.name])
		}
	}
}

// CHAOS-14249: outside a backfill chunk that knows the tiling, the
// run_started_at cutoff stays: incremental and full_resync claims, a backfill
// claim with no tiling, a run with no updated_at, and a zero-width claim read
// these shapes as before. An incremental claim that holds a UTC midnight also
// lists the late re-run deep range of the artifacts phase: in the two shapes
// whose re-run starts in the second window, that window's deep range reads it
// ([0 1] and [0 1 0]); the start cutoff of the first window is unchanged.
func TestGitHubTestsLateStartKeepsTheStartCutoffOutsideKnownTiling(t *testing.T) {
	t.Parallel()
	want := map[string][]int{
		"lone short chunk":                          {0},
		"lone short chunk, start at before":         {1},
		"short last chunk":                          {0, 0},
		"full chunk before a short last chunk":      {0, 0},
		"full chunk, re-run in the next full chunk": {0, 0, 0},
		"next chunk lists it":                       {0, 1},
	}
	wantNoTiling := map[string][]int{
		"lone short chunk":                          {0},
		"lone short chunk, start at before":         {0},
		"short last chunk":                          {0, 0},
		"full chunk before a short last chunk":      {0, 0},
		"full chunk, re-run in the next full chunk": {0, 0, 0},
		"next chunk lists it":                       {0, 1},
	}
	wantIncremental := maps.Clone(want)
	wantIncremental["full chunk before a short last chunk"] = []int{0, 1}
	wantIncremental["full chunk, re-run in the next full chunk"] = []int{0, 1, 0}
	for _, shape := range githubTestsLateStartShapes() {
		for _, claim := range []struct {
			mode  string
			tiled bool
			want  []int
		}{
			{"incremental", false, wantIncremental[shape.name]},
			{"full_resync", false, want[shape.name]},
			{"backfill", false, wantNoTiling[shape.name]},
		} {
			perChunk := githubTestsReadsOfRunOverWindows(t, claim.mode, claim.tiled, shape.edges, shape.created, shape.started, shape.updated)
			t.Logf("CELL start cutoff %s %s: reads per window=%v", claim.mode, shape.name, perChunk)
			if !slices.Equal(perChunk, claim.want) {
				t.Errorf("%s %s: reads per window=%v, want %v", claim.mode, shape.name, perChunk, claim.want)
			}
		}
	}
	day := func(d, h int) time.Time { return time.Date(2026, 7, d, h, 0, 0, 0, time.UTC) }
	lone := []time.Time{day(1, 6), day(1, 18)}
	if perChunk := githubTestsReadsOfRunOverWindows(t, "backfill", true, lone, day(1, 17), day(1, 19), time.Time{}); !slices.Equal(perChunk, []int{0}) {
		t.Errorf("tiled backfill, no updated_at, started after before: reads=%v, want [0]", perChunk)
	}
	if perChunk := githubTestsReadsOfRunOverWindows(t, "backfill", true, lone, day(1, 17), day(1, 17).Add(time.Minute), time.Time{}); !slices.Equal(perChunk, []int{1}) {
		t.Errorf("tiled backfill, no updated_at, started before before: reads=%v, want [1] (fail open)", perChunk)
	}
	zero := []time.Time{day(1, 18), day(1, 18)}
	if perChunk := githubTestsReadsOfRunOverWindows(t, "backfill", true, zero, day(1, 17), day(1, 19), day(1, 20)); !slices.Equal(perChunk, []int{0}) {
		t.Errorf("tiled zero-width backfill claim, started after before: reads=%v, want [0]", perChunk)
	}
}

// CHAOS-14249: the non-chunked comparison route shares the rule.
func TestGitHubTestsOracleReadsALateStartRunOfATiledBackfillChunk(t *testing.T) {
	t.Parallel()
	day := func(d, h int) time.Time { return time.Date(2026, 7, d, h, 0, 0, 0, time.UTC) }
	run := listingRun{id: 1, created: day(1, 17), startedAt: day(1, 19), updated: day(1, 20), status: "completed", conclusion: "success"}
	for _, cell := range []struct {
		mode string
		want int
	}{{"backfill", 1}, {"incremental", 0}} {
		fake := newListingFake(t, []listingRun{run})
		claim := listingClaim(day(1, 6), day(1, 18))
		claim.Mode = cell.mode
		if cell.mode == "backfill" {
			claim.BackfillNext = &BackfillNextWindow{}
		}
		if _, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{},
			githubTestsClient(t, fakehttp.Client(fake)), day(1, 21)); err != nil {
			t.Fatal(err)
		}
		t.Logf("CELL oracle %s: artifact lists=%d", cell.mode, fake.artifactGets[run.id])
		if fake.artifactGets[run.id] != cell.want {
			t.Errorf("oracle %s: artifact lists=%d, want %d", cell.mode, fake.artifactGets[run.id], cell.want)
		}
	}
}
