package providersync

import (
	"context"
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
// returns how many times the chunk read the run's reports.
func githubTestsBackfillChunkReads(t *testing.T, since, before time.Time, created, updated time.Time) int {
	t.Helper()
	doer := githubTestsReportWindowDoerFor(t, updated.UTC().Format(time.RFC3339))
	doer.createdAt = created.UTC().Format(time.RFC3339)
	doer.startedAt = created.Add(time.Minute).UTC().Format(time.RFC3339)
	doer.filterCreated = true
	claim := githubTestsWindowClaim(t, since, before)
	claim.Mode = "backfill"
	_, cases := collectGitHubTestsReportRows(t, doer, claim, updated.Add(time.Hour))
	lists, zips := githubTestsReportReads(doer)
	if lists != zips || (lists > 0) != (cases > 0) {
		t.Fatalf("chunk [%s, %s): artifact lists=%d downloads=%d cases=%d, want a whole read or none",
			since.Format(time.RFC3339), before.Format(time.RFC3339), lists, zips, cases)
	}
	return lists
}

// githubTestsBackfillTotalReads sums the reads of one run over contiguous
// backfill chunks of one width starting at start.
func githubTestsBackfillTotalReads(t *testing.T, start time.Time, width time.Duration, chunks int, created, updated time.Time) (int, []int) {
	t.Helper()
	perChunk := make([]int, chunks)
	total := 0
	for index := range chunks {
		since := start.Add(time.Duration(index) * width)
		perChunk[index] = githubTestsBackfillChunkReads(t, since, since.Add(width), created, updated)
		total += perChunk[index]
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
		// Residual (no unit knows it is the last chunk): a run listed by the
		// last chunk's next window and updated past the last chunk is read by
		// no backfill chunk; that window's own unit reads it.
		created, updated := shape.start.Add(3*shape.width+shape.width/2), shape.start.Add(chunks*shape.width+time.Hour)
		total, perChunk := githubTestsBackfillTotalReads(t, shape.start, shape.width, chunks, created, updated)
		t.Logf("RESIDUAL %s created=%s updated=%s: reads per chunk=%v total=%d", shape.name,
			created.Format(time.RFC3339), updated.Format(time.RFC3339), perChunk, total)
		if total != 0 {
			t.Errorf("%s residual: reads per chunk=%v, want 0 (left to the window of its updated_at)", shape.name, perChunk)
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
// backfill chunks of any widths (edges[i], edges[i+1]).
func githubTestsBackfillReadsOverWindows(t *testing.T, edges []time.Time, created, updated time.Time) []int {
	t.Helper()
	perChunk := make([]int, len(edges)-1)
	for index := range perChunk {
		perChunk[index] = githubTestsBackfillChunkReads(t, edges[index], edges[index+1], created, updated)
	}
	return perChunk
}

// RESIDUAL (ticket 14222): a chunk takes the next chunk's listing floor from a
// next window of its OWN width. The planner's last chunk can be shorter
// (a range [D, D+8d) gives [D, D+7d) and [D+7d, D+8d)), and its own floor is
// later. A run created between the two floors and updated inside the last
// chunk is read by no chunk. WANT = today's value 0; a fix flips it.
func TestGitHubTestsBackfillRESIDUALShorterLastChunkReadsNothing(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	edges := []time.Time{d, d.AddDate(0, 0, 7), d.AddDate(0, 0, 8)}
	created, updated := d.AddDate(0, 0, 3), d.AddDate(0, 0, 7).Add(12*time.Hour)
	perChunk := githubTestsBackfillReadsOverWindows(t, edges, created, updated)
	t.Logf("RESIDUAL shorter last chunk: chunks [%s,%s) [%s,%s) created=%s updated=%s reads per chunk=%v",
		edges[0].Format(time.RFC3339), edges[1].Format(time.RFC3339), edges[1].Format(time.RFC3339), edges[2].Format(time.RFC3339),
		created.Format(time.RFC3339), updated.Format(time.RFC3339), perChunk)
	if !slices.Equal(perChunk, []int{0, 0}) {
		t.Errorf("reads per chunk=%v, want today's residual [0 0]", perChunk)
	}
}

// RESIDUAL (ticket 14222): a chunk a whole day or more shorter than the chunk
// after it computes a later next floor than the next chunk really uses, so a
// run created between the two floors and updated in the next chunk is read by
// both. WANT = today's value 2 (the second read writes no new row: the
// integration cell TestGitHubTestsBackfillIntegrationRESIDUALSecondReadWritesNoNewRow).
// The planner's first chunk runs from since to the 7th date after it, so it is
// less than one day short and the DateOnly floor absorbs it: 1 read.
func TestGitHubTestsBackfillRESIDUALShorterFirstChunkReadsTwice(t *testing.T) {
	t.Parallel()
	a := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	for _, cell := range []struct {
		name             string
		edges            []time.Time
		created, updated time.Time
		want             []int
	}{
		{"1-day chunk before a 7-day chunk", []time.Time{a, a.AddDate(0, 0, 1), a.AddDate(0, 0, 8)},
			a.Add(-12 * time.Hour), a.AddDate(0, 0, 3), []int{1, 1}},
		{"planner first chunk from 12:00 before a 7-day chunk", []time.Time{a.Add(12 * time.Hour), a.AddDate(0, 0, 7), a.AddDate(0, 0, 14)},
			a.Add(-12 * time.Hour), a.AddDate(0, 0, 10), []int{1, 0}},
	} {
		perChunk := githubTestsBackfillReadsOverWindows(t, cell.edges, cell.created, cell.updated)
		t.Logf("RESIDUAL %s: created=%s updated=%s reads per chunk=%v", cell.name,
			cell.created.Format(time.RFC3339), cell.updated.Format(time.RFC3339), perChunk)
		if !slices.Equal(perChunk, cell.want) {
			t.Errorf("%s: reads per chunk=%v, want today's value %v", cell.name, perChunk, cell.want)
		}
	}
}
