package providersync

import (
	"context"
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
