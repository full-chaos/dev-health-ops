package providersync

import (
	"strings"
	"testing"
	"time"
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
		{"over one window before since", "incremental", "2026-07-22T10:59:59Z", hourSince, hourBefore, false},
		{"previous hourly window", "full_resync", "2026-07-22T11:30:00Z", hourSince, hourBefore, true},
	} {
		runGitHubTestsReportScopeCell(t, cell)
	}
}
