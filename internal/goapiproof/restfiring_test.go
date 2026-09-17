package goapiproof

import (
	"strings"
	"testing"
	"time"
)

// testBuildSHA renders a plain, constructed 40-hex-shaped literal from a
// short marker -- never a real-looking random value -- so a receipt's
// own candidate_build can be pinned in these tests without reading as a
// captured production sha.
func testBuildSHA(marker string) string {
	return strings.Repeat(marker, 40/len(marker))
}

// testRuns returns n synthetic run timestamps, OLDEST first (index 0),
// evenly spaced -- the reverse of ComputeFiringHistory's own "newest
// first" contract, so every test below builds its input with
// testRuns(n) then reverses (see newestFirst) exactly where the
// contract requires it, rather than hand-counting offsets per case.
func testRuns(n int) []time.Time {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([]time.Time, n)
	for i := range out {
		out[i] = base.Add(time.Duration(i) * time.Minute)
	}
	return out
}

// newestFirst reverses a testRuns(n) slice into the newest-first order
// ComputeFiringHistory's globalRuns/ownRuns both require.
func newestFirst(runs []time.Time) []time.Time {
	out := make([]time.Time, len(runs))
	for i, t := range runs {
		out[len(runs)-1-i] = t
	}
	return out
}

const testTicket = "ABC-123"

// knownLiveRun builds a POST-0135 row that declares testTicket (and
// nothing else) -- the ordinary "known, live" case every pre-existing
// determination test below models. fired, when true, adds testTicket to
// the row's own FiredTickets too.
func knownLiveRun(at time.Time, build string, fired bool) DeclarationRun {
	run := DeclarationRun{
		ObservedAt:      at,
		CandidateBuild:  build,
		DeclaredKnown:   true,
		DeclaredTickets: []string{testTicket},
	}
	if fired {
		run.FiredTickets = []string{testTicket}
	}
	return run
}

// unknownRun builds a PRE-0135 row: a receipt exists (the request was
// proven that run), but baseline_defect_declared is NULL -- this row
// was written before the declared-tickets column existed, so its
// DeclaredKnown/DeclaredTickets stay at their zero values.
func unknownRun(at time.Time, build string) DeclarationRun {
	return DeclarationRun{ObservedAt: at, CandidateBuild: build}
}

// TestComputeFiringHistory_FiresInTheNewestOfFiveIsNotFlagged proves a
// declaration that fired in the MOST RECENT of the five-run window is
// never NEVER_FIRED, however silent the other four runs were.
func TestComputeFiringHistory_FiresInTheNewestOfFiveIsNotFlagged(t *testing.T) {
	global := newestFirst(testRuns(5))
	own := []DeclarationRun{
		knownLiveRun(global[0], testBuildSHA("aa"), true),
		knownLiveRun(global[1], testBuildSHA("bb"), false),
		knownLiveRun(global[2], testBuildSHA("cc"), false),
		knownLiveRun(global[3], testBuildSHA("dd"), false),
		knownLiveRun(global[4], testBuildSHA("ee"), false),
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.NeverFired {
		t.Fatalf("NeverFired = true, want false -- the newest of the five runs fired: %+v", history)
	}
	if history.RunsFired != 1 {
		t.Fatalf("RunsFired = %d, want 1", history.RunsFired)
	}
	if history.LastFiredBuild != testBuildSHA("aa") {
		t.Fatalf("LastFiredBuild = %q, want %q", history.LastFiredBuild, testBuildSHA("aa"))
	}
	if history.WindowKnownRuns != FiringWindow {
		t.Fatalf("WindowKnownRuns = %d, want %d (all five known runs examined)", history.WindowKnownRuns, FiringWindow)
	}
}

// TestComputeFiringHistory_FiresInTheOldestOfFiveIsNotFlagged is the
// mirror case: the OLDEST of the five-run window fired, and nothing
// since -- still not NEVER_FIRED, and LastFiredBuild still names that
// run's own build, not the newest run's.
func TestComputeFiringHistory_FiresInTheOldestOfFiveIsNotFlagged(t *testing.T) {
	global := newestFirst(testRuns(5))
	own := []DeclarationRun{
		knownLiveRun(global[0], testBuildSHA("aa"), false),
		knownLiveRun(global[1], testBuildSHA("bb"), false),
		knownLiveRun(global[2], testBuildSHA("cc"), false),
		knownLiveRun(global[3], testBuildSHA("dd"), false),
		knownLiveRun(global[4], testBuildSHA("ee"), true),
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.NeverFired {
		t.Fatalf("NeverFired = true, want false -- the oldest of the five runs fired: %+v", history)
	}
	if history.RunsFired != 1 {
		t.Fatalf("RunsFired = %d, want 1", history.RunsFired)
	}
	if history.LastFiredBuild != testBuildSHA("ee") {
		t.Fatalf("LastFiredBuild = %q, want %q (the run it actually fired in, not the newest run)", history.LastFiredBuild, testBuildSHA("ee"))
	}
}

// TestComputeFiringHistory_LiveAndSilentAcrossAllFiveIsFlagged is the
// positive case the whole determination exists for: five consecutive
// KNOWN live runs, every one of them silent.
func TestComputeFiringHistory_LiveAndSilentAcrossAllFiveIsFlagged(t *testing.T) {
	global := newestFirst(testRuns(5))
	own := make([]DeclarationRun, len(global))
	for i, at := range global {
		own[i] = knownLiveRun(at, testBuildSHA("aa"), false)
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if !history.NeverFired {
		t.Fatalf("NeverFired = false, want true -- live and silent across all five known runs: %+v", history)
	}
	if history.RunsLive != 5 || history.RunsFired != 0 {
		t.Fatalf("RunsLive/RunsFired = %d/%d, want 5/0", history.RunsLive, history.RunsFired)
	}
	if history.WindowKnownRuns != FiringWindow {
		t.Fatalf("WindowKnownRuns = %d, want %d", history.WindowKnownRuns, FiringWindow)
	}
	if history.LastFiredBuild != "" {
		t.Fatalf("LastFiredBuild = %q, want empty -- this declaration has never fired", history.LastFiredBuild)
	}
}

// TestComputeFiringHistory_FewerThanFiveRunsIsNotFlagged proves a
// declaration is never flagged before it has even accumulated five
// KNOWN runs to be judged against, however silent every one of the
// runs it does have was -- and that WindowKnownRuns reports the
// partial fill rather than staying silent about why nothing flagged.
func TestComputeFiringHistory_FewerThanFiveRunsIsNotFlagged(t *testing.T) {
	global := newestFirst(testRuns(4))
	own := make([]DeclarationRun, len(global))
	for i, at := range global {
		own[i] = knownLiveRun(at, testBuildSHA("aa"), false)
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.NeverFired {
		t.Fatalf("NeverFired = true, want false -- only 4 runs exist in this table's own history: %+v", history)
	}
	if history.RunsLive != 4 {
		t.Fatalf("RunsLive = %d, want 4", history.RunsLive)
	}
	if history.WindowKnownRuns != 4 {
		t.Fatalf("WindowKnownRuns = %d, want 4 -- the window is still filling, and must say so", history.WindowKnownRuns)
	}
}

// TestComputeFiringHistory_AGapInLivenessBreaksConsecutiveness proves the
// five-run window is read from the GLOBAL run timeline, not assembled
// from whichever of this declaration's own runs happen to be live: five
// global runs exist, but this request produced NO RECEIPT AT ALL in one
// of them (live, live, ABSENT, live, live, newest to oldest) -- even
// though every run it WAS live in was silent and known, the missing
// middle run stops the window from filling past what it had already
// accumulated. Filtering ownRuns down to "the five most recent it was
// live in" and ignoring the gap would wrongly flag this case; the gap
// must stop the determination instead.
func TestComputeFiringHistory_AGapInLivenessBreaksConsecutiveness(t *testing.T) {
	global := newestFirst(testRuns(5))
	own := []DeclarationRun{
		knownLiveRun(global[0], testBuildSHA("aa"), false),
		knownLiveRun(global[1], testBuildSHA("bb"), false),
		// global[2] deliberately absent: this request produced no
		// receipt in that run (refused, or not yet part of the corpus).
		knownLiveRun(global[3], testBuildSHA("dd"), false),
		knownLiveRun(global[4], testBuildSHA("ee"), false),
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.NeverFired {
		t.Fatalf("NeverFired = true, want false -- a gap in this request's own liveness must break the five-run window: %+v", history)
	}
	if history.RunsLive != 4 {
		t.Fatalf("RunsLive = %d, want 4 -- RunsLive/RunsFired are all-time totals, unaffected by the five-run window", history.RunsLive)
	}
	if history.WindowKnownRuns != 2 {
		t.Fatalf("WindowKnownRuns = %d, want 2 -- the walk accumulates global[0] and global[1], then stops dead at the gap at global[2]", history.WindowKnownRuns)
	}
}

// TestComputeFiringHistory_NewlyDeclaredTicketIsNotFlaggedBeforeFiveKnownLiveRuns
// is the concrete scenario 0135 exists to prevent: a ticket added to the
// corpus TODAY has known (post-0135) receipts for its request in every
// recent run, but the two OLDEST of the five most recent runs predate
// the ticket's own addition -- their own (known, real)
// baseline_defect_declared arrays simply do not name it. That is real,
// KNOWN evidence the declaration was not live then, not silence to
// explain away, so the window must stop there rather than reaching
// three "declared, silent" runs and calling it done.
func TestComputeFiringHistory_NewlyDeclaredTicketIsNotFlaggedBeforeFiveKnownLiveRuns(t *testing.T) {
	global := newestFirst(testRuns(5))
	oldRunBeforeTicketExisted := func(at time.Time, build string) DeclarationRun {
		return DeclarationRun{ObservedAt: at, CandidateBuild: build, DeclaredKnown: true, DeclaredTickets: []string{"OTHER-1"}}
	}
	own := []DeclarationRun{
		knownLiveRun(global[0], testBuildSHA("aa"), false),       // ticket declared, silent
		knownLiveRun(global[1], testBuildSHA("bb"), false),       // ticket declared, silent
		oldRunBeforeTicketExisted(global[2], testBuildSHA("cc")), // known, but does NOT declare testTicket yet
		oldRunBeforeTicketExisted(global[3], testBuildSHA("dd")),
		oldRunBeforeTicketExisted(global[4], testBuildSHA("ee")),
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.NeverFired {
		t.Fatalf("NeverFired = true, want false -- this ticket has only 2 known-live runs behind it, not 5: %+v", history)
	}
	if history.WindowKnownRuns != 2 {
		t.Fatalf("WindowKnownRuns = %d, want 2 -- the walk stops at global[2], the first run that KNOWINGLY did not declare this ticket", history.WindowKnownRuns)
	}
}

// TestComputeFiringHistory_NullEraRowIsExcludedFromTheWindow proves a
// single PRE-0135 (NULL baseline_defect_declared) row sitting among
// otherwise known-live runs is skipped -- neither counted toward the
// window nor treated as a gap that stops it -- so the window keeps
// filling from runs further back. Six global runs: known, known,
// UNKNOWN, known, known, known (newest to oldest), all silent. A correct
// walk skips the unknown slot and still reaches five known-live runs
// using the sixth (oldest) one; an incorrect walk that either counted
// the unknown row as live or let it break the window like a real gap
// would stop at 2 or fail to reach 5 at all.
func TestComputeFiringHistory_NullEraRowIsExcludedFromTheWindow(t *testing.T) {
	global := newestFirst(testRuns(6))
	own := []DeclarationRun{
		knownLiveRun(global[0], testBuildSHA("aa"), false),
		knownLiveRun(global[1], testBuildSHA("bb"), false),
		unknownRun(global[2], testBuildSHA("cc")), // pre-0135: NULL declared column
		knownLiveRun(global[3], testBuildSHA("dd"), false),
		knownLiveRun(global[4], testBuildSHA("ee"), false),
		knownLiveRun(global[5], testBuildSHA("ff"), false),
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.WindowKnownRuns != FiringWindow {
		t.Fatalf("WindowKnownRuns = %d, want %d -- the unknown row at global[2] must be skipped, not counted and not a stop", history.WindowKnownRuns, FiringWindow)
	}
	if !history.NeverFired {
		t.Fatalf("NeverFired = false, want true -- five known-live runs were found (skipping the unknown one), all silent: %+v", history)
	}
}

// TestComputeFiringHistory_PostChangeRowCountsTowardTheWindow is the
// direct contrast test ruling #3 asks for by name: a single KNOWN
// (post-0135) row counts toward WindowKnownRuns; an otherwise identical
// UNKNOWN (pre-0135) row does not. Constructed minimally -- one global
// run -- so nothing else about the determination can be responsible for
// the difference.
func TestComputeFiringHistory_PostChangeRowCountsTowardTheWindow(t *testing.T) {
	global := newestFirst(testRuns(1))

	known := ComputeFiringHistory(testTicket, []DeclarationRun{knownLiveRun(global[0], testBuildSHA("aa"), false)}, global)
	if known.WindowKnownRuns != 1 {
		t.Fatalf("a post-0135 known row: WindowKnownRuns = %d, want 1", known.WindowKnownRuns)
	}

	unknown := ComputeFiringHistory(testTicket, []DeclarationRun{unknownRun(global[0], testBuildSHA("aa"))}, global)
	if unknown.WindowKnownRuns != 0 {
		t.Fatalf("a pre-0135 NULL-era row: WindowKnownRuns = %d, want 0 -- it must not count", unknown.WindowKnownRuns)
	}
}

// TestComputeFiringHistory_CountersAgainstAConstructedLongerHistory pins
// RunsLive, RunsFired and LastFiredBuild independently of the five-run
// determination, against a history longer than the five-run window: 3
// of 7 total live runs fired, most recently in the third-newest one, so
// LastFiredBuild must name THAT run's build, not the newest run's.
func TestComputeFiringHistory_CountersAgainstAConstructedLongerHistory(t *testing.T) {
	global := newestFirst(testRuns(7))
	own := []DeclarationRun{
		knownLiveRun(global[0], testBuildSHA("a0"), false),
		knownLiveRun(global[1], testBuildSHA("a1"), false),
		knownLiveRun(global[2], testBuildSHA("a2"), true),
		knownLiveRun(global[3], testBuildSHA("a3"), false),
		knownLiveRun(global[4], testBuildSHA("a4"), true),
		knownLiveRun(global[5], testBuildSHA("a5"), false),
		knownLiveRun(global[6], testBuildSHA("a6"), true),
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.RunsLive != 7 {
		t.Fatalf("RunsLive = %d, want 7", history.RunsLive)
	}
	if history.RunsFired != 3 {
		t.Fatalf("RunsFired = %d, want 3", history.RunsFired)
	}
	if history.LastFiredBuild != testBuildSHA("a2") {
		t.Fatalf("LastFiredBuild = %q, want %q (the most recent run it actually fired in)", history.LastFiredBuild, testBuildSHA("a2"))
	}
	// The five-run window (global[0:5]) has this ticket firing at
	// global[2] and global[4] -- not NEVER_FIRED.
	if history.NeverFired {
		t.Fatalf("NeverFired = true, want false: %+v", history)
	}
}

// TestComputeFiringHistory_NeverFiredEmptyLastFiredSHA pins the empty-sha
// case explicitly: a declaration with RunsFired == 0 must report an
// empty LastFiredBuild, never a zero-valued or placeholder sha.
func TestComputeFiringHistory_NeverFiredEmptyLastFiredSHA(t *testing.T) {
	global := newestFirst(testRuns(5))
	own := make([]DeclarationRun, len(global))
	for i, at := range global {
		own[i] = knownLiveRun(at, testBuildSHA("aa"), false)
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.LastFiredBuild != "" {
		t.Fatalf("LastFiredBuild = %q, want empty string", history.LastFiredBuild)
	}
	if history.RunsFired != 0 {
		t.Fatalf("RunsFired = %d, want 0", history.RunsFired)
	}
}

// TestComputeFiringHistory_OldFiredRowCountsAsLiveEvenWhenUnknown proves
// RunsLive/RunsFired's own, wider rule (see FiringHistory.RunsLive's
// doc comment): a PRE-0135 row that actually FIRED is unambiguous
// evidence of liveness on its own terms, even though its declared
// column is NULL -- it still counts toward RunsLive/RunsFired, but
// still never toward the five-run WINDOW, which starts its clock fresh
// at 0135 regardless.
func TestComputeFiringHistory_OldFiredRowCountsAsLiveEvenWhenUnknown(t *testing.T) {
	global := newestFirst(testRuns(1))
	own := []DeclarationRun{
		{ObservedAt: global[0], CandidateBuild: testBuildSHA("aa"), FiredTickets: []string{testTicket}},
	}
	history := ComputeFiringHistory(testTicket, own, global)
	if history.RunsLive != 1 || history.RunsFired != 1 {
		t.Fatalf("RunsLive/RunsFired = %d/%d, want 1/1 -- firing is self-evident even for a NULL-era row", history.RunsLive, history.RunsFired)
	}
	if history.LastFiredBuild != testBuildSHA("aa") {
		t.Fatalf("LastFiredBuild = %q, want %q", history.LastFiredBuild, testBuildSHA("aa"))
	}
	if history.WindowKnownRuns != 0 {
		t.Fatalf("WindowKnownRuns = %d, want 0 -- a NULL-era row never counts toward the window, fired or not", history.WindowKnownRuns)
	}
}
