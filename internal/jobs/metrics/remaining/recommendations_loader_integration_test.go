//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// End-to-end loader parity against a real ClickHouse.
//
// The no-container corpus (recommendations_loader_test.go) pins the
// POST-PROCESSING by feeding canned rows to both sides. It cannot see the SQL,
// so a Go query that reads the wrong rows -- a missing argMax, a wrong GROUP BY,
// a `<=` where the reference has `<` -- would pass it while diverging in
// production. This test closes that gap the only way it can be closed: run the
// SHIPPED PYTHON LOADER and the Go loader against the SAME DATABASE and compare.
//
// It also carries the CHAOS-4897 two-team fixture. Teams A and B are given
// DIFFERENT underlying data AND disjoint team_repo_ownership rows, and the
// four owned-repo-scoped signals (review latency, rework, hotspot complexity
// delta, hotspot churn overlap) are asserted to come back DIFFERENT for both
// -- the fix, executed (assertCHAOS4897FixIsPresent). This inverts what the
// fixture asserted before the join landed (both IDENTICAL, i.e. org-wide);
// see that function's comment for the history. Because Go now scopes these
// four fields and the live Python reference (recommendations/loader.py)
// deliberately does not, compareSnapshotAgainstPython's strict Go==Python
// check is skipped for exactly these four fields on this fixture -- see its
// expectOwnedRepoScopeDivergence parameter.

const (
	loaderOrgID = "org-loader-parity"
	// A SECOND tenant, carrying the SAME team_id on the SAME days.
	//
	// Without it, `orgClause()` can be deleted outright and every oracle still
	// passes: the corpus's fake client ignores query parameters, and a
	// single-org fixture cannot tell a filtered read from an unfiltered one.
	// That is a cross-tenant data-read regression, so the fixture has to make
	// the org predicate load-bearing.
	loaderOtherOrgID = "org-loader-other-tenant"
	// A THIRD org with NO rows of its own.
	//
	// It exists because `total_hotspots` is consumed ONLY as `== 0` -- a
	// boolean. Adding a foreign hotspot row to a tenant that already has one
	// moves the count from 1 to 2, which nothing downstream can see, so the org
	// predicate on file_hotspot_daily is observable ONLY at the zero boundary.
	// Loading an EMPTY org makes it observable: correct behaviour finds no
	// hotspots and leaves churn_overlap ABSENT, while a dropped predicate picks
	// up the other orgs' rows and turns it PRESENT.
	loaderEmptyOrgID = "org-loader-empty"
	// The POSITIVE half of the same boundary: exactly ONE hotspot row.
	//
	// The empty-org assertion alone is vacuous, and lane-4441 named the shape:
	// two implementations agreeing on NOTHING is not evidence. It passes if the
	// predicate works, and equally if the org id is misspelled, if the fixture
	// stopped seeding, or if the loader bails early -- each of which makes both
	// sides empty for a reason unrelated to what is being tested.
	//
	// One point is not a slope. Pinning absent-at-zero AND present-at-one turns
	// "absent" into a measurement rather than a default.
	loaderOneHotspotOrgID = "org-loader-one-hotspot"
	// An org whose repo_metrics_daily rows OVERFLOW to +Inf under the loader's
	// own avg().
	//
	// SafeFloat drops NaN and PASSES ±Inf through, and the comment on it calls
	// that asymmetry load-bearing -- but no fixture row ever produced an Inf on
	// that path, so the claim was STATED and never ASSERTED. lane-4441 found it
	// by mutating SafeFloat to drop Inf as well: it survived, with controls
	// proving SafeFloat was reached and NaN was exercised.
	//
	// Two rows at 1.7976931348623157e308 sum to +Inf before avg() divides, so
	// the infinity comes out of the loader's ARITHMETIC rather than being
	// stored directly -- which is how it would actually arise in production.
	// Its own org, so the primary fixture's assertions are undisturbed.
	loaderInfOrgID = "org-loader-infinite"
	loaderTeamA    = "team-alpha"
	loaderTeamB    = "team-beta"
	// A team in the PRIMARY org with NO team_repo_ownership rows at all --
	// the empty-ownership boundary team-lead asked to pin explicitly: a team
	// that owns zero repos must get ABSENT for the four CHAOS-4897 signals,
	// never an org-wide fallback (that fallback was the original defect's
	// exact shape). Deliberately in loaderOrgID, alongside alpha/beta and
	// their real repo_metrics_daily/repo_complexity_daily/file_hotspot_daily
	// rows, so an absent result here is provably "no owned repos", not
	// "no data existed to find" (loaderEmptyOrgID already covers that
	// different case, where the ORG itself has no rows anywhere).
	loaderTeamNoOwnedRepos = "team-gamma-no-owned-repos"
	loaderWindowStart      = "2026-08-01"
	loaderWindowEnd        = "2026-09-01"
)

// repoAlpha's and repoLateAcquired's repo_metrics_daily p75/rework values,
// named so assertOwnershipIsResolvedAsOfWindowEnd can compute its expected
// averages from the SAME source of truth the seed data uses, with the SAME
// float64 arithmetic Go itself performs at runtime -- rather than a
// separately hand-typed decimal literal that has to happen to match the
// arithmetic's actual rounding (team-lead review: a pinned literal states FP
// noise, not intent).
//
// EXPLICITLY TYPED float64, not left as untyped constants -- this is load-
// bearing, not stylistic. An UNTYPED `0.40 + 0.99` is folded by the Go
// compiler at ARBITRARY PRECISION and rounded to float64 only ONCE, at the
// point of assignment; `avg()` in both ClickHouse and ordinary Go runtime
// code rounds EACH operand to float64 first and THEN performs float64
// addition/division, which is a DIFFERENT computation with its own
// intermediate rounding. Measured directly: untyped constant folding of
// `(0.40+0.99)/2` gives exactly 0.695; the runtime float64 path (and the
// real avg() this loader executes) gives 0.6950000000000001 -- a different
// bit pattern, which sameFloat64's bitwise comparison would then reject as a
// self-inflicted failure. `float64` typing here forces the SAME two-step
// per-operand rounding as the runtime path, so the constant expression below
// reproduces the actual computed value instead of a more "exact" one that
// avg() never actually produces.
const (
	repoAlphaLatency        float64 = 30.0
	repoAlphaRework         float64 = 0.40
	repoLateAcquiredLatency float64 = 999.0
	repoLateAcquiredRework  float64 = 0.99
)

type pythonSnapshot struct {
	TeamID                 string   `json:"team_id"`
	OrgID                  string   `json:"org_id"`
	WindowStart            string   `json:"window_start"`
	WindowEnd              string   `json:"window_end"`
	WIPByDay               []string `json:"wip_by_day"`
	ThroughputByCycle      []string `json:"throughput_by_cycle"`
	ReviewLatencyP75Hours  *string  `json:"review_latency_p75_hours"`
	ReviewerGini           *string  `json:"reviewer_gini"`
	ReworkChurnRatio       *string  `json:"rework_churn_ratio"`
	AfterHoursRatio        *string  `json:"after_hours_ratio"`
	CycleTimeByDay         []string `json:"cycle_time_by_day"`
	HotspotComplexityDelta *string  `json:"hotspot_complexity_delta"`
	HotspotChurnOverlap    *string  `json:"hotspot_churn_overlap"`
	CompoundingRiskScore   *string  `json:"compounding_risk_score"`
	CompoundingRiskSever   string   `json:"compounding_risk_severity"`
}

func TestRecommendationsLoaderMatchesPythonAgainstClickHouse(t *testing.T) {
	ctx := context.Background()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	defer seedLoaderFixture(t, ctx, conn)()

	loader, err := NewRecommendationsLoader(conn, loaderOrgID)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}
	windowStart := mustDate(t, loaderWindowStart)
	windowEnd := mustDate(t, loaderWindowEnd)

	snapshots := map[string]MetricsSnapshot{}
	for _, teamID := range []string{loaderTeamA, loaderTeamB} {
		got, loadErr := loader.LoadTeamMetricsWindow(ctx, teamID, loaderOrgID, windowStart, windowEnd)
		if loadErr != nil {
			t.Fatalf("go loader (%s): %v", teamID, loadErr)
		}
		snapshots[teamID] = got

		want := runPythonLoader(t, dsn, teamID, loaderOrgID)
		// true: alpha and beta own disjoint repo sets in this fixture (seeded
		// below), so the four CHAOS-4897 fields are SUPPOSED to diverge from
		// Python here -- see assertCHAOS4897FixIsPresent.
		compareSnapshotAgainstPython(t, teamID, got, want, true)
	}

	assertCHAOS4897FixIsPresent(t, snapshots[loaderTeamA], snapshots[loaderTeamB])
	assertOwnershipIsResolvedAsOfWindowEnd(t, snapshots[loaderTeamA])
	assertZeroOwnedReposIsAbsentNotOrgWide(t, ctx, conn, loader, windowStart, windowEnd)
	assertHotspotBoundaryIsMeasured(t, ctx, conn, dsn)
	assertArgMaxKeysAreUnique(t, ctx, conn)
}

// assertOwnershipIsResolvedAsOfWindowEnd closes TWO related codex-review
// fixture gaps for the same underlying reason: every OTHER ownership row in
// seedLoaderFixture is either always-active (valid_from long before the
// window, valid_to NULL) or, before this function's fixtures existed,
// entirely outside it -- so nothing could tell a wrong `asOf` boundary apart
// from the right one, in either direction.
//
// (1, round 1 P2) repoLateAcquired's ownership ACTIVATES 2026-08-20 --
// inside the window, so only windowEnd (09-01) sees it as owned; windowStart
// (08-01) would not. Alpha's review_latency_p75_hours is therefore the
// average of repoAlpha's (30.0) and repoLateAcquired's (999.0) p75 -- 514.5
// -- ONLY if windowEnd is what actually reached teamownership.OwnedRepoIDs.
// A windowStart mutation would silently drop back to repoAlpha alone (30.0),
// a plausible-looking number this fixture is deliberately built to make
// unmistakable instead.
//
// (2, round 2 P2) repoExpiresAtWindowEnd's ownership EXPIRES exactly AT
// loaderWindowEnd (valid_to = 2026-09-01T00:00:00Z, the same instant used as
// `asOf`). `OwnedRepoIDs`'s `valid_to > asOf` is a STRICT inequality, so this
// repo must be EXCLUDED at asOf=windowEnd -- correct code leaves alpha's
// values exactly as case (1) computed them (514.5 / 0.6950000000000001). A
// boundary slip (`>=`, or an `asOf` shifted by even one instant before
// windowEnd, e.g. the codex-round-2-constructed `windowEnd.Add(-time.
// Millisecond)`) would include repoExpiresAtWindowEnd's deliberately extreme
// p75/rework values (100000.0 / 0.9999) instead, moving alpha's average by
// orders of magnitude -- unmistakable, not a plausible near-miss.
func assertOwnershipIsResolvedAsOfWindowEnd(t *testing.T, alpha MetricsSnapshot) {
	t.Helper()

	// Computed with the SAME arithmetic the loader's avg() performs, from the
	// SAME named constants the seed data above uses -- not a separately
	// hand-typed decimal literal. Neither 0.40 nor 0.99 is exactly
	// representable in float64, so (repoAlphaRework+repoLateAcquiredRework)/2
	// is not the mathematically exact 0.695; computing it here rather than
	// pinning a literal states the INTENT (this average) rather than a
	// snapshot of whatever rounding happened to produce.
	wantLatency := (repoAlphaLatency + repoLateAcquiredLatency) / 2
	wantRework := (repoAlphaRework + repoLateAcquiredRework) / 2

	if !alpha.ReviewLatencyP75HoursKnown || !sameFloat64(alpha.ReviewLatencyP75Hours, wantLatency) {
		t.Errorf("alpha review_latency_p75_hours = %v/%v, want %v/true -- "+
			"repoLateAcquired (owned starting 2026-08-20, inside the window) is "+
			"missing from the average. Either the ownership lookup's `asOf` "+
			"regressed from windowEnd to windowStart (windowStart predates "+
			"repoLateAcquired's valid_from, so it would see the repo as not yet "+
			"owned), or the fixture's ownership row for it did not land.",
			alpha.ReviewLatencyP75Hours, alpha.ReviewLatencyP75HoursKnown, wantLatency)
	}
	if !alpha.ReworkChurnRatioKnown || !sameFloat64(alpha.ReworkChurnRatio, wantRework) {
		t.Errorf("alpha rework_churn_ratio = %v/%v, want %v/true -- same "+
			"windowEnd-vs-windowStart boundary as review_latency_p75_hours above",
			alpha.ReworkChurnRatio, alpha.ReworkChurnRatioKnown, wantRework)
	}
}

// assertZeroOwnedReposIsAbsentNotOrgWide pins the empty-ownership boundary
// directly (team-lead, CHAOS-4897 review): a team with NO team_repo_ownership
// rows must see the four owned-repo-scoped signals as ABSENT, never falling
// back to the org-wide read across loaderOrgID's real repoAlpha/repoBeta
// data. That fallback-on-empty is the exact shape of the original defect, so
// this is checked as its own assertion rather than folded into the
// alpha/beta comparison, which never exercises a zero-repo team at all.
func assertZeroOwnedReposIsAbsentNotOrgWide(
	t *testing.T, ctx context.Context, conn driver.Conn,
	loader *RecommendationsLoader, windowStart, windowEnd time.Time,
) {
	t.Helper()

	got, err := loader.LoadTeamMetricsWindow(ctx, loaderTeamNoOwnedRepos, loaderOrgID, windowStart, windowEnd)
	if err != nil {
		t.Fatalf("go loader (%s): %v", loaderTeamNoOwnedRepos, err)
	}

	// PRECONDITION: confirm the team genuinely has zero rows in
	// team_repo_ownership for this org -- if the fixture accidentally seeded
	// one, every assertion below would pass vacuously for the wrong reason.
	var ownershipRows uint64
	if scanErr := conn.QueryRow(ctx,
		"SELECT count() FROM team_repo_ownership WHERE org_id = ? AND team_id = ?",
		loaderOrgID, loaderTeamNoOwnedRepos,
	).Scan(&ownershipRows); scanErr != nil {
		t.Fatalf("count team_repo_ownership rows for %s: %v", loaderTeamNoOwnedRepos, scanErr)
	}
	if ownershipRows != 0 {
		t.Fatalf("precondition failed: %s has %d team_repo_ownership row(s) in %s; "+
			"this assertion needs a team that owns NOTHING",
			loaderTeamNoOwnedRepos, ownershipRows, loaderOrgID)
	}

	for _, signal := range []struct {
		name  string
		known bool
		value float64
	}{
		{"review_latency_p75_hours", got.ReviewLatencyP75HoursKnown, got.ReviewLatencyP75Hours},
		{"rework_churn_ratio", got.ReworkChurnRatioKnown, got.ReworkChurnRatio},
		{"hotspot_complexity_delta", got.HotspotComplexityDeltaKnown, got.HotspotComplexityDelta},
		{"hotspot_churn_overlap", got.HotspotChurnOverlapKnown, got.HotspotChurnOverlap},
	} {
		if signal.known {
			t.Errorf("team with zero owned repos: %s is PRESENT (%v) -- expected ABSENT. "+
				"loaderOrgID has real repoAlpha/repoBeta data (owned by alpha/beta, not this "+
				"team), so a present value here means the owned-repo filter fell back to an "+
				"unscoped, org-wide read on empty ownership -- exactly the CHAOS-4897 defect "+
				"this fix closes.",
				signal.name, signal.value)
		}
	}
}

// assertCHAOS4897FixIsPresent executes the FIX rather than describing it.
//
// INVERTED from assertCHAOS4897DefectIsPresent (this test's own prior
// comment said to invert rather than delete when the owned-repo join landed
// -- this is that inversion, not a new test written from scratch).
//
// Teams alpha and beta have different work-item, review and commit data AND
// (as of this fix) DISJOINT owned-repo sets (alpha owns repoAlpha only, beta
// owns repoBeta only -- seeded in seedLoaderFixture's team_repo_ownership
// block). The four signals read from repo-level tables have no team_id
// column, but are now scoped through teamownership.OwnedRepoIDs, so they
// MUST differ between the two teams -- exactly like every other per-team
// signal, and unlike before this fix landed.
//
// A failure here means either the owned-repo join regressed back to an
// org-wide read, or the fixture stopped giving the two teams disjoint
// ownership -- either way, real per-team scoping is not observably in
// effect and this must not go green silently.
func assertCHAOS4897FixIsPresent(t *testing.T, alpha, beta MetricsSnapshot) {
	t.Helper()

	// Team-scoped signals: these MUST differ, or the fixture is not actually
	// giving the two teams different data and the assertions below prove
	// nothing.
	if len(alpha.WIPByDay) == 0 || len(beta.WIPByDay) == 0 {
		t.Fatal("fixture gave a team no wip rows; the two-team comparison is vacuous")
	}
	if sameFloats(alpha.WIPByDay, beta.WIPByDay) {
		t.Fatal("alpha and beta have identical wip_by_day; the fixture is not " +
			"differentiating the teams, so the ownership-scoping assertions below prove nothing")
	}

	for _, signal := range []struct {
		name           string
		a, b           float64
		aKnown, bKnown bool
	}{
		{"review_latency_p75_hours", alpha.ReviewLatencyP75Hours, beta.ReviewLatencyP75Hours,
			alpha.ReviewLatencyP75HoursKnown, beta.ReviewLatencyP75HoursKnown},
		{"rework_churn_ratio", alpha.ReworkChurnRatio, beta.ReworkChurnRatio,
			alpha.ReworkChurnRatioKnown, beta.ReworkChurnRatioKnown},
		{"hotspot_complexity_delta", alpha.HotspotComplexityDelta, beta.HotspotComplexityDelta,
			alpha.HotspotComplexityDeltaKnown, beta.HotspotComplexityDeltaKnown},
		{"hotspot_churn_overlap", alpha.HotspotChurnOverlap, beta.HotspotChurnOverlap,
			alpha.HotspotChurnOverlapKnown, beta.HotspotChurnOverlapKnown},
	} {
		if signal.aKnown == signal.bKnown && sameFloat64(signal.a, signal.b) {
			t.Errorf("CHAOS-4897 fix: %s is IDENTICAL between the teams "+
				"(alpha=%v/%v, beta=%v/%v) despite alpha and beta owning disjoint "+
				"repos -- the owned-repo join is not actually scoping this signal, "+
				"or the fixture's team_repo_ownership rows stopped giving them "+
				"disjoint ownership.",
				signal.name, signal.a, signal.aKnown, signal.b, signal.bKnown)
		}
	}
	t.Logf("CHAOS-4897 fix executed: the four repo-derived signals differ for "+
		"two teams with disjoint owned repos (alpha latency=%v/%v rework=%v/%v "+
		"complexity=%v/%v overlap=%v/%v; beta latency=%v/%v rework=%v/%v "+
		"complexity=%v/%v overlap=%v/%v)",
		alpha.ReviewLatencyP75Hours, alpha.ReviewLatencyP75HoursKnown,
		alpha.ReworkChurnRatio, alpha.ReworkChurnRatioKnown,
		alpha.HotspotComplexityDelta, alpha.HotspotComplexityDeltaKnown,
		alpha.HotspotChurnOverlap, alpha.HotspotChurnOverlapKnown,
		beta.ReviewLatencyP75Hours, beta.ReviewLatencyP75HoursKnown,
		beta.ReworkChurnRatio, beta.ReworkChurnRatioKnown,
		beta.HotspotComplexityDelta, beta.HotspotComplexityDeltaKnown,
		beta.HotspotChurnOverlap, beta.HotspotChurnOverlapKnown)
}

// pythonStringOrAbsent renders a pythonSnapshot optional field for a log
// line: the Python reference encodes an absent value as a JSON null, which
// decodes to a nil *string -- printed as "<absent>" rather than an empty
// string, which would be indistinguishable from a genuinely empty value if
// one ever existed on this field.
func pythonStringOrAbsent(value *string) string {
	if value == nil {
		return "<absent>"
	}
	return *value
}

func sameFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if !sameFloat64(a[index], b[index]) {
			return false
		}
	}
	return true
}

// expectOwnedRepoScopeDivergence is true for a comparison where the two
// teams being compared were seeded with genuinely DIFFERENT, DISJOINT
// owned-repo sets (the primary teamA/teamB fixture) -- there, Go's four
// CHAOS-4897 fields are SUPPOSED to differ from the still-org-wide Python
// reference, and strict comparison on them is skipped rather than made to
// fail on purpose. It is false for every other call in this file (the
// inf/empty/one-hotspot orgs), where the seeded team owns EVERY repo the org
// has, so Go's owned-repo-scoped read reduces to the same org-wide read
// Python computes and parity still holds -- keeping strict comparison there
// is what continues to catch a real regression in those queries' SQL.
func compareSnapshotAgainstPython(t *testing.T, teamID string, got MetricsSnapshot, want pythonSnapshot, expectOwnedRepoScopeDivergence bool) {
	t.Helper()
	// Identity and window first. These are echoes of the loader's arguments on
	// both sides, so they look untestable -- which is why they were uncompared,
	// and why a codex round could mutate WindowEnd to windowStart and watch
	// every suite stay green. recommendations_daily is keyed on window_end, so
	// the wrong bound writes to a different partition entirely.
	if got.TeamID != want.TeamID || got.OrgID != want.OrgID {
		t.Errorf("%s identity: (%q,%q), want (%q,%q)",
			teamID, got.TeamID, got.OrgID, want.TeamID, want.OrgID)
	}
	if got.WindowStart.Format("2006-01-02") != want.WindowStart {
		t.Errorf("%s window_start: %s, want %s",
			teamID, got.WindowStart.Format("2006-01-02"), want.WindowStart)
	}
	if got.WindowEnd.Format("2006-01-02") != want.WindowEnd {
		t.Errorf("%s window_end: %s, want %s -- recommendations_daily is keyed on "+
			"window_end, so this decides which row is written",
			teamID, got.WindowEnd.Format("2006-01-02"), want.WindowEnd)
	}
	compareList(t, teamID, "wip_by_day", got.WIPByDay, want.WIPByDay)
	compareList(t, teamID, "throughput_by_cycle", got.ThroughputByCycle, want.ThroughputByCycle)
	compareList(t, teamID, "cycle_time_by_day", got.CycleTimeByDay, want.CycleTimeByDay)
	compareOptional(t, teamID, "reviewer_gini", got.ReviewerGini, got.ReviewerGiniKnown, want.ReviewerGini)
	compareOptional(t, teamID, "after_hours_ratio", got.AfterHoursRatio, got.AfterHoursRatioKnown, want.AfterHoursRatio)
	compareOptional(t, teamID, "compounding_risk_score", got.CompoundingRiskScore, got.CompoundingRiskScoreKnown, want.CompoundingRiskScore)
	if expectOwnedRepoScopeDivergence {
		// CHAOS-4897: Go scopes these four to the team's owned repos; Python
		// (recommendations/loader.py) still reads every repo in the org. That
		// is the fix, not a regression -- see the package doc on
		// recommendations_loader.go. Logged, not silently skipped, so a
		// reader scanning test output can see what each side actually
		// produced rather than inferring it from an absent assertion.
		// pythonStringOrAbsent, not a bare %v on a *string: pythonSnapshot's
		// fields are *string (want.ReviewLatencyP75Hours etc.), and %v on a
		// pointer prints its ADDRESS, not the value it points to -- caught by
		// actually reading this log's output on bigboy (0x2871e509a670
		// instead of a number), not by inspection.
		t.Logf("%s: CHAOS-4897 owned-repo-scoped fields, Go vs Python (expected to "+
			"differ): review_latency_p75_hours go=%v/%v py=%s, "+
			"rework_churn_ratio go=%v/%v py=%s, "+
			"hotspot_complexity_delta go=%v/%v py=%s, "+
			"hotspot_churn_overlap go=%v/%v py=%s",
			teamID, got.ReviewLatencyP75Hours, got.ReviewLatencyP75HoursKnown, pythonStringOrAbsent(want.ReviewLatencyP75Hours),
			got.ReworkChurnRatio, got.ReworkChurnRatioKnown, pythonStringOrAbsent(want.ReworkChurnRatio),
			got.HotspotComplexityDelta, got.HotspotComplexityDeltaKnown, pythonStringOrAbsent(want.HotspotComplexityDelta),
			got.HotspotChurnOverlap, got.HotspotChurnOverlapKnown, pythonStringOrAbsent(want.HotspotChurnOverlap))
	} else {
		compareOptional(t, teamID, "review_latency_p75_hours", got.ReviewLatencyP75Hours, got.ReviewLatencyP75HoursKnown, want.ReviewLatencyP75Hours)
		compareOptional(t, teamID, "rework_churn_ratio", got.ReworkChurnRatio, got.ReworkChurnRatioKnown, want.ReworkChurnRatio)
		compareOptional(t, teamID, "hotspot_complexity_delta", got.HotspotComplexityDelta, got.HotspotComplexityDeltaKnown, want.HotspotComplexityDelta)
		compareOptional(t, teamID, "hotspot_churn_overlap", got.HotspotChurnOverlap, got.HotspotChurnOverlapKnown, want.HotspotChurnOverlap)
	}
	if got.CompoundingRiskSeverity != want.CompoundingRiskSever {
		t.Errorf("%s compounding_risk_severity: %q, want %q",
			teamID, got.CompoundingRiskSeverity, want.CompoundingRiskSever)
	}
}

func runPythonLoader(t *testing.T, dsn, teamID, orgID string) pythonSnapshot {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	script := filepath.Join(root,
		"internal/jobs/metrics/remaining/testdata/run_recommendations_loader_against_clickhouse.py")
	python := loaderPythonBinary(t)

	command := exec.Command(python, script,
		"--dsn", dsn, "--team", teamID, "--org", orgID,
		"--window-start", loaderWindowStart, "--window-end", loaderWindowEnd)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("python loader (%s) failed: %v\n%s", teamID, err, output)
	}
	// The script prints exactly one JSON object on the last non-empty line;
	// anything the client library logs before it is tolerated rather than
	// assumed absent.
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var snapshot pythonSnapshot
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &snapshot); err != nil {
		t.Fatalf("decode python snapshot (%s): %v\nfull output:\n%s", teamID, err, output)
	}
	return snapshot
}

func openLoaderClickHouse(t *testing.T, ctx context.Context, dsn string) driver.Conn {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	password, _ := parsed.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustDate(t *testing.T, text string) time.Time {
	t.Helper()
	value, err := time.ParseInLocation("2006-01-02", text, time.UTC)
	if err != nil {
		t.Fatalf("parse date %q: %v", text, err)
	}
	return value
}

// seedLoaderFixture writes rows for two teams with DELIBERATELY DIFFERENT data.
//
// Superseded rows are written for several keys -- an earlier computed_at with a
// wrong value -- so a Go query that dropped an argMax would read the wrong
// number and fail against Python rather than passing on a fixture where every
// key has exactly one row.
func seedLoaderFixture(t *testing.T, ctx context.Context, conn driver.Conn) (restoreMergesFn func()) {
	t.Helper()
	exec := func(query string, args ...any) {
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed failed: %v\nquery: %s", err, query)
		}
	}

	// STOP MERGES BEFORE SEEDING, or this fixture cannot test what it claims to.
	//
	// work_item_metrics_daily is a ReplacingMergeTree (migration 055), keyed on
	// (org_id, provider, day, work_scope_id, team_id). A background merge
	// collapses same-key rows to the highest computed_at -- which is precisely
	// what the reference's argMax(..., computed_at) computes. So once a merge
	// has run, argMax and a plain max() return the SAME answer and a query that
	// dropped the dedup passes.
	//
	// Found by mutation: replacing argMax with max here SURVIVED, and the seed
	// dump showed why -- the superseded row was already gone. The dedup only
	// matters BETWEEN insert and merge, which is exactly the window production
	// reads in, and it is the window a fixture has to preserve deliberately
	// because the test's own inserts are small enough to merge immediately.
	//
	// SCOPED PER TABLE, and restarted. The bare `SYSTEM STOP MERGES` this used
	// to issue is SERVER-WIDE, not per-table (CHAOS-4952), so one fixture
	// silently disabled merges for every table in the instance and never turned
	// them back on. On a shared or reused server that leaks into other tests as
	// a condition none of them declared -- and a test that depends on a global
	// another test happens to have set is not a test, it is a coincidence.
	//
	// recommendations_daily is in this list even though the fixture never seeds
	// it: it is the WRITE target and is ReplacingMergeTree(computed_at).
	//
	// It earns its place for ONE assertion, not the two I first claimed. The
	// two-run supersession test writes the SAME ORDER BY keys twice, differing
	// only in computed_at -- precisely what an RMT collapses -- so a merge there
	// turns two generations into one and destroys the property. The single-run
	// row count does NOT need this: its keys are all distinct, so no merge can
	// change it either way (established by mutation; see the FINAL comment in
	// the round-trip test).
	//
	// Note what that means for anyone tempted to narrow this list further:
	// dropping recommendations_daily does not fail the suite deterministically.
	// Merges are opportunistic, so the two-run test would pass most of the time
	// and fail occasionally -- a latent flake rather than a visible break, which
	// is the worse outcome and the reason this entry is explicit.
	mergeStopped := []string{
		"work_item_metrics_daily", "repo_metrics_daily", "user_metrics_daily",
		"team_metrics_daily", "repo_complexity_daily", "file_hotspot_daily",
		"compounding_risk_daily", "recommendations_daily", "team_repo_ownership",
	}
	for _, table := range mergeStopped {
		exec("SYSTEM STOP MERGES " + table)
	}
	// The caller DEFERS the returned restore. Not t.Cleanup: cleanups run after
	// every deferred call in the test, so a t.Cleanup restart would fire after
	// conn.Close() and after the container teardown -- too late to restart
	// anything, and silently so (4752-go, who nearly shipped that inversion by
	// copying a precedent's form without its teardown lifecycle).
	//
	// The restart uses a FRESH context: the test's own is usually cancelled by
	// the time defers run, and a restart on a cancelled context is a no-op that
	// looks like a restart.
	restoreMerges := func() {
		restartCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, table := range mergeStopped {
			if err := conn.Exec(restartCtx, "SYSTEM START MERGES "+table); err != nil {
				// Errorf, not Fatalf: FailNow from a deferred function does not
				// stop the remaining tables from being restarted.
				t.Errorf("restart merges on %s: %v", table, err)
			}
		}
	}

	repoAlpha := "11111111-1111-1111-1111-111111111111"
	repoBeta := "22222222-2222-2222-2222-222222222222"
	// Owned by alpha starting MID-WINDOW (2026-08-20, strictly between
	// loaderWindowStart and loaderWindowEnd) -- see
	// assertOwnershipIsResolvedAsOfWindowEnd. Its own repo_metrics_daily row
	// is only correctly included if the owned-repo lookup uses windowEnd
	// (fully activated by 2026-09-01) rather than windowStart (not yet
	// activated on 2026-08-01) as its `asOf`.
	repoLateAcquired := "77777777-7777-7777-7777-777777777777"
	// Owned by alpha, but ownership EXPIRES exactly AT loaderWindowEnd (see
	// assertOwnershipExpiryIsExclusiveAtWindowEnd). teamownership.OwnedRepoIDs
	// filters `valid_to > asOf`, a STRICT inequality -- a row whose valid_to
	// equals asOf exactly must be excluded, not included. This repo has an
	// extreme, unmistakable metric row so a wrong `>=` (or any other boundary
	// slip) changes alpha's aggregate rather than silently agreeing by luck.
	repoExpiresAtWindowEnd := "88888888-8888-8888-8888-888888888888"

	// work_item_metrics_daily: wip/throughput and cycle time, per team.
	// The (day, provider, work_scope_id) triple is the argMax key; the
	// superseded row carries an absurd value on an earlier computed_at.
	// A GRID on one day, not a diagonal.
	//
	// The inner query dedups per (day, provider, work_scope_id) and the outer
	// sums across scopes. A fixture that pins both dimensions to one value
	// cannot tell that apart from a bare `GROUP BY day`, which would take one
	// scope's latest values and drop the other -- and every assertion would
	// still pass. Day 2026-08-02 for team-alpha therefore carries scope-1 and
	// three cells of the (provider, work_scope_id) grid: (github, scope-1),
	// (github, scope-2) and (jira, scope-1).
	//
	// A DIAGONAL is not enough, and the first version of this fixture was one:
	// with only (github, scope-1) and (jira, scope-2), both coordinates vary
	// TOGETHER, so `GROUP BY day, provider` and `GROUP BY day, work_scope_id`
	// each still form two groups and give the same answer as the correct
	// composite key. Dropping either coordinate was undetectable.
	//
	// The grid separates them: github now spans two scopes, so dropping
	// work_scope_id collapses them; scope-1 now spans two providers, so
	// dropping provider collapses those. Each cell also keeps its own
	// superseded row where it has one, so the per-cell argMax stays
	// load-bearing too.
	for _, seed := range []struct {
		team           string
		day            string
		provider       string
		scope          string
		wip, completed int
		cycle          float64
		computedAt     string
	}{
		{loaderTeamA, "2026-08-02", "github", "scope-1", 3, 5, 10.5, "2026-08-03 00:00:00"},
		{loaderTeamA, "2026-08-02", "github", "scope-1", 999, 999, 999.0, "2026-08-02 00:00:00"}, // superseded
		{loaderTeamA, "2026-08-02", "github", "scope-2", 7, 2, 6.5, "2026-08-03 00:00:00"},
		{loaderTeamA, "2026-08-02", "github", "scope-2", 555, 555, 555.0, "2026-08-02 00:00:00"}, // superseded
		{loaderTeamA, "2026-08-02", "jira", "scope-1", 4, 1, 8.25, "2026-08-04 00:00:00"},        // distinct computed_at: no argMax tie under a collapsing mutant
		{loaderTeamA, "2026-08-05", "github", "scope-1", 7, 2, 14.25, "2026-08-06 00:00:00"},
		{loaderTeamA, "2026-08-09", "github", "scope-1", 9, 1, 20.0, "2026-08-10 00:00:00"},
		// THE WINDOW BOUNDARIES. Without a row ON each edge, `day >= {start}`
		// and `day < {end}` cannot be told from `day > {start}` and
		// `day <= {end}` -- the fixture's earliest row was Aug 2 and its latest
		// Aug 9, so both mutations were invisible.
		//
		// Aug 1 is the INCLUSIVE start: it must appear. Sep 1 is the EXCLUSIVE
		// end: it must NOT, and it is seeded precisely so that a `<=` mutant
		// pulls it in and changes both the list length and its values.
		{loaderTeamA, "2026-08-01", "github", "scope-1", 2, 3, 9.5, "2026-08-02 12:00:00"},
		{loaderTeamA, "2026-09-01", "github", "scope-1", 6000, 7000, 6000.0, "2026-09-02 00:00:00"},
		{loaderTeamB, "2026-08-02", "github", "scope-1", 1, 8, 4.0, "2026-08-03 00:00:00"},
		{loaderTeamB, "2026-08-06", "github", "scope-1", 2, 9, 5.5, "2026-08-07 00:00:00"},
	} {
		exec(`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
			 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
			 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
			 story_points_completed, computed_at, org_id)
			VALUES (?, ?, ?, ?, '', 0, ?, 0, 0, ?, 0, ?, 0, 0, ?, ?)`,
			mustDate(t, seed.day), seed.provider, seed.scope, seed.team,
			uint32(seed.completed), uint32(seed.wip),
			seed.cycle, mustTimestamp(t, seed.computedAt), loaderOrgID)
	}

	// The SAME team_id and days under a DIFFERENT org, with values chosen to be
	// impossible to confuse with this tenant's.
	//
	// This is what makes `orgClause()` load-bearing on work_item_metrics_daily
	// (team-scoped, so the leak needs the org predicate to be the only thing
	// separating the tenants): delete the predicate there and Go aggregates
	// both tenants while Python keeps only the requested one.
	//
	// The repo_metrics_daily foreign row below (repo 33333333...) no longer
	// tests orgClause() the same way, post-CHAOS-4897: that query is now ALSO
	// filtered to `repo_id IN (<team's owned repos>)`, and 33333333... is not
	// in loaderTeamA's owned set (only repoAlpha is, seeded further down) --
	// so the ownership filter alone excludes this foreign row even if
	// orgClause() were dropped from this specific query. org_id stays on the
	// query as defense in depth (two orgs should never share a repo_id at
	// all), but this fixture cannot prove it is load-bearing there any more;
	// it is kept for the tables where it still is.
	for _, seed := range []struct {
		day            string
		provider       string
		scope          string
		wip, completed int
		computedAt     string
	}{
		// computed_at LATER than the primary org's row for the same
		// (day, provider, work_scope_id, team_id). The uniqueness invariant
		// caught these two sharing a timestamp with it -- and since the group
		// key deliberately excludes org_id, a dropped org predicate would have
		// merged them into one group and let argMax tie-break. Found by the
		// invariant itself, on rows added while fixing a different tie.
		{"2026-08-02", "github", "scope-1", 400, 700, "2026-08-04 00:00:00"},
		{"2026-08-05", "github", "scope-1", 500, 800, "2026-08-07 00:00:00"},
	} {
		exec(`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
			 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
			 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
			 story_points_completed, computed_at, org_id)
			VALUES (?, ?, ?, ?, '', 0, ?, 0, 0, ?, 0, 77.5, 0, 0, ?, ?)`,
			mustDate(t, seed.day), seed.provider, seed.scope, loaderTeamA,
			uint32(seed.completed), uint32(seed.wip),
			mustTimestamp(t, seed.computedAt), loaderOtherOrgID)
	}
	for _, seed := range []struct {
		repo        string
		day         string
		p75, rework float64
		computedAt  string
	}{
		{"33333333-3333-3333-3333-333333333333", "2026-08-03", 900.0, 0.99, "2026-08-04 00:00:00"},
	} {
		exec(`INSERT INTO repo_metrics_daily
			(repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
			 large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours,
			 pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio,
			 change_failure_rate, computed_at, org_id)
			VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?, 0, 0, 0, ?, 0, ?, ?)`,
			seed.repo, mustDate(t, seed.day), seed.p75, seed.rework,
			mustTimestamp(t, seed.computedAt), loaderOtherOrgID)
	}

	// The second tenant must reach EVERY table the loader reads, not just two.
	//
	// orgClause() has NINE call sites across SEVEN tables. Seeding the other
	// tenant into work_item_metrics_daily, user_metrics_daily, team_metrics_daily
	// and compounding_risk_daily makes the org predicate load-bearing at those
	// (team-scoped or scope_id-scoped) sites. repo_complexity_daily and
	// file_hotspot_daily below get the SAME foreign-repo treatment as
	// repo_metrics_daily above, and the same caveat applies post-CHAOS-4897:
	// their queries are now also filtered to the team's owned-repo set, which
	// already excludes repo 33333333... on its own, so this fixture no longer
	// independently proves orgClause() is load-bearing on those two either.
	// Deleting the whole helper is still caught everywhere; a per-query
	// removal on one of these three specific queries is not, until a fixture
	// gives a foreign org the SAME repo_id as an owned one (deliberately not
	// done here -- repo_id collisions across orgs are not a real shape).
	//
	// Every value below is deliberately extreme, so a leak shows up as an
	// obviously wrong number rather than a plausible one.
	for _, seed := range []struct {
		email      string
		reviews    int
		computedAt string
	}{
		{"leak-1@other.example", 5000, "2026-08-03 00:00:00"},
		{"leak-2@other.example", 1, "2026-08-03 00:00:00"},
	} {
		exec(`INSERT INTO user_metrics_daily
			(repo_id, day, author_email, commits_count, team_id, reviews_given,
			 computed_at, org_id)
			VALUES (?, ?, ?, 0, ?, ?, ?, ?)`,
			repoAlpha, mustDate(t, "2026-08-02"), seed.email, loaderTeamA,
			uint32(seed.reviews), mustTimestamp(t, seed.computedAt), loaderOtherOrgID)
	}
	exec(`INSERT INTO team_metrics_daily
		(day, team_id, repo_id, commits_count, after_hours_commits_count,
		 computed_at, org_id)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		mustDate(t, "2026-08-02"), loaderTeamA, repoAlpha, uint32(1000), uint32(1000),
		// LATER than this tenant's row for the same (day, repo_id). The inner
		// query dedups on that pair, so with the org predicate gone both
		// tenants' rows land in ONE group and argMax decides. An equal
		// computed_at is a TIE, and a tie let this mutation survive: the
		// original row happened to win. The foreign row must win deterministically.
		mustTimestamp(t, "2026-08-04 00:00:00"), loaderOtherOrgID)
	exec(`INSERT INTO repo_complexity_daily
		(repo_id, day, cyclomatic_per_kloc, computed_at, org_id)
		VALUES (?, ?, ?, ?, ?)`,
		"33333333-3333-3333-3333-333333333333", mustDate(t, "2026-08-25"), 900.0,
		mustTimestamp(t, "2026-08-26 00:00:00"), loaderOtherOrgID)
	exec(`INSERT INTO file_hotspot_daily
		(repo_id, day, file_path, risk_score, computed_at, org_id)
		VALUES (?, ?, ?, ?, ?, ?)`,
		"33333333-3333-3333-3333-333333333333", mustDate(t, "2026-08-25"),
		"other-tenant/leak.py", 0.95, mustTimestamp(t, "2026-08-26 00:00:00"),
		loaderOtherOrgID)
	// A LATER computed_at than this tenant's row, so if the org predicate goes
	// the foreign row WINS the argMax rather than merely joining the pool.
	exec(`INSERT INTO compounding_risk_daily
		(org_id, day, scope, scope_id, compounding_risk, severity,
		 w_churn, w_complexity, w_ownership, w_review, computed_at)
		VALUES (?, ?, 'team', ?, ?, 'high', 0, 0, 0, 0, ?)`,
		loaderOtherOrgID, mustDate(t, "2026-08-22"), loaderTeamA, 0.99,
		mustTimestamp(t, "2026-08-23 00:00:00"))

	// Exactly one hotspot row, in the window's second half, and nothing else
	// for this org. See loaderOneHotspotOrgID.
	exec(`INSERT INTO file_hotspot_daily
		(repo_id, day, file_path, risk_score, computed_at, org_id)
		VALUES (?, ?, ?, ?, ?, ?)`,
		"44444444-4444-4444-4444-444444444444", mustDate(t, "2026-08-25"),
		"one-hotspot/only.py", 0.5, mustTimestamp(t, "2026-08-26 00:00:00"),
		loaderOneHotspotOrgID)
	// loadForOrg always reads loaderTeamA (see its own comment). Ownership
	// gives it EVERY repo this org has, so the owned-repo-scoped read reduces
	// to the same org-wide read it was before CHAOS-4897's join -- the
	// hotspot-boundary pin below tests the zero/one-hotspot COUNT, not
	// ownership scoping, and must not be reshaped by it.
	exec(`INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type,
		 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', ?, ?, 'one-hotspot/only', 'exact', 'inferred', 0, 1, 0, ?, NULL, ?)`,
		loaderOneHotspotOrgID, loaderTeamA, "44444444-4444-4444-4444-444444444444",
		mustTimestamp(t, "2026-01-01 00:00:00"), mustTimestamp(t, "2026-01-01 00:00:00"))

	// See loaderInfOrgID: two max-float rows whose SUM overflows, so avg()
	// yields +Inf and SafeFloat must pass it through.
	for _, seed := range []struct {
		repo, day, computedAt string
	}{
		{"55555555-5555-5555-5555-555555555555", "2026-08-03", "2026-08-04 00:00:00"},
		{"66666666-6666-6666-6666-666666666666", "2026-08-04", "2026-08-05 00:00:00"},
	} {
		exec(`INSERT INTO repo_metrics_daily
			(repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
			 large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours,
			 pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio,
			 change_failure_rate, computed_at, org_id)
			VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?, 0, 0, 0, ?, 0, ?, ?)`,
			seed.repo, mustDate(t, seed.day), math.MaxFloat64, math.MaxFloat64,
			mustTimestamp(t, seed.computedAt), loaderInfOrgID)
		// Same reasoning as the one-hotspot org above: loadForOrg reads
		// loaderTeamA only, so giving it BOTH inf repos keeps the
		// owned-repo-scoped average identical to the pre-fix org-wide one --
		// this fixture pins the +Inf-survives-SafeFloat behaviour, not
		// ownership scoping.
		exec(`INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type,
			 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, NULL, ?)`,
			loaderInfOrgID, loaderTeamA, seed.repo, seed.repo,
			mustTimestamp(t, "2026-01-01 00:00:00"), mustTimestamp(t, "2026-01-01 00:00:00"))
	}

	// repo_metrics_daily: latency and rework. NO team column -- this is the
	// CHAOS-4897 surface. Two repos so the org-wide avg is over both.
	for _, seed := range []struct {
		repo        string
		day         string
		p75, rework float64
		computedAt  string
	}{
		{repoAlpha, "2026-08-02", repoAlphaLatency, repoAlphaRework, "2026-08-03 00:00:00"},
		{repoAlpha, "2026-08-02", 1.0, 0.01, "2026-08-02 00:00:00"}, // superseded
		{repoBeta, "2026-08-04", 50.0, 0.60, "2026-08-05 00:00:00"},
		// repoLateAcquired: see assertOwnershipIsResolvedAsOfWindowEnd.
		// Deliberately extreme values so a wrong asOf (windowStart, which
		// excludes this repo) is unmistakable rather than a plausible number.
		{repoLateAcquired, "2026-08-22", repoLateAcquiredLatency, repoLateAcquiredRework, "2026-08-23 00:00:00"},
		// repoExpiresAtWindowEnd: see assertOwnershipExpiryIsExclusiveAtWindowEnd.
		// A MUCH more extreme value than repoLateAcquired's, so this row alone
		// moving alpha's average is unmistakable even alongside that repo.
		{repoExpiresAtWindowEnd, "2026-08-10", 100000.0, 0.9999, "2026-08-11 00:00:00"},
	} {
		exec(`INSERT INTO repo_metrics_daily
			(repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
			 large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours,
			 pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio,
			 change_failure_rate, computed_at, org_id)
			VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?, 0, 0, 0, ?, 0, ?, ?)`,
			seed.repo, mustDate(t, seed.day), seed.p75, seed.rework,
			mustTimestamp(t, seed.computedAt), loaderOrgID)
	}

	// team_repo_ownership: the CHAOS-4897 join's other half. Alpha owns
	// repoAlpha ONLY, beta owns repoBeta ONLY -- disjoint on purpose, so the
	// four owned-repo-scoped signals (review latency, rework, complexity
	// delta, hotspot churn overlap) are FORCED to differ between the two
	// teams post-fix: repo_complexity_daily and file_hotspot_daily below have
	// rows ONLY for repoAlpha, so beta's complexity/hotspot come back
	// ABSENT while alpha's are present -- a Known-mismatch, which
	// assertCHAOS4897FixIsPresent treats as "differs" exactly like a value
	// mismatch. Without this table seeded, ownedRepoIDs is empty for BOTH
	// teams and all four signals come back absent for both -- identical, by
	// coincidence, to the pre-fix defect's "both org-wide" outcome, which
	// would let this fixture stop proving anything without ever failing.
	for _, seed := range []struct {
		team, repo string
	}{
		{loaderTeamA, repoAlpha},
		{loaderTeamB, repoBeta},
	} {
		exec(`INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type,
			 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, NULL, ?)`,
			loaderOrgID, seed.team, seed.repo, seed.repo,
			mustTimestamp(t, "2026-01-01 00:00:00"), mustTimestamp(t, "2026-01-01 00:00:00"))
	}
	// repoLateAcquired: alpha's ownership activates 2026-08-20, strictly
	// between loaderWindowStart (08-01) and loaderWindowEnd (09-01). See
	// assertOwnershipIsResolvedAsOfWindowEnd -- this is the codex-review
	// (2026-09-04, P2) fixture gap: every OTHER ownership row in this file
	// activates well before either window bound, so no existing assertion
	// could tell a windowEnd `asOf` apart from a windowStart one.
	exec(`INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type,
		 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, NULL, ?)`,
		loaderOrgID, loaderTeamA, repoLateAcquired, repoLateAcquired,
		mustTimestamp(t, "2026-08-20 00:00:00"), mustTimestamp(t, "2026-08-20 00:00:00"))
	// repoExpiresAtWindowEnd: alpha's ownership is active for most of the
	// window but EXPIRES exactly at loaderWindowEnd (2026-09-01T00:00:00Z) --
	// the codex-review (2026-09-04, round-2 P2) boundary this lane's
	// windowEnd fixture didn't yet cover: OwnedRepoIDs' `valid_to > asOf` is
	// STRICT, so a row expiring AT asOf must be excluded, not included.
	// valid_to is bound as a real parameter here (every other row in this
	// fixture hardcodes a literal NULL) because this is the one row that
	// needs an actual, non-NULL expiry.
	exec(`INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type,
		 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, ?, ?)`,
		loaderOrgID, loaderTeamA, repoExpiresAtWindowEnd, repoExpiresAtWindowEnd,
		mustTimestamp(t, "2026-01-01 00:00:00"), mustTimestamp(t, "2026-09-01 00:00:00"),
		mustTimestamp(t, "2026-01-01 00:00:00"))

	// user_metrics_daily: reviewer gini, team-scoped. Skewed for alpha, even
	// for beta, so the two teams' gini genuinely differ.
	for _, seed := range []struct {
		team, email string
		day         string
		reviews     int
		computedAt  string
	}{
		{loaderTeamA, "a@example.com", "2026-08-02", 40, "2026-08-03 00:00:00"},
		{loaderTeamA, "b@example.com", "2026-08-02", 2, "2026-08-03 00:00:00"},
		{loaderTeamA, "c@example.com", "2026-08-02", 1, "2026-08-03 00:00:00"},
		{loaderTeamB, "d@example.com", "2026-08-02", 10, "2026-08-03 00:00:00"},
		{loaderTeamB, "e@example.com", "2026-08-02", 10, "2026-08-03 00:00:00"},
	} {
		exec(`INSERT INTO user_metrics_daily
			(repo_id, day, author_email, commits_count, team_id, reviews_given,
			 computed_at, org_id)
			VALUES (?, ?, ?, 0, ?, ?, ?, ?)`,
			repoAlpha, mustDate(t, seed.day), seed.email, seed.team,
			uint32(seed.reviews), mustTimestamp(t, seed.computedAt), loaderOrgID)
	}

	// team_metrics_daily: after-hours ratio, team-scoped, exercising the
	// CHAOS-4329 legacy repo_id='' discipline -- alpha gets BOTH a legacy
	// aggregate row and real per-repo rows on the same day, so a Go query that
	// dropped the window-function filter would double-count that day.
	for _, seed := range []struct {
		team, repo     string
		day            string
		commits, after int
		computedAt     string
	}{
		{loaderTeamA, "", "2026-08-02", 100, 50, "2026-08-03 00:00:00"}, // legacy, must be dropped
		{loaderTeamA, repoAlpha, "2026-08-02", 10, 4, "2026-08-03 00:00:00"},
		{loaderTeamA, repoBeta, "2026-08-02", 10, 2, "2026-08-03 00:00:00"},
		{loaderTeamA, "", "2026-08-05", 8, 6, "2026-08-06 00:00:00"}, // legacy only, must be KEPT
		{loaderTeamB, repoAlpha, "2026-08-02", 20, 1, "2026-08-03 00:00:00"},
	} {
		exec(`INSERT INTO team_metrics_daily
			(day, team_id, repo_id, commits_count, after_hours_commits_count,
			 computed_at, org_id)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			mustDate(t, seed.day), seed.team, seed.repo, uint32(seed.commits),
			uint32(seed.after), mustTimestamp(t, seed.computedAt), loaderOrgID)
	}

	// repo_complexity_daily: the halves either side of the midpoint. NO team
	// column -- CHAOS-4897 surface.
	for _, seed := range []struct {
		repo, day  string
		cpk        float64
		computedAt string
	}{
		{repoAlpha, "2026-08-02", 4.0, "2026-08-03 00:00:00"}, // first half
		{repoAlpha, "2026-08-25", 6.0, "2026-08-26 00:00:00"}, // second half
		// EXACTLY THE MIDPOINT. mid = window_start + max(1, days/2) = Aug 16,
		// and the reference splits on `day < mid` / `day >= mid`, so this row
		// belongs to the SECOND half and to that half only. With `day <= mid`
		// it is counted in BOTH, which moves the first-half average and so the
		// normalised delta. No row sat on the midpoint before, so the
		// off-by-one was invisible.
		{repoAlpha, "2026-08-16", 100.0, "2026-08-17 00:00:00"},
	} {
		exec(`INSERT INTO repo_complexity_daily
			(repo_id, day, cyclomatic_per_kloc, computed_at, org_id)
			VALUES (?, ?, ?, ?, ?)`,
			seed.repo, mustDate(t, seed.day), seed.cpk,
			mustTimestamp(t, seed.computedAt), loaderOrgID)
	}

	// file_hotspot_daily: the second-half hotspot count. NO team column.
	for _, seed := range []struct {
		path, day  string
		risk       float64
		computedAt string
	}{
		{"src/a.py", "2026-08-25", 0.9, "2026-08-26 00:00:00"},
		{"src/b.py", "2026-08-25", 0.0, "2026-08-26 00:00:00"}, // risk 0 -> not counted
	} {
		exec(`INSERT INTO file_hotspot_daily
			(repo_id, day, file_path, risk_score, computed_at, org_id)
			VALUES (?, ?, ?, ?, ?, ?)`,
			repoAlpha, mustDate(t, seed.day), seed.path, seed.risk,
			mustTimestamp(t, seed.computedAt), loaderOrgID)
	}

	// compounding_risk_daily: the persisted composite, team-scoped via
	// scope/scope_id. Only alpha has one, so beta exercises the fallback.
	exec(`INSERT INTO compounding_risk_daily
		(org_id, day, scope, scope_id, compounding_risk, severity,
		 w_churn, w_complexity, w_ownership, w_review, computed_at)
		VALUES (?, ?, 'team', ?, ?, 'elevated', 0, 0, 0, 0, ?)`,
		loaderOrgID, mustDate(t, "2026-08-20"), loaderTeamA, 0.62,
		mustTimestamp(t, "2026-08-21 00:00:00"))

	return restoreMerges
}

func mustTimestamp(t *testing.T, text string) time.Time {
	t.Helper()
	value, err := time.ParseInLocation("2006-01-02 15:04:05", text, time.UTC)
	if err != nil {
		t.Fatalf("parse timestamp %q: %v", text, err)
	}
	return value
}

// assertEmptyOrgSeesNothing makes the org predicate observable on the queries
// whose output cannot otherwise reveal it.
//
// `total_hotspots` reaches the snapshot only through `totalHotspots == 0`, so a
// foreign hotspot row added to a tenant that already has one is invisible: the
// count moves from 1 to 2 and nothing downstream can tell. The predicate is
// observable ONLY at the zero boundary, which needs an org with no rows.
//
// Loading a team in an empty org must therefore return an ABSENT churn_overlap.
// With the org predicate dropped from the hotspot query, the other orgs' rows
// are counted, the count becomes non-zero, and churn_overlap turns PRESENT --
// which is the divergence this asserts.
//
// Compared against the Python reference on the same empty org rather than
// against a hard-coded expectation, so it stays a parity assertion.
func assertHotspotBoundaryIsMeasured(t *testing.T, ctx context.Context, conn driver.Conn, dsn string) {
	t.Helper()

	// PRECONDITION: the instrument must be live before a null reading is
	// believed. If the PRIMARY org sees nothing either, "the empty org sees
	// nothing" is true of everything and proves nothing -- the same reason
	// lane-4441's comparison of two empty hashes reported IDENTICAL.
	control := loadForOrg(t, ctx, conn, loaderOrgID)
	if len(control.WIPByDay) == 0 || !control.HotspotChurnOverlapKnown {
		t.Fatalf("precondition failed: the PRIMARY org sees nothing either "+
			"(wip rows %d, churn_overlap known %v). Every assertion below would "+
			"pass vacuously; the fixture has stopped seeding.",
			len(control.WIPByDay), control.HotspotChurnOverlapKnown)
	}

	// ZERO side.
	empty := loadForOrg(t, ctx, conn, loaderEmptyOrgID)
	if empty.HotspotChurnOverlapKnown {
		t.Errorf("empty org: hotspot_churn_overlap is PRESENT (%v); an org with no "+
			"rows must see none. The org predicate has probably been dropped from "+
			"the file_hotspot_daily query -- that count is consumed only as `== 0`, "+
			"so this boundary is the only place it is observable.",
			empty.HotspotChurnOverlap)
	}
	if len(empty.WIPByDay) != 0 {
		t.Errorf("empty org: %d wip rows, want 0 -- a foreign tenant's rows are "+
			"reaching an org that has none", len(empty.WIPByDay))
	}

	// ONE side. Without this the zero side cannot distinguish "correctly zero"
	// from "trivially nothing".
	// ±Inf must SURVIVE SafeFloat. NaN is dropped, Inf is kept, and that
	// asymmetry had no fixture row behind it until now.
	infinite := loadForOrg(t, ctx, conn, loaderInfOrgID)
	if !infinite.ReviewLatencyP75HoursKnown {
		t.Errorf("inf org: review_latency_p75_hours is ABSENT; avg() over two " +
			"max-float rows overflows to +Inf, and SafeFloat must PASS Inf through " +
			"(it drops only NaN). If this fails, SafeFloat is discarding infinities " +
			"-- which diverges from Python's _safe_float, and the asymmetry this " +
			"fixture exists to pin has been lost.")
	} else if !math.IsInf(infinite.ReviewLatencyP75Hours, 1) {
		t.Errorf("inf org: review_latency_p75_hours is %v, want +Inf -- the fixture "+
			"no longer overflows, so the Inf path is unexercised and a SafeFloat "+
			"mutation dropping Inf would survive again",
			infinite.ReviewLatencyP75Hours)
	}
	compareSnapshotAgainstPython(t, "inf-org", infinite,
		runPythonLoader(t, dsn, loaderTeamA, loaderInfOrgID), false)

	one := loadForOrg(t, ctx, conn, loaderOneHotspotOrgID)
	if !one.HotspotChurnOverlapKnown {
		t.Errorf("one-hotspot org: hotspot_churn_overlap is ABSENT; an org with " +
			"exactly one hotspot row must see it. If this fails while the empty " +
			"case passes, the empty case is passing trivially -- a misspelled org " +
			"id or an unseeded fixture, not a working predicate.")
	}

	// Both halves against the Python reference on the same orgs, so this stays
	// parity rather than a hard-coded expectation.
	compareSnapshotAgainstPython(t, "empty-org", empty,
		runPythonLoader(t, dsn, loaderTeamA, loaderEmptyOrgID), false)
	compareSnapshotAgainstPython(t, "one-hotspot-org", one,
		runPythonLoader(t, dsn, loaderTeamA, loaderOneHotspotOrgID), false)
}

func loadForOrg(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) MetricsSnapshot {
	t.Helper()
	loader, err := NewRecommendationsLoader(conn, orgID)
	if err != nil {
		t.Fatalf("new loader (%s): %v", orgID, err)
	}
	got, err := loader.LoadTeamMetricsWindow(ctx, loaderTeamA, orgID,
		mustDate(t, loaderWindowStart), mustDate(t, loaderWindowEnd))
	if err != nil {
		t.Fatalf("go loader (%s): %v", orgID, err)
	}
	return got
}

// assertArgMaxKeysAreUnique makes a tie impossible to introduce silently.
//
// Every read in this loader picks a row with argMax(..., computed_at). If two
// rows share a group key AND a computed_at, argMax picks arbitrarily -- so a
// mutation that merges rows into one group can SURVIVE because the tie happened
// to favour the original row. That is not a detected mutation; it is a coin
// flip that landed right, and it reads exactly like a pass.
//
// It has already happened twice in this fixture, in two different tables within
// an hour: the grouping fixture and the team_metrics_daily tenant row. Two
// instances is not carelessness, it is a missing invariant -- so rather than
// fixing each timestamp and hoping, this asserts uniqueness once and no future
// row can reintroduce the problem.
//
// The group keys deliberately EXCLUDE org_id. The mutations this fixture exists
// to catch are exactly the ones that drop the org predicate and merge tenants,
// and uniqueness has to hold in the MERGED population for the foreign row to
// win or lose deterministically rather than by tie-break.
func assertArgMaxKeysAreUnique(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()

	for _, group := range []struct {
		table   string
		keyCols string
	}{
		{"work_item_metrics_daily", "day, provider, work_scope_id, team_id"},
		{"team_metrics_daily", "day, repo_id, team_id"},
		{"user_metrics_daily", "repo_id, author_email, day, team_id"},
		{"repo_metrics_daily", "repo_id, day"},
		{"repo_complexity_daily", "repo_id, day"},
		{"file_hotspot_daily", "file_path, day"},
		{"compounding_risk_daily", "scope, scope_id, day"},
	} {
		query := fmt.Sprintf(
			"SELECT count() FROM (SELECT %s, computed_at, count() AS n FROM %s "+
				"GROUP BY %s, computed_at HAVING n > 1)",
			group.keyCols, group.table, group.keyCols)
		var duplicates uint64
		if err := conn.QueryRow(ctx, query).Scan(&duplicates); err != nil {
			t.Fatalf("uniqueness check on %s: %v", group.table, err)
		}
		if duplicates != 0 {
			t.Errorf("%s: %d (%s, computed_at) group(s) hold more than one row. "+
				"argMax would tie-break arbitrarily there, so any mutation merging "+
				"rows into that group could survive on luck. Give the rows distinct "+
				"computed_at values.",
				group.table, duplicates, group.keyCols)
		}
	}
}

// loaderPythonBinary returns the interpreter chschema itself resolved, and
// LOGS it so the CI record proves which Python actually ran.
//
// This test previously hard-coded <root>/.venv/bin/python. That works on a dev
// host and on bigboy and FAILS IN CI, which has no .venv -- the comparison died
// with "fork/exec .../.venv/bin/python: no such file or directory" while
// chschema had already resolved python3 from PATH and run the migration chain.
//
// It went unnoticed because the `Go` workflow never COMPLETED on any earlier
// tip; every run was superseded by the next push, so the first time the merge
// oracle ran was the first time this surfaced.
//
// Calling chschema's own exported resolver rather than reimplementing its order
// is the point: a duplicate would be the same defect one refactor later.
//
// Deliberately NO skip path. A skip would make this test silently vacuous
// exactly where it matters, which is the defect class this file exists to
// close, so a missing interpreter is a hard failure.
func loaderPythonBinary(t *testing.T) string {
	t.Helper()
	python, err := chschema.Interpreter()
	if err != nil {
		t.Fatalf("no Python to run the reference loader: %v. This test compares the "+
			"SHIPPED Python loader against the Go one, so without an interpreter it "+
			"proves nothing -- failing rather than skipping is deliberate.", err)
	}
	t.Logf("reference interpreter resolved to: %s", python)
	return python
}
