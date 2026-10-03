//go:build integration

package remaining

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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
// CHAOS-8303: the Python loader's answers are FROZEN. They were produced once by the record verb on the pinned build
// (loaderPythonBuild) against this fixture, one per (team, org) of loaderCases, and are replayed from
// testdata/golden/recommendations_loader_clickhouse.json with no Python started; the Go loader still runs against a real ClickHouse
// seeded with the same fixture and is compared field by field (floats as exact bit patterns) with those answers.
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
	// An org that holds SEVERAL computed_at versions of the same key in every table the loader reads with argMax, and whose only
	// team owns EVERY repo of the org, so the four owned-repo-scoped fields equal the org-wide Python reference and are compared
	// strictly (CHAOS-8319). See seedVersionedOrg.
	loaderVersionsOrgID = "org-loader-versions"
	loaderTeamA         = "team-alpha"
	loaderTeamB         = "team-beta"
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

func TestRecommendationsLoaderMatchesFrozenPythonAgainstClickHouse(t *testing.T) {
	ctx := context.Background()
	frozen := venueoracle.OpenGolden(t, loaderGolden(t.Name(), loaderGoldenDigest))

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

	// The Python loader's answers for every (team, org) this test compares: recorded once on the pinned build against this fixture
	// by the record verb, frozen, and replayed here with no Python started.
	python := frozenPythonSnapshots(t, frozen, dsn)

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

		want := python.of(t, teamID, loaderOrgID)
		// true: alpha and beta own disjoint repo sets in this fixture (seeded
		// below), so the four CHAOS-4897 fields are SUPPOSED to diverge from
		// Python here -- see assertCHAOS4897FixIsPresent.
		compareSnapshotAgainstPython(t, teamID, got, want, true)
	}

	assertCHAOS4897FixIsPresent(t, snapshots[loaderTeamA], snapshots[loaderTeamB])
	assertOwnershipIsResolvedAsOfWindowEnd(t, snapshots[loaderTeamA])
	assertZeroOwnedReposIsAbsentNotOrgWide(t, ctx, conn, loader, windowStart, windowEnd)
	assertHotspotBoundaryIsMeasured(t, ctx, conn, python)
	assertArgMaxKeysAreUnique(t, ctx, conn)

	frozen.SkipDiff(t)
	frozen.Finish(t)
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

// loaderPythonBuild is the build whose Python recommendations loader the golden holds the answers of: main before the Python loader
// was deleted.
const loaderPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// loaderGoldenDigest is the SHA-256 of the golden file, pinned by the record verb ("PIN:..." until its first recording).
const loaderGoldenDigest = "02985f130dbeb78b985c0e49156833b90b336e18ec932f97274447f77223c4c8"

// loaderGolden is the GoldenSpec of the loader oracle: test is the oracle's function name, digest the SHA-256 the test pins.
func loaderGolden(test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/golden/recommendations_loader_clickhouse.json",
		PythonBuild: loaderPythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/jobs/metrics/remaining/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			loaderPythonBuild, test),
	}
}

// loaderProducerEnv is the environment entries that shape the producer's answer; with PATH, HOME and the pinned sources on PYTHONPATH
// they are the whole environment the producer gets.
var loaderProducerEnv = map[string]string{"PYTHONHASHSEED": "0", "PYTHONUTF8": "1"}

// loaderCases is every (team, org) pair the test compares with the Python loader, in the order they are asked and recorded.
var loaderCases = [][2]string{
	{loaderTeamA, loaderOrgID},
	{loaderTeamB, loaderOrgID},
	{loaderTeamA, loaderInfOrgID},
	{loaderTeamA, loaderEmptyOrgID},
	{loaderTeamA, loaderOneHotspotOrgID},
	{loaderTeamA, loaderVersionsOrgID},
}

// frozenSnapshots is the Python loader's answer per (team, org).
type frozenSnapshots map[string]pythonSnapshot

func (f frozenSnapshots) of(t *testing.T, team, org string) pythonSnapshot {
	t.Helper()
	snapshot, ok := f[team+"|"+org]
	if !ok {
		t.Fatalf("no Python loader answer for team %q org %q: loaderCases does not list it", team, org)
	}
	return snapshot
}

// frozenPythonSnapshots asks the golden for the Python loader's answers for loaderCases: replayed from the file (no Python started), or,
// while the record verb records, produced by the shipped Python loader reading the SAME rows from the SAME database the Go loader reads.
func frozenPythonSnapshots(t *testing.T, golden *venueoracle.Golden, dsn string) frozenSnapshots {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	root = golden.PythonRoot(t, root)
	program, err := os.ReadFile("testdata/run_recommendations_loader_against_clickhouse.py")
	if err != nil {
		t.Fatal(err)
	}
	var requests []venueoracle.Request
	var stdins [][]byte
	for _, c := range loaderCases {
		stdin, marshalErr := json.Marshal(map[string]string{
			"team": c[0], "org": c[1], "window_start": loaderWindowStart, "window_end": loaderWindowEnd})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		stdins = append(stdins, stdin)
		requests = append(requests, venueoracle.ProgramRequest(
			fmt.Sprintf("recommendations loader team=%s org=%s", c[0], c[1]), string(program), stdin, loaderProducerEnv))
	}
	answers := golden.Produce(t, root, requests, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		producer.RequireDeployed()
		live := make([]venueoracle.Response, 0, len(requests))
		for index, c := range loaderCases {
			command, commandErr := producer.Command(context.Background(), loaderProducerEnv, []string{"CLICKHOUSE_URI=" + dsn}, "-c", string(program))
			if commandErr != nil {
				t.Fatal(commandErr)
			}
			command.Dir = root
			command.Stdin = bytes.NewReader(stdins[index])
			var stdout, stderr bytes.Buffer
			command.Stdout = &stdout
			command.Stderr = &stderr
			if runErr := command.Run(); runErr != nil {
				t.Fatalf("python loader (%s, %s) failed: %v\nstdout:\n%s\nstderr:\n%s", c[0], c[1], runErr, stdout.Bytes(), stderr.Bytes())
			}
			live = append(live, venueoracle.Response{Status: 0, Body: lastJSONLine(t, stdout.String())})
		}
		return live
	})
	golden.Consumed(t, answers...)
	snapshots := frozenSnapshots{}
	for index, c := range loaderCases {
		var snapshot pythonSnapshot
		if decodeErr := json.Unmarshal([]byte(answers[index].Body), &snapshot); decodeErr != nil {
			t.Fatalf("decode the Python snapshot (%s, %s): %v\nbody:\n%s", c[0], c[1], decodeErr, answers[index].Body)
		}
		snapshots[c[0]+"|"+c[1]] = snapshot
	}
	return snapshots
}

// lastJSONLine is the producer's answer: the script prints exactly one JSON object on its last non-empty line; anything the client library
// logs before it is tolerated rather than assumed absent.
func lastJSONLine(t *testing.T, output string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	return lines[len(lines)-1]
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

	// DISCOVERY-SAFE ROWS (CHAOS-5190 hotfix, "calendar-fragile fixture,
	// exposed at 00:00Z"): every row above is pinned to the fixed
	// [loaderWindowStart, loaderWindowEnd) = [2026-08-01, 2026-09-01) window
	// the loader-parity/window-boundary tests query with an EXPLICIT asOf --
	// those tests are not clock-dependent and must not be disturbed here.
	//
	// DiscoverTeamIDs (recommendations_native_clickhouse.go) and
	// resolveScopes's all_teams branch (capacity_native_clickhouse.go) are
	// DIFFERENT: they use ClickHouse's own `today()` (`WHERE day >=
	// today() - 30`), not a caller-supplied asOf. Once real wall-clock time
	// passed 2026-09-05, the fixed window above aged out of that rolling
	// 30-day lookback entirely -- team-beta's newest fixed row (2026-08-06)
	// was excluded from `today() - 30` for any run executing on or after
	// 2026-09-06, discovery returned only team-alpha, and TWO tests
	// (TestOneOrgSurvivesDiscoveryComputeAndWrite,
	// TestCancellationAfterTheLastTeamStillPersistsOnAContainer -- both call
	// this fixture) failed identically regardless of which commit's code was
	// under test. Confirmed live: the fixed rows above are calendar-fragile
	// FOREVER, not just today, since `loaderWindowEnd` never moves but
	// `today() - 30` always does.
	//
	// Fixed by seeding ONE additional row per team, dated relative to
	// time.Now() rather than a fixed calendar date, so `today() - 30`
	// always includes both teams no matter when this test runs. "Yesterday"
	// keeps a comfortable multi-week margin inside the 30-day window without
	// landing back inside [loaderWindowStart, loaderWindowEnd) as long as
	// this test runs after 2026-09-01, which it always will from here on.
	// A distinct scope ("discovery-only") keeps these rows out of every
	// window-scoped aggregate/argMax assertion, which all filter to
	// [loaderWindowStart, loaderWindowEnd) explicitly and would otherwise
	// need updating every time this row's date rolls forward.
	discoveryDay := time.Now().UTC().AddDate(0, 0, -1)
	for _, team := range []string{loaderTeamA, loaderTeamB} {
		exec(`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
			 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
			 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
			 story_points_completed, computed_at, org_id)
			VALUES (?, 'github', 'discovery-only', ?, '', 0, 1, 0, 0, 0, 0, 0, 0, 0, ?, ?)`,
			discoveryDay, team, discoveryDay, loaderOrgID)
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

	seedVersionedOrg(t, ctx, conn)

	return restoreMerges
}

// seedVersionedOrg seeds loaderVersionsOrgID: for every table the loader reads with argMax(..., computed_at), the same key holds an
// OLDER and a NEWER computed_at version whose values differ, with the NEWER one the lower (or NULL) so that max() or argMin() in
// place of argMax answers differently. Every row has the shape the daily writers emit: an append-only day row, a re-run of the
// day under a later computed_at (the design's append-only daily tables read by argMax, never merged away in the window a reader
// sees: the merges of these tables are stopped above), a Nullable cycle time that the work-item writer leaves NULL for a day
// with nothing completed, a team owning every repo of the org with an open-ended ownership row as the ownership sync writes.
// loaderTeamA owns both repos, so the owned-repo fields are the org-wide ones and are compared with the Python reference.
func seedVersionedOrg(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	exec := func(query string, args ...any) {
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed versioned org failed: %v\nquery: %s", err, query)
		}
	}
	const (
		repoOne = "55555555-5555-5555-5555-555555555551"
		repoTwo = "55555555-5555-5555-5555-555555555552"
	)
	for _, repo := range []string{repoOne, repoTwo} {
		exec(`INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type,
			 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, 'versions/' || ?, 'exact', 'inferred', 0, 1, 0, ?, NULL, ?)`,
			loaderVersionsOrgID, loaderTeamA, repo, repo,
			mustTimestamp(t, "2026-01-01 00:00:00"), mustTimestamp(t, "2026-01-01 00:00:00"))
	}

	// review latency and rework (argMax over a repo's rows in the window): repoOne's NEWEST row is the LOWER one.
	for _, seed := range []struct {
		repo, day, computedAt string
		p75, rework           float64
	}{
		{repoOne, "2026-08-05", "2026-08-06 00:00:00", 90.0, 0.90},
		{repoOne, "2026-08-10", "2026-08-11 00:00:00", 20.0, 0.10},
		{repoTwo, "2026-08-08", "2026-08-09 00:00:00", 40.0, 0.40},
	} {
		exec(`INSERT INTO repo_metrics_daily
			(repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
			 large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours,
			 pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio,
			 change_failure_rate, computed_at, org_id)
			VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?, 0, 0, 0, ?, 0, ?, ?)`,
			seed.repo, mustDate(t, seed.day), seed.p75, seed.rework,
			mustTimestamp(t, seed.computedAt), loaderVersionsOrgID)
	}

	// complexity halves (argMax per (day, repo)): the first-half day of repoOne is re-run with a LOWER value; the second half rises.
	for _, seed := range []struct {
		repo, day, computedAt string
		cpk                   float64
	}{
		{repoOne, "2026-08-05", "2026-08-06 00:00:00", 100.0},
		{repoOne, "2026-08-05", "2026-08-07 00:00:00", 40.0},
		{repoOne, "2026-08-20", "2026-08-21 00:00:00", 60.0},
	} {
		exec(`INSERT INTO repo_complexity_daily
			(repo_id, day, cyclomatic_per_kloc, computed_at, org_id)
			VALUES (?, ?, ?, ?, ?)`,
			seed.repo, mustDate(t, seed.day), seed.cpk, mustTimestamp(t, seed.computedAt), loaderVersionsOrgID)
	}

	// hotspots: the only file of the second half has risk_score 0 (a file the hotspot job scored and found no risk in), twice
	// (an older and a newer version, both 0): `risk_score > 0` counts none, so churn_overlap is ABSENT although complexity rises.
	for _, computedAt := range []string{"2026-08-21 00:00:00", "2026-08-22 00:00:00"} {
		exec(`INSERT INTO file_hotspot_daily
			(repo_id, day, file_path, risk_score, computed_at, org_id)
			VALUES (?, ?, ?, ?, ?, ?)`,
			repoOne, mustDate(t, "2026-08-20"), "versions/quiet.py", 0.0,
			mustTimestamp(t, computedAt), loaderVersionsOrgID)
	}

	// reviewer gini (argMax per (repo, author, day)): author one's day is re-run with FEWER reviews; author two has one version.
	for _, seed := range []struct {
		email, computedAt string
		reviews           uint32
	}{
		{"one@versions.example", "2026-08-06 00:00:00", 10},
		{"one@versions.example", "2026-08-07 00:00:00", 2},
		{"two@versions.example", "2026-08-06 00:00:00", 10},
	} {
		exec(`INSERT INTO user_metrics_daily
			(repo_id, day, author_email, commits_count, team_id, reviews_given,
			 computed_at, org_id)
			VALUES (?, ?, ?, 0, ?, ?, ?, ?)`,
			repoOne, mustDate(t, "2026-08-05"), seed.email, loaderTeamA,
			seed.reviews, mustTimestamp(t, seed.computedAt), loaderVersionsOrgID)
	}

	// after-hours ratio (argMax of commits and of after-hours commits, same (day, repo_id)): the re-run has FEWER commits.
	for _, seed := range []struct {
		computedAt     string
		commits, after uint32
	}{
		{"2026-08-06 00:00:00", 100, 50},
		{"2026-08-07 00:00:00", 10, 5},
	} {
		exec(`INSERT INTO team_metrics_daily
			(day, team_id, repo_id, commits_count, after_hours_commits_count,
			 computed_at, org_id)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			mustDate(t, "2026-08-05"), loaderTeamA, repoOne, seed.commits, seed.after,
			mustTimestamp(t, seed.computedAt), loaderVersionsOrgID)
	}

	// cycle times: 08-05 and 08-06 have one version each. 08-07 holds an older value and a NEWER NULL: the Go loader tuple-wraps its
	// argMax (the NULL newest row wins and the day is DROPPED), the Python reference's plain argMax skips the NULL (the day keeps
	// the older value). A recorded Go-plane decision (CHAOS-4547, PR 2568), a NAMED divergence declared in the comparison below.
	for _, seed := range []struct {
		day, computedAt string
		cycle           *float64
	}{
		{"2026-08-05", "2026-08-06 00:00:00", ptr(12.0)},
		{"2026-08-06", "2026-08-07 00:00:00", ptr(8.0)},
		// 08-07 is re-run with a NULL cycle time (the work-item writer leaves it NULL for a day with nothing completed): the
		// DECLARED divergence of CHAOS-4547 / PR 2568 (see assertVersionedRowsAreReadAsNewest).
		{"2026-08-07", "2026-08-08 00:00:00", ptr(15.0)},
		{"2026-08-07", "2026-08-09 00:00:00", nil},
	} {
		exec(`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
			 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
			 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
			 story_points_completed, computed_at, org_id)
			VALUES (?, 'github', 'versions/scope', ?, '', 0, 1, 0, 0, 1, 0, ?, 0, 0, ?, ?)`,
			mustDate(t, seed.day), loaderTeamA, seed.cycle,
			mustTimestamp(t, seed.computedAt), loaderVersionsOrgID)
	}

	// persisted compounding risk (argMax over the (score, severity) tuple): an OLDER row with another score and severity.
	for _, seed := range []struct {
		day, computedAt, severity string
		score                     float64
	}{
		{"2026-08-03", "2026-08-04 06:30:00", "low", 0.30},
		{"2026-08-20", "2026-08-27 06:30:00", "high", 0.80},
	} {
		exec(`INSERT INTO compounding_risk_daily
			(org_id, day, scope, scope_id, compounding_risk, severity,
			 w_churn, w_complexity, w_ownership, w_review, computed_at)
			VALUES (?, ?, 'team', ?, ?, ?, 0, 0, 0, 0, ?)`,
			loaderVersionsOrgID, mustDate(t, seed.day), loaderTeamA, seed.score, seed.severity,
			mustTimestamp(t, seed.computedAt))
	}
}

func ptr(value float64) *float64 { return &value }

// bitsHex is the exact-bit encoding the Python producer prints for a float.
func bitsHex(values []float64) []string {
	encoded := make([]string, 0, len(values))
	for _, value := range values {
		encoded = append(encoded, strconv.FormatUint(math.Float64bits(value), 16))
	}
	return encoded
}

func decodeBits(t *testing.T, encoded []string) []float64 {
	t.Helper()
	values := make([]float64, 0, len(encoded))
	for _, text := range encoded {
		raw, err := strconv.ParseUint(text, 16, 64)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		values = append(values, math.Float64frombits(raw))
	}
	return values
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
func assertHotspotBoundaryIsMeasured(t *testing.T, ctx context.Context, conn driver.Conn, python frozenSnapshots) {
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
		python.of(t, loaderTeamA, loaderInfOrgID), false)

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
		python.of(t, loaderTeamA, loaderEmptyOrgID), false)
	compareSnapshotAgainstPython(t, "one-hotspot-org", one,
		python.of(t, loaderTeamA, loaderOneHotspotOrgID), false)

	assertVersionedRowsAreReadAsNewest(t, ctx, conn, python)
}

// assertVersionedRowsAreReadAsNewest reads loaderVersionsOrgID (seedVersionedOrg: every argMax table holds an older and a newer
// version of one key, the newer the lower or NULL) and compares the Go snapshot, field by field and strictly (the team owns every
// repo, so the owned-repo fields equal the Python reference's org-wide ones), with the frozen Python answer. The pinned values below
// are the PRECONDITION that the fixture is live and that the newest version is what is read: a max() or argMin() in place of argMax,
// or a loader that stopped reading the rows, moves one of them (CHAOS-8319).
func assertVersionedRowsAreReadAsNewest(t *testing.T, ctx context.Context, conn driver.Conn, python frozenSnapshots) {
	t.Helper()
	got := loadForOrg(t, ctx, conn, loaderVersionsOrgID)
	if !got.ReviewLatencyP75HoursKnown || got.ReviewLatencyP75Hours != 30.0 {
		t.Errorf("versions org: review latency = %v (known %v), want 30 (avg of the newest p75 of each repo: 20 and 40)",
			got.ReviewLatencyP75Hours, got.ReviewLatencyP75HoursKnown)
	}
	if !got.ReworkChurnRatioKnown || got.ReworkChurnRatio != 0.25 {
		t.Errorf("versions org: rework = %v (known %v), want 0.25 (avg of the newest rework of each repo: 0.1 and 0.4)",
			got.ReworkChurnRatio, got.ReworkChurnRatioKnown)
	}
	if got.HotspotChurnOverlapKnown {
		t.Errorf("versions org: hotspot_churn_overlap is PRESENT (%v); the only hotspot file has risk_score 0, which is not a hotspot",
			got.HotspotChurnOverlap)
	}
	if !got.CompoundingRiskScoreKnown || got.CompoundingRiskScore != 0.80 || got.CompoundingRiskSeverity != "high" {
		t.Errorf("versions org: compounding risk = %v/%q (known %v), want 0.8/high (the NEWEST row)",
			got.CompoundingRiskScore, got.CompoundingRiskSeverity, got.CompoundingRiskScoreKnown)
	}
	// DECLARED DIVERGENCE (CHAOS-4547, PR 2568): day 08-07 holds an older cycle time 15 and a NEWER NULL. The Python reference's plain
	// argMax SKIPS the NULL and keeps the older value; the Go loader's tuple-wrapped argMax lets the NULL win and DROPS the day. The
	// frozen Python answer and Go's behaviour are both pinned, and the rest of the snapshot is compared strictly: a loader that
	// starts to agree with Python here (the tuple wrap removed) fails this assertion too, so the divergence cannot change unseen.
	want := python.of(t, loaderTeamA, loaderVersionsOrgID)
	if frozen := decodeBits(t, want.CycleTimeByDay); !sameFloats(frozen, []float64{12.0, 8.0, 15.0}) {
		t.Errorf("versions org: the frozen Python cycle times are %v, want [12 8 15] (plain argMax skips the NULL newest row of 08-07)", frozen)
	}
	if !sameFloats(got.CycleTimeByDay, []float64{12.0, 8.0}) {
		t.Errorf("versions org: Go cycle times = %v, want [12 8]: the tuple-wrapped argMax lets the NULL newest row of 08-07 win and drops the day (CHAOS-4547, PR 2568)", got.CycleTimeByDay)
	}
	want.CycleTimeByDay = bitsHex(got.CycleTimeByDay)
	compareSnapshotAgainstPython(t, "versions-org", got, want, false)
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
