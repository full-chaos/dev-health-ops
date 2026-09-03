//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"fmt"
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
// DIFFERENT underlying data, and the four org-wide signals are asserted to come
// back IDENTICAL for both -- which is the defect, executed. That assertion is
// the before-evidence for the join fix, and it is expected to INVERT when the
// owned-repo scoping lands; the comment on it says so, so whoever changes it
// knows it is a deliberate flip rather than a regression.

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
	loaderTeamA           = "team-alpha"
	loaderTeamB           = "team-beta"
	loaderWindowStart     = "2026-08-01"
	loaderWindowEnd       = "2026-09-01"
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
	seedLoaderFixture(t, ctx, conn)

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
		compareSnapshotAgainstPython(t, teamID, got, want)
	}

	assertCHAOS4897DefectIsPresent(t, snapshots[loaderTeamA], snapshots[loaderTeamB])
	assertHotspotBoundaryIsMeasured(t, ctx, conn, dsn)
	assertArgMaxKeysAreUnique(t, ctx, conn)
}

// assertCHAOS4897DefectIsPresent executes the defect rather than describing it.
//
// Teams alpha and beta have different work-item, review and commit data, so the
// TEAM-SCOPED signals must differ. The four signals read from repo-level tables
// have no team predicate -- there is no team_id column on those tables -- so
// they must come back IDENTICAL.
//
// WHEN THE OWNED-REPO JOIN LANDS, THIS TEST MUST BE INVERTED, not deleted: the
// four will then differ per team, and that inversion is the after-evidence.
// A failure here after the join is expected; a failure here before it means the
// fixture stopped exercising the defect.
func assertCHAOS4897DefectIsPresent(t *testing.T, alpha, beta MetricsSnapshot) {
	t.Helper()

	// Team-scoped signals: these MUST differ, or the fixture is not actually
	// giving the two teams different data and the assertions below prove
	// nothing.
	if len(alpha.WIPByDay) == 0 || len(beta.WIPByDay) == 0 {
		t.Fatal("fixture gave a team no wip rows; the two-team comparison is vacuous")
	}
	if sameFloats(alpha.WIPByDay, beta.WIPByDay) {
		t.Fatal("alpha and beta have identical wip_by_day; the fixture is not " +
			"differentiating the teams, so the org-wide assertions below prove nothing")
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
		if signal.aKnown != signal.bKnown ||
			!sameFloat64(signal.a, signal.b) {
			t.Errorf("CHAOS-4897 fixture: %s differs between the teams "+
				"(alpha=%v/%v, beta=%v/%v). Either the owned-repo join has landed -- "+
				"in which case INVERT this assertion, it is the after-evidence -- or "+
				"the fixture stopped exercising the defect.",
				signal.name, signal.a, signal.aKnown, signal.b, signal.bKnown)
		}
	}
	t.Logf("CHAOS-4897 executed: the four repo-derived signals are identical for "+
		"two teams with different data (latency=%v, rework=%v, complexity=%v, overlap=%v)",
		alpha.ReviewLatencyP75Hours, alpha.ReworkChurnRatio,
		alpha.HotspotComplexityDelta, alpha.HotspotChurnOverlap)
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

func compareSnapshotAgainstPython(t *testing.T, teamID string, got MetricsSnapshot, want pythonSnapshot) {
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
	compareOptional(t, teamID, "review_latency_p75_hours", got.ReviewLatencyP75Hours, got.ReviewLatencyP75HoursKnown, want.ReviewLatencyP75Hours)
	compareOptional(t, teamID, "reviewer_gini", got.ReviewerGini, got.ReviewerGiniKnown, want.ReviewerGini)
	compareOptional(t, teamID, "rework_churn_ratio", got.ReworkChurnRatio, got.ReworkChurnRatioKnown, want.ReworkChurnRatio)
	compareOptional(t, teamID, "after_hours_ratio", got.AfterHoursRatio, got.AfterHoursRatioKnown, want.AfterHoursRatio)
	compareOptional(t, teamID, "hotspot_complexity_delta", got.HotspotComplexityDelta, got.HotspotComplexityDeltaKnown, want.HotspotComplexityDelta)
	compareOptional(t, teamID, "hotspot_churn_overlap", got.HotspotChurnOverlap, got.HotspotChurnOverlapKnown, want.HotspotChurnOverlap)
	compareOptional(t, teamID, "compounding_risk_score", got.CompoundingRiskScore, got.CompoundingRiskScoreKnown, want.CompoundingRiskScore)
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
	python := filepath.Join(root, ".venv/bin/python")

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
func seedLoaderFixture(t *testing.T, ctx context.Context, conn driver.Conn) {
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
	exec("SYSTEM STOP MERGES")

	repoAlpha := "11111111-1111-1111-1111-111111111111"
	repoBeta := "22222222-2222-2222-2222-222222222222"

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
	// This is what makes `orgClause()` load-bearing. Delete the org predicate
	// and Go aggregates both tenants while Python keeps only the requested one
	// -- a cross-tenant read. Seeded into work_item_metrics_daily (team-scoped,
	// so the leak needs the org predicate to be the only thing separating them)
	// AND repo_metrics_daily, where org is the ONLY scope the query has at all,
	// which makes it the more exposed of the two.
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
	// tenant into only work_item_metrics_daily and repo_metrics_daily makes the
	// org predicate load-bearing at those sites and NOWHERE ELSE -- removing it
	// from any of the other seven survives, because there is no foreign row for
	// it to let through. Deleting the whole helper is caught; weakening one
	// query is not, and a per-query removal is the more plausible edit.
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

	// repo_metrics_daily: latency and rework. NO team column -- this is the
	// CHAOS-4897 surface. Two repos so the org-wide avg is over both.
	for _, seed := range []struct {
		repo        string
		day         string
		p75, rework float64
		computedAt  string
	}{
		{repoAlpha, "2026-08-02", 30.0, 0.40, "2026-08-03 00:00:00"},
		{repoAlpha, "2026-08-02", 1.0, 0.01, "2026-08-02 00:00:00"}, // superseded
		{repoBeta, "2026-08-04", 50.0, 0.60, "2026-08-05 00:00:00"},
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
		runPythonLoader(t, dsn, loaderTeamA, loaderEmptyOrgID))
	compareSnapshotAgainstPython(t, "one-hotspot-org", one,
		runPythonLoader(t, dsn, loaderTeamA, loaderOneHotspotOrgID))
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
