//go:build integration

package remaining

import (
	"context"
	"encoding/json"
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
// DIFFERENT underlying data, and the four org-wide signals are asserted to come
// back IDENTICAL for both -- which is the defect, executed. That assertion is
// the before-evidence for the join fix, and it is expected to INVERT when the
// owned-repo scoping lands; the comment on it says so, so whoever changes it
// knows it is a deliberate flip rather than a regression.

const (
	loaderOrgID       = "org-loader-parity"
	loaderTeamA       = "team-alpha"
	loaderTeamB       = "team-beta"
	loaderWindowStart = "2026-08-01"
	loaderWindowEnd   = "2026-09-01"
)

type pythonSnapshot struct {
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

		want := runPythonLoader(t, dsn, teamID)
		compareSnapshotAgainstPython(t, teamID, got, want)
	}

	assertCHAOS4897DefectIsPresent(t, snapshots[loaderTeamA], snapshots[loaderTeamB])
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
			math.Float64bits(signal.a) != math.Float64bits(signal.b) {
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
		if math.Float64bits(a[index]) != math.Float64bits(b[index]) {
			return false
		}
	}
	return true
}

func compareSnapshotAgainstPython(t *testing.T, teamID string, got MetricsSnapshot, want pythonSnapshot) {
	t.Helper()
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

func runPythonLoader(t *testing.T, dsn, teamID string) pythonSnapshot {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	script := filepath.Join(root,
		"internal/jobs/metrics/remaining/testdata/run_recommendations_loader_against_clickhouse.py")
	python := filepath.Join(root, ".venv/bin/python")

	command := exec.Command(python, script,
		"--dsn", dsn, "--team", teamID, "--org", loaderOrgID,
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
	for _, seed := range []struct {
		team           string
		day            string
		wip, completed int
		cycle          float64
		computedAt     string
	}{
		{loaderTeamA, "2026-08-02", 3, 5, 10.5, "2026-08-03 00:00:00"},
		{loaderTeamA, "2026-08-02", 999, 999, 999.0, "2026-08-02 00:00:00"}, // superseded
		{loaderTeamA, "2026-08-05", 7, 2, 14.25, "2026-08-06 00:00:00"},
		{loaderTeamA, "2026-08-09", 9, 1, 20.0, "2026-08-10 00:00:00"},
		{loaderTeamB, "2026-08-02", 1, 8, 4.0, "2026-08-03 00:00:00"},
		{loaderTeamB, "2026-08-06", 2, 9, 5.5, "2026-08-07 00:00:00"},
	} {
		exec(`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
			 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
			 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
			 story_points_completed, computed_at, org_id)
			VALUES (?, 'github', 'scope-1', ?, '', 0, ?, 0, 0, ?, 0, ?, 0, 0, ?, ?)`,
			mustDate(t, seed.day), seed.team, uint32(seed.completed), uint32(seed.wip),
			seed.cycle, mustTimestamp(t, seed.computedAt), loaderOrgID)
	}

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
