//go:build integration

// The TestOps TEAM series read the owners of a repository at the request (team_repo_ownership, the rule of
// teamscope.RepoCondition), not the team stored on the daily row. Every cell runs on a real ClickHouse with the
// migrated schema.
package analytics

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopswindow"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type ownersEnv struct {
	t      *testing.T
	ctx    context.Context
	admin  stdclickhouse.Conn
	client QueryClient
}

func startOwnersEnv(t *testing.T) *ownersEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &ownersEnv{t: t, ctx: ctx, admin: admin, client: client}
}

func (e *ownersEnv) exec(q string, args ...any) {
	e.t.Helper()
	if err := e.admin.Exec(e.ctx, q, args...); err != nil {
		e.t.Fatalf("seed: %v\n%s", err, q)
	}
}

func (e *ownersEnv) repo(org, id, name string) {
	now := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, id, name, "github", org, now, now)
}

func (e *ownersEnv) team(org, id string, active uint8) {
	now := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	e.exec(`INSERT INTO teams (id, team_uuid, name, org_id, provider, is_active, updated_at, last_synced) VALUES (?, generateUUIDv4(), ?, ?, 'linear', ?, ?, ?)`, id, id, org, active, now, now)
}

// owner writes one ownership row; repoID "" leaves it unresolved by id (resolved by full name), validTo nil = open.
func (e *ownersEnv) owner(org, team, repoID, fullName string, primary uint8, validFrom time.Time, validTo *time.Time) {
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, "github", team, repoID, fullName, "exact", "inferred", primary, uint16(0), int32(0), validFrom, validTo, validFrom)
}

type repoDay struct {
	repo, store string // store is the stored team_id the writer put on the row ("" = NULL)
	day         time.Time
	runs, ok    uint32
	passed, bad uint32
	flakeCases  uint32
	distinct    uint32
	linesTotal  uint32
	linesCov    uint32
}

// daily writes one repository day into the three TestOps tables, with the same stored team on each.
func (e *ownersEnv) daily(org string, r repoDay) {
	at := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	var team any
	if r.store != "" {
		team = r.store
	}
	rate := func(n, d uint32) float64 {
		if d == 0 {
			return 0
		}
		return float64(n) / float64(d)
	}
	e.exec(`INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, failure_count, success_rate, failure_rate, rerun_rate, p95_duration_seconds, avg_queue_seconds, team_id, org_id, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.repo, r.day, r.runs, r.ok, r.runs-r.ok, rate(r.ok, r.runs), rate(r.runs-r.ok, r.runs), 0.1, float64(r.runs)/2, float64(r.runs)/10, team, org, at)
	e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate, flake_rate, flake_cases, distinct_cases, suite_duration_p95_seconds, team_id, org_id, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.repo, r.day, r.passed+r.bad, r.passed, r.bad, rate(r.passed, r.passed+r.bad), rate(r.bad, r.passed+r.bad), rate(r.flakeCases, r.distinct), r.flakeCases, r.distinct, float64(r.passed)/10, team, org, at)
	e.exec(`INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, branch_coverage_pct, coverage_delta_pct, lines_total, lines_covered, team_id, org_id, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.repo, r.day, 100*rate(r.linesCov, r.linesTotal), 50.0, 1.5, r.linesTotal, r.linesCov, team, org, at)
	// The P95 durations and the queue time are pooled over the raw samples of the stored repository-days: one run and
	// one suite whose samples are the stored values, so the one-sample pool gives the value of the stored row.
	runID := fmt.Sprintf("seed-%s-%s", r.repo, r.day.Format("20060102"))
	started := r.day.Add(time.Hour)
	e.exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id, duration_seconds, queue_seconds, team_id) VALUES (?, ?, 'success', ?, ?, ?, ?, ?, ?)`,
		r.repo, runID, started, at, org, float64(r.runs)/2, float64(r.runs)/10, team)
	e.exec(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, duration_seconds, started_at, team_id, org_id, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)`,
		r.repo, runID, "suite-"+runID, "suite", r.passed+r.bad, r.passed, r.bad, float64(r.passed)/10, started, team, org, at)
}

var allTestopsMeasures = []model.MeasureInput{
	model.MeasureInputPipelineSuccessRate, model.MeasureInputPipelineFailureRate, model.MeasureInputPipelineDurationP95,
	model.MeasureInputPipelineQueueTime, model.MeasureInputPipelineRerunRate, model.MeasureInputTestPassRate,
	model.MeasureInputTestFailureRate, model.MeasureInputTestFlakeRate, model.MeasureInputTestSuiteDurationP95,
	model.MeasureInputCoverageLinePct, model.MeasureInputCoverageBranchPct, model.MeasureInputCoverageDeltaPct,
}

// series returns the TEAM series (team -> one value per bucket, in date order) and the org series of a measure.
func (e *ownersEnv) series(org string, measure model.MeasureInput, filters *model.FilterInput) (map[string][]*float64, []*float64) {
	e.t.Helper()
	got, err := resolveOneTimeseries(e.ctx, e.client, org, tsInput(model.DimensionInputTeam, measure), false, filters, true)
	if err != nil {
		e.t.Fatalf("%s: %v", measure, err)
	}
	teams := map[string][]*float64{}
	var orgValues []*float64
	for _, s := range got {
		var values []*float64
		for _, b := range s.Buckets {
			values = append(values, b.Value)
		}
		if s.Scope == model.TimeseriesScopeOrg {
			orgValues = values
		} else {
			teams[s.DimensionValue] = values
		}
	}
	return teams, orgValues
}

// repoSeries is the same measure on the REPOSITORY dimension scoped to one team: the scoped-tile rule
// (teamscope.RepoCondition on repo_id).
func (e *ownersEnv) repoSeries(org string, measure model.MeasureInput, team string) map[string][]*float64 {
	e.t.Helper()
	filters := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{team}}}
	got, err := resolveOneTimeseries(e.ctx, e.client, org, tsInput(model.DimensionInputRepo, measure), false, filters, false)
	if err != nil {
		e.t.Fatalf("%s repo series: %v", measure, err)
	}
	out := map[string][]*float64{}
	for _, s := range got {
		for _, b := range s.Buckets {
			out[s.DimensionValue] = append(out[s.DimensionValue], b.Value)
		}
	}
	return out
}

func same(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return math.Abs(*a-*b) < 1e-9
}

func str(p *float64) string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%.6f", *p)
}

func openFrom() time.Time { return time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) }

var (
	day1 = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	day2 = time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	day3 = time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)
)

// RED at the base commit: a repository co-owned by team-a (main) and team-b has ONE daily row, stored under team-a.
// Every one of the 12 measures must give team-b the same value as team-a, and that value is the scoped tile's.
func TestTestopsTeamSeriesCoOwnedRepoShowsInFullInBothOwnerSeries(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13804-co"
	const repo = "13804000-0000-4000-8000-0000000000a1"
	e.repo(org, repo, "acme/co-owned")
	e.owner(org, "team-a", repo, "acme/co-owned", 1, openFrom(), nil)
	e.owner(org, "team-b", repo, "acme/co-owned", 0, openFrom(), nil)
	e.daily(org, repoDay{repo: repo, store: "team-a", day: day1, runs: 100, ok: 90, passed: 90, bad: 10, flakeCases: 5, distinct: 50, linesTotal: 1000, linesCov: 900})

	for _, measure := range allTestopsMeasures {
		teams, _ := e.series(org, measure, nil)
		a, b := teams["team-a"], teams["team-b"]
		if len(a) != 1 || len(b) != 1 {
			t.Fatalf("%s: team-a %v team-b %v series, want one bucket each (team-b is a co-owner of the only repository)", measure, len(a), len(b))
		}
		if !same(a[0], b[0]) {
			t.Fatalf("%s: team-a %s != team-b %s: a co-owned repository goes in full to each owner", measure, str(a[0]), str(b[0]))
		}
		for _, team := range []string{"team-a", "team-b"} {
			scoped := e.repoSeries(org, measure, team)[repo]
			if len(scoped) != 1 || !same(scoped[0], teams[team][0]) {
				t.Fatalf("%s %s: team series %s != scoped read %v", measure, team, str(teams[team][0]), scoped)
			}
		}
	}

	// The scoped tile (testopswindow) of each owner gives the pass, failure and flake rates of the same repository.
	since := graphqldate.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	until := graphqldate.New(time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC))
	for _, team := range []string{"team-a", "team-b"} {
		res, err := testopswindow.Resolve(e.ctx, e.client, org, since, until, testopswindow.Scope{TeamIDs: []string{team}})
		if err != nil {
			t.Fatal(err)
		}
		teams, _ := e.series(org, model.MeasureInputTestPassRate, nil)
		if res.PassRatePct == nil || !same(res.PassRatePct, teams[team][0]) || res.Cases.Total != 100 {
			t.Fatalf("%s: scoped tile pass rate %v (cases %d) != series %s", team, res.PassRatePct, res.Cases.Total, str(teams[team][0]))
		}
	}
}

// The ORG series counts each repository once: a co-owned repository (90 %) and a sole-owner repository (50 %) of unequal weight.
// The org rate is their summed numerators over summed denominators, never a sum over the team series.
func TestTestopsOrgSeriesCountsACoOwnedRepoOnce(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13804-org"
	const co, solo = "13804000-0000-4000-8000-0000000000b1", "13804000-0000-4000-8000-0000000000b2"
	e.repo(org, co, "acme/co")
	e.repo(org, solo, "acme/solo")
	e.owner(org, "team-a", co, "acme/co", 1, openFrom(), nil)
	e.owner(org, "team-b", co, "acme/co", 0, openFrom(), nil)
	e.owner(org, "team-c", solo, "acme/solo", 1, openFrom(), nil)
	e.daily(org, repoDay{repo: co, store: "team-a", day: day1, runs: 100, ok: 90, passed: 900, bad: 100, flakeCases: 0, distinct: 10, linesTotal: 1000, linesCov: 900})
	e.daily(org, repoDay{repo: solo, store: "team-c", day: day1, runs: 100, ok: 50, passed: 500, bad: 500, flakeCases: 0, distinct: 10, linesTotal: 1000, linesCov: 500})

	teams, orgSeries := e.series(org, model.MeasureInputPipelineSuccessRate, nil)
	if len(orgSeries) != 1 || orgSeries[0] == nil || math.Abs(*orgSeries[0]-70.0) > 1e-9 {
		t.Fatalf("org success rate %v, want 70 = (90+50)/(100+100) over the two repositories once", orgSeries)
	}
	if got := *teams["team-a"][0]; math.Abs(got-90) > 1e-9 {
		t.Fatalf("team-a %v, want 90", got)
	}
	if got := *teams["team-c"][0]; math.Abs(got-50) > 1e-9 {
		t.Fatalf("team-c %v, want 50", got)
	}
}

// Ownership change inside a window: team-a owned the repository until day 2 of the window, team-b since. The series reads the
// owners AS OF THE REQUEST for every day: team-b holds all three days, team-a none.
func TestTestopsTeamSeriesAttributesAWholeWindowToTheOwnersToday(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13804-move"
	const repo = "13804000-0000-4000-8000-0000000000c1"
	e.repo(org, repo, "acme/moved")
	closed := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	e.owner(org, "team-a", repo, "acme/moved", 1, openFrom(), &closed)
	e.owner(org, "team-b", repo, "acme/moved", 1, closed, nil)
	for _, d := range []time.Time{day1, day2, day3} {
		store := "team-a"
		if !d.Before(closed) {
			store = "team-b" // what the writer stored on each day
		}
		e.daily(org, repoDay{repo: repo, store: store, day: d, runs: 10, ok: 9, passed: 90, bad: 10, flakeCases: 1, distinct: 10, linesTotal: 100, linesCov: 90})
	}
	teams, _ := e.series(org, model.MeasureInputPipelineSuccessRate, nil)
	if got := teams["team-b"]; len(got) != 3 {
		t.Fatalf("team-b holds %d days, want all 3 (the owner today)", len(got))
	}
	if got, ok := teams["team-a"]; ok {
		t.Fatalf("team-a holds %d days, want none (its ownership ended before the request)", len(got))
	}
	scoped := e.repoSeries(org, model.MeasureInputPipelineSuccessRate, "team-b")[repo]
	if len(scoped) != 3 {
		t.Fatalf("scoped read of team-b holds %d days, want 3", len(scoped))
	}
}

// An unowned repository has no team bucket and is in the org series; an inactive team, an expired row and an ownership row of a
// repository outside the catalog give no bucket either (the rules of teamscope.RepoCondition); tied main owners both read the
// repository; two ownership rows of one team for one repository count once.
func TestTestopsTeamSeriesFollowTheOwnershipRules(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13804-rules"
	const unowned, inactive, expired, tied, dup, other = "13804000-0000-4000-8000-0000000000d1", "13804000-0000-4000-8000-0000000000d2", "13804000-0000-4000-8000-0000000000d3", "13804000-0000-4000-8000-0000000000d4", "13804000-0000-4000-8000-0000000000d5", "13804000-0000-4000-8000-0000000000d6"
	for id, name := range map[string]string{unowned: "acme/unowned", inactive: "acme/inactive", expired: "acme/expired", tied: "acme/tied", dup: "acme/dup", other: "acme/other"} {
		e.repo(org, id, name)
	}
	e.team(org, "team-live", 1)
	e.team(org, "team-gone", 0)
	past := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	e.owner(org, "team-gone", inactive, "acme/inactive", 1, openFrom(), nil)
	e.owner(org, "team-expired", expired, "acme/expired", 1, openFrom(), &past)
	e.owner(org, "team-orphan", "00000000-0000-4000-8000-0000000000ff", "acme/not-in-catalog", 1, openFrom(), nil)
	e.owner(org, "team-t1", tied, "acme/tied", 1, openFrom(), nil)
	e.owner(org, "team-t2", tied, "acme/tied", 1, openFrom(), nil)
	// team-dup owns two repositories; one of them through two ownership rows. A repeated row must not move its rate.
	e.owner(org, "team-dup", dup, "acme/dup", 1, openFrom(), nil)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, "github", "team-dup", dup, "acme/dup", "pattern", "native", uint8(1), uint16(1), int32(0), openFrom(), nil, openFrom())
	e.owner(org, "team-dup", other, "acme/other", 1, openFrom(), nil)
	for _, r := range []struct {
		repo  string
		store string
		runs  uint32
		ok    uint32
	}{{unowned, "", 10, 0}, {inactive, "team-gone", 10, 10}, {expired, "team-expired", 10, 10}, {tied, "team-t1", 10, 10}, {dup, "team-dup", 100, 100}, {other, "team-dup", 100, 0}} {
		e.daily(org, repoDay{repo: r.repo, store: r.store, day: day1, runs: r.runs, ok: r.ok, passed: 1, bad: 0, distinct: 1, linesTotal: 10, linesCov: 5})
	}
	teams, orgSeries := e.series(org, model.MeasureInputPipelineSuccessRate, nil)
	for _, absent := range []string{"", "team-gone", "team-expired", "team-orphan"} {
		if _, ok := teams[absent]; ok {
			t.Fatalf("team %q has a series, want none (unowned, inactive, expired, outside the catalog): %v", absent, teams)
		}
	}
	for _, both := range []string{"team-t1", "team-t2"} {
		if v := teams[both]; len(v) != 1 || v[0] == nil || math.Abs(*v[0]-100) > 1e-9 {
			t.Fatalf("tied owner %s reads %v, want the repository (100)", both, v)
		}
	}
	// team-dup: dup (100 of 100) + other (0 of 100) = 50 of 200 runs once each; a repeated ownership row of dup would give (200+0)/(300).
	if v := teams["team-dup"]; len(v) != 1 || v[0] == nil || math.Abs(*v[0]-50) > 1e-9 {
		t.Fatalf("team-dup reads %v, want 50 (two ownership rows of one repository count once)", v)
	}
	// Org: all six repositories once: (0+10+10+10+100+0) / (10+10+10+10+100+100) = 130/240.
	want := 130.0 / 240.0 * 100
	if len(orgSeries) != 1 || orgSeries[0] == nil || math.Abs(*orgSeries[0]-want) > 1e-9 {
		t.Fatalf("org %v, want %v with every repository once, the unowned one included", orgSeries, want)
	}
}

// ownerSeedSQL is the repository and the main-owner row a stored team of a seeded TestOps day needs now that the team
// series read ownership: the statements for the tests that seed by SQL text.
func ownerSeedSQL(org, repo, team string) []string {
	name := "owned-" + repo[len(repo)-6:]
	return []string{
		fmt.Sprintf("INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', '%s', 'github', '%s', now(), now())", repo, name, org),
		fmt.Sprintf("INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES ('%s', 'github', '%s', '%s', '%s', 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', NULL, '2025-01-01 00:00:00')", org, team, repo, name),
	}
}

// A team that owns no repository: its stored rows (the team of the daily row, as the writer stored it) give it NO result in the
// TEAM series, not a result of zero buckets and not a value of 0, while the ORG series still holds the repositories. The state
// "no owned repositories" is deferred; a team that owns nothing answers an empty team series, as a team with no rows does today.
func TestTestopsTeamWithNoOwnedRepositoryHasNoTeamResultAndTheOrgKeepsTheRepos(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13804-noowner"
	const owned, orphan = "13804000-0000-4000-8000-0000000000e1", "13804000-0000-4000-8000-0000000000e2"
	e.repo(org, owned, "acme/owned")
	e.repo(org, orphan, "acme/orphan")
	e.owner(org, "team-a", owned, "acme/owned", 1, openFrom(), nil)
	// team-x holds stored rows only: no ownership row names it.
	e.daily(org, repoDay{repo: owned, store: "team-a", day: day1, runs: 100, ok: 90, passed: 90, bad: 10, flakeCases: 5, distinct: 50, linesTotal: 1000, linesCov: 900})
	e.daily(org, repoDay{repo: orphan, store: "team-x", day: day1, runs: 100, ok: 10, passed: 10, bad: 90, flakeCases: 5, distinct: 50, linesTotal: 1000, linesCov: 100})
	for _, measure := range allTestopsMeasures {
		teams, orgSeries := e.series(org, measure, nil)
		if got, present := teams["team-x"]; present {
			t.Fatalf("%s: team-x owns no repository and has a team result of %d buckets, want none", measure, len(got))
		}
		if len(teams["team-a"]) != 1 {
			t.Fatalf("%s: team-a lost its series: %v", measure, teams)
		}
		// The rate measures and the pooled measures have an organization series (the two coverage means do not:
		// CompileOrgTimeseries, orgRateTerms, orgPooledTerms).
		hasOrg := map[model.MeasureInput]bool{
			model.MeasureInputPipelineSuccessRate: true, model.MeasureInputPipelineFailureRate: true, model.MeasureInputPipelineRerunRate: true,
			model.MeasureInputTestPassRate: true, model.MeasureInputTestFailureRate: true, model.MeasureInputTestFlakeRate: true,
			model.MeasureInputCoverageLinePct: true, model.MeasureInputPipelineDurationP95: true, model.MeasureInputPipelineQueueTime: true,
			model.MeasureInputTestSuiteDurationP95: true,
		}[measure]
		if hasOrg && (len(orgSeries) != 1 || orgSeries[0] == nil) {
			t.Fatalf("%s: the org series is %v, want one bucket holding both repositories", measure, orgSeries)
		}
	}
	// The org success rate holds BOTH repositories: (90+10)/(100+100) = 50, not team-a's 90.
	_, orgSeries := e.series(org, model.MeasureInputPipelineSuccessRate, nil)
	if math.Abs(*orgSeries[0]-50) > 1e-9 {
		t.Fatalf("org success rate %v, want 50 over both repositories", *orgSeries[0])
	}
}
