package compoundingrisk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// CHAOS-14480: team big owns maxRows+1 repositories big-000..big-500; big-500
// sorts last. A set the team scope resolves is not a request list and is never
// cut at maxRows.

func bigRepoID(i int) string { return fmt.Sprintf("big-%03d", i) }

func bigRiskRow(day time.Time, id string, score float64, now time.Time) sweepRiskRow {
	return sweepRiskRow{day: day, scope: scopeRepo, scopeID: id, severity: "low", score: sweepF(score), churn: sweepF(score),
		wChurn: .4, wComplexity: .3, wOwnership: .2, wReview: .1, thresholdElevated: .4, thresholdHigh: .7,
		computedAt: time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC)}
}

// bigTeamWorld: every repository scores 0.25 on the day before now except
// big-500 at 1. With sharedName, big-499 and big-500 share one full name.
func bigTeamWorld(now time.Time, sharedName bool) sweepWorld {
	d := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	w := sweepWorld{}
	var owns []string
	for i := 0; i <= maxRows; i++ {
		id := bigRepoID(i)
		name := "acme/" + id
		if sharedName && i >= maxRows-1 {
			name = "acme/shared"
		}
		score := .25
		if i == maxRows {
			score = 1
		}
		w.repos = append(w.repos, sweepRepo{id, name})
		owns = append(owns, id)
		w.risk = append(w.risk, bigRiskRow(d, id, score, now))
	}
	w.teams = []sweepTeam{{id: "big", name: "Big", active: true, owns: owns}}
	return w
}

func bigResolve(t *testing.T, world sweepWorld, repoIDs []string, now time.Time) (rows, trend []string) {
	t.Helper()
	store := &sweepStore{world: world}
	got, err := Resolve(context.Background(), store, sweepOrg, &model.CompoundingRiskFilterInput{
		Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"big"}, RepoIds: repoIDs, TrendDays: 30,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range store.unknown {
		t.Fatalf("a statement the model does not know:\n%s", st)
	}
	for _, r := range got.Rows {
		rows = append(rows, r.ScopeID+"@"+r.Day.String()+"="+sweepNum(r.Score))
	}
	for _, p := range got.Trend {
		trend = append(trend, p.Day.String()+"="+sweepNum(p.Score))
	}
	return rows, trend
}

func TestNamedTeamWithMoreThan500OwnedRepositoriesKeepsEveryRepository(t *testing.T) {
	now := sweepNow()
	tokens := make([]string, 0, maxRows)
	for i := 0; i < maxRows-1; i++ {
		tokens = append(tokens, bigRepoID(i))
	}
	tokens = append(tokens, "acme/shared")
	want := sweepNum(sweepF(126.0 / 501))
	for _, c := range []struct {
		name       string
		sharedName bool
		repoIDs    []string
	}{
		{"501-owned-no-repoIds", false, nil},
		{"500-tokens-resolve-to-501", true, tokens},
	} {
		t.Run(c.name, func(t *testing.T) {
			rows, trend := bigResolve(t, bigTeamWorld(now, c.sharedName), c.repoIDs, now)
			if len(rows) != 1 || rows[0] != "big@2026-03-09="+want {
				t.Errorf("rows %v; want [big@2026-03-09=%s] (the mean over all 501 repositories; 0.25 is the mean without big-500)", rows, want)
			}
			if len(trend) != 1 || trend[0] != "2026-03-09="+want {
				t.Errorf("trend %v; want [2026-03-09=%s] (the mean over all 501 repositories)", trend, want)
			}
		})
	}
}

// The stored team row of big covers all 501 repositories; repoIds keeps 500 of
// them, so the row is derived from the 500 selected rows.
func TestNamedTeamWithMoreThan500OwnedRepositoriesNarrowedByRepoIdsDerivesTheRow(t *testing.T) {
	now := sweepNow()
	world := bigTeamWorld(now, false)
	stored := bigRiskRow(time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC), "big", .9, now)
	stored.scope, stored.severity = scopeTeam, "high"
	world.risk = append(world.risk, stored)
	var keep []string
	for i := 1; i <= maxRows; i++ {
		keep = append(keep, bigRepoID(i))
	}
	want := sweepNum(sweepF(125.75 / 500))
	rows, trend := bigResolve(t, world, keep, now)
	if len(rows) != 1 || rows[0] != "big@2026-03-09="+want || len(trend) != 1 || trend[0] != "2026-03-09="+want {
		t.Fatalf("rows %v trend %v; want rows [big@2026-03-09=%s] and trend [2026-03-09=%s] (derived from big-001..big-500; 0.9 is the stored row of all 501)",
			rows, trend, want, want)
	}
}

// Only big-500 has a row on the newest day: the day pick of the named team
// sees it.
func TestNamedTeamWithMoreThan500OwnedRepositoriesPicksTheDayOfTheLastRepository(t *testing.T) {
	now := sweepNow()
	world := bigTeamWorld(now, false)
	older := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	world.risk = world.risk[:0]
	for i := 0; i < maxRows; i++ {
		world.risk = append(world.risk, bigRiskRow(older, bigRepoID(i), .25, now))
	}
	world.risk = append(world.risk, bigRiskRow(time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC), bigRepoID(maxRows), 1, now))
	rows, trend := bigResolve(t, world, nil, now)
	if len(rows) != 1 || rows[0] != "big@2026-03-09=1" || len(trend) != 2 || trend[0] != "2026-03-08=0.25" || trend[1] != "2026-03-09=1" {
		t.Fatalf("rows %v trend %v; want rows [big@2026-03-09=1] (the newest day, big-500 only) and trend [2026-03-08=0.25 2026-03-09=1]", rows, trend)
	}
}

// sweepRecorder is the sweep model that also records each statement and its
// bindings.
type sweepRecorder struct {
	*sweepStore
	statements []string
	bindings   [][]clickhouse.Binding
}

func (r *sweepRecorder) Query(ctx context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	r.statements = append(r.statements, st)
	r.bindings = append(r.bindings, b)
	return r.sweepStore.Query(ctx, st, b)
}

// A request list stays cut at maxRows on the TEAM breakout too: with no team
// named, the 501 repoIds tokens are bound as the first 500.
func TestTeamBreakoutWithNoTeamNamedCutsRepoIdsTokensAt500(t *testing.T) {
	now := sweepNow()
	var tokens []string
	for i := 0; i <= maxRows; i++ {
		tokens = append(tokens, bigRepoID(i))
	}
	rec := &sweepRecorder{sweepStore: &sweepStore{world: bigTeamWorld(now, false)}}
	if _, err := Resolve(context.Background(), rec, sweepOrg, &model.CompoundingRiskFilterInput{
		Breakout: model.CompoundingRiskScopeTeam, RepoIds: tokens, TrendDays: 30,
	}, now); err != nil {
		t.Fatal(err)
	}
	for _, st := range rec.unknown {
		t.Fatalf("a statement the model does not know:\n%s", st)
	}
	var lens []int
	for _, b := range rec.bindings {
		if v, ok := sweepBinding(b, "repo_ids"); ok {
			lens = append(lens, len(v.([]string)))
		}
	}
	if len(lens) != 3 {
		t.Fatalf("repo_ids bound %d times %v; want 3 (repository day pick, repository rows, trend)", len(lens), lens)
	}
	for _, n := range lens {
		if n != maxRows {
			t.Fatalf("repo_ids lengths %v; want every one %d (a request list is cut at maxRows)", lens, maxRows)
		}
	}
}

// On TEAM with teams named and a day given, the owned set of now is not read
// for the rows: they use the owned set of the selected day. Per named team one
// ownership read for that set and one for the grouping.
func TestTeamBreakoutWithADayReadsNoOwnershipOfNowForTheRows(t *testing.T) {
	now := sweepNow()
	day := graphqldate.New(time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC))
	served := func(filter *model.CompoundingRiskFilterInput) ([]string, int) {
		rec := &sweepRecorder{sweepStore: &sweepStore{world: sweepFixture("derived", "under-cap", now)}}
		got, err := Resolve(context.Background(), rec, sweepOrg, filter, now)
		for _, st := range rec.unknown {
			t.Fatalf("a statement the model does not know:\n%s", st)
		}
		ownership := 0
		for _, st := range rec.statements {
			if strings.Contains(st, "SELECT toString(rp.id) AS repo_id, rp.repo AS full_name") {
				ownership++
			}
		}
		return sweepServed(got, err), ownership
	}
	teams := []string{"t1", "t2"}
	withDay, reads := served(&model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: teams, Day: &day, TrendDays: 30})
	if reads != 2*len(teams) {
		t.Errorf("%d ownership reads; want %d (the row scope and the grouping of the selected day, one per named team)", reads, 2*len(teams))
	}
	picked, _ := served(&model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: teams, TrendDays: 30})
	if strings.Join(withDay, "\n") != strings.Join(picked, "\n") {
		t.Errorf("served with the day given:\n%s\nwith the day picked:\n%s", strings.Join(withDay, "\n"), strings.Join(picked, "\n"))
	}
}

func sweepAnswer(t *testing.T, world sweepWorld, filter *model.CompoundingRiskFilterInput) string {
	t.Helper()
	store := &sweepStore{world: world}
	got, err := Resolve(context.Background(), store, sweepOrg, filter, sweepNow())
	for _, st := range store.unknown {
		t.Fatalf("a statement the model does not know:\n%s", st)
	}
	return strings.Join(sweepServed(got, err), "\n")
}

// repoIds is cut at maxRows before the team scope resolves it: its entry 501
// selects nothing. teamIds is not cut: every named team answers its row, as on
// main. An explicit empty repoIds keeps its answer of main.
func TestARepoIdsListOver500IsCutBeforeTheTeamScopeResolvesIt(t *testing.T) {
	now := sweepNow()
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	team := func(teamIDs, repoIDs []string) *model.CompoundingRiskFilterInput {
		return &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: teamIDs, RepoIds: repoIDs, TrendDays: 30}
	}
	t.Run("a-repoIds-entry-501", func(t *testing.T) {
		world := bigTeamWorld(now, false)
		var repoIDs []string
		for i := 0; i < maxRows; i++ {
			id := fmt.Sprintf("other-%03d", i)
			world.repos = append(world.repos, sweepRepo{id, "acme/" + id})
			world.risk = append(world.risk, bigRiskRow(d, id, .5, now))
			repoIDs = append(repoIDs, id)
		}
		full := sweepAnswer(t, world, team([]string{"big"}, append(repoIDs, bigRepoID(maxRows))))
		first := sweepAnswer(t, world, team([]string{"big"}, repoIDs))
		if full != first || !strings.HasPrefix(full, "  served ") || strings.Contains(full, "row ") {
			t.Errorf("served with big-500 as entry 501:\n%s\nwith the first 500 entries:\n%s\nwant both equal, a served answer, with no row", full, first)
		}
	})
	t.Run("b-teamIds-entry-501", func(t *testing.T) {
		world := sweepWorld{repos: []sweepRepo{{"repo-t", "acme/tail"}}, risk: []sweepRiskRow{bigRiskRow(d, "repo-t", .5, now)}}
		var teamIDs []string
		for i := 0; i < maxRows; i++ {
			id := fmt.Sprintf("none-%03d", i)
			world.teams = append(world.teams, sweepTeam{id: id, name: id, active: true})
			teamIDs = append(teamIDs, id)
		}
		world.teams = append(world.teams, sweepTeam{id: "tail", name: "Tail", active: true, owns: []string{"repo-t"}})
		full := sweepAnswer(t, world, team(append(teamIDs, "tail"), nil))
		want := strings.Join([]string{`  served breakout="TEAM" rows=1 trend=1`,
			`  row TEAM tail "Tail" score=0.5 coverage=0.4000000000000001 severity=ELEVATED day=2026-03-09`,
			`  trend 2026-03-09 score=0.5`}, "\n")
		if full != want {
			t.Errorf("served with team tail as entry 501:\n%s\nwant the answer of main (tail is not cut):\n%s", full, want)
		}
	})
	t.Run("c-explicit-empty-repoIds", func(t *testing.T) {
		world := bigTeamWorld(now, false)
		none := `  served breakout="TEAM" rows=0 trend=0`
		if got := sweepAnswer(t, world, team(nil, []string{})); got != none {
			t.Errorf("TEAM, no team, repoIds []:\n%s\nwant\n%s", got, none)
		}
		zero := &model.CompoundingRiskFilterInput{TeamIds: []string{"big"}, RepoIds: []string{}, TrendDays: 30}
		if got, want := sweepAnswer(t, world, zero), `  served breakout="" rows=0 trend=0`; got != want {
			t.Errorf("zero breakout, teamIds [big], repoIds []:\n%s\nwant\n%s", got, want)
		}
		if got, absent := sweepAnswer(t, world, team([]string{"big"}, []string{})), sweepAnswer(t, world, team([]string{"big"}, nil)); got != absent || !strings.Contains(got, "row TEAM big") {
			t.Errorf("TEAM, teamIds [big], repoIds []:\n%s\nwant the answer of repoIds absent, as on main:\n%s", got, absent)
		}
	})
}

// 501 named teams and 501 repoIds tokens: every named team answers its row
// (tm-500 owns rp-000 too) and repoIds entry 501 (x-tail, owned by tm-000) has
// no effect.
func TestFiveHundredOneNamedTeamsAnswerEveryRowWhileRepoIdsIsCutAt500(t *testing.T) {
	now := sweepNow()
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	world := sweepWorld{}
	var teamIDs, repoIDs []string
	for i := 0; i <= maxRows; i++ {
		tm, rp := fmt.Sprintf("tm-%03d", i), fmt.Sprintf("rp-%03d", i)
		owns := []string{rp}
		switch i {
		case 0:
			owns = append(owns, "x-tail")
		case maxRows:
			owns = []string{"rp-000"}
		}
		world.teams = append(world.teams, sweepTeam{id: tm, name: tm, active: true, owns: owns})
		teamIDs = append(teamIDs, tm)
		if i < maxRows {
			world.repos = append(world.repos, sweepRepo{rp, "acme/" + rp})
			world.risk = append(world.risk, bigRiskRow(d, rp, .5, now))
			repoIDs = append(repoIDs, rp)
		}
	}
	world.repos = append(world.repos, sweepRepo{"x-tail", "acme/x-tail"})
	world.risk = append(world.risk, bigRiskRow(d, "x-tail", 1, now))
	filter := func(repos []string) *model.CompoundingRiskFilterInput {
		return &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: teamIDs, RepoIds: repos, TrendDays: 30}
	}
	full := sweepAnswer(t, world, filter(append(repoIDs, "x-tail")))
	first := sweepAnswer(t, world, filter(repoIDs))
	if !strings.HasPrefix(full, fmt.Sprintf(`  served breakout="TEAM" rows=%d trend=1`, maxRows+1)) || strings.Contains(full, "score=0.75") || full != first {
		t.Errorf("served with x-tail as repoIds entry 501:\n%.400s\nwith the first 500 entries:\n%.400s\nwant both equal, %d rows (one per named team), no tm-000 row mixed with x-tail (0.75)",
			full, first, maxRows+1)
	}
}

// CHAOS-14521: n named teams tm-000.. in request order; team i owns rp-i.
// Repository rows score 0.5 and a stored team row scores 0.6 plus i/100000, so
// a derived row reads 0.5. With lastOnly every row is on the day before d except
// the stored row of the last team, the only row on d.
func namedTeamsWorld(n int, d time.Time, lastOnly bool) (sweepWorld, []string) {
	now := sweepNow()
	w := sweepWorld{}
	var teamIDs []string
	for i := 0; i < n; i++ {
		tm, rp := fmt.Sprintf("tm-%03d", i), fmt.Sprintf("rp-%03d", i)
		w.teams = append(w.teams, sweepTeam{id: tm, name: tm, active: true, owns: []string{rp}})
		w.repos = append(w.repos, sweepRepo{rp, "acme/" + rp})
		teamIDs = append(teamIDs, tm)
		rowDay := d
		if lastOnly {
			rowDay = d.AddDate(0, 0, -1)
		}
		w.risk = append(w.risk, bigRiskRow(rowDay, rp, .5, now))
		if lastOnly && i == n-1 {
			rowDay = d
		}
		stored := bigRiskRow(rowDay, tm, .6+float64(i)/100000, now)
		stored.scope = scopeTeam
		w.risk = append(w.risk, stored)
	}
	return w, teamIDs
}

// lastNamedTeamStoredRow is the stored row of tm-500 on d as servedTeamRow names it.
var lastNamedTeamStoredRow = "score=" + sweepNum(sweepF(.6+float64(maxRows)/100000)) + " day=2026-03-09"

func namedTeamsFilter(teamIDs []string, day *graphqldate.Date) *model.CompoundingRiskFilterInput {
	return &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: teamIDs, Day: day, TrendDays: 30}
}

// servedTeamRow is the served row of team id as "score=<s> day=<d>", or "absent".
func servedTeamRow(got *model.CompoundingRiskResult, id string) string {
	if got == nil {
		return "absent"
	}
	for _, r := range got.Rows {
		if r.ScopeID == id {
			return "score=" + sweepNum(r.Score) + " day=" + r.Day.String()
		}
	}
	return "absent"
}

// resolveNamedTeams answers filter over world on the sweep model and returns the
// lengths of every scope_ids binding.
func resolveNamedTeams(t *testing.T, world sweepWorld, filter *model.CompoundingRiskFilterInput) (*model.CompoundingRiskResult, []int) {
	t.Helper()
	rec := &sweepRecorder{sweepStore: &sweepStore{world: world}}
	got, err := Resolve(context.Background(), rec, sweepOrg, filter, sweepNow())
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range rec.unknown {
		t.Fatalf("a statement the model does not know:\n%s", st)
	}
	var lens []int
	for _, b := range rec.bindings {
		if v, ok := sweepBinding(b, "scope_ids"); ok {
			lens = append(lens, len(v.([]string)))
		}
	}
	return got, lens
}

func allEqual(lens []int, n int) bool {
	for _, l := range lens {
		if l != n {
			return false
		}
	}
	return true
}

// 501 named teams with a stored row each on the given day: every named team
// answers its stored row; the stored team read binds all 501 ids.
func TestFiveHundredOneNamedTeamsStoredRowsOnAGivenDayAnswerEveryRow(t *testing.T) {
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	world, teamIDs := namedTeamsWorld(maxRows+1, d, false)
	day := graphqldate.New(d)
	got, lens := resolveNamedTeams(t, world, namedTeamsFilter(teamIDs, &day))
	last, want := servedTeamRow(got, "tm-500"), lastNamedTeamStoredRow
	if len(got.Rows) != maxRows+1 || last != want || len(lens) != 1 || !allEqual(lens, maxRows+1) {
		t.Errorf("rows=%d tm-500 row %s scope_ids lengths %v; want %d rows, tm-500 row %s (its stored row), one stored team read binding %d ids",
			len(got.Rows), last, lens, maxRows+1, want, maxRows+1)
	}
}

// Same world, the day picked: the stored day pick and the stored team read
// both bind all 501 ids.
func TestFiveHundredOneNamedTeamsStoredRowsOnAPickedDayAnswerEveryRow(t *testing.T) {
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	world, teamIDs := namedTeamsWorld(maxRows+1, d, false)
	got, lens := resolveNamedTeams(t, world, namedTeamsFilter(teamIDs, nil))
	last, want := servedTeamRow(got, "tm-500"), lastNamedTeamStoredRow
	if len(got.Rows) != maxRows+1 || last != want || len(lens) != 2 || !allEqual(lens, maxRows+1) {
		t.Errorf("rows=%d tm-500 row %s scope_ids lengths %v; want %d rows, tm-500 row %s (its stored row), day pick and stored team read each binding %d ids",
			len(got.Rows), last, lens, maxRows+1, want, maxRows+1)
	}
}

// Only tm-500, the 501st named team, has a stored row on d (every other row is
// on the day before): the picked day is d and tm-500 answers its stored row.
func TestFiveHundredOneNamedTeamsPickTheDayOnlyTheLastTeamHasAStoredRowOn(t *testing.T) {
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	world, teamIDs := namedTeamsWorld(maxRows+1, d, true)
	got, lens := resolveNamedTeams(t, world, namedTeamsFilter(teamIDs, nil))
	last, want := servedTeamRow(got, "tm-500"), lastNamedTeamStoredRow
	if len(got.Rows) != 1 || last != want || len(lens) != 2 || !allEqual(lens, maxRows+1) {
		t.Errorf("rows=%d tm-500 row %s scope_ids lengths %v; want 1 row, tm-500 row %s (the only row on the newest day), day pick and stored team read each binding %d ids",
			len(got.Rows), last, lens, want, maxRows+1)
	}
}

// Digests of the answers and of the statements with their bindings for every
// case of TestStoredTeamReadsWith500OrFewerNamedTeamsAreUnchanged, recorded at
// the commit before CHAOS-14521.
const (
	namedTeamsServedDigest     = "399d05df5cd8eefb284df45408218eff57976d4fd8218e3f59ad52c0c989100f"
	namedTeamsStatementsDigest = "4ad43864e9d47c6456cb877c1ac7bebf29057c00ef5f055ef95365983f44fc81"
)

// With 500 or fewer named teams the stored team reads are unchanged: the served
// columns, the statements and their bindings are byte-equal to the commit
// before CHAOS-14521. Variants: every team stored; the last team with no stored
// row (derived from its repository); the last id a repeat of the first.
func TestStoredTeamReadsWith500OrFewerNamedTeamsAreUnchanged(t *testing.T) {
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	given := graphqldate.New(d)
	served, statements := sha256.New(), sha256.New()
	for _, n := range []int{2, maxRows - 1, maxRows} {
		for _, variant := range []string{"all-stored", "last-unstored", "repeated-id"} {
			for _, day := range []*graphqldate.Date{&given, nil} {
				world, teamIDs := namedTeamsWorld(n, d, false)
				switch variant {
				case "last-unstored":
					world.risk = world.risk[:len(world.risk)-1]
				case "repeated-id":
					teamIDs[n-1] = teamIDs[0]
				}
				rec := &sweepRecorder{sweepStore: &sweepStore{world: world}}
				got, err := Resolve(context.Background(), rec, sweepOrg, namedTeamsFilter(teamIDs, day), sweepNow())
				for _, st := range rec.unknown {
					t.Fatalf("a statement the model does not know:\n%s", st)
				}
				name := fmt.Sprintf("n=%d %s day-given=%v", n, variant, day != nil)
				text := name + "\n" + strings.Join(sweepServed(got, err), "\n") + "\n"
				var sts strings.Builder
				sts.WriteString(name + "\n")
				for i, st := range rec.statements {
					fmt.Fprintf(&sts, "%s\n%v\n", st, rec.bindings[i])
				}
				one, two := sha256.Sum256([]byte(text)), sha256.Sum256([]byte(sts.String()))
				t.Logf("%s: rows=%d served %s statements %d %s", name, len(got.Rows), hex.EncodeToString(one[:])[:12], len(rec.statements), hex.EncodeToString(two[:])[:12])
				served.Write([]byte(text))
				statements.Write([]byte(sts.String()))
			}
		}
	}
	if got := hex.EncodeToString(served.Sum(nil)); got != namedTeamsServedDigest {
		t.Errorf("served digest %s; want %s (the answers before CHAOS-14521)", got, namedTeamsServedDigest)
	}
	if got := hex.EncodeToString(statements.Sum(nil)); got != namedTeamsStatementsDigest {
		t.Errorf("statements digest %s; want %s (the statements and bindings before CHAOS-14521)", got, namedTeamsStatementsDigest)
	}
}
