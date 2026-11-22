package compoundingrisk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-14462: the equality sweep. It runs the real Resolve over every
// combination of breakout, teamIds, repoIds, day kind and stored team read,
// against an in-memory model of the reads of this package, and compares the
// served answers with a golden file. The golden written at a main commit is
// main's table: a change to Resolve must differ from it only in the rows it
// declares.
const (
	sweepGoldenPath     = "testdata/equality_sweep.golden"
	sweepGoldenSHA256   = "b0600abc84fdaf55475d981382fa7edc67e214fec8d61d5177a9fcbd36b74c68"
	sweepDeclaredPath   = "testdata/equality_sweep.declared"
	sweepDeclaredSHA256 = "12258f8ae53b06284298cd7ac2bcde871b95674f6db5a884e1fcc992e7fa647d"
	sweepRewriteEnv     = "COMPOUNDINGRISK_SWEEP_REWRITE"
	sweepOrg            = "org-sweep"
)

type sweepRiskRow struct {
	day                                         time.Time
	scope, scopeID, severity                    string
	score, churn, complexity, ownership, review *float64
	wChurn, wComplexity, wOwnership, wReview    float64
	thresholdElevated, thresholdHigh            float64
	computedAt                                  time.Time
}

// live is the live-row rule of the table for the newest row of a key: a
// retraction marker holds 0 in every weight and threshold and NULL in every
// nullable measure.
func (r sweepRiskRow) live() bool {
	for _, v := range []float64{r.wChurn, r.wComplexity, r.wOwnership, r.wReview, r.thresholdElevated, r.thresholdHigh} {
		if v != 0 {
			return true
		}
	}
	for _, v := range []*float64{r.score, r.churn, r.complexity, r.ownership, r.review} {
		if v != nil {
			return true
		}
	}
	return false
}

type sweepTeam struct {
	id, name string
	active   bool
	owns     []string
}

type sweepRepo struct{ id, name string }

// sweepOwner is one ownership row with its validity window [from, to); to nil
// is open.
type sweepOwner struct {
	team, repo string
	from       time.Time
	to         *time.Time
}

type sweepWorld struct {
	repos  []sweepRepo
	teams  []sweepTeam
	owners []sweepOwner
	risk   []sweepRiskRow
}

// ownership is every ownership row of the world: a team's owns are valid from
// 2020-01-01 with no end.
func (w sweepWorld) ownership() []sweepOwner {
	var out []sweepOwner
	for _, t := range w.teams {
		for _, id := range t.owns {
			out = append(out, sweepOwner{team: t.id, repo: id, from: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)})
		}
	}
	return append(out, w.owners...)
}

// sweepStore answers the statements of this package from a sweepWorld. A
// statement it does not know is recorded and answered with an error.
type sweepStore struct {
	world   sweepWorld
	unknown []string
}

type sweepRows struct {
	rows [][]any
	i    int
}

func (s *sweepRows) Next() bool   { s.i++; return s.i <= len(s.rows) }
func (s *sweepRows) Err() error   { return nil }
func (s *sweepRows) Close() error { return nil }
func (s *sweepRows) Scan(dest ...any) error {
	row := s.rows[s.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("sweep: scan of %d columns into %d targets", len(row), len(dest))
	}
	for i, v := range row {
		switch p := dest[i].(type) {
		case *string:
			*p = v.(string)
		case *float64:
			*p = v.(float64)
		case **float64:
			*p = v.(*float64)
		case *time.Time:
			*p = v.(time.Time)
		case **time.Time:
			*p = v.(*time.Time)
		case *uint8:
			*p = v.(uint8)
		default:
			return fmt.Errorf("sweep: scan target %T", dest[i])
		}
	}
	return nil
}

func sweepBinding(b []clickhouse.Binding, name string) (any, bool) {
	for _, x := range b {
		if x.Name == name {
			return x.Value, true
		}
	}
	return nil, false
}

func sweepDate(v any) (time.Time, error) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("sweep: date binding %T", v)
	}
	return time.Parse(dateLayout, s)
}

func (c *sweepStore) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	switch {
	case st == activeteams.IDsSubquery:
		return c.activeTeams(b)
	case strings.Contains(st, "maxOrNull(day) AS day") && strings.Contains(st, measuredRowHaving):
		return c.latestDay(st, b)
	case strings.Contains(st, "tupleElement(latest_row, 19) AS latest_computed_at") && strings.Contains(st, measuredRowHaving):
		return c.latestRows(st, b)
	case strings.Contains(st, "avg(score) AS avg_score"):
		return c.trend(b)
	case strings.Contains(st, "SELECT toString(id) AS repo_id, repo AS full_name"):
		return c.repoLabels(b)
	case strings.Contains(st, "SELECT toString(rp.id) AS repo_id, rp.repo AS full_name"):
		return c.ownedRepos(b)
	case strings.Contains(st, "AS listable") && strings.Contains(st, "FROM teams FINAL"):
		return c.teamLabels(b)
	}
	c.unknown = append(c.unknown, st)
	return nil, errors.New("sweep: statement the model does not know")
}

func (c *sweepStore) org(b []clickhouse.Binding, name string) error {
	v, ok := sweepBinding(b, name)
	if !ok || v != sweepOrg {
		return fmt.Errorf("sweep: binding %s = %v", name, v)
	}
	return nil
}

func (c *sweepStore) activeIDs() map[string]bool {
	out := map[string]bool{}
	for _, t := range c.world.teams {
		if t.active {
			out[t.id] = true
		}
	}
	return out
}

func (c *sweepStore) activeTeams(b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if err := c.org(b, "org_id"); err != nil {
		return nil, err
	}
	var ids []string
	for id := range c.activeIDs() {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows := &sweepRows{}
	for _, id := range ids {
		rows.rows = append(rows.rows, []any{id})
	}
	return rows, nil
}

// scopeFilter is the id filter of a compounding_risk_daily read: repo_ids
// keeps the repository rows whose scope id is a repository named by id or
// full name; scope_ids keeps the named scope ids.
func (c *sweepStore) scopeFilter(scope string, b []clickhouse.Binding) (func(string) bool, error) {
	if v, ok := sweepBinding(b, "repo_ids"); ok {
		if scope != scopeRepo {
			return nil, fmt.Errorf("sweep: repo_ids on scope %s", scope)
		}
		names := map[string]bool{}
		for _, n := range v.([]string) {
			names[n] = true
		}
		keep := map[string]bool{}
		for _, r := range c.world.repos {
			if names[r.id] || names[r.name] {
				keep[r.id] = true
			}
		}
		return func(id string) bool { return keep[id] }, nil
	}
	if v, ok := sweepBinding(b, "scope_ids"); ok {
		keep := map[string]bool{}
		for _, n := range v.([]string) {
			keep[n] = true
		}
		return func(id string) bool { return keep[id] }, nil
	}
	return func(string) bool { return true }, nil
}

// newest is the newest row per (day, scope id) of one scope whose day passes
// inDay and whose scope id passes keep.
func (c *sweepStore) newest(scope string, inDay func(time.Time) bool, keep func(string) bool) map[[2]string]sweepRiskRow {
	out := map[[2]string]sweepRiskRow{}
	for _, r := range c.world.risk {
		if r.scope != scope || !inDay(r.day) || !keep(r.scopeID) {
			continue
		}
		k := [2]string{r.day.Format(dateLayout), r.scopeID}
		if cur, seen := out[k]; !seen || r.computedAt.After(cur.computedAt) {
			out[k] = r
		}
	}
	return out
}

func (c *sweepStore) latestDay(st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if err := c.org(b, "org_id"); err != nil {
		return nil, err
	}
	scope, _ := sweepBinding(b, "scope")
	start, err := sweepDate(mustSweepBinding(b, "start_day"))
	if err != nil {
		return nil, err
	}
	end, err := sweepDate(mustSweepBinding(b, "end_day"))
	if err != nil {
		return nil, err
	}
	keep, err := c.scopeFilter(scope.(string), b)
	if err != nil {
		return nil, err
	}
	var best *time.Time
	for _, r := range c.newest(scope.(string), func(d time.Time) bool { return !d.Before(start) && !d.After(end) }, keep) {
		if !r.live() || r.score == nil {
			continue
		}
		if d := r.day; best == nil || d.After(*best) {
			best = &d
		}
	}
	return &sweepRows{rows: [][]any{{best}}}, nil
}

func (c *sweepStore) latestRows(st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if err := c.org(b, "org_id"); err != nil {
		return nil, err
	}
	scope, _ := sweepBinding(b, "scope")
	day, err := sweepDate(mustSweepBinding(b, "day"))
	if err != nil {
		return nil, err
	}
	keep, err := c.scopeFilter(scope.(string), b)
	if err != nil {
		return nil, err
	}
	var picked []sweepRiskRow
	for _, r := range c.newest(scope.(string), func(d time.Time) bool { return d.Equal(day) }, keep) {
		if r.live() {
			picked = append(picked, r)
		}
	}
	// ORDER BY score DESC NULLS LAST; ClickHouse leaves ties unordered, the
	// model breaks them by scope id (the fixture holds no tie in one read).
	sort.Slice(picked, func(i, j int) bool {
		a, z := picked[i].score, picked[j].score
		if (a == nil) != (z == nil) {
			return z == nil
		}
		if a != nil && *a != *z {
			return *a > *z
		}
		return picked[i].scopeID < picked[j].scopeID
	})
	if m := regexp.MustCompile(`\nLIMIT (\d+)`).FindStringSubmatch(st); m != nil {
		n, _ := strconv.Atoi(m[1])
		if len(picked) > n {
			picked = picked[:n]
		}
	}
	rows := &sweepRows{}
	for _, r := range picked {
		rows.rows = append(rows.rows, []any{r.scopeID, r.score, r.severity, r.churn, r.complexity, r.ownership, r.review,
			(*float64)(nil), (*float64)(nil), (*float64)(nil), (*float64)(nil), (*float64)(nil), (*float64)(nil),
			r.wChurn, r.wComplexity, r.wOwnership, r.wReview, r.thresholdElevated, r.thresholdHigh, r.computedAt})
	}
	return rows, nil
}

func (c *sweepStore) trend(b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if err := c.org(b, "org_id"); err != nil {
		return nil, err
	}
	start, err := sweepDate(mustSweepBinding(b, "start"))
	if err != nil {
		return nil, err
	}
	end, err := sweepDate(mustSweepBinding(b, "end"))
	if err != nil {
		return nil, err
	}
	keep, err := c.scopeFilter(scopeRepo, b)
	if err != nil {
		return nil, err
	}
	type acc struct {
		sum float64
		n   int
	}
	byDay := map[string]*acc{}
	for k, r := range c.newest(scopeRepo, func(d time.Time) bool { return !d.Before(start) && !d.After(end) }, keep) {
		a := byDay[k[0]]
		if a == nil {
			a = &acc{}
			byDay[k[0]] = a
		}
		if r.score != nil {
			a.sum += *r.score
			a.n++
		}
	}
	var days []string
	for d := range byDay {
		days = append(days, d)
	}
	sort.Strings(days)
	rows := &sweepRows{}
	for _, d := range days {
		t, _ := time.Parse(dateLayout, d)
		var avg *float64
		if a := byDay[d]; a.n > 0 {
			v := a.sum / float64(a.n)
			avg = &v
		}
		rows.rows = append(rows.rows, []any{t, avg})
	}
	return rows, nil
}

func (c *sweepStore) repoLabels(b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if err := c.org(b, "org_id"); err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, id := range mustSweepBinding(b, "repo_ids").([]string) {
		want[id] = true
	}
	rows := &sweepRows{}
	for _, r := range c.world.repos {
		if want[r.id] {
			rows.rows = append(rows.rows, []any{r.id, r.name})
		}
	}
	return rows, nil
}

// ownedRepos is the ownership read of teamscope.RepoCondition: the
// repositories the named ACTIVE teams own at the bound instant. As
// ownershipwindow.Window, a row is valid when from <= at < to, where at is the
// bound instant or, when later, the first from of the repository's rows of
// active teams.
func (c *sweepStore) ownedRepos(b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if err := c.org(b, "org_id"); err != nil {
		return nil, err
	}
	if err := c.org(b, teamscope.BindingOrgID); err != nil {
		return nil, err
	}
	named := map[string]bool{}
	for _, id := range mustSweepBinding(b, teamscope.BindingTeamIDs).([]string) {
		named[id] = true
	}
	asOf, ok := mustSweepBinding(b, teamscope.BindingAsOf).(time.Time)
	if !ok {
		return nil, errors.New("sweep: ownership as-of binding is not a time")
	}
	active := c.activeIDs()
	first := map[string]time.Time{}
	for _, o := range c.world.ownership() {
		if f, seen := first[o.repo]; active[o.team] && (!seen || o.from.Before(f)) {
			first[o.repo] = o.from
		}
	}
	owned := map[string]bool{}
	for _, o := range c.world.ownership() {
		at := asOf
		if first[o.repo].After(at) {
			at = first[o.repo]
		}
		if named[o.team] && active[o.team] && !o.from.After(at) && (o.to == nil || o.to.After(at)) {
			owned[o.repo] = true
		}
	}
	rows := &sweepRows{}
	for _, r := range c.world.repos {
		if owned[r.id] {
			rows.rows = append(rows.rows, []any{r.id, r.name})
		}
	}
	return rows, nil
}

func (c *sweepStore) teamLabels(b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if err := c.org(b, "org_id"); err != nil {
		return nil, err
	}
	rows := &sweepRows{}
	for _, t := range c.world.teams {
		var listable uint8
		if t.active {
			listable = 1
		}
		rows.rows = append(rows.rows, []any{t.id, t.name, listable})
	}
	return rows, nil
}

func mustSweepBinding(b []clickhouse.Binding, name string) any {
	v, _ := sweepBinding(b, name)
	return v
}

func sweepF(v float64) *float64 { return &v }

// sweepFixture builds the worlds. Team t1 owns repo-a, repo-c and repo-r
// (review-only: no repository risk row); team t2 owns repo-x; repo-u has no
// owner. On the stored day t1 has a stored row (0.6, not the 0.5 mean of
// repo-a and repo-c) and t2's newest row is a retraction marker: t2 has
// repository rows and no live stored row.
func sweepFixture(dayKind, read string, now time.Time) sweepWorld {
	d := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	at := func(h, m int) time.Time { return time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, time.UTC) }
	repoRow := func(id string, score float64, sev string, churn, complexity, ownership, review *float64) sweepRiskRow {
		return sweepRiskRow{day: d, scope: scopeRepo, scopeID: id, severity: sev, score: sweepF(score),
			churn: churn, complexity: complexity, ownership: ownership, review: review,
			wChurn: .4, wComplexity: .3, wOwnership: .2, wReview: .1, thresholdElevated: .4, thresholdHigh: .7, computedAt: at(1, 0)}
	}
	w := sweepWorld{
		repos: []sweepRepo{{"repo-a", "acme/web"}, {"repo-c", "acme/lib"}, {"repo-r", "acme/docs"}, {"repo-u", "acme/tools"}, {"repo-x", "acme/ops"}},
		teams: []sweepTeam{
			{id: "t1", name: "Platform", active: true, owns: []string{"repo-a", "repo-c", "repo-r"}},
			{id: "t2", name: "Infra", active: true, owns: []string{"repo-x"}},
		},
		risk: []sweepRiskRow{
			repoRow("repo-a", .25, "low", sweepF(.25), sweepF(.25), sweepF(.25), sweepF(.25)),
			repoRow("repo-c", .75, "high", sweepF(.75), sweepF(.75), nil, nil),
			repoRow("repo-u", .875, "high", sweepF(.875), sweepF(.875), sweepF(.875), sweepF(.875)),
			repoRow("repo-x", .625, "elevated", sweepF(.625), nil, nil, nil),
		},
	}
	if dayKind != "stored" {
		return w
	}
	t1 := repoRow("t1", .6, "elevated", sweepF(.6), sweepF(.6), sweepF(.6), nil)
	t1.scope, t1.computedAt = scopeTeam, at(2, 0)
	t2 := repoRow("t2", .5, "elevated", sweepF(.5), sweepF(.5), sweepF(.5), sweepF(.5))
	t2.scope, t2.computedAt = scopeTeam, at(0, 30)
	t2retracted := sweepRiskRow{day: d, scope: scopeTeam, scopeID: "t2", computedAt: at(3, 0)}
	w.risk = append(w.risk, t1, t2, t2retracted)
	if read == "under-cap" {
		return w
	}
	// maxRows team rows that sort above t1 and own no repository: the stored
	// team read ends at its cap and t1 is cut off.
	for i := 0; i < maxRows; i++ {
		id := fmt.Sprintf("cap-%03d", i)
		w.teams = append(w.teams, sweepTeam{id: id, name: fmt.Sprintf("Cap %03d", i), active: read == "at-cap"})
		row := repoRow(id, .9+float64(i)/10000, "high", sweepF(.9), nil, nil, nil)
		row.scope, row.computedAt = scopeTeam, at(2, 0)
		w.risk = append(w.risk, row)
	}
	return w
}

// sweepMovedFixture is sweepFixture with repo-m: t1 owns it on the selected
// day, t2 from 06:00 of the next day (before now).
func sweepMovedFixture(dayKind string, now time.Time) sweepWorld {
	w := sweepFixture(dayKind, "under-cap", now)
	d := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	moved := time.Date(now.Year(), now.Month(), now.Day(), 6, 0, 0, 0, time.UTC)
	w.repos = append(w.repos, sweepRepo{"repo-m", "acme/move"})
	w.owners = append(w.owners,
		sweepOwner{team: "t1", repo: "repo-m", from: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), to: &moved},
		sweepOwner{team: "t2", repo: "repo-m", from: moved})
	w.risk = append(w.risk, sweepRiskRow{day: d, scope: scopeRepo, scopeID: "repo-m", severity: "low", score: sweepF(.125), churn: sweepF(.125),
		wChurn: .4, wComplexity: .3, wOwnership: .2, wReview: .1, thresholdElevated: .4, thresholdHigh: .7,
		computedAt: time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC)})
	return w
}

func sweepNum(v *float64) string {
	if v == nil {
		return "null"
	}
	return strconv.FormatFloat(*v, 'g', -1, 64)
}

func sweepServed(got *model.CompoundingRiskResult, err error) []string {
	if err != nil {
		return []string{"  error: " + err.Error()}
	}
	out := []string{fmt.Sprintf("  served breakout=%q rows=%d trend=%d", string(got.Breakout), len(got.Rows), len(got.Trend))}
	for _, r := range got.Rows {
		out = append(out, fmt.Sprintf("  row %s %s %q score=%s coverage=%s severity=%s day=%s",
			r.Scope, r.ScopeID, r.ScopeLabel, sweepNum(r.Score), sweepNum(r.Coverage), r.Severity, r.Day.String()))
	}
	for _, p := range got.Trend {
		out = append(out, fmt.Sprintf("  trend %s score=%s", p.Day.String(), sweepNum(p.Score)))
	}
	return out
}

// sweepResolve answers one combination: the world is the whole store of the
// org sweepOrg when Resolve runs.
type sweepResolve func(header string, world sweepWorld, filter *model.CompoundingRiskFilterInput) (*model.CompoundingRiskResult, error)

func sweepNow() time.Time { return time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC) }

// sweepTable runs every combination through resolve and returns the served
// text in the golden's form. The unit and the real-ClickHouse variants share it.
func sweepTable(t *testing.T, resolve sweepResolve) []byte {
	t.Helper()
	now := sweepNow()
	type idsArg struct {
		name string
		ids  []string
	}
	breakouts := []struct {
		name  string
		scope model.CompoundingRiskScope
	}{
		{"REPO", model.CompoundingRiskScopeRepo},
		{"REPO-default", ""},
		{"TEAM", model.CompoundingRiskScopeTeam},
	}
	teamArgs := []idsArg{{"absent", nil}, {"named", []string{"t1", "t2"}}}
	repoArgs := []idsArg{
		{"absent", nil},
		{"empty", []string{}},
		{"narrowing", []string{"repo-a", "repo-u"}},
		{"keeps-all", []string{"repo-a", "repo-c", "repo-r", "repo-u", "repo-x"}},
	}
	lines := []string{
		"# compoundingRisk equality sweep (CHAOS-14462): the answers of Resolve over the fixture of equality_sweep_test.go.",
		"# Rewrite: " + sweepRewriteEnv + "=1 go test -run '^TestCompoundingRiskEqualitySweep$' ./internal/queryapi/compoundingrisk/",
		"# REPO-default is the zero breakout a Go caller passes (the GraphQL schema fills REPO); filter=nil is an omitted filter.",
	}
	combinations := 0
	var notApplicable []string
	run := func(header string, world sweepWorld, filter *model.CompoundingRiskFilterInput) {
		combinations++
		got, err := resolve(header, world, filter)
		lines = append(lines, "== "+header)
		lines = append(lines, sweepServed(got, err)...)
	}
	for _, dayKind := range []string{"stored", "derived"} {
		run(fmt.Sprintf("filter=nil day=%s read=n/a", dayKind), sweepFixture(dayKind, "under-cap", now), nil)
	}
	for _, bo := range breakouts {
		for _, ta := range teamArgs {
			for _, ra := range repoArgs {
				for _, dayKind := range []string{"stored", "derived"} {
					for _, read := range []string{"under-cap", "at-cap", "at-cap-none-active"} {
						header := fmt.Sprintf("breakout=%s teamIds=%s repoIds=%s day=%s read=%s", bo.name, ta.name, ra.name, dayKind, read)
						if read != "under-cap" {
							switch {
							case dayKind != "stored":
								notApplicable = append(notApplicable, header+": no stored team row on the day")
								continue
							case bo.scope == model.CompoundingRiskScopeRepo:
								notApplicable = append(notApplicable, header+": the REPO breakout reads no stored team row")
								continue
							case ta.ids != nil:
								notApplicable = append(notApplicable, header+": the read is bounded by the named ids (2 < maxRows) and the active-team filter runs only with no team named")
								continue
							}
						}
						// A zero breakout takes the team rows arm of Resolve, so it reads stored team rows.
						if dayKind != "stored" || bo.scope == model.CompoundingRiskScopeRepo {
							header = fmt.Sprintf("breakout=%s teamIds=%s repoIds=%s day=%s read=n/a", bo.name, ta.name, ra.name, dayKind)
						}
						run(header, sweepFixture(dayKind, read, now), &model.CompoundingRiskFilterInput{
							Breakout: bo.scope, RepoIds: ra.ids, TeamIds: ta.ids, TrendDays: 30,
						})
					}
				}
			}
		}
	}
	for _, extra := range []struct {
		teams idsArg
		repos idsArg
	}{
		{idsArg{"absent", nil}, idsArg{"all-but-unowned", []string{"repo-a", "repo-c", "repo-x"}}},
		{idsArg{"absent", nil}, idsArg{"no-row-only", []string{"repo-r"}}},
		{idsArg{"named", []string{"t1", "t2"}}, idsArg{"outside-teams", []string{"repo-u"}}},
	} {
		run(fmt.Sprintf("breakout=TEAM teamIds=%s repoIds=%s day=stored read=under-cap", extra.teams.name, extra.repos.name),
			sweepFixture("stored", "under-cap", now), &model.CompoundingRiskFilterInput{
				Breakout: model.CompoundingRiskScopeTeam, RepoIds: extra.repos.ids, TeamIds: extra.teams.ids, TrendDays: 30,
			})
	}
	for _, mv := range []struct {
		teams   idsArg
		repos   idsArg
		dayKind string
	}{
		{idsArg{"absent", nil}, idsArg{"absent", nil}, "derived"},
		{idsArg{"named", []string{"t1", "t2"}}, idsArg{"absent", nil}, "derived"},
		{idsArg{"named-t1", []string{"t1"}}, idsArg{"absent", nil}, "derived"},
		{idsArg{"named-t2", []string{"t2"}}, idsArg{"absent", nil}, "derived"},
		{idsArg{"absent", nil}, idsArg{"absent", nil}, "stored"},
		{idsArg{"absent", nil}, idsArg{"moved-narrowing", []string{"repo-a", "repo-m", "repo-u"}}, "stored"},
		{idsArg{"named", []string{"t1", "t2"}}, idsArg{"moved-narrowing", []string{"repo-a", "repo-m"}}, "stored"},
	} {
		read := "n/a"
		if mv.dayKind == "stored" {
			read = "under-cap"
		}
		run(fmt.Sprintf("breakout=TEAM teamIds=%s repoIds=%s day=%s read=%s world=moved", mv.teams.name, mv.repos.name, mv.dayKind, read),
			sweepMovedFixture(mv.dayKind, now), &model.CompoundingRiskFilterInput{
				Breakout: model.CompoundingRiskScopeTeam, RepoIds: mv.repos.ids, TeamIds: mv.teams.ids, TrendDays: 30,
			})
	}
	t.Logf("combinations run: %d; not applicable: %d", combinations, len(notApplicable))
	for _, na := range notApplicable {
		t.Logf("not applicable: %s", na)
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

func TestCompoundingRiskEqualitySweep(t *testing.T) {
	text := sweepTable(t, func(header string, world sweepWorld, filter *model.CompoundingRiskFilterInput) (*model.CompoundingRiskResult, error) {
		store := &sweepStore{world: world}
		got, err := Resolve(context.Background(), store, sweepOrg, filter, sweepNow())
		for _, st := range store.unknown {
			t.Errorf("%s: a statement the model does not know:\n%s", header, st)
		}
		return got, err
	})
	path := filepath.FromSlash(sweepGoldenPath)
	if os.Getenv(sweepRewriteEnv) == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, text, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(text)
		t.Fatalf("golden rewritten: set sweepGoldenSHA256 to %s and add the manifest row", hex.EncodeToString(sum[:]))
	}
	sweepCompare(t, text)
}

// sweepCompare compares served text with main's golden, one combination at a
// time: a combination of the declared file serves its declared block, which
// differs from main's; every other combination serves main's block.
func sweepCompare(t *testing.T, text []byte) {
	t.Helper()
	golden := sweepRead(t, sweepGoldenPath, sweepGoldenSHA256)
	declaredText := sweepRead(t, sweepDeclaredPath, sweepDeclaredSHA256)
	want, have, declared := sweepBlocks(string(golden)), sweepBlocks(string(text)), sweepBlocks(string(declaredText))
	differ := 0
	for _, h := range declared.order {
		class, block, _ := strings.Cut(declared.blocks[h], "\n")
		switch {
		case class != "class G" && class != "class S":
			differ++
			t.Errorf("%s: declared %q is not one of the classes G (grouping at the selected day), S (named scope at the selected day)", h, class)
		case want.blocks[h] == "":
			differ++
			t.Errorf("%s: declared, but not a combination of the golden", h)
		case block == want.blocks[h]:
			differ++
			t.Errorf("%s: declared (%s) but equal to main's block: a dead declaration", h, class)
		}
	}
	for _, h := range have.order {
		w, ok := want.blocks[h]
		if d, isDeclared := declared.blocks[h]; isDeclared {
			_, w, _ = strings.Cut(d, "\n")
		}
		switch {
		case !ok:
			differ++
			t.Errorf("%s: not in the golden; served:\n%s", h, have.blocks[h])
		case w != have.blocks[h]:
			differ++
			t.Errorf("%s:\n want:\n%s served:\n%s", h, w, have.blocks[h])
		}
	}
	for _, h := range want.order {
		if _, ok := have.blocks[h]; !ok {
			differ++
			t.Errorf("%s: in the golden, not served", h)
		}
	}
	if want.preamble != have.preamble {
		t.Errorf("golden and served text differ before the first combination")
	}
	if differ > 0 {
		t.Errorf("%d combinations differ from main's golden and the declared differences", differ)
	}
}

func sweepRead(t *testing.T, path, pin string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != pin {
		t.Errorf("%s has sha256 %s, the cell pins %s", path, hex.EncodeToString(sum[:]), pin)
	}
	return b
}

type sweepText struct {
	preamble string
	order    []string
	blocks   map[string]string
}

func sweepBlocks(text string) sweepText {
	out := sweepText{blocks: map[string]string{}}
	header := ""
	for _, line := range strings.SplitAfter(text, "\n") {
		if strings.HasPrefix(line, "== ") {
			header = strings.TrimSuffix(line, "\n")
			out.order = append(out.order, header)
			continue
		}
		if header != "" {
			out.blocks[header] += line
		} else {
			out.preamble += line
		}
	}
	return out
}
