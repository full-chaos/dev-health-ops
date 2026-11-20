//go:build integration

package home

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/recommendations"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Two versions of one key that share computed_at serve ONE version whatever the scan order. Versions go in as separate
// INSERT statements (same-key rows of ONE block collapse under ReplacingMergeTree) into a table with merges stopped.
// CHAOS-14355.

func startHomeTieStore(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return ctx, conn, client
}

func optScore(p *float64) string {
	if p == nil {
		return "NULL"
	}
	return fmt.Sprint(*p)
}

type homeTie struct {
	t    *testing.T
	ctx  context.Context
	conn stdclickhouse.Conn
}

func (h homeTie) exec(q string) {
	h.t.Helper()
	if err := h.conn.Exec(h.ctx, q); err != nil {
		h.t.Fatalf("%s: %v", q, err)
	}
}

// served runs the seed then the two versions in both orders (3 times each) on a truncated table and returns the distinct
// reads; with one version it is a single-version reference read.
func (h homeTie) served(table string, seed []string, versions []string, read func() string) []string {
	h.t.Helper()
	h.exec(`SYSTEM STOP MERGES ` + table)
	orders := [][]string{versions}
	if len(versions) == 2 {
		orders = append(orders, []string{versions[1], versions[0]})
	}
	seen := map[string]bool{}
	for _, order := range orders {
		for range 3 {
			h.exec(`TRUNCATE TABLE ` + table)
			for _, q := range append(append([]string{}, seed...), order...) {
				h.exec(q)
			}
			seen[read()] = true
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func oneOf(got []string, allowed ...string) bool {
	if len(got) != 1 {
		return false
	}
	for _, a := range allowed {
		if got[0] == a {
			return true
		}
	}
	return false
}

const (
	tieOrg   = "org-14355"
	recInto  = "INSERT INTO recommendations_daily (team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) "
	riskInto = "INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm, ownership_norm, review_norm, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at) "
)

// recVersion is one recommendations_daily row of (team-a, rule-1) at the window end and computed_at given; every value
// column carries the tag, so a mixed row is visible.
func recVersion(windowEnd, tag string, fired bool, at string) string {
	return recInto + fmt.Sprintf("SELECT 'team-a', '%s', 'rule-1', toDate('2026-07-28'), toDate('%s'), %v, 'sev-%s', 'title-%s', 'why-%s', 'done-%s', '[]', toDateTime64('%s', 3, 'UTC')",
		tieOrg, windowEnd, fired, tag, tag, tag, tag, at)
}

// riskVersion is one compounding_risk_daily row; a NULL score leaves the norms NULL too.
func riskVersion(day, scope, scopeID, score, at string) string {
	norm := score
	severity := "elevated"
	if score == "NULL" {
		severity = "unknown"
	}
	return riskInto + fmt.Sprintf("SELECT '%s', toDate('%s'), '%s', '%s', %s, '%s', %s, %s, %s, %s, 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, toDateTime('%s', 'UTC')",
		tieOrg, day, scope, scopeID, score, severity, norm, norm, norm, norm, at)
}

// riskRetraction is the row of zeros the writer stores over a key it no longer produces: every measure 0 or NULL.
func riskRetraction(day, scope, scopeID, at string) string {
	return "INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, computed_at) " +
		fmt.Sprintf("SELECT '%s', toDate('%s'), '%s', '%s', toDateTime('%s', 'UTC')", tieOrg, day, scope, scopeID, at)
}

func TestArgMaxTieHomeSignals14355(t *testing.T) {
	ctx, conn, client := startHomeTieStore(t)
	h := homeTie{t: t, ctx: ctx, conn: conn}
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	team := DefaultFilters()
	team.Scope = ScopeFilter{Level: "team", IDs: []string{"team-a"}}
	readRec := func() string {
		rows, err := fetchRecommendationSignals(ctx, client, team, start, end, tieOrg)
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, r := range rows {
			parts = append(parts, strings.Join([]string{r.RuleID, r.LatestSeverity, r.LatestTitle, r.LatestRationale, r.LatestSuccess, r.LatestEvidence}, " "))
		}
		return strings.Join(parts, ";")
	}
	readRisk := func() string {
		rows, err := fetchRiskSignals(ctx, client, DefaultFilters(), start, end, tieOrg)
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, r := range rows {
			parts = append(parts, fmt.Sprintf("%s/%s score=%s severity=%s coverage=%s", r.Scope, r.ScopeID, optScore(r.Score), r.Severity, optScore(r.Coverage)))
		}
		sort.Strings(parts)
		return strings.Join(parts, ";")
	}
	ref := func(table string, read func() string, rows ...string) string {
		t.Helper()
		got := h.served(table, rows, nil, read)
		if len(got) != 1 {
			t.Fatalf("reference read not stable: %v", got)
		}
		return got[0]
	}
	const ms0, ms1 = "2026-08-03 12:00:00.000", "2026-08-03 12:00:00.001"

	t.Run("recommendations inner pick: a tie serves one version, every column from it", func(t *testing.T) {
		a, b := recVersion("2026-08-03", "a", true, ms0), recVersion("2026-08-03", "b", true, ms0)
		refA, refB := ref("recommendations_daily", readRec, a), ref("recommendations_daily", readRec, b)
		if got := h.served("recommendations_daily", nil, []string{a, b}, readRec); !oneOf(got, refA, refB) {
			t.Errorf("served %q; want one of %q / %q", got, refA, refB)
		}
	})
	t.Run("recommendations inner pick: a newer computed_at wins over the tiebreak", func(t *testing.T) {
		for _, v := range [][2]string{{"a", "b"}, {"b", "a"}} {
			want := ref("recommendations_daily", readRec, recVersion("2026-08-03", v[1], true, ms1))
			if got := h.served("recommendations_daily", nil, []string{recVersion("2026-08-03", v[0], true, ms0), recVersion("2026-08-03", v[1], true, ms1)}, readRec); !oneOf(got, want) {
				t.Errorf("older %s newer %s: served %q; want %q", v[0], v[1], got, want)
			}
		}
	})
	t.Run("recommendations outer pick by window_end: cannot tie, output as main", func(t *testing.T) {
		// window_end is a GROUP BY key of the inner read, so the outer argMax(.., window_end) sees one row per window_end.
		superseded := ref("recommendations_daily", readRec, recVersion("2026-08-02", "old", true, ms0), recVersion("2026-08-03", "new", false, ms0))
		newer := ref("recommendations_daily", readRec, recVersion("2026-08-02", "old", true, ms0), recVersion("2026-08-03", "new", true, ms0))
		if got := superseded + "|" + newer; got != noTieWant["recommendations outer"] {
			t.Errorf("served %q; want the base output %q", got, noTieWant["recommendations outer"])
		}
	})

	const s0, s1 = "2026-08-03 12:00:00", "2026-08-03 12:00:01"
	complete := []string{riskVersion("2026-08-02", "repo", "repo-1", "0.4", s0), riskVersion("2026-08-02", "repo", "repo-2", "0.6", s0)}
	t.Run("risk served pick: a tie serves one version, every column from it", func(t *testing.T) {
		a, b := riskVersion("2026-08-03", "repo", "repo-1", "0.5", s0), riskVersion("2026-08-03", "repo", "repo-1", "0.9", s0)
		refA, refB := ref("compounding_risk_daily", readRisk, a), ref("compounding_risk_daily", readRisk, b)
		if got := h.served("compounding_risk_daily", nil, []string{a, b}, readRisk); !oneOf(got, refA, refB) {
			t.Errorf("served %q; want one of %q / %q", got, refA, refB)
		}
	})
	t.Run("risk served pick: a newer computed_at wins over the tiebreak", func(t *testing.T) {
		for _, v := range [][2]string{{"0.5", "0.9"}, {"0.9", "0.5"}} {
			want := ref("compounding_risk_daily", readRisk, riskVersion("2026-08-03", "repo", "repo-1", v[1], s1))
			if got := h.served("compounding_risk_daily", nil, []string{riskVersion("2026-08-03", "repo", "repo-1", v[0], s0), riskVersion("2026-08-03", "repo", "repo-1", v[1], s1)}, readRisk); !oneOf(got, want) {
				t.Errorf("older %s newer %s: served %q; want %q", v[0], v[1], got, want)
			}
		}
	})
	t.Run("risk: a NULL score of the newest version stays NULL and refuses the day", func(t *testing.T) {
		got := ref("compounding_risk_daily", readRisk, append(append([]string{}, complete...),
			riskVersion("2026-08-03", "repo", "repo-1", "0.5", s0), riskVersion("2026-08-03", "repo", "repo-1", "NULL", s1))...)
		if want := ref("compounding_risk_daily", readRisk, complete...); got != want {
			t.Errorf("served %q; want the complete day %q", got, want)
		}
	})
	t.Run("risk: on a tie the day picker and the served rows take the same version", func(t *testing.T) {
		// 40 scored values: the scored version is the pick of a hash for some of them and not for others, so a day
		// picker and a served pick that order by different hashes split on at least one pair.
		for k := 1; k <= 40; k++ {
			scored := riskVersion("2026-08-03", "repo", "repo-2", fmt.Sprintf("0.%02d", k), s0)
			unscored := riskVersion("2026-08-03", "repo", "repo-2", "NULL", s0)
			seed := append(append([]string{}, complete...), riskVersion("2026-08-03", "repo", "repo-1", "0.5", s0))
			refScored := ref("compounding_risk_daily", readRisk, append(append([]string{}, seed...), scored)...)
			refUnscored := ref("compounding_risk_daily", readRisk, append(append([]string{}, seed...), unscored)...)
			// scored: the newest day is complete and repo-2's score is served; unscored: the newest day is refused.
			if got := h.served("compounding_risk_daily", seed, []string{scored, unscored}, readRisk); !oneOf(got, refScored, refUnscored) {
				t.Errorf("pair %d: served %q; want %q (scored) or %q (unscored)", k, got, refScored, refUnscored)
			}
		}
	})
	t.Run("risk: a tie of a measurement and a retraction row, one of them, never a mix", func(t *testing.T) {
		seed := []string{riskVersion("2026-08-03", "repo", "repo-1", "0.5", s0)}
		live, dead := riskVersion("2026-08-03", "team", "team-x", "0.6", s0), riskRetraction("2026-08-03", "team", "team-x", s0)
		refLive := ref("compounding_risk_daily", readRisk, append(append([]string{}, seed...), live)...)
		refDead := ref("compounding_risk_daily", readRisk, append(append([]string{}, seed...), dead)...)
		if refDead != ref("compounding_risk_daily", readRisk, seed...) {
			t.Errorf("a key whose only row retires it served %q; want no key", refDead)
		}
		if got := h.served("compounding_risk_daily", seed, []string{live, dead}, readRisk); !oneOf(got, refLive, refDead) {
			t.Errorf("served %q; want %q or %q", got, refLive, refDead)
		}
	})
	t.Run("risk: no tie, rows as main serves them, a retired key stays absent", func(t *testing.T) {
		got := ref("compounding_risk_daily", readRisk,
			riskVersion("2026-08-03", "repo", "repo-1", "0.3", "2026-08-03 11:00:00"), riskVersion("2026-08-03", "repo", "repo-1", "0.5", s0),
			riskVersion("2026-08-03", "team", "team-x", "0.6", "2026-08-03 11:00:00"), riskRetraction("2026-08-03", "team", "team-x", s0),
			riskVersion("2026-08-03", "repo", "repo-3", "0.4", s0),
			riskVersion("2026-08-02", "repo", "repo-1", "0.2", s0))
		if want := ref("compounding_risk_daily", readRisk,
			riskVersion("2026-08-03", "repo", "repo-1", "0.5", s0), riskVersion("2026-08-03", "repo", "repo-3", "0.4", s0),
			riskVersion("2026-08-02", "repo", "repo-1", "0.2", s0)); got != want {
			t.Errorf("served %q; want (team-x absent) %q", got, want)
		}
		if want := noTieWant["risk"]; got != want {
			t.Errorf("served %q; want the base output %q", got, want)
		}
	})
}

// On a tie of two recommendations_daily versions the Home panel and the recommendations resolver serve the SAME
// version: both order by the same hash of the 7 value columns.
func TestArgMaxTieHomeAndRecommendationsServeOneVersion14355(t *testing.T) {
	ctx, conn, client := startHomeTieStore(t)
	h := homeTie{t: t, ctx: ctx, conn: conn}
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	team := DefaultFilters()
	team.Scope = ScopeFilter{Level: "team", IDs: []string{"team-a"}}
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	for k := 1; k <= 8; k++ {
		versions := []string{recVersion("2026-08-03", fmt.Sprintf("a%d", k), true, "2026-08-03 12:00:00.000"), recVersion("2026-08-03", fmt.Sprintf("b%d", k), true, "2026-08-03 12:00:00.000")}
		home := h.served("recommendations_daily", nil, versions, func() string {
			rows, err := fetchRecommendationSignals(ctx, client, team, start, end, tieOrg)
			if err != nil || len(rows) != 1 {
				t.Fatalf("home rows %v %v", rows, err)
			}
			return rows[0].LatestTitle
		})
		resolver := h.served("recommendations_daily", nil, versions, func() string {
			rows, err := recommendations.Resolve(ctx, client, tieOrg, "team-a", model.WindowInput{Value: 4, Unit: model.WindowUnitWeek}, now)
			if err != nil || len(rows) != 1 {
				t.Fatalf("resolver rows %v %v", rows, err)
			}
			return rows[0].Title
		})
		if len(home) != 1 || len(resolver) != 1 || home[0] != resolver[0] {
			t.Errorf("pair %d: the Home panel served %v, the recommendations resolver served %v; want one and the same version", k, home, resolver)
		}
	}
}

// noTieWant is what the reads served for the no-tie seeds at base (main 93aa580ed7ae, before slice 4 of CHAOS-14355).
var noTieWant = map[string]string{
	"recommendations outer": "|rule-1 sev-new title-new why-new done-new []",
	"risk":                  "repo/repo-1 score=0.5 severity=elevated coverage=1;repo/repo-3 score=0.4 severity=elevated coverage=1",
}
