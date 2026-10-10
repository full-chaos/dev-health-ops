//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The home risk reader picks the newest day on which NO row of the window has
// a missing score (CHAOS-6545): "no day is picked from rows the score cannot
// stand on". A row that scores from some of its inputs carries a score and
// stands; a row with no input at all has none and keeps its day from being
// picked.
func TestHomeRiskSignalsPickTheNewestDayWhoseRowsAllCarryAScore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	const (
		org   = "home-risk-partial-it"
		repoA = "11111111-1111-1111-1111-1111111111a1"
		repoB = "22222222-2222-2222-2222-2222222222b2"
	)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	put := func(day, repo, score, severity, churnNorm string) {
		t.Helper()
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
VALUES ('%s', '%s', 'repo', '%s', %s, '%s', %s, 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, toDateTime('%s 09:00:00'))`,
			org, day, repo, score, severity, churnNorm, day)); err != nil {
			t.Fatal(err)
		}
	}
	// 01-03: both rows carry a score; repoB scores from ONE input (churn).
	put("2026-01-03", repoA, "0.5", "elevated", "0.5")
	put("2026-01-03", repoB, "0.9", "high", "0.9")
	// 01-05 (newer): repoA scores, repoB has NO input at all (no score).
	put("2026-01-05", repoA, "0.4", "elevated", "0.4")
	put("2026-01-05", repoB, "NULL", "unknown", "NULL")

	rows, err := fetchRiskSignals(ctx, client, DefaultFilters(), start, end, org)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("risk rows = %+v, want the two rows of 01-03: the newer day holds a row with no score and is not picked", rows)
	}
	var partial *RiskRow
	for i := range rows {
		if rows[i].ScopeID == repoB {
			partial = &rows[i]
		}
	}
	if partial == nil || partial.Score == nil || *partial.Score != 0.9 || partial.Severity != "high" {
		t.Fatalf("the row that scores from one input is served with its score: %+v", partial)
	}
	// ...and with the coverage it stands on: churn alone is 0.3 of the weight (the
	// other norms are NULL), so a one-input score is never shown bare.
	if partial.Coverage == nil || *partial.Coverage < 0.3-1e-12 || *partial.Coverage > 0.3+1e-12 {
		t.Fatalf("the one-input row is served with coverage 0.3, got %v", partial.Coverage)
	}
	partial.ScopeDisplayName = "partial-repo" // the label is a name lookup of its own
	signal, ok := RiskSignal(*partial, DefaultFilters(), DataConfidence{})
	if !ok || signal.Coverage == nil || *signal.Coverage != *partial.Coverage {
		t.Fatalf("the Home risk signal carries the coverage of the score it shows: %+v (ok %v)", signal.Coverage, ok)
	}

	// Only rows with no input: no day is picked, no row is served.
	const empty = "home-risk-no-input-it"
	for _, day := range []string{"2026-01-03", "2026-01-05"} {
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
VALUES ('%s', '%s', 'repo', '%s', NULL, 'unknown', 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, toDateTime('%s 09:00:00'))`,
			empty, day, repoA, day)); err != nil {
			t.Fatal(err)
		}
	}
	none, err := fetchRiskSignals(ctx, client, DefaultFilters(), start, end, empty)
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Fatalf("rows with no score stand on nothing: no day is picked, got %+v", none)
	}
}

// The coverage the Home reader derives in SQL is the share of the weight that was
// present, for every input mix: each weight index and each norm is pinned by a row
// that has that input alone, and by mixes (CHAOS-6545). Distinct weights make a
// wrong index change the answer.
func TestHomeRiskCoverageForEveryInputMix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	const org = "home-risk-coverage-mix-it"
	// weights churn 0.1, complexity 0.2, ownership 0.3, review 0.4 (distinct)
	mixes := []struct {
		id                                   string
		churn, complexity, ownership, review bool
		want                                 float64
	}{
		{"mix-churn", true, false, false, false, 0.1},
		{"mix-complexity", false, true, false, false, 0.2},
		{"mix-ownership", false, false, true, false, 0.3},
		{"mix-review", false, false, false, true, 0.4},
		{"mix-all", true, true, true, true, 1.0},
	}
	norm := func(present bool) string {
		if present {
			return "0.5"
		}
		return "NULL"
	}
	for _, m := range mixes {
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm, ownership_norm, review_norm,
 w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
VALUES ('%s', '2026-01-03', 'repo', '%s', 0.5, 'elevated', %s, %s, %s, %s, 0.1, 0.2, 0.3, 0.4, 0.4, 0.65, toDateTime('2026-01-03 09:00:00'))`,
			org, m.id, norm(m.churn), norm(m.complexity), norm(m.ownership), norm(m.review))); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := fetchRiskSignals(ctx, client, DefaultFilters(), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), org)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]*float64{}
	for _, r := range rows {
		got[r.ScopeID] = r.Coverage
	}
	for _, m := range mixes {
		c := got[m.id]
		if c == nil || *c < m.want-1e-9 || *c > m.want+1e-9 {
			t.Errorf("%s: coverage = %v, want %v", m.id, c, m.want)
		}
	}
}
