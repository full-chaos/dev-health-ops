//go:build integration

package investment

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
)

type countingProvider struct {
	inner *int
}

func (p countingProvider) Complete(ctx context.Context, request categorize.CompletionRequest) (categorize.CompletionResult, error) {
	*p.inner++
	return categorize.MockProvider{}.Complete(ctx, request)
}
func (countingProvider) Close() error  { return nil }
func (countingProvider) Model() string { return "mock" }

// latestVersion is the model version of the row the latest-row readers serve.
func (h *completenessHarness) latestVersion() string {
	h.t.Helper()
	var version string
	if err := h.conn.QueryRow(h.ctx, `SELECT argMax(categorization_model_version, computed_at) FROM work_unit_investments WHERE org_id = ?`,
		hierarchyCascadeTestOrg).Scan(&version); err != nil {
		h.t.Fatal(err)
	}
	return version
}

// rollbackCase drives A -> B -> A -> provider change -> A and asserts, for each
// step, the requests made, the units skipped and the version the readers serve.
// merge forces a ClickHouse merge between every pair of steps: the result must
// not depend on whether the superseded row has been merged away yet.
func rollbackCase(t *testing.T, merge bool) {
	h := newCompletenessHarness(t)
	h.setEvidence("1")
	calls := 0
	provider := countingProvider{inner: &calls}
	hour := 0
	type step struct {
		name, provider, model string
		requests, skipped     int
		served                string
	}
	a := categorize.EffectiveModelVersion("openai", "model-a")
	b := categorize.EffectiveModelVersion("openai", "model-b")
	c := categorize.EffectiveModelVersion("anthropic", "model-a")
	steps := []step{
		{"first run on A", "openai", "model-a", 1, 0, a},
		{"steady state on A", "openai", "model-a", 0, 1, a},
		{"model change A->B", "openai", "model-b", 1, 0, b},
		{"steady state on B", "openai", "model-b", 0, 1, b},
		{"rollback B->A", "openai", "model-a", 1, 0, a},
		{"provider change A->C", "anthropic", "model-a", 1, 0, c},
		{"rollback C->A", "openai", "model-a", 1, 0, a},
		{"steady state on A again", "openai", "model-a", 0, 1, a},
	}
	for _, st := range steps {
		hour++
		before := calls
		cfg := h.cfg("run-"+st.name, h.within.Add(time.Duration(hour)*time.Hour), false)
		cfg.ProviderName, cfg.Model = st.provider, st.model
		stats, err := h.run(h.materializer(h.conn, provider), cfg)
		if err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		if got := calls - before; got != st.requests || stats.SkippedExisting != st.skipped {
			t.Fatalf("%s: requests=%d skipped=%d, want requests=%d skipped=%d", st.name, got, stats.SkippedExisting, st.requests, st.skipped)
		}
		if got := h.latestVersion(); got != st.served {
			t.Fatalf("%s: served row version = %q, want %q", st.name, got, st.served)
		}
		if merge {
			if err := h.conn.Exec(h.ctx, "OPTIMIZE TABLE work_unit_investments FINAL"); err != nil {
				t.Fatal(err)
			}
			if got := h.latestVersion(); got != st.served {
				t.Fatalf("%s: after a merge the served version = %q, want %q", st.name, got, st.served)
			}
		}
	}
}

func TestRollbackOfTheServedModelRecategorisesAndServesTheRolledBackRow(t *testing.T) {
	for _, merge := range []bool{false, true} {
		name := "no merge between steps"
		if merge {
			name = "merge between steps"
		}
		t.Run(name, func(t *testing.T) { rollbackCase(t, merge) })
	}
}

// TestAnInputRevertIsRecategorisedNotSkipped: the unit has one served row, the
// latest; evidence H1 -> H2 -> H1 must ask again at the last step, as the
// config rollback does.
func TestAnInputRevertIsRecategorisedNotSkipped(t *testing.T) {
	h := newCompletenessHarness(t)
	calls := 0
	provider := countingProvider{inner: &calls}
	for i, st := range []struct {
		evidence string
		requests int
	}{{"1", 1}, {"2", 1}, {"1", 1}, {"1", 0}} {
		h.setEvidence(st.evidence)
		before := calls
		cfg := h.cfg("run-"+string(rune('a'+i)), h.within.Add(time.Duration(i+1)*time.Hour), false)
		cfg.ProviderName, cfg.Model = "openai", "model-a"
		if _, err := h.run(h.materializer(h.conn, provider), cfg); err != nil {
			t.Fatal(err)
		}
		if got := calls - before; got != st.requests {
			t.Fatalf("step %d (evidence %s): requests=%d, want %d", i, st.evidence, got, st.requests)
		}
	}
}

// TestTheServedSkipExistingPredicateClauseByClause pins every clause of
// FetchExistingInvestmentKeys on the row the readers serve.
func TestTheServedSkipExistingPredicateClauseByClause(t *testing.T) {
	h := newCompletenessHarness(t)
	h.setEvidence("1")
	calls := 0
	cfg := h.cfg("run-seed", h.within, false)
	cfg.ProviderName, cfg.Model = "openai", "model-a"
	if _, err := h.run(h.materializer(h.conn, countingProvider{inner: &calls}), cfg); err != nil {
		t.Fatal(err)
	}
	var unit, hash string
	if err := h.conn.QueryRow(h.ctx, `SELECT work_unit_id, categorization_input_hash FROM work_unit_investments WHERE org_id = ?`,
		hierarchyCascadeTestOrg).Scan(&unit, &hash); err != nil {
		t.Fatal(err)
	}
	a := categorize.EffectiveModelVersion("openai", "model-a")
	b := categorize.EffectiveModelVersion("openai", "model-b")
	key := chquery.InvestmentKey{WorkUnitID: unit, InputHash: hash}
	// newer inserts a copy of the latest row, one hour later, with replaced columns.
	newer := func(hours int, replace string) {
		t.Helper()
		q := `INSERT INTO work_unit_investments SELECT * REPLACE (` + replace + `, computed_at + toIntervalHour(` + strconv.Itoa(hours) + `) AS computed_at)
			FROM work_unit_investments FINAL WHERE org_id = '` + hierarchyCascadeTestOrg + `'`
		if err := h.conn.Exec(h.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	has := func(org, version string, k chquery.InvestmentKey) bool {
		t.Helper()
		got, err := h.reader.FetchExistingInvestmentKeys(h.ctx, org, []chquery.InvestmentKey{k}, version)
		if err != nil {
			t.Fatal(err)
		}
		_, ok := got[k]
		return ok
	}
	check := func(clause string, got, want bool) {
		t.Helper()
		if got != want {
			t.Fatalf("%s: existing = %v, want %v", clause, got, want)
		}
	}
	check("same org, version, hash, ok status", has(hierarchyCascadeTestOrg, a, key), true)
	check("version clause: another version", has(hierarchyCascadeTestOrg, b, key), false)
	check("hash clause: another input hash", has(hierarchyCascadeTestOrg, a, chquery.InvestmentKey{WorkUnitID: unit, InputHash: "other-hash"}), false)
	check("org clause: another org", has("another-org", a, key), false)
	check("unit clause: another unit", has(hierarchyCascadeTestOrg, a, chquery.InvestmentKey{WorkUnitID: "other-unit", InputHash: hash}), false)

	newer(1, `'repaired' AS categorization_status`)
	check("status set: repaired counts", has(hierarchyCascadeTestOrg, a, key), true)
	newer(2, `'invalid_llm_output' AS categorization_status`)
	check("status clause: latest failed, an older ok must not count", has(hierarchyCascadeTestOrg, a, key), false)
	newer(3, `'ok' AS categorization_status`)
	check("status clause: a newer ok counts again", has(hierarchyCascadeTestOrg, a, key), true)

	newer(4, `'`+b+`' AS categorization_model_version`)
	check("latest-row clause: the served row is B, A must not count", has(hierarchyCascadeTestOrg, a, key), false)
	check("latest-row clause: the served row is B, B counts", has(hierarchyCascadeTestOrg, b, key), true)
	newer(5, `'other-hash' AS categorization_input_hash`)
	check("latest-row clause: the served row has another hash, the old hash must not count", has(hierarchyCascadeTestOrg, b, key), false)
}
