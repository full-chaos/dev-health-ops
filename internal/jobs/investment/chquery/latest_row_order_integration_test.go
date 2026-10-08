//go:build integration

package chquery_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// INVARIANT (CHAOS-8908): the skip check (FetchExistingInvestmentKeys) and the
// served reader (analytics.LatestWorkUnitInvestmentsSource) choose the SAME
// latest row of a work unit, before any ClickHouse merge, whatever the insert
// order and even when two rows share computed_at. Every case reads the served
// row's run id and compares the skip decision with what THAT row says.

type orderRow struct {
	unit, version, hash, status, runID string
	at                                 time.Time
}

type orderEnv struct {
	ctx    context.Context
	conn   driver.Conn
	reader *chquery.Reader
}

var orderT0 = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

func orderAt(hours int) time.Time { return orderT0.Add(time.Duration(hours) * time.Hour) }

func orderSetup(t *testing.T) *orderEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	// Background merges (and optimize_on_insert) would collapse equal-computed_at
	// rows at a time the test does not control: the point is the UNMERGED table.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES work_unit_investments"); err != nil {
		t.Fatal(err)
	}
	return &orderEnv{ctx: ctx, conn: conn, reader: reader}
}

func (e *orderEnv) insert(t *testing.T, org string, rows ...orderRow) {
	t.Helper()
	vals := make([]string, 0, len(rows))
	for _, r := range rows {
		vals = append(vals, fmt.Sprintf("('%s','%s','%s','%s','%s','%s', toDateTime64('%s', 3, 'UTC'), '{\"run\":\"%s\"}', %d)",
			org, r.unit, r.status, r.version, r.hash, r.runID, r.at.Format("2006-01-02 15:04:05.000"), r.runID, int(orderEffort(r.runID))))
	}
	q := `INSERT INTO work_unit_investments (org_id, work_unit_id, categorization_status, categorization_model_version,
		categorization_input_hash, categorization_run_id, computed_at, structural_evidence_json, effort_value) VALUES ` + strings.Join(vals, ",")
	noCollapse := clickhouse.Context(e.ctx, clickhouse.WithSettings(clickhouse.Settings{"optimize_on_insert": 0}))
	if err := e.conn.Exec(noCollapse, q); err != nil {
		t.Fatal(err)
	}
}

func (e *orderEnv) optimize(t *testing.T) {
	t.Helper()
	for _, q := range []string{
		"SYSTEM START MERGES work_unit_investments",
		"OPTIMIZE TABLE work_unit_investments FINAL",
		"SYSTEM STOP MERGES work_unit_investments",
	} {
		if err := e.conn.Exec(e.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
}

func (e *orderEnv) skip(t *testing.T, org, version string, keys ...chquery.InvestmentKey) map[chquery.InvestmentKey]bool {
	t.Helper()
	got, err := e.reader.FetchExistingInvestmentKeys(e.ctx, org, keys, version)
	if err != nil {
		t.Fatal(err)
	}
	out := map[chquery.InvestmentKey]bool{}
	for _, key := range keys {
		_, out[key] = got[key]
	}
	return out
}

type servedRow struct {
	version, status, runID, evidence string
	effort                           float64
}

// served is what the served reader picks per work unit.
func (e *orderEnv) served(t *testing.T, org string) map[string]servedRow {
	t.Helper()
	q := `SELECT work_unit_id, categorization_model_version, categorization_status, categorization_run_id,
		structural_evidence_json, effort_value FROM ` + analytics.LatestWorkUnitInvestmentsSource()
	rows, err := e.conn.Query(clickhouse.Context(e.ctx, clickhouse.WithParameters(clickhouse.Parameters{"org_id": org})), q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]servedRow{}
	for rows.Next() {
		var unit string
		var r servedRow
		if err := rows.Scan(&unit, &r.version, &r.status, &r.runID, &r.evidence, &r.effort); err != nil {
			t.Fatal(err)
		}
		out[unit] = r
	}
	return out
}

// checkServedIsOneRow fails when the served row mixes columns of different rows.
func (e *orderEnv) checkServedIsOneRow(t *testing.T, label, org, unit string, s servedRow) (hash string, found bool) {
	t.Helper()
	version, hash, status, found := e.rowOfRun(t, org, unit, s.runID)
	if !found {
		t.Errorf("%s: served run %q is not a stored row", label, s.runID)
		return "", false
	}
	if s.version != version || s.status != status {
		t.Errorf("%s: served row mixes rows: version/status %s/%s but run %s stored %s/%s", label, s.version, s.status, s.runID, version, status)
	}
	if want := fmt.Sprintf(`{"run":"%s"}`, s.runID); s.evidence != want || s.effort != orderEffort(s.runID) {
		t.Errorf("%s: served evidence/effort %q/%v are not those of run %s", label, s.evidence, s.effort, s.runID)
	}
	return hash, true
}

// rowOfRun reads the ONE stored row that carries the run id.
func (e *orderEnv) rowOfRun(t *testing.T, org, unit, run string) (version, hash, status string, found bool) {
	t.Helper()
	rows, err := e.conn.Query(e.ctx, `SELECT categorization_model_version, categorization_input_hash, categorization_status
		FROM work_unit_investments WHERE org_id = $1 AND work_unit_id = $2 AND categorization_run_id = $3 LIMIT 1`, org, unit, run)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		if err := rows.Scan(&version, &hash, &status); err != nil {
			t.Fatal(err)
		}
		found = true
	}
	return
}

// orderEffort is a per-run effort_value, so a served effort of another row shows.
func orderEffort(runID string) float64 {
	sum := 0
	for _, c := range runID {
		sum += int(c)
	}
	return float64(sum)
}

func orderWants(version, hash, status, askVersion, askHash string) bool {
	return version == askVersion && hash == askHash && (status == "ok" || status == "repaired")
}

func orderKey(unit, hash string) chquery.InvestmentKey {
	return chquery.InvestmentKey{WorkUnitID: unit, InputHash: hash}
}

func TestLatestRowOrderInsertOrder(t *testing.T) {
	e := orderSetup(t)
	A, B := "openai:model-a", "openai:model-b"
	type ask struct {
		version, hash string
		want          bool
	}
	cases := []struct {
		name string
		rows [][]orderRow
		asks []ask
	}{
		{"version: newer row inserted first", [][]orderRow{{{"u", A, "H1", "ok", "r-a", orderAt(2)}}, {{"u", B, "H1", "ok", "r-b", orderAt(1)}}},
			[]ask{{A, "H1", true}, {B, "H1", false}}},
		{"hash: newer row inserted first", [][]orderRow{{{"u", A, "H1", "ok", "r-a", orderAt(2)}}, {{"u", A, "H2", "ok", "r-b", orderAt(1)}}},
			[]ask{{A, "H1", true}, {A, "H2", false}}},
		{"status: newer failed row inserted first", [][]orderRow{{{"u", A, "H1", "invalid_llm_output", "r-a", orderAt(2)}}, {{"u", A, "H1", "ok", "r-b", orderAt(1)}}},
			[]ask{{A, "H1", false}}},
		{"status: newer ok row inserted first", [][]orderRow{{{"u", A, "H1", "ok", "r-a", orderAt(2)}}, {{"u", A, "H1", "invalid_llm_output", "r-b", orderAt(1)}}},
			[]ask{{A, "H1", true}}},
		{"one insert, newest row listed first", [][]orderRow{{{"u", A, "H1", "ok", "r-a", orderAt(2)}, {"u", B, "H2", "ok", "r-b", orderAt(1)}}},
			[]ask{{A, "H1", true}, {B, "H2", false}, {A, "H2", false}, {B, "H1", false}}},
	}
	for i, c := range cases {
		org := fmt.Sprintf("org-order-%02d", i)
		for _, ins := range c.rows {
			e.insert(t, org, ins...)
		}
		for _, merged := range []bool{false, true} {
			if merged {
				e.optimize(t)
			}
			served := e.served(t, org)["u"]
			hash, found := e.checkServedIsOneRow(t, fmt.Sprintf("%s merged=%v", c.name, merged), org, "u", served)
			version, status := served.version, served.status
			for _, a := range c.asks {
				key := orderKey("u", a.hash)
				got := e.skip(t, org, a.version, key)[key]
				if got != a.want {
					t.Errorf("%s merged=%v ask %s/%s: skip=%v want %v (served run %q)", c.name, merged, a.version, a.hash, got, a.want, served.runID)
				}
				if found && got != orderWants(version, hash, status, a.version, a.hash) {
					t.Errorf("%s merged=%v ask %s/%s: skip=%v but served row %s/%s/%s", c.name, merged, a.version, a.hash, got, version, hash, status)
				}
			}
		}
	}
}

func TestLatestRowOrderSameComputedAt(t *testing.T) {
	e := orderSetup(t)
	A, B := "openai:model-a", "openai:model-b"
	const units = 160
	org := "org-ties"
	same := orderAt(5)
	// Two rows per unit share computed_at; the row INSERTED LAST is the one a
	// ClickHouse merge keeps, so it is the one both readers must pick, before
	// and after the merge. Four insert shapes put "run-2" (version B, hash H2)
	// last or first, in its own part or in one part. Every 8th group of units
	// gives run-2 a failed status, so a status read of the wrong row shows.
	highOK := func(u int) bool { return u%8 < 4 }
	highLast := func(u int) bool { return u%4 == 0 || u%4 == 2 }
	for u := 0; u < units; u++ {
		unit := fmt.Sprintf("t%03d", u)
		low := orderRow{unit, A, "H1", "ok", "run-1-" + unit, same}
		status := "ok"
		if !highOK(u) {
			status = "invalid_llm_output"
		}
		high := orderRow{unit, B, "H2", status, "run-2-" + unit, same}
		switch u % 4 {
		case 0:
			e.insert(t, org, low)
			e.insert(t, org, high)
		case 1:
			e.insert(t, org, high)
			e.insert(t, org, low)
		case 2:
			e.insert(t, org, low, high)
		default:
			e.insert(t, org, high, low)
		}
	}
	asks := []struct{ version, hash string }{{A, "H1"}, {B, "H2"}, {A, "H2"}, {B, "H1"}}
	for round := 0; round < 3; round++ {
		merged := round == 2
		if merged {
			e.optimize(t)
		}
		served := e.served(t, org)
		mismatch, wrongRow, wrongDecision := 0, 0, 0
		for u := 0; u < units; u++ {
			unit := fmt.Sprintf("t%03d", u)
			lastRun := "run-1-" + unit
			if highLast(u) {
				lastRun = "run-2-" + unit
			}
			if served[unit].runID != lastRun {
				wrongRow++
			}
			hash, found := e.checkServedIsOneRow(t, fmt.Sprintf("round %d unit %s", round, unit), org, unit, served[unit])
			version, status := served[unit].version, served[unit].status
			for _, a := range asks {
				key := orderKey(unit, a.hash)
				got := e.skip(t, org, a.version, key)[key]
				if !found || got != orderWants(version, hash, status, a.version, a.hash) {
					mismatch++
				}
				want := false
				if highLast(u) {
					want = a.version == B && a.hash == "H2" && highOK(u)
				} else {
					want = a.version == A && a.hash == "H1"
				}
				if got != want {
					wrongDecision++
				}
			}
		}
		t.Logf("round %d merged=%v: units=%d served-not-last-inserted=%d skip-vs-served mismatches=%d decisions-not-of-last-inserted=%d",
			round, merged, units, wrongRow, mismatch, wrongDecision)
		if mismatch != 0 || wrongRow != 0 || wrongDecision != 0 {
			t.Errorf("round %d merged=%v: %d skip decisions disagree with the served row, %d units served a row other than the last inserted, %d decisions are not those of the last inserted row",
				round, merged, mismatch, wrongRow, wrongDecision)
		}
	}
}
