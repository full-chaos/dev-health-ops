//go:build integration

package analytics

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// bigOrgRuns seeds n raw runs of one repository-day and that day's stored key row, without the writer (the read bound
// is about rows read, which the writer does not change).
func (e *ownersEnv) bigOrgRuns(org, repo string, n int) {
	e.t.Helper()
	e.exec(fmt.Sprintf(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id, duration_seconds)
SELECT toUUID('%s'), concat('big-', toString(number)), 'success', toDateTime64('2026-01-02 01:00:00', 3, 'UTC') + number, toDateTime64('2026-01-09 00:00:00', 3, 'UTC'), '%s', toFloat64(number %% 1000)
FROM numbers(%d)`, repo, org, n))
	e.exec(`INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, failure_count, success_rate, failure_rate, rerun_rate, org_id, computed_at) VALUES (?, ?, ?, ?, 0, 1, 0, 0, ?, ?)`,
		repo, day1, uint32(n), uint32(n), org, poolSynced)
}

type estimateRow struct {
	table       string
	rows, marks uint64
}

// explainEstimate runs EXPLAIN ESTIMATE of a compiled statement on the admin connection (the query client refuses an
// EXPLAIN), with its bindings as server-side parameters.
func (e *ownersEnv) explainEstimate(q compiledQuery) []estimateRow {
	e.t.Helper()
	params := stdclickhouse.Parameters{}
	for _, b := range q.bindings {
		switch v := b.Value.(type) {
		case string:
			params[b.Name] = v
		case []string:
			quoted := make([]string, len(v))
			for i, s := range v {
				quoted[i] = "'" + strings.ReplaceAll(s, "'", "\\'") + "'"
			}
			params[b.Name] = "[" + strings.Join(quoted, ",") + "]"
		case time.Time:
			params[b.Name] = v.UTC().Format("2006-01-02 15:04:05")
		default:
			params[b.Name] = fmt.Sprint(v)
		}
	}
	rows, err := e.admin.Query(stdclickhouse.Context(e.ctx, stdclickhouse.WithParameters(params)), "EXPLAIN ESTIMATE "+q.sql)
	if err != nil {
		e.t.Fatalf("EXPLAIN ESTIMATE: %v", err)
	}
	defer rows.Close()
	var out []estimateRow
	for rows.Next() {
		var database, table string
		var parts, rowCount, marks uint64
		if err := rows.Scan(&database, &table, &parts, &rowCount, &marks); err != nil {
			e.t.Fatalf("scan estimate: %v", err)
		}
		out = append(out, estimateRow{table: table, rows: rowCount, marks: marks})
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// B1: the estimated read of a pooled statement. The raw table is keyed by org_id first, so the estimate of a small
// organization does not hold a big one's rows, and the whole statement is far under the bound. The rows per table
// are logged as the measured basis of the bound.
func TestTestopsPooledReadEstimateStaysInTheOrganizationsKeyRange(t *testing.T) {
	e := startOwnersEnv(t)
	const small, big = "org-13858-small", "org-13858-big"
	const repoSmall, repoBig = "13858000-0000-4000-8000-0000000020a1", "13858000-0000-4000-8000-0000000020b1"
	e.repo(small, repoSmall, "acme/small")
	e.owner(small, "team-a", repoSmall, "acme/small", 1, openFrom(), nil)
	e.durations(small, repoSmall, day1, 10, 20, 30)
	e.write(small, day1, repoSmall)
	const bigRuns = 200_000
	e.repo(big, repoBig, "acme/big")
	e.bigOrgRuns(big, repoBig, bigRuns)
	e.exec(`OPTIMIZE TABLE ci_pipeline_runs FINAL`)

	req := TimeseriesRequest{Dimension: DimensionTeam, Measure: MeasurePipelineDurationP95, Interval: BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"), EndDate: mustDate(t, "2026-01-08")}
	team, err := CompileTimeseries(req, small, 30, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	orgQ, ok, err := CompileOrgTimeseries(req, small, 30, false, teamScope("team-a"))
	if err != nil || !ok {
		t.Fatalf("org: ok=%v err=%v", ok, err)
	}
	estimate := func(phase string, wantRaw func(raw uint64) bool, want string) {
		for name, q := range map[string]compiledQuery{"TEAM": team, "ORG": orgQ} {
			var total, raw uint64
			for _, row := range e.explainEstimate(q) {
				total += row.rows
				if row.table == "ci_pipeline_runs" {
					raw += row.rows
				}
				t.Logf("B1 %s %s estimate: table %s rows %d marks %d", phase, name, row.table, row.rows, row.marks)
			}
			if !wantRaw(raw) {
				t.Fatalf("B1 %s %s: ci_pipeline_runs estimate %d rows, want %s", phase, name, raw, want)
			}
			if total >= TestopsPooledMaxRowsToRead {
				t.Fatalf("B1 %s %s: estimate %d rows is over the bound %d", phase, name, total, TestopsPooledMaxRowsToRead)
			}
		}
	}
	// The writer stored the day's samples with its row (CHAOS-14025): the raw read is only the legacy arm, still in the
	// organization's key range.
	estimate("stored", func(raw uint64) bool { return raw < bigRuns },
		fmt.Sprintf("under the other organization's %d rows (org_id-first key)", bigRuns))
	// A newest row without samples (written before migration 161) reads the raw runs, in the organization's key range.
	e.exec(`INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, failure_count, success_rate, failure_rate, rerun_rate, org_id, computed_at) VALUES (?, ?, 3, 3, 0, 1, 0, 0, ?, now() + INTERVAL 1 HOUR)`,
		repoSmall, day1, small)
	estimate("legacy", func(raw uint64) bool { return raw > 0 && raw < bigRuns },
		fmt.Sprintf("> 0 and under the other organization's %d rows (org_id-first key)", bigRuns))
}

// B2: a pooled read past the row bound is refused as a PooledReadBoundError (ClickHouse TOO_MANY_ROWS), never served in
// part; under the bound the same read is served. The bound counts the rows of the statement, the joined tables included:
// the cell finds the refusal point against the raw rows of the organization.
func TestTestopsPooledReadPastTheRowBoundIsRefused(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13858-bound"
	const repo = "13858000-0000-4000-8000-0000000021a1"
	e.repo(org, repo, "acme/bound")
	e.owner(org, "team-a", repo, "acme/bound", 1, openFrom(), nil)
	const runs = 50_000
	e.bigOrgRuns(org, repo, runs)
	saved := TestopsPooledMaxRowsToRead
	t.Cleanup(func() { TestopsPooledMaxRowsToRead = saved })

	served := func(bound uint64, measure model.MeasureInput, filters *model.FilterInput) error {
		TestopsPooledMaxRowsToRead = bound
		_, err := resolveOneTimeseries(e.ctx, e.client, org, tsInput(model.DimensionInputTeam, measure), false, filters, true)
		return err
	}
	if err := served(saved, model.MeasureInputPipelineDurationP95, nil); err != nil {
		t.Fatalf("B2 under the default bound: %v", err)
	}
	for _, m := range []model.MeasureInput{model.MeasureInputPipelineDurationP95, model.MeasureInputPipelineQueueTime} {
		err := served(runs/2, m, nil)
		var refused *PooledReadBoundError
		if !errors.As(err, &refused) || refused.Code != 158 || refused.Reason() != "testops_pooled_read_bound" {
			t.Fatalf("B2 %s with a bound of half the raw rows: err %v, want a PooledReadBoundError of code 158", m, err)
		}
		if !strings.Contains(err.Error(), "is unavailable") {
			t.Fatalf("B2 %s: %q does not say unavailable", m, err)
		}
		// The ORG statement alone (scoped) is refused too.
		if err := served(runs/2, m, teamScope("team-a")); !errors.As(err, &refused) {
			t.Fatalf("B2 %s scoped: err %v, want a refusal", m, err)
		}
	}
	// Where the refusal starts: the smallest bound that serves, against the raw rows.
	low, high := uint64(1), uint64(4*runs)
	if served(high, model.MeasureInputPipelineDurationP95, nil) != nil {
		t.Fatalf("B2: a bound of %d does not serve", high)
	}
	for low < high {
		mid := (low + high) / 2
		if served(mid, model.MeasureInputPipelineDurationP95, nil) == nil {
			high = mid
		} else {
			low = mid + 1
		}
	}
	t.Logf("B2: smallest serving bound %d rows for %d raw rows of the organization (the remainder is the daily, ownership and catalog rows of the statement)", low, runs)
	if low < runs {
		t.Fatalf("B2: a bound of %d under the %d raw rows serves: the bound does not count the raw read", low, runs)
	}
	// A rate of the same batch is not a pooled read: no row bound.
	if err := served(1, model.MeasureInputPipelineSuccessRate, nil); err != nil {
		t.Fatalf("B2: the success rate with the pooled bound at 1: %v", err)
	}
}
