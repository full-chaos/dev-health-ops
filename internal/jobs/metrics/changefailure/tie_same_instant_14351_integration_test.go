//go:build integration

package changefailure_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Two stored versions of one repository day written with the same computed_at give one row from LatestRowsSQL whatever
// the insert order across separate statements (same-key rows of ONE insert block collapse at insert); a version from a
// newer computed_at still wins over the tiebreak. CHAOS-14351.
func TestSameInstantVersionsGiveOneLatestRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	opts, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES `+changefailure.Table); err != nil {
		t.Fatal(err)
	}
	const org = "org-14351"
	const repo = "14351000-0000-4000-8000-0000000000a1"
	const same = "2026-11-19 12:00:00.000"
	row := func(deployments int, at string) string {
		d := "4"
		if deployments == 9 {
			d = "9"
		}
		return `SELECT '` + org + `', toUUID('` + repo + `'), toDate('2026-11-17'), toUInt32(` + d + `), toDateTime64('` + at + `', 3, 'UTC')`
	}
	scenario := func(statements ...string) uint64 {
		t.Helper()
		if err := conn.Exec(ctx, `TRUNCATE TABLE `+changefailure.Table); err != nil {
			t.Fatal(err)
		}
		for _, v := range statements {
			if err := conn.Exec(ctx, `INSERT INTO `+changefailure.Table+` (org_id, repo_id, day, deployments_count, computed_at) `+v); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := conn.Query(ctx, `SELECT deployments_count FROM `+changefailure.LatestRowsSQL("start", "end", ""),
			clickhouse.Named("org_id", org), clickhouse.Named("start", "2026-11-01"), clickhouse.Named("end", "2026-12-01"))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []uint64
		for rows.Next() {
			var n uint32
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			got = append(got, uint64(n))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("latest rows = %v, want one", got)
		}
		return got[0]
	}
	nineFirst := scenario(row(9, same), row(4, same))
	fourFirst := scenario(row(4, same), row(9, same))
	if nineFirst != fourFirst {
		t.Errorf("same rows, same computed_at, latest: 9 inserted first=%v, 4 inserted first=%v; want one value", nineFirst, fourFirst)
	}
	if got := scenario(row(9, same), row(4, "2026-11-19 12:00:00.001")); got != 4 {
		t.Errorf("older 9 + newer 4 gave %v: the newer computed_at must win over the tiebreak", got)
	}
	if got := scenario(row(4, same), row(9, "2026-11-19 12:00:00.001")); got != 9 {
		t.Errorf("older 4 + newer 9 gave %v: the newer computed_at must win over the tiebreak", got)
	}
}
