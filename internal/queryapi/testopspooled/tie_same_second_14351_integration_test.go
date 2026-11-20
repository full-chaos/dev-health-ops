//go:build integration

package testopspooled

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Two stored versions of one repository day written in the same computed_at second give one newest row from
// NewestStoredRows whatever the insert order across separate statements (same-key rows of ONE insert block collapse
// at insert; eight days, so a tiebreak that ignores computed_at cannot pass the control by luck); a version from a newer second still wins over the tiebreak. The versions differ in pooled_samples_stored
// and in a named column. CHAOS-14351.
func TestSameSecondVersionsGiveOneNewestStoredRow(t *testing.T) {
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
	const table = "testops_test_metrics_daily"
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES `+table); err != nil {
		t.Fatal(err)
	}
	const org = "org-14351"
	const repo = "14351000-0000-4000-8000-0000000000a1"
	const same = "2026-11-19 12:00:00"
	row := func(stored int, total int, at string) string {
		return `SELECT toUUID('` + repo + `'), '` + org + `', toDate('2026-11-10') + number, toUInt8(` + itoa(stored) + `), toUInt32(` + itoa(total) + `), toDateTime('` + at + `', 'UTC') FROM numbers(8)`
	}
	scenario := func(statements ...string) [2]uint64 {
		t.Helper()
		if err := conn.Exec(ctx, `TRUNCATE TABLE `+table); err != nil {
			t.Fatal(err)
		}
		for _, v := range statements {
			if err := conn.Exec(ctx, `INSERT INTO `+table+` (repo_id, org_id, day, pooled_samples_stored, total_cases, computed_at) `+v); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := conn.Query(ctx, `SELECT pooled_samples_stored, total_cases FROM `+NewestStoredRows(table, "total_cases"),
			clickhouse.Named("org_id", org), clickhouse.Named("start_date", "2026-11-01"), clickhouse.Named("end_date", "2026-12-01"))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var sum [2]uint64
		n := 0
		for rows.Next() {
			var stored uint8
			var total uint32
			if err := rows.Scan(&stored, &total); err != nil {
				t.Fatal(err)
			}
			sum[0] += uint64(stored)
			sum[1] += uint64(total)
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if n != 8 {
			t.Fatalf("newest rows = %d, want 8 (one per day)", n)
		}
		return sum
	}
	aFirst := scenario(row(1, 11, same), row(0, 22, same))
	bFirst := scenario(row(0, 22, same), row(1, 11, same))
	if aFirst != bFirst {
		t.Errorf("same rows, same second, newest: A inserted first=%v, B inserted first=%v; want one row", aFirst, bFirst)
	}
	if got := scenario(row(1, 11, same), row(0, 22, "2026-11-19 12:00:01")); got != [2]uint64{0, 8 * 22} {
		t.Errorf("older A + newer B gave %v: the newer second must win over the tiebreak", got)
	}
	if got := scenario(row(0, 22, same), row(1, 11, "2026-11-19 12:00:01")); got != [2]uint64{8, 8 * 11} {
		t.Errorf("older B + newer A gave %v: the newer second must win over the tiebreak", got)
	}
}

func itoa(n int) string {
	switch n {
	case 0:
		return "0"
	case 1:
		return "1"
	case 11:
		return "11"
	}
	return "22"
}
