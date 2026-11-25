//go:build integration

package analytics

import (
	"fmt"
	"sort"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// CHAOS-14616: the pooled live gate (testopspooled.LiveRepoDays) keeps a repository-day whose newest row is a measurement.
// A live row and a retraction row of one repository-day at ONE computed_at second are a tie of the newest row: the rule is
// that the day is excluded only when every row at the newest stamp is a retraction, so a mixed tie keeps the day, on every
// query. The cell reads the gate 20 times per group and table, by count, and wants the same answer every read.

const (
	tieGateKeys  = 200
	tieGateReads = 20
	tieGateStamp = "2026-01-05 00:00:00"
	tieGateLater = "2026-01-06 00:00:00"
)

// The tables of the seven LiveRepoDays call sites (repo_metrics_daily is read at two of them).
var tieGateTables = []string{
	"repo_metrics_daily", "deploy_metrics_daily", "incident_metrics_daily",
	"testops_pipeline_metrics_daily", "testops_test_metrics_daily", "cicd_metrics_daily",
}

// tieGateInsert writes tieGateKeys rows in one statement (one part): the repository ids are the numbers 0..tieGateKeys-1
// in a fixed synthetic prefix, all of one day inside the read window.
func tieGateInsert(e *ownersEnv, table, org string, retracted int, computedAt string) {
	e.exec(fmt.Sprintf(`INSERT INTO %s (org_id, repo_id, day, computed_at, retracted)
SELECT ?, toUUID(concat('f0f0f0f0-0000-4000-8000-', lpad(hex(number), 12, '0'))), toDate('2026-01-02'), toDateTime(?, 'UTC'), %d
FROM numbers(%d)`, table, retracted, tieGateKeys), org, computedAt)
}

func TestPooledLiveGateSameSecondTieIsDeterministic(t *testing.T) {
	e := startOwnersEnv(t)
	for _, table := range tieGateTables {
		e.exec("SYSTEM STOP MERGES " + table)
	}
	groups := []struct {
		name string
		want uint64 // the repository-days the gate keeps, on every read
		// stamps is the number of distinct computed_at values of the group's rows; low and high the retracted values present.
		stamps    uint64
		low, high uint8
		insert    func(table, org string)
	}{
		{"mixed tie, live row first", tieGateKeys, 1, 0, 1, func(table, org string) {
			tieGateInsert(e, table, org, 0, tieGateStamp)
			tieGateInsert(e, table, org, 1, tieGateStamp)
		}},
		{"mixed tie, retraction first", tieGateKeys, 1, 0, 1, func(table, org string) {
			tieGateInsert(e, table, org, 1, tieGateStamp)
			tieGateInsert(e, table, org, 0, tieGateStamp)
		}},
		{"unanimous retraction at one stamp", 0, 1, 1, 1, func(table, org string) {
			tieGateInsert(e, table, org, 1, tieGateStamp)
			tieGateInsert(e, table, org, 1, tieGateStamp)
		}},
		{"live row newer than a retraction", tieGateKeys, 2, 0, 1, func(table, org string) {
			tieGateInsert(e, table, org, 1, tieGateStamp)
			tieGateInsert(e, table, org, 0, tieGateLater)
		}},
		{"retraction newer than a live row", 0, 2, 0, 1, func(table, org string) {
			tieGateInsert(e, table, org, 0, tieGateStamp)
			tieGateInsert(e, table, org, 1, tieGateLater)
		}},
	}
	for _, table := range tieGateTables {
		t.Run(table, func(t *testing.T) {
			for index, group := range groups {
				org := fmt.Sprintf("f0f0f0f0-0000-4000-8000-00000014616%d", index)
				group.insert(table, org)
				var rows, stamps uint64
				var low, high uint8
				if err := e.admin.QueryRow(e.ctx, `SELECT count(), uniqExact(computed_at), min(retracted), max(retracted) FROM `+table+` WHERE org_id = ?`, org).
					Scan(&rows, &stamps, &low, &high); err != nil {
					t.Fatal(err)
				}
				if rows != 2*tieGateKeys || stamps != group.stamps || low != group.low || high != group.high {
					t.Fatalf("control %s: %d rows at %d stamp(s), retracted %d..%d; want %d rows at %d stamp(s), retracted %d..%d",
						group.name, rows, stamps, low, high, 2*tieGateKeys, group.stamps, group.low, group.high)
				}
				params := []any{stdclickhouse.Named("org_id", org), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-08")}
				query := `SELECT count() FROM (SELECT DISTINCT repo_id, day FROM ` + table + `
					WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}) WHERE ` + testopspooled.LiveRepoDays(table)
				var served []uint64
				seen := map[uint64]int{}
				for read := 0; read < tieGateReads; read++ {
					var kept uint64
					if err := e.admin.QueryRow(e.ctx, query, params...).Scan(&kept); err != nil {
						t.Fatalf("%v\n%s", err, query)
					}
					served = append(served, kept)
					seen[kept]++
				}
				distinct := make([]uint64, 0, len(seen))
				for kept := range seen {
					distinct = append(distinct, kept)
				}
				sort.Slice(distinct, func(i, j int) bool { return distinct[i] < distinct[j] })
				t.Logf("CELL14616 %s / %s: the gate kept %v of %d repository-days over %d reads (want %d every read); counts by read %v",
					table, group.name, distinct, tieGateKeys, tieGateReads, group.want, served)
				if len(seen) != 1 || seen[group.want] != tieGateReads {
					t.Errorf("%s / %s: the gate kept %v repository-days over %d reads, want %d on every read", table, group.name, distinct, tieGateReads, group.want)
				}
			}
		})
	}
}
