package pgmigrate

import (
	"regexp"
	"strings"
	"testing"
)

var (
	auditTableLock   = regexp.MustCompile(`(?i)LOCK\s+TABLE\s+worker_operator_audits\s+IN\s+ACCESS\s+EXCLUSIVE\s+MODE\s*;`)
	auditActionCount = regexp.MustCompile(`(?i)SELECT\s+count\(\*\)[^;]*FROM\s+worker_operator_audits`)
)

// The down file of the marker-backfill audit action revision counts the rows that hold the action and then swaps the
// check. The table is locked before the count, so no audit insert can commit between the two and make the swap fail
// with a generic constraint error instead of the count.
func TestMarkerBackfillDownLocksTheAuditTableBeforeItCounts(t *testing.T) {
	down, err := LoadDownChain()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, file := range down {
		if strings.Contains(file.Name, "widen_worker_operator_audits_action_marker_backfill") {
			sql = file.SQL
		}
	}
	if sql == "" {
		t.Fatal("the down file of the marker backfill audit action revision is not in the chain")
	}
	lock := auditTableLock.FindStringIndex(sql)
	count := auditActionCount.FindStringIndex(sql)
	if lock == nil || count == nil || lock[0] > count[0] {
		t.Fatalf("down file must lock worker_operator_audits in ACCESS EXCLUSIVE MODE before the row count (lock %v, count %v):\n%s", lock, count, sql)
	}
}
