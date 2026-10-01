//go:build integration

package providersync

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// currentOperationalRows is the FROM source that yields one current row per
// (org_id, id) for the shape the table actually has: ReplacingMergeTree FINAL
// for contract 1, the revision-ordered LIMIT 1 BY read for contract 2 (the
// one internal/jobs/metrics/remaining's currentOperationalRowsSQL uses), where
// FINAL keeps one row per (org_id, id, source_revision, source_conflict_key)
// and so cannot tell a tombstone from the row it deletes.
func currentOperationalRows(ctx context.Context, t *testing.T, conn driver.Conn, table string) string {
	t.Helper()
	contract, err := (*operationalTableContracts)(nil).resolve(ctx, conn, table)
	if err != nil {
		t.Fatalf("resolve the contract of %s: %v", table, err)
	}
	if contract == operationalCurrentContract {
		return "(SELECT * FROM " + table + " ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1 BY org_id, id)"
	}
	return table + " FINAL"
}
