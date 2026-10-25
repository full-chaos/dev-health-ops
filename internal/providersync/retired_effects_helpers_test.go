package providersync

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var derivedTestDestinations = []string{
	"ai_attribution",
	"estimate_coverage_metrics_daily",
	"issue_type_metrics_daily",
	"work_item_cycle_times",
	"work_item_metrics_daily",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_user_metrics_daily",
}

// emptyEffectsFor builds one effect with no rows for each destination.
func emptyEffectsFor(t *testing.T, destinations []string) []EffectBatch {
	t.Helper()
	effects := make([]EffectBatch, 0, len(destinations))
	for _, destination := range destinations {
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, nil)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	return effects
}

// inertGitHubDerivedConn is a connection that fails on any use: a sink that
// reaches the store through it panics.
type inertGitHubDerivedConn struct{ driver.Conn }

// ownershipReasonWriteConn records whether a write reached the store.
type ownershipReasonWriteConn struct {
	driver.Conn
	batch *ownershipReasonWriteBatch
}

func (conn *ownershipReasonWriteConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if conn.batch == nil {
		conn.batch = &ownershipReasonWriteBatch{}
	}
	return conn.batch, nil
}

type ownershipReasonWriteBatch struct {
	driver.Batch
	Appended []string
}

func (batch *ownershipReasonWriteBatch) Append(args ...any) error {
	if len(args) >= 7 {
		if workItemID, ok := args[2].(string); ok {
			if source, ok := args[6].(string); ok {
				batch.Appended = append(batch.Appended, workItemID+"|"+source)
			}
		}
	}
	return nil
}
func (batch *ownershipReasonWriteBatch) Send() error  { return nil }
func (batch *ownershipReasonWriteBatch) Abort() error { return nil }
