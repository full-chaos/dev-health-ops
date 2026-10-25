//go:build integration

package providersync

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// assertSyncSinkRefusesDailyJobTable holds the contract of a work-items sync
// sink for one of the nine tables the daily job writes: the write and the
// readback are refused as an invalid configuration, and the table holds no row
// of the tenant after the refused write. The effect must carry rows: a refusal
// of an empty effect would prove nothing about the store.
func assertSyncSinkRefusesDailyJobTable(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	sink interface {
		EffectSink
		EffectReadback
	},
	claim Claim,
	effect EffectBatch,
) {
	t.Helper()
	if !slices.Contains(githubWorkItemDerivedDestinations, effect.Destination) {
		t.Fatalf("%s is not a table of the daily job", effect.Destination)
	}
	if len(effect.Rows) == 0 {
		t.Fatalf("%s: the effect has no row; the refusal would not show that the store stays empty", effect.Destination)
	}
	if err := sink.WriteEffect(ctx, claim, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("%s: the sync sink wrote a table of the daily job: error=%v", effect.Destination, err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
		t.Fatalf("%s: readback=%v error=%v want a refused conflict", effect.Destination, inspection, err)
	}
	var stored uint64
	// The destination is one of the nine fixed table names checked above.
	query := "SELECT count() FROM " + effect.Destination + " WHERE org_id = ?"
	if err := conn.QueryRow(ctx, query, claim.OrgID).Scan(&stored); err != nil {
		t.Fatalf("%s: count stored rows: %v", effect.Destination, err)
	}
	if stored != 0 {
		t.Fatalf("%s: %d rows of the tenant are stored after the refused write", effect.Destination, stored)
	}
}

func timePtr(value time.Time) *time.Time { return &value }

func floatPtr(value float64) *float64 { return &value }
