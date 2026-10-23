package remaining

import (
	"context"
	"errors"
	"strings"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// engineFormsConn answers the schema probes of verifyCapacitySchema and
// verifyReleaseImpactSchema with one engine / engine_full pair, so the guards are
// driven over the self-hosted, Replicated and ClickHouse Cloud Shared spellings
// without a server.
type engineFormsConn struct {
	driverConnStub
	engine     string
	engineFull string
}

func (conn engineFormsConn) Query(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
	if strings.Contains(query, "system.columns") {
		return &stubColumnRows{names: allCapacityRequiredColumns()}, nil
	}
	return nil, errStubExhausted
}

func (conn engineFormsConn) QueryRow(_ context.Context, query string, args ...any) chdriver.Row {
	switch {
	case strings.Contains(query, "sorting_key"):
		if queriedTable(args) == "release_impact_daily" {
			return stubRow{value: releaseImpactExpectedSortingKey}
		}
		return stubRow{value: strings.Join(capacityTableRequirements["work_item_metrics_daily"].sortingKey, ", ")}
	case strings.Contains(query, "engine_full"):
		return stubRow{value: conn.engineFull}
	case strings.Contains(query, "SELECT engine"):
		return stubRow{value: conn.engine}
	}
	return stubRow{err: errStubExhausted}
}

const cloudPath = "'/clickhouse/tables/{uuid}/{shard}', '{replica}'"

func TestSchemaGuardsReadEveryEngineSpelling(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		engine     string
		engineFull string
		accepted   bool
	}{
		{"self-hosted", "ReplacingMergeTree", "ReplacingMergeTree(computed_at) ORDER BY (org_id, day)", true},
		{"self-hosted backticked", "ReplacingMergeTree", "ReplacingMergeTree(`computed_at`) ORDER BY (org_id, day)", true},
		{"replicated", "ReplicatedReplacingMergeTree", "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/t', '{replica}', computed_at) ORDER BY (org_id, day)", true},
		{"cloud shared", "SharedReplacingMergeTree", "SharedReplacingMergeTree(" + cloudPath + ", computed_at) ORDER BY (org_id, day)", true},
		{"cloud shared backticked", "SharedReplacingMergeTree", "SharedReplacingMergeTree(" + cloudPath + ", `computed_at`) ORDER BY (org_id, day)", true},
		{"cloud shared wrong version column", "SharedReplacingMergeTree", "SharedReplacingMergeTree(" + cloudPath + ", updated_at) ORDER BY (org_id, day)", false},
		{"cloud shared no version column", "SharedReplacingMergeTree", "SharedReplacingMergeTree(" + cloudPath + ") ORDER BY (org_id, day)", false},
		{"cloud shared plain MergeTree", "SharedMergeTree", "SharedMergeTree(" + cloudPath + ") ORDER BY (org_id, day)", false},
		{"cloud shared aggregating", "SharedAggregatingMergeTree", "SharedAggregatingMergeTree(" + cloudPath + ") ORDER BY (org_id, day)", false},
		{"cloud shared without replication arguments", "SharedReplacingMergeTree", "SharedReplacingMergeTree(computed_at) ORDER BY (org_id, day)", false},
		{"unparseable engine_full", "SharedReplacingMergeTree", "SharedReplacingMergeTree('/p', '{replica}', computed_at ORDER BY x", false},
		{"not a MergeTree", "View", "View", false},
	}
	for _, tc := range cases {
		conn := engineFormsConn{engine: tc.engine, engineFull: tc.engineFull}
		t.Run("capacity/"+tc.name, func(t *testing.T) {
			t.Parallel()
			err := verifyCapacitySchema(context.Background(), conn)
			if tc.accepted && err != nil {
				t.Fatalf("verifyCapacitySchema refused an accepted engine: %v", err)
			}
			if !tc.accepted && !errors.Is(err, ErrCapacitySchemaIncompatible) {
				t.Fatalf("verifyCapacitySchema = %v, want ErrCapacitySchemaIncompatible", err)
			}
		})
		t.Run("release_impact/"+tc.name, func(t *testing.T) {
			t.Parallel()
			err := verifyReleaseImpactSchema(context.Background(), conn)
			if tc.accepted && err != nil {
				t.Fatalf("verifyReleaseImpactSchema refused an accepted engine: %v", err)
			}
			if !tc.accepted && !errors.Is(err, ErrReleaseImpactSchemaIncompatible) {
				t.Fatalf("verifyReleaseImpactSchema = %v, want ErrReleaseImpactSchemaIncompatible", err)
			}
		})
	}
}
