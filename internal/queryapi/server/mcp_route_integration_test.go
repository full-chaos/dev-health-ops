//go:build integration

package server

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestMCPClickHouseCeilingAgainstARealClickHouse is CHAOS-7091's acceptance
// against a real engine: the production MCP client sends the class's
// ceilings (read back from system.settings through the REAL constructor), a
// read over a ceiling comes back as ClickHouse's own TOO_MANY_BYTES
// exception that the class classifies as a typed bytes_ceiling refusal --
// through the same observed client the MCP resolvers use, with the error
// swallowed the way a resolver could swallow it -- and a read just under the
// ceiling serves every row.
//
// The over/under cases lower MaxBytesToRead on a copy of the production
// options (the production 4 GiB is not a size a test seeds): the property
// under test is the mechanism -- a real 307 reaches mcpBudgetReason through
// dev-health-go's error wrapping -- and the readback subtest pins the
// production value itself.
func TestMCPClickHouseCeilingAgainstARealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() {
		if ch != nil && ch.Container != nil {
			_ = ch.Container.Terminate(context.Background())
		}
	}()

	t.Run("production_client_sends_the_class_ceilings", func(t *testing.T) {
		client, err := newMCPClickHouseClient(ch.URI)
		if err != nil {
			t.Fatalf("newMCPClickHouseClient: %v", err)
		}
		defer func() { _ = client.Close() }()
		// max_execution_time is NOT read back here: clickhouse-go derives the
		// server's value from the query's context deadline (10 s configured
		// reads back as 14), so the ceiling that fires is the client-side
		// deadline -- pinned by the time_ceiling subtest below instead.
		for setting, want := range map[string]string{
			"max_bytes_to_read": "4294967296",
		} {
			rows, err := client.Query(ctx, "SELECT value FROM system.settings WHERE name = {name:String}", []clickhouse.Binding{{Name: "name", Value: setting}})
			if err != nil {
				t.Fatalf("read %s: %v", setting, err)
			}
			var got string
			if !rows.Next() {
				t.Fatalf("system.settings has no row for %s", setting)
			}
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("scan %s: %v", setting, err)
			}
			_ = rows.Close()
			if got != want {
				t.Fatalf("%s reached ClickHouse as %q, want %q", setting, got, want)
			}
		}
	})

	t.Run("over_the_time_ceiling_is_a_typed_time_refusal", func(t *testing.T) {
		opts := newMCPClickHouseOptions(ch.URI)
		opts.MaxExecutionTime = 1 // the mechanism, not the production 10 s
		client, err := clickhouse.NewClickHouseQueryClientWithOptions(opts)
		if err != nil {
			t.Fatalf("construct ClickHouse client: %v", err)
		}
		defer func() { _ = client.Close() }()
		obs := &mcpObservation{}
		observed := mcpObservedClient{next: client}
		started := time.Now()
		rows, err := observed.Query(context.WithValue(ctx, mcpObservationKey{}, obs), "SELECT sleepEachRow(0.5) FROM numbers(8) SETTINGS max_block_size = 1", nil)
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			_ = rows.Close()
		}
		if got := obs.budgetReason(); got != mcpReasonTimeCeiling {
			t.Fatalf("budget reason %q after %s (error: %v), want %q", got, time.Since(started), err, mcpReasonTimeCeiling)
		}
	})

	const (
		rowCount   = 400
		payload    = 100 * 1024 // ~40 MiB of real MergeTree column data
		totalBytes = rowCount * payload
	)
	seedByteBudgetProbeTable(t, ctx, ch.URI, rowCount, payload)

	readProbe := func(t *testing.T, maxBytes uint64) (int, *mcpObservation) {
		t.Helper()
		opts := newMCPClickHouseOptions(ch.URI)
		opts.MaxBytesToRead = &maxBytes
		client, err := clickhouse.NewClickHouseQueryClientWithOptions(opts)
		if err != nil {
			t.Fatalf("construct ClickHouse client: %v", err)
		}
		defer func() { _ = client.Close() }()
		obs := &mcpObservation{}
		observed := mcpObservedClient{next: client}
		rows, err := observed.Query(context.WithValue(ctx, mcpObservationKey{}, obs), "SELECT id, payload FROM byte_budget_probe", nil)
		if err != nil {
			return 0, obs
		}
		n := 0
		for rows.Next() {
			var id uint64
			var body string
			if rows.Scan(&id, &body) != nil {
				break
			}
			n++
		}
		_ = rows.Err() // swallowed, as a resolver could
		_ = rows.Close()
		return n, obs
	}

	// max_result_rows: a real server answers code 396 (not 158) -- r1 on
	// #3425. The production cap is queryRouteMaxResultRows; lowered here to
	// trip it on the 400-row probe.
	t.Run("over_the_result_row_ceiling_is_a_typed_rows_refusal", func(t *testing.T) {
		opts := newMCPClickHouseOptions(ch.URI)
		oneRow := uint(1)
		opts.MaxResultRows = &oneRow
		client, err := clickhouse.NewClickHouseQueryClientWithOptions(opts)
		if err != nil {
			t.Fatalf("construct ClickHouse client: %v", err)
		}
		defer func() { _ = client.Close() }()
		obs := &mcpObservation{}
		observed := mcpObservedClient{next: client}
		rows, err := observed.Query(context.WithValue(ctx, mcpObservationKey{}, obs), "SELECT id FROM byte_budget_probe", nil)
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err() // swallowed below, as a resolver could
			_ = rows.Close()
		}
		if got := obs.budgetReason(); got != mcpReasonRowsCeiling {
			t.Fatalf("budget reason %q (error: %v), want %q", got, err, mcpReasonRowsCeiling)
		}
	})

	t.Run("over_the_ceiling_is_a_typed_bytes_refusal", func(t *testing.T) {
		n, obs := readProbe(t, 8<<20) // 8 MiB < ~40 MiB
		if got := obs.budgetReason(); got != mcpReasonBytesCeiling {
			t.Fatalf("budget reason %q after reading %d rows, want %q (ClickHouse TOO_MANY_BYTES classified)", got, n, mcpReasonBytesCeiling)
		}
	})

	t.Run("just_under_the_ceiling_serves_every_row", func(t *testing.T) {
		n, obs := readProbe(t, uint64(totalBytes)+1<<20) // the payload plus 1 MiB
		if got := obs.budgetReason(); got != "" {
			t.Fatalf("budget reason %q under the ceiling, want none", got)
		}
		if n != rowCount {
			t.Fatalf("read %d rows under the ceiling, want %d", n, rowCount)
		}
	})
}
