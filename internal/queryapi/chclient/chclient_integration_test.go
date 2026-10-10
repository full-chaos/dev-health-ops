//go:build integration

package chclient

import (
	"context"
	"errors"
	"fmt"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Executed against a real ClickHouse: what each bound looks like to BoundHit
// (round 94 F1: a time bound arrives as a wrapped context deadline, not as a
// ClickHouse exception).
func TestBoundHitClassifiesWhatTheRealClientReturns(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	run := func(t *testing.T, opts dhclickhouse.Options, statement string) error {
		t.Helper()
		client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		rows, err := client.Query(ctx, statement, nil)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
		}
		return rows.Err()
	}

	rowsOpts := Options(instance.URI)
	tenRows := uint(10)
	rowsOpts.MaxResultRows = &tenRows
	if err := run(t, rowsOpts, "SELECT number FROM numbers(50)"); err == nil {
		t.Fatal("a 50-row read under a 10-row bound did not fail")
	} else if bound, _ := BoundHit(err); bound != BoundRows {
		t.Errorf("row bound: BoundHit = %q for %T %v, want %q", bound, err, err, BoundRows)
	}

	// The deadline and the server's own timeout race: read it several times, every
	// answer must classify as the time bound.
	for attempt := 1; attempt <= 6; attempt++ {
		timeOpts := Options(instance.URI)
		timeOpts.MaxExecutionTime = 1
		err := run(t, timeOpts, "SELECT sleepEachRow(0.2) FROM numbers(30) SETTINGS max_block_size = 1")
		if err == nil {
			t.Fatal("a 6 second read under a 1 second execution bound did not fail")
		}
		if bound, _ := BoundHit(err); bound != BoundTime {
			var chain []string
			for e := err; e != nil; e = errors.Unwrap(e) {
				chain = append(chain, fmt.Sprintf("%T(%v)", e, e))
			}
			t.Errorf("attempt %d: time bound: BoundHit = %q, want %q; chain %v", attempt, bound, BoundTime, chain)
		}
	}
}
