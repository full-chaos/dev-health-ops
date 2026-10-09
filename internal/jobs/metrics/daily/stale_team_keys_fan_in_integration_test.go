//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// endStaleKeyRun does what the finalize of a run does before its families:
// the run-level retraction of the stale team keys (RunStaleKeyRetractor), for
// the repositories of every partition of the run. It returns the number of
// rows of zeros it wrote.
//
// The acceptance tests of the rule call it after the partitions of each run
// and use no other symbol of the rule. To run them on a tree that has no
// run-level retraction, this one file is replaced by a function of the same
// name that does nothing.
func endStaleKeyRun(t *testing.T, ctx context.Context, conn driver.Conn, run Run, clock time.Time) int {
	t.Helper()
	retractor, err := NewRunStaleKeyRetractor(conn)
	if err != nil {
		t.Fatal(err)
	}
	retractor.nowUTC = func() time.Time { return clock }
	written, err := retractor.RetractStaleKeys(ctx, run)
	if err != nil {
		t.Fatalf("the run-level retraction at %s: %v", clock.Format(time.RFC3339), err)
	}
	return written
}
