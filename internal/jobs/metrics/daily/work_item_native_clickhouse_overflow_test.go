package daily

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// TestWorkItemWritersRefuseUInt32OverflowInsteadOfWrapping closes the class
// team-lead ruled on 2026-09-01 for the file-hotspots port: a Go-computed int
// narrowed into a fixed-width ClickHouse column by a bare uint32(...) does not
// fail, it WRAPS. 4_294_967_296 becomes 0 and -1 becomes 4_294_967_295, so the
// partition is written with a plausible-looking wrong number rather than
// refused.
//
// Python cannot do this: clickhouse_connect's encoder raises a DataError
// narrowing an out-of-range int into a UInt32 column. Refusing here is
// therefore fidelity-correct, not merely defensive -- and because these two
// families are post_bridge, a refusal is also the only honest outcome.
//
// The counters are asserted through the WRITERS rather than through the range
// helper alone: a helper test proves the helper works, not that the writer
// calls it. The parity corpus cannot reach this at all, since it exercises
// computation and never records batch arguments.
func TestWorkItemWritersRefuseUInt32OverflowInsteadOfWrapping(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	// A variable, not a const: `uint32(constant)` is rejected at COMPILE time
	// when the constant does not fit, which is exactly the safety the runtime
	// conversion does not give us -- and is the whole point of this test.
	overflow := math.MaxUint32 + 1

	for _, testCase := range []struct {
		name  string
		write func(conn workItemBatchConn) (int, error)
	}{
		{"work_item_metrics_daily", func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemMetricsDaily(ctx, conn, "org-a", day,
				[]workitemmetrics.MetricsDailyRow{{
					Day: day, Provider: "github", WorkScopeID: "acme/platform",
					TeamID: "team-a", TeamName: "Core", ItemsStarted: overflow,
				}}, computedAt)
		}},
		{"work_item_user_metrics_daily", func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemUserMetricsDaily(ctx, conn, "org-a", day,
				[]workitemmetrics.UserMetricsDailyRow{{
					Day: day, Provider: "github", WorkScopeID: "acme/platform",
					UserIdentity: "alice", TeamID: "team-a", TeamName: "Core",
					ItemsCompleted: overflow,
				}}, computedAt)
		}},
		{"estimate_coverage_metrics_daily", func(conn workItemBatchConn) (int, error) {
			return WriteEstimateCoverageMetricsDaily(ctx, conn, "org-a", day,
				[]workitemmetrics.EstimateCoverageRow{{
					Day: day, Provider: "github", WorkScopeID: "acme/platform",
					TeamID: "team-a", TeamName: "Core", BacklogSize: overflow,
				}}, computedAt)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			batch := &recordingBatch{}
			written, err := testCase.write(&recordingBatchConn{batch: batch})
			if !errors.Is(err, ErrInvalidState) {
				t.Fatalf("writing %d into a UInt32 column returned (%d, %v), want ErrInvalidState -- a bare uint32() cast wraps it to %d instead",
					overflow, written, err, uint32(overflow))
			}
			if batch.sent {
				t.Fatalf("%s: batch was SENT despite an out-of-range counter; the refusal must happen before the write lands", testCase.name)
			}
		})
	}
}

// TestWorkItemWritersRefuseNegativeCounters pins the other half of the range.
// A negative int does not wrap to something implausible -- -1 becomes
// 4_294_967_295, which reads as a real (enormous) count rather than an error.
func TestWorkItemWritersRefuseNegativeCounters(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	batch := &recordingBatch{}
	_, err := WriteWorkItemMetricsDaily(ctx, &recordingBatchConn{batch: batch}, "org-a", day,
		[]workitemmetrics.MetricsDailyRow{{
			Day: day, Provider: "github", WorkScopeID: "acme/platform",
			TeamID: "team-a", TeamName: "Core", WIPCountEndOfDay: -1,
		}}, day)
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("writing -1 into a UInt32 column returned %v, want ErrInvalidState -- a bare cast turns it into %d", err, uint32(4294967295))
	}
}
