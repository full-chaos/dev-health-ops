package daily

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// TestopsReportSlotsRevivedCounter counts the report slots a test-day read found brought back after their retirement
// (CHAOS-14555): a later sync kept a report the planner had dropped, so the slot counts again while the cases of its retired
// attempts stay hidden. A recompute of the day counts the same slot again: the series counts reads that saw a revived slot.
const TestopsReportSlotsRevivedCounter = "dev_health_testops_report_slots_revived_total"

// testopsRevivedSlots is the revived-slot count of the day's runs of one repository list (the readers' scope).
var testopsRevivedSlots = testopspooled.RevivedSlots(testopsRuleScope)

// loadNativeTestopsRevivedSlots counts the repository-day's revived report slots and, when there is one, adds them to the counter
// and logs one Warn line with the count only.
func loadNativeTestopsRevivedSlots(ctx context.Context, conn testopsNativeConn, orgID string, repoID uuid.UUID, start, end time.Time) error {
	rows, err := conn.Query(ctx, testopsRevivedSlots, testopsSourceArgs(orgID, []uuid.UUID{repoID}, start, end)...)
	if err != nil {
		return fmt.Errorf("load native testops revived slots: %w", err)
	}
	defer rows.Close()
	var revived uint64
	for rows.Next() {
		if err := rows.Scan(&revived); err != nil {
			return fmt.Errorf("scan native testops revived slots: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate native testops revived slots: %w", err)
	}
	if revived == 0 {
		return nil
	}
	counter, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily").Int64Counter(
		TestopsReportSlotsRevivedCounter, metric.WithUnit("{slot}"),
		metric.WithDescription("report slots a testops test-day read found brought back after their retirement"))
	if err == nil {
		counter.Add(context.WithoutCancel(ctx), int64(revived))
	}
	slog.Warn("testops day read found report slots revived after their retirement; the cases of their retired attempts stay hidden",
		"slots", revived)
	return nil
}
