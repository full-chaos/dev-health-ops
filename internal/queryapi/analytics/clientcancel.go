package analytics

import (
	"context"
	"errors"
	"log/slog"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// A statement that ends because the CLIENT closed the request is not a failed
// query. The browser leaves a page while its queries are in flight, the
// request context is cancelled, ClickHouse stops the statement (code 735) and
// the read returns an error. Reported as a failure (an ERROR line, the
// failure counter) it reads as a broken query to the person on call, who then
// searches for a defect that is not there, and it hides the real failures of
// the same statement among the cancels.
//
// Two things must hold, never one alone:
//
//   - the request context ended with a cancel. A context that ended because
//     its DEADLINE passed is not a client cancel: that is a budget of this
//     service, and it stays a failure.
//   - the ERROR says the statement was cancelled: ClickHouse answered that the
//     query was cancelled (code 735 or 394), or the chain holds no ClickHouse
//     exception and holds the context's own cancel error. A statement that
//     failed for its own reason (an unknown table, a statement this process
//     refused before it sent it) is a failure also when the client has left
//     since: the cancel did not cause it, and it will fail again for the next
//     client.

var clientCancelledCounter = mustAnalyticsCounter(
	"devhealth_query_api_analytics_client_cancelled_total",
	"analytics statements that ended because the client closed the request, by phase",
)

// The ClickHouse codes of a statement that was cancelled.
const (
	clickHouseQueryWasCancelled         = 394
	clickHouseQueryWasCancelledByClient = 735
)

// clientCancelled reports whether err is the end of a statement that the
// client's cancel of the request ended.
func clientCancelled(ctx context.Context, err error) bool {
	if ctx == nil || err == nil || !errors.Is(ctx.Err(), context.Canceled) {
		return false
	}
	var exception *clickhousedriver.Exception
	if logging.ErrorAs(err, &exception) && exception != nil {
		return exception.Code == clickHouseQueryWasCancelledByClient || exception.Code == clickHouseQueryWasCancelled
	}
	return logging.ErrorClass(err) == logging.ErrorClassCanceled
}

// recordClientCancel is a package var for the reason recordDegradation is
// one: a test must be able to see that a cancel was reported as a cancel.
var recordClientCancel = defaultRecordClientCancel

// defaultRecordClientCancel reports one statement that the client's cancel
// ended: a counter, a span event and ONE line at INFO. attrs are the fields
// of the line (the organization, what was asked, the query id).
func defaultRecordClientCancel(ctx context.Context, phase string, attrs ...any) {
	clientCancelledCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("phase", phase)))
	trace.SpanFromContext(ctx).AddEvent("analytics.client_cancelled", trace.WithAttributes(attribute.String("phase", phase)))
	slog.InfoContext(ctx, "analytics: query cancelled by the client", append([]any{"phase", phase}, attrs...)...)
}

// reportedAsClientCancel reports the end of a statement as a client cancel
// when the client's cancel ended it (clientCancelled), and says whether it
// did. A caller reports a failure only when this returns false.
func reportedAsClientCancel(ctx context.Context, err error, phase string, attrs ...any) bool {
	if !clientCancelled(ctx, err) {
		return false
	}
	recordClientCancel(ctx, phase, attrs...)
	return true
}
