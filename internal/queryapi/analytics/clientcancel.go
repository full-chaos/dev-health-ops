package analytics

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// A statement that ends because the CLIENT closed the request is not a failed
// query. The browser leaves a page while its queries are in flight, the
// request context is cancelled, ClickHouse stops the statement (code 735) and
// the read returns an error. Reported as a failure (an ERROR line, the
// failure counter) it reads as a broken query to the person on call, who then
// searches for a defect that is not there, and it hides the real failures of
// the same statement among the cancels.
//
// The test is the request context, not the form of the error: the error of a
// cancelled statement comes back as a driver error, a ClickHouse exception or
// a wrapped context error, by the moment the cancel landed. A context that
// ended because its DEADLINE passed is not a client cancel: that is a budget
// of this service, and it stays a failure.

var clientCancelledCounter = mustAnalyticsCounter(
	"devhealth_query_api_analytics_client_cancelled_total",
	"analytics statements that ended because the client closed the request, by phase",
)

// clientCancelled reports whether the request was cancelled by its client.
func clientCancelled(ctx context.Context) bool {
	return ctx != nil && errors.Is(ctx.Err(), context.Canceled)
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
// when the request was cancelled by its client, and says whether it did. A
// caller reports a failure only when this returns false.
func reportedAsClientCancel(ctx context.Context, phase string, attrs ...any) bool {
	if !clientCancelled(ctx) {
		return false
	}
	recordClientCancel(ctx, phase, attrs...)
	return true
}
