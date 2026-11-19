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
// client's cancel of the request ended: every leaf of the error tree is the
// context's cancel or a ClickHouse cancel exception. A joined error that also
// holds a deadline or any other failure is a failure, and so is a tree too
// large to read to its end.
func clientCancelled(ctx context.Context, err error) bool {
	if ctx == nil {
		return false
	}
	return clientCancelledAt(ctx.Err(), err)
}

// clientCancelledAt is clientCancelled for a read whose end was judged with
// the context state ctxErr seen when it ended, not the state at report time.
// An error that a read ended as a failure (endedAsFailure) stays one.
func clientCancelledAt(ctxErr, err error) bool {
	if err == nil || !errors.Is(ctxErr, context.Canceled) {
		return false
	}
	var failed *endedAsFailure
	if logging.ErrorAs(err, &failed) {
		return false
	}
	leaves, complete := logging.ErrorLeaves(err)
	if !complete || len(leaves) == 0 {
		return false
	}
	for _, leaf := range leaves {
		if leaf == context.Canceled {
			continue
		}
		var exception *clickhousedriver.Exception
		if !errors.As(leaf, &exception) || exception == nil ||
			(exception.Code != clickHouseQueryWasCancelledByClient && exception.Code != clickHouseQueryWasCancelled) {
			return false
		}
	}
	return true
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
// did. A caller reports a failure only when this returns false. An error a
// phase reported already (ClientCancelReported) is not reported again: one
// read shared by several phases is one report.
func reportedAsClientCancel(ctx context.Context, err error, phase string, attrs ...any) bool {
	return reportedAsClientCancelAt(ctx, ctx.Err(), err, phase, attrs...)
}

// reportedAsClientCancelAt is reportedAsClientCancel for a read that ended
// when the context state was ctxErr.
func reportedAsClientCancelAt(ctx context.Context, ctxErr, err error, phase string, attrs ...any) bool {
	if !clientCancelledAt(ctxErr, err) {
		return false
	}
	if !ClientCancelReported(err) {
		recordClientCancel(ctx, phase, attrs...)
	}
	return true
}

// reportedClientCancel marks an error whose client cancel a phase of this
// package reported already, so the caller that ends the request on it (the
// GraphQL presenter, a REST route) does not report it a second time.
type reportedClientCancel struct{ err error }

func (r *reportedClientCancel) Error() string { return r.err.Error() }
func (r *reportedClientCancel) Unwrap() error { return r.err }

// markedIfClientCancel reports err as a client cancel of phase when it is one
// and returns it marked as reported; any other err is returned as it is.
func markedIfClientCancel(ctx context.Context, err error, phase string, attrs ...any) error {
	return markedIfClientCancelAt(ctx, ctx.Err(), err, phase, attrs...)
}

// endedAsFailure marks an error of a read that ended as a failure while the
// request was still alive: a client that leaves afterwards does not turn it
// into a cancel (a server-side 394, for one).
type endedAsFailure struct{ err error }

func (f *endedAsFailure) Error() string { return f.err.Error() }
func (f *endedAsFailure) Unwrap() error { return f.err }

// markedIfClientCancelAt is markedIfClientCancel for a read that ended when
// the context state was ctxErr. Any other error comes back as it is, marked
// as a failure when the request has been cancelled since.
func markedIfClientCancelAt(ctx context.Context, ctxErr, err error, phase string, attrs ...any) error {
	if !reportedAsClientCancelAt(ctx, ctxErr, err, phase, attrs...) {
		if ctxErr == nil && ctx.Err() != nil {
			return &endedAsFailure{err: err}
		}
		return err
	}
	return &reportedClientCancel{err: err}
}

// ClientCancelReported says whether a phase of this package reported err as
// a client cancel already (the error carries the mark).
func ClientCancelReported(err error) bool {
	var reported *reportedClientCancel
	return logging.ErrorAs(err, &reported)
}

// ClientCancelled is the client cancel rule (clientCancelled) for a caller
// outside this package that is not an analytics read: it reports nothing.
func ClientCancelled(ctx context.Context, err error) bool {
	return clientCancelled(ctx, err)
}

// ReportedAsClientCancel is reportedAsClientCancel for an analytics read
// whose end a caller outside this package sees (a REST read route, under its
// own phase): it counts in the analytics client_cancelled counter.
func ReportedAsClientCancel(ctx context.Context, err error, phase string, attrs ...any) bool {
	return reportedAsClientCancel(ctx, err, phase, attrs...)
}
