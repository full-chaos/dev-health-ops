package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
)

// The request span measures one Do or DoUnauthenticated call (all its wire attempts) for every provider. Its string
// values come from closed sources only: the client's provider name, the caller's method, the route TEMPLATE the caller
// named (else requestRouteUnrouted), the ledger route family, the target class, the outcome word and the ProviderError
// class. It never carries a URL, a path, a query, a host, a header or an error text: the redirect target of an artifact
// download is a signed URL.
const (
	requestTracerName    = "github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	requestSpanName      = "dev_health.provider.request"
	requestRouteUnrouted = "unrouted"

	requestTargetAPI      = "api"
	requestTargetRedirect = "redirect"

	requestOutcomeResponse        = "response"
	requestOutcomeFailed          = "failed"
	requestOutcomeBudgetContended = "budget_contended"
	requestOutcomeCancelled       = "cancelled"
	requestErrorTypeOther         = "other"

	requestAttrProvider  = "provider"
	requestAttrMethod    = "http.request.method"
	requestAttrRoute     = "http.route"
	requestAttrFamily    = "family"
	requestAttrTarget    = "dev_health.provider.target"
	requestAttrStatus    = "http.response.status_code"
	requestAttrAttempts  = "dev_health.provider.attempts"
	requestAttrGateWait  = "dev_health.provider.gate_wait_ms"
	requestAttrBudget    = "dev_health.provider.budget_ms"
	requestAttrLease     = "dev_health.provider.lease_ms"
	requestAttrOutcome   = "outcome"
	requestAttrErrorType = "error.type"
)

// requestProbe gathers the measurements of one call. A nil probe (no recording parent) measures nothing and reads no
// clock. status is the status of the response to the last wire attempt sent (0: that attempt got none).
type requestProbe struct {
	span     trace.Span
	attempts int
	status   int
	gateWait time.Duration
	budget   time.Duration
	lease    time.Duration
}

type requestSpanParentKey struct{}

// WithRequestSpanParent makes parent the parent of the request spans of the calls made with the returned context. The
// span of ctx itself, which log lines read, does not change. A nil parent returns ctx.
func WithRequestSpanParent(ctx context.Context, parent trace.Span) context.Context {
	if ctx == nil || parent == nil {
		return ctx
	}
	return context.WithValue(ctx, requestSpanParentKey{}, parent)
}

// startRequestSpan opens the span of one call only under a recording parent: never a new root. The span is never put
// in the context of the call, so the call's log lines keep the caller's span.
func (c *HTTPClient) startRequestSpan(ctx context.Context, method, target string) *requestProbe {
	if c == nil || ctx == nil {
		return nil
	}
	parent := ctx
	if span, ok := ctx.Value(requestSpanParentKey{}).(trace.Span); ok && span.IsRecording() {
		parent = trace.ContextWithSpan(ctx, span)
	}
	if !trace.SpanFromContext(parent).IsRecording() {
		return nil
	}
	route := routeTemplate(ctx)
	if route == "" {
		route = requestRouteUnrouted
	}
	_, span := tracing.StartWorkSpan(parent, requestTracerName, requestSpanName,
		attribute.String(requestAttrProvider, c.Provider),
		attribute.String(requestAttrMethod, method),
		attribute.String(requestAttrRoute, route),
		attribute.String(requestAttrFamily, requestRouteFamily(ctx)),
		attribute.String(requestAttrTarget, target),
	)
	return &requestProbe{span: span}
}

// requestRouteFamily is LedgerKey.RouteFamily of the call: the family the route labelled, else LedgerUnclassified.
func requestRouteFamily(ctx context.Context) string {
	account, _ := ctx.Value(requestAccountKey{}).(requestAccount)
	if account.routeFamily == "" {
		return LedgerUnclassified
	}
	return account.routeFamily
}

func (p *requestProbe) clock() time.Time {
	if p == nil {
		return time.Time{}
	}
	return time.Now()
}

func (p *requestProbe) addLease(start time.Time) {
	if p != nil {
		p.lease += time.Since(start)
	}
}

func (p *requestProbe) addGateWait(start time.Time) {
	if p != nil {
		p.gateWait += time.Since(start)
	}
}

func (p *requestProbe) addBudget(start time.Time) {
	if p != nil {
		p.budget += time.Since(start)
	}
}

func (p *requestProbe) sent(attempt int) {
	if p != nil {
		p.attempts, p.status = attempt, 0
	}
}

// responded records the status of the response a wire attempt got, before the call decides what to do with it.
func (p *requestProbe) responded(response *http.Response) {
	if p != nil && response != nil {
		p.status = response.StatusCode
	}
}

// end closes the span with what the call returned. It only reads response and err.
func (p *requestProbe) end(ctx context.Context, err error) {
	if p == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.Int(requestAttrAttempts, p.attempts),
		attribute.Int64(requestAttrGateWait, p.gateWait.Milliseconds()),
		attribute.Int64(requestAttrBudget, p.budget.Milliseconds()),
		attribute.Int64(requestAttrLease, p.lease.Milliseconds()),
	}
	if p.status > 0 {
		attrs = append(attrs, attribute.Int(requestAttrStatus, p.status))
	}
	switch {
	case err == nil:
		attrs = append(attrs, attribute.String(requestAttrOutcome, requestOutcomeResponse))
	case errors.Is(err, ErrBudgetContended):
		attrs = append(attrs, attribute.String(requestAttrOutcome, requestOutcomeBudgetContended))
	case CallCancelled(ctx, err):
		attrs = append(attrs, attribute.String(requestAttrOutcome, requestOutcomeCancelled))
	default:
		class := requestErrorType(err)
		attrs = append(attrs, attribute.String(requestAttrOutcome, requestOutcomeFailed), attribute.String(requestAttrErrorType, class))
		p.span.SetStatus(codes.Error, class)
	}
	p.span.SetAttributes(attrs...)
	tracing.EndWorkSpan(p.span, nil)
}

// requestErrorType is the ProviderError class of err as one of the class constants, else requestErrorTypeOther.
func requestErrorType(err error) string {
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		return requestErrorTypeOther
	}
	switch providerErr.Class {
	case ErrorAuthentication, ErrorNotFound, ErrorConflict, ErrorRateLimited, ErrorTransient, ErrorCancelled, ErrorPermanent:
		return string(providerErr.Class)
	default:
		return requestErrorTypeOther
	}
}

// CallCancelled reports a call that ended only because ctx was cancelled: ctx is cancelled and every leaf of err is
// context.Canceled, a transport failure of reason canceled, or one of labels (a sentinel that names the step, wrapped
// next to the cause), with at least one cancellation. A leaf of any other failure (a failed budget release joined on)
// makes it a failure, and so does a cancellation under a live ctx (a context the code cancelled itself).
func CallCancelled(ctx context.Context, err error, labels ...error) bool {
	if ctx == nil || err == nil || !errors.Is(ctx.Err(), context.Canceled) {
		return false
	}
	cancelled, other := cancellationLeaves(err, labels)
	return cancelled && !other
}

func cancellationLeaves(err error, labels []error) (cancelled, other bool) {
	if providerErr, ok := err.(*ProviderError); ok && providerErr.Reason == TransportReasonCanceled {
		return true, false
	}
	if slices.Contains(labels, err) {
		return false, false
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		leaves := wrapped.Unwrap()
		if len(leaves) == 0 {
			return false, true
		}
		for _, leaf := range leaves {
			leafCancelled, leafOther := cancellationLeaves(leaf, labels)
			cancelled, other = cancelled || leafCancelled, other || leafOther
		}
		return cancelled, other
	case interface{ Unwrap() error }:
		if inner := wrapped.Unwrap(); inner != nil {
			return cancellationLeaves(inner, labels)
		}
	}
	if errors.Is(err, context.Canceled) {
		return true, false
	}
	return false, true
}

func (c *HTTPClient) assertLeaseMeasured(ctx context.Context, probe *requestProbe) error {
	start := probe.clock()
	err := c.Lease.Assert(ctx)
	probe.addLease(start)
	return err
}

func (c *HTTPClient) acquireMeasured(ctx context.Context, probe *requestProbe) (Reservation, error) {
	start := probe.clock()
	reservation, err := c.Budget.Acquire(ctx, c.BudgetKey)
	probe.addBudget(start)
	return reservation, err
}

func (c *HTTPClient) releaseMeasured(ctx context.Context, probe *requestProbe, reservation Reservation) error {
	start := probe.clock()
	err := c.releaseReservation(ctx, reservation)
	probe.addBudget(start)
	return err
}
