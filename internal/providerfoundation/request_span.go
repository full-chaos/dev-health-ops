package providerfoundation

import (
	"context"
	"errors"
	"net/http"
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
// clock.
type requestProbe struct {
	span     trace.Span
	attempts int
	gateWait time.Duration
	budget   time.Duration
	lease    time.Duration
}

// startRequestSpan opens the span of one call only under a recording parent: never a new root.
func (c *HTTPClient) startRequestSpan(ctx context.Context, method, target string) (context.Context, *requestProbe) {
	if c == nil || ctx == nil || !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}
	route := routeTemplate(ctx)
	if route == "" {
		route = requestRouteUnrouted
	}
	ctx, span := tracing.StartWorkSpan(ctx, requestTracerName, requestSpanName,
		attribute.String(requestAttrProvider, c.Provider),
		attribute.String(requestAttrMethod, method),
		attribute.String(requestAttrRoute, route),
		attribute.String(requestAttrFamily, requestRouteFamily(ctx)),
		attribute.String(requestAttrTarget, target),
	)
	return ctx, &requestProbe{span: span}
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
		p.attempts = attempt
	}
}

// end closes the span with what the call returned. It only reads response and err.
func (p *requestProbe) end(response *http.Response, err error) {
	if p == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.Int(requestAttrAttempts, p.attempts),
		attribute.Int64(requestAttrGateWait, p.gateWait.Milliseconds()),
		attribute.Int64(requestAttrBudget, p.budget.Milliseconds()),
		attribute.Int64(requestAttrLease, p.lease.Milliseconds()),
	}
	var providerErr *ProviderError
	switch {
	case response != nil:
		attrs = append(attrs, attribute.Int(requestAttrStatus, response.StatusCode))
	case errors.As(err, &providerErr) && providerErr.StatusCode > 0:
		attrs = append(attrs, attribute.Int(requestAttrStatus, providerErr.StatusCode))
	}
	switch {
	case err == nil:
		attrs = append(attrs, attribute.String(requestAttrOutcome, requestOutcomeResponse))
	case errors.Is(err, ErrBudgetContended):
		attrs = append(attrs, attribute.String(requestAttrOutcome, requestOutcomeBudgetContended))
	case tracing.IsCancellation(err):
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

func (c *HTTPClient) assertLeaseMeasured(ctx context.Context, probe *requestProbe) error {
	start := probe.clock()
	err := c.Lease.Assert(ctx)
	probe.addLease(start)
	return err
}

func (c *HTTPClient) awaitGateMeasured(ctx context.Context, probe *requestProbe) error {
	start := probe.clock()
	err := c.awaitGate(ctx)
	probe.addGateWait(start)
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
