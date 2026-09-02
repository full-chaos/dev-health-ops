// Package httpapi is the Auth Control Plane's HTTP transport.
//
// It owns routing, the middleware stack, and the error envelope. It owns no
// domain behaviour: a route is constructed from a Route value whose Handler
// closes over a domain interface, so nothing in this package knows what an
// identity, a session or a token is. Transport types must not leak into domain
// code, or vice versa.
//
// ACP-ADR-01 §3 pins the router: net/http with http.ServeMux and explicit
// method-and-path registration. No third-party router, in this wave or later.
//
// # Dormancy
//
// CHAOS-4881 builds this service DORMANT: nothing in production calls it and
// no business route is mounted. The dormancy lives at the CALL SITE -- the
// runtime passes an empty route set -- not in this package, which is fully
// implemented and fully tested. A middleware stack that only exists once a
// route needs it is a middleware stack nobody has ever run.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// Code is the closed set of machine-readable error codes this transport
// emits. It is closed on purpose: a caller can branch on it, and no handler
// can invent a code by returning an arbitrary string.
//
// CHAOS-4884 pins the cross-language auth error envelope in
// contracts/auth/v1. This type is the Go-side shape it will be reconciled
// against; the field names below (code, message, request_id) are chosen to
// match the envelope described in the TRD rather than to be re-invented there.
type Code string

const (
	// CodeNotFound is returned for a path no route claims.
	CodeNotFound Code = "not_found"
	// CodeMethodNotAllowed is returned for a known path with an unregistered
	// method. The response carries an Allow header.
	CodeMethodNotAllowed Code = "method_not_allowed"
	// CodePayloadTooLarge is returned when a request body exceeds the
	// configured bound.
	CodePayloadTooLarge Code = "payload_too_large"
	// CodeRateLimited is returned when a route's bucket is empty.
	CodeRateLimited Code = "rate_limited"
	// CodeInvalidRequest is returned when the request itself could not be
	// read — a transport failure part-way through a body, not a size
	// violation. It is deliberately distinct from CodePayloadTooLarge so a
	// caller is not told it sent too much when it sent too little.
	CodeInvalidRequest Code = "invalid_request"
	// CodeInternal is returned for a recovered panic or an unclassified
	// handler failure. Its message is fixed and carries no detail.
	CodeInternal Code = "internal_error"
)

// message is the fixed, operator-safe text for each code.
//
// The text is a compile-time constant per code and never interpolates
// anything: not the path, not a dependency error, not the request body. That
// is the CHAOS-4724 rule applied at construction rather than at each call
// site -- an unauthenticated caller of this surface must not be able to read
// a DSN, a filesystem path or an upstream error off the wire, and the only
// structural way to guarantee that is for the response body to have no slot
// an arbitrary string can reach.
var message = map[Code]string{
	CodeNotFound:         "no route matches this path",
	CodeMethodNotAllowed: "this method is not allowed on this path",
	CodePayloadTooLarge:  "the request body exceeds the configured limit",
	CodeRateLimited:      "too many requests for this route",
	CodeInvalidRequest:   "the request body could not be read",
	CodeInternal:         "the request could not be processed",
}

// status is the HTTP status each code renders as.
var status = map[Code]int{
	CodeNotFound:         http.StatusNotFound,
	CodeMethodNotAllowed: http.StatusMethodNotAllowed,
	CodePayloadTooLarge:  http.StatusRequestEntityTooLarge,
	CodeRateLimited:      http.StatusTooManyRequests,
	CodeInvalidRequest:   http.StatusBadRequest,
	CodeInternal:         http.StatusInternalServerError,
}

// Envelope is the wire shape of every error this transport emits.
type Envelope struct {
	Error EnvelopeError `json:"error"`
}

// EnvelopeError is the error body. RequestID lets an operator join a client
// report to a log line without the response carrying any other detail.
type EnvelopeError struct {
	Code      Code   `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

// WriteError renders one envelope. It is the ONLY way this package writes an
// error response, so there is one place to audit for leakage.
//
// A code with no registered message or status is rendered as CodeInternal
// rather than as an empty body with a zero status: an unregistered code is a
// programming error, and failing to the least informative response is the
// safe direction.
func WriteError(w http.ResponseWriter, r *http.Request, code Code) {
	text, known := message[code]
	httpStatus, hasStatus := status[code]
	if !known || !hasStatus {
		code, text, httpStatus = CodeInternal, message[CodeInternal], http.StatusInternalServerError
	}
	body := Envelope{Error: EnvelopeError{
		Code:      code,
		Message:   text,
		RequestID: RequestIDFrom(r.Context()),
	}}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// An error response must never be cached: a 429 or a 503 cached by an
	// intermediary outlives the condition that produced it.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(httpStatus)
	// The encode cannot fail on this closed, string-only shape; a write error
	// means the client is gone, which is not actionable here.
	_ = json.NewEncoder(w).Encode(body)
}

// logAttrs renders the safe attributes describing a request. The path is
// included because these are this service's OWN registered route patterns,
// never a caller-supplied string: unmatched paths are logged as the literal
// pattern "<unmatched>" instead.
func logAttrs(r *http.Request, pattern string) []slog.Attr {
	return []slog.Attr{
		slog.String("method", r.Method),
		slog.String("route", pattern),
		slog.String("request_id", RequestIDFrom(r.Context())),
	}
}
