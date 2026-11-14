package authruntime

import (
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// untrustedCallerID is what an access line carries when a peer that is not a
// trusted proxy sent an X-Request-ID: the value is never logged.
const untrustedCallerID = "untrusted"

// accessLogIDs adds two fields to every API access line, beside request_id
// (the transport id this service mints):
//   - upstream_request_id: this service's own wire id from the answer's
//     X-Auth-Request-ID, under the key go-api logs the same value with, so one
//     filter finds both lines. Not auth_request_id: the logging package masks
//     every key with the word "auth". An inbound X-Auth-Request-ID is never
//     read: go-api relays the client's copy.
//   - caller_request_id: the X-Request-ID go-api sends (its own request_id),
//     under go-api's log rule (canonical UUID or 32 hex, else "invalid"), only
//     from a believed forwarder (clientaddr.Policy.Forwarded: a valid proof, or a
//     trusted-proxy peer where the mode reads the list); "untrusted" from any
//     other peer; "" when absent.
func accessLogIDs(policy clientaddr.Policy) func(*http.Request, http.Header) []slog.Attr {
	return func(r *http.Request, answer http.Header) []slog.Attr {
		return []slog.Attr{
			slog.String("upstream_request_id", requestid.LoggableWire(answer.Get(requestid.Header))),
			slog.String("caller_request_id", callerRequestID(policy, r)),
		}
	}
}

func callerRequestID(policy clientaddr.Policy, r *http.Request) string {
	values := r.Header.Values(httpapi.RequestIDHeader)
	switch {
	case len(values) == 0:
		return ""
	case !policy.Forwarded(r):
		return untrustedCallerID
	case len(values) != 1:
		return "invalid"
	}
	return httpapi.LoggableID(values[0])
}
