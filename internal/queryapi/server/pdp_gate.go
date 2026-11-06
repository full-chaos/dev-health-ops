package server

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// gatedPlaneHandler builds the process's one PDP gate from QUERY_API_PDP_MODE (off asks nothing; a bad setting never
// fails the start, the gate then refuses every PDP-gated request) and the plane handler that carries it.
func gatedPlaneHandler(lookup func(string) (string, bool), mux *http.ServeMux) (http.Handler, *pdp.Gate) {
	gate := pdp.FromSettingsWithRows(lookup, slog.Default(), graphQLShadowRows())
	return planeHandler(mux, gate), gate
}

// planeHandler is the plane's one handler: the response-model marker over mux, with the process's PDP gate on every
// request's context, so each REST route's authentication reads the same gate.
func planeHandler(mux *http.ServeMux, gate *pdp.Gate) http.Handler {
	next := markResponseModelRoutes(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gated := r.WithContext(pdp.WithGate(r.Context(), gate))
		next.ServeHTTP(w, gated)
		// The server span outside reads the pattern the inner mux recorded on the copy.
		r.Pattern = gated.Pattern
	})
}

// writePDPDenied answers a PDP refusal: 503 when there is no PDP verdict (the PDP is down or the gate is
// misconfigured), else 403. The log line names the closed reason only.
func writePDPDenied(w http.ResponseWriter, r *http.Request, component string, err error) {
	var denied *pdp.DenyError
	local := !errors.As(err, &denied) || denied.Local
	reason := ""
	if denied != nil {
		reason = string(denied.Reason)
	}
	requestID := envelopeRequestID(r)
	if !acceptableCorrelationID(requestID) {
		requestID = ""
	}
	rid := strings.ReplaceAll(strings.ReplaceAll(requestID, "\n", ""), "\r", "")
	slog.WarnContext(r.Context(), "query-api: the PDP refused the request", slog.String("component", component),
		slog.String("request_id", rid), slog.String("reason", reason), slog.Bool("unavailable", local))
	if local {
		policy.WriteDetail(w, http.StatusServiceUnavailable, "Authorization unavailable", nil)
		return
	}
	policy.WriteDetail(w, http.StatusForbidden, "Not authorized", nil)
}
