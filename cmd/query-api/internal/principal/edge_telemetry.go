package principal

import (
	"context"
	"log/slog"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The envelope path's own telemetry (verifyOutcomeCounter,
// envelopeRejectedCounter, logEnvelopeRejection) exists because a bare
// 401 with nothing in the logs at all cost a live incident 40 minutes to
// diagnose. EdgeVerifier.Verify answers the SAME class of rejection for
// the credential a real user's browser actually carries, so it carries
// the same three deliverables here, at the same "reason is classified
// where it is known" site: an outcome counter, a per-reason counter, and
// a WARN log naming which reason, for which request -- never the token
// or any key material.

// edgeVerifyOutcomeCounter mirrors verifyOutcomeCounter (telemetry.go)
// for the edge access token: an operator needs to be able to tell "no
// edge-token requests yet" apart from "every one is being rejected" for
// THIS credential kind too, not only the envelope's.
var edgeVerifyOutcomeCounter = mustEdgeVerifyOutcomeCounter()

func mustEdgeVerifyOutcomeCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal")
	counter, err := meter.Int64Counter(
		"devhealth_query_api_edge_verify_total",
		metric.WithDescription("Edge access-token verifications by query-api, by outcome"),
	)
	if err != nil {
		// Same otel guarantee mustVerifyOutcomeCounter relies on: Add
		// below is always safe to call even on this fallback path.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_query_api_edge_verify_total")
	}
	return counter
}

// recordEdgeVerifyOutcome increments edgeVerifyOutcomeCounter. outcome is
// "verified" or "rejected".
func recordEdgeVerifyOutcome(outcome string) {
	edgeVerifyOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// edgeRejectedCounter mirrors envelopeRejectedCounter's per-REASON
// breakdown, for the edge access token: an unknown_kid-shaped spike has
// no equivalent here (HS256 carries no kid), but a wrong-secret,
// wrong-audience or expired-token spike is exactly as diagnosable at a
// glance from this counter's reason label as the envelope's own is.
var edgeRejectedCounter = mustEdgeRejectedCounter()

func mustEdgeRejectedCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal")
	counter, err := meter.Int64Counter(
		"dev_health_query_api_edge_rejected_total",
		metric.WithDescription("Edge access-token rejections by query-api, by reason"),
	)
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("dev_health_query_api_edge_rejected_total")
	}
	return counter
}

// recordEdgeRejected increments edgeRejectedCounter. reason is one of
// classifyEdgeRejectionReason's values, or one of the three
// claim-presence reasons Verify classifies directly (missing_sub,
// missing_type, type_mismatch).
func recordEdgeRejected(reason string) {
	edgeRejectedCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// logEdgeRejection mirrors logEnvelopeRejection: a WARN log at the
// rejection site naming WHY, so an operator looking at a 401 on a REST
// route can tell, from the logs alone, which credential kind was
// presented, why it was refused, and which request it was -- the same
// standard the envelope path already meets. iss/aud are read from
// whatever claims decoded before the rejection (possibly none, for a
// token too malformed to decode at all); sub is deliberately never
// logged, matching logEnvelopeRejection's own choice not to log a
// caller-identifying claim. tokenString and any key material are never
// passed to this function at all.
func logEdgeRejection(ctx context.Context, reason string, claims jwt.MapClaims, meta requestMeta) {
	slog.WarnContext(ctx, "query_api.edge.rejected",
		"reason", reason,
		"iss", edgeClaimString(claims, "iss"),
		"aud", edgeClaimString(claims, "aud"),
		"remote_addr", meta.remoteAddr,
		"request_id", meta.requestID,
	)
}

// edgeClaimString best-effort renders a decoded MapClaims value as a
// single string for logEdgeRejection: the JWT spec allows "aud" to be
// either a bare string or an array of strings (RFC 7519 §4.1.3), and
// claims may be nil or missing the key entirely (a token too malformed
// to decode past its header never populates this map at all) -- every
// one of those shapes renders as "", never a log call that panics on an
// unexpected claim shape.
func edgeClaimString(claims jwt.MapClaims, key string) string {
	if claims == nil {
		return ""
	}
	switch v := claims[key].(type) {
	case string:
		return v
	case []interface{}:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, ",")
	default:
		return ""
	}
}
