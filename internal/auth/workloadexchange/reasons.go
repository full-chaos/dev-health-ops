package workloadexchange

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-go/authverify"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Outcomes of one exchange.
const (
	outcomeIssued      = "issued"
	outcomeRefused     = "refused"
	outcomeUnavailable = "unavailable"
	outcomeInvalid     = "invalid"
)

// The closed reason set. Only the log line and the counter carry a reason;
// the caller sees one answer per outcome.
const (
	reasonIssued         = "issued"
	reasonInvalidRequest = "invalid_request"

	reasonSubjectTokenInvalid = "subject_token_invalid"
	reasonResolveRefused      = "resolve_refused"
	reasonAudienceRefused     = "audience_refused"
	reasonScopeRefused        = "scope_refused"
	reasonLifetimeTooShort    = "lifetime_too_short"

	reasonTokenReviewUnavailable = "tokenreview_unavailable"
	reasonSaturated              = "saturated"
	reasonSignerUnavailable      = "signer_unavailable"
	reasonAuditUnavailable       = "audit_unavailable"
	selfCheckPrefix              = "selfcheck_"
)

// resolveReasons are the reasons the workload store names for a refused
// Resolve. A reason outside the set counts as resolve_refused.
var resolveReasons = []string{
	"unknown_tuple", "workload_disabled", "principal_disabled", "binding_disabled",
	"uid_mismatch", "no_audience", "wrong_kind", "bad_input", "store_error",
}

var refusedReasons = append(append([]string{reasonSubjectTokenInvalid}, resolveReasons...),
	reasonResolveRefused, reasonAudienceRefused, reasonScopeRefused, reasonLifetimeTooShort)

// selfCheckReasons are authverify's workload refusal reasons.
var selfCheckReasons = []authverify.Reason{
	authverify.ReasonContract, authverify.ReasonUnknownKid, authverify.ReasonSignature, authverify.ReasonIssuer,
	authverify.ReasonAudience, authverify.ReasonNotYetValid, authverify.ReasonExpired,
}

func unavailableReasons() []string {
	out := []string{reasonTokenReviewUnavailable, reasonSaturated, reasonSignerUnavailable, reasonAuditUnavailable}
	for _, r := range selfCheckReasons {
		out = append(out, selfCheckPrefix+string(r))
	}
	return out
}

// Audience labels: the two workload audiences, or none for anything else (a
// requested value never becomes a label).
const audienceNone = "none"

var workloadAudiences = []string{"auth-service", "dev-health-api"}

func isWorkloadAudience(audience string) bool {
	for _, a := range workloadAudiences {
		if a == audience {
			return true
		}
	}
	return false
}

func audienceLabel(audience string) string {
	if isWorkloadAudience(audience) {
		return audience
	}
	return audienceNone
}

func resolveReason(named string) string {
	for _, r := range resolveReasons {
		if r == named {
			return r
		}
	}
	return reasonResolveRefused
}

func selfCheckReason(err error) string {
	reason := authverify.ReasonOf(err)
	for _, r := range selfCheckReasons {
		if r == reason {
			return selfCheckPrefix + string(r)
		}
	}
	return selfCheckPrefix + string(authverify.ReasonContract)
}

// recorder logs and counts every outcome.
type recorder struct {
	logger    *slog.Logger
	exchanges metric.Int64Counter
}

func newRecorder(logger *slog.Logger, meter metric.Meter) (recorder, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange")
	}
	exchanges, err := meter.Int64Counter("auth.workload.exchanges",
		metric.WithDescription("Workload-token exchanges, by outcome, reason and audience."))
	if err != nil {
		return recorder{}, fmt.Errorf("workloadexchange: registering the exchange counter: %w", err)
	}
	rec := recorder{logger: logger, exchanges: exchanges}
	rec.declare()
	return rec, nil
}

// declare registers every series at 0.
func (r recorder) declare() {
	ctx := context.Background()
	labels := append(append([]string(nil), workloadAudiences...), audienceNone)
	for _, audience := range labels {
		for _, reason := range refusedReasons {
			r.add(ctx, 0, outcomeRefused, reason, audience)
		}
		for _, reason := range unavailableReasons() {
			r.add(ctx, 0, outcomeUnavailable, reason, audience)
		}
	}
	for _, audience := range workloadAudiences {
		r.add(ctx, 0, outcomeIssued, reasonIssued, audience)
	}
	r.add(ctx, 0, outcomeInvalid, reasonInvalidRequest, audienceNone)
}

func (r recorder) add(ctx context.Context, n int64, outcome, reason, audience string) {
	r.exchanges.Add(ctx, n, metric.WithAttributes(
		attribute.String("outcome", outcome), attribute.String("reason", reason), attribute.String("audience", audience)))
}

// record logs one outcome by reason and counts it. No token, claim, key,
// ServiceAccount or upstream error text is ever logged.
func (r recorder) record(ctx context.Context, outcome, reason, audience string) {
	r.add(ctx, 1, outcome, reason, audience)
	attrs := []any{
		slog.String("route", Pattern), slog.String("outcome", outcome), slog.String("reason", reason),
		slog.String("audience", audience), slog.String("request_id", httpapi.LoggableRequestID(ctx)),
	}
	switch outcome {
	case outcomeUnavailable:
		r.logger.ErrorContext(ctx, "workload exchange unavailable", attrs...)
	case outcomeIssued:
		r.logger.InfoContext(ctx, "workload token issued", attrs...)
	default:
		r.logger.WarnContext(ctx, "workload exchange refused", attrs...)
	}
}
