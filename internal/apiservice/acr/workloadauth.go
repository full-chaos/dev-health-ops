package acr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	workloadTokenTyp = "workload+jwt"
	routeLLMConfig   = "llm_config"

	outcomeWorkloadRefused   = "workload_refused"
	outcomeWorkloadForbidden = "workload_forbidden"
)

// Workload refusal reasons: the authverify reasons plus the two go-api adds.
// The set is closed; an authverify reason outside it counts as contract.
const (
	ReasonContract      = "contract"
	ReasonUnknownKid    = "unknown_kid"
	ReasonSignature     = "signature"
	ReasonIssuer        = "issuer"
	ReasonAudience      = "audience"
	ReasonNotYetValid   = "not_yet_valid"
	ReasonExpired       = "expired"
	ReasonPrincipal     = "principal"
	ReasonNotConfigured = "not_configured"
)

var workloadReasons = []string{
	ReasonContract, ReasonUnknownKid, ReasonSignature, ReasonIssuer, ReasonAudience,
	ReasonNotYetValid, ReasonExpired, ReasonPrincipal, ReasonNotConfigured,
}

var workloadRoutes = []string{routeHealth, routeEntitlement, routeLLMConfig}

// WorkloadVerifier is satisfied by *authverify.WorkloadVerifier.
type WorkloadVerifier interface {
	Verify(token string, now time.Time) (*authverify.WorkloadClaims, error)
}

// WorkloadAuth lets the internal acr routes accept the acr-api workload token.
// Subject is the one workload principal (prn_) the routes allow; the token has
// no scope claim, so this equality is the per-route action binding. A nil
// WorkloadAuth, a nil Verifier or an empty Subject refuses every workload
// token as not_configured.
type WorkloadAuth struct {
	Verifier WorkloadVerifier
	Subject  string
	Now      func() time.Time
}

var (
	workloadMeter    = otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/acr")
	workloadRefused  = mustCounter("devhealth_acr_workload_refused_total", "Workload tokens refused by the internal acr routes, by route and reason.")
	workloadAccepted = mustCounter("devhealth_acr_workload_accepted_total", "Workload tokens accepted by the internal acr routes, by route.")
	svcAcrCompat     = mustCounter("devhealth_acr_svc_acr_compat_total", "svc_acr_ credentials accepted by the internal acr routes during the workload-token migration, by route.")
)

func mustCounter(name, description string) metric.Int64Counter {
	counter, err := workloadMeter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// declareWorkloadSeries registers every counter series at 0.
func declareWorkloadSeries() {
	ctx := context.Background()
	for _, route := range workloadRoutes {
		for _, reason := range workloadReasons {
			workloadRefused.Add(ctx, 0, metric.WithAttributes(attribute.String("route", route), attribute.String("reason", reason)))
		}
		workloadAccepted.Add(ctx, 0, metric.WithAttributes(attribute.String("route", route)))
		svcAcrCompat.Add(ctx, 0, metric.WithAttributes(attribute.String("route", route)))
	}
}

// logWorkloadRefused logs one refusal by reason. Only a principal refusal
// names the subject (a prn_ id of a verified token); no token, claim or key
// byte is ever logged.
func logWorkloadRefused(ctx context.Context, logger *slog.Logger, route, subject, reason string) {
	attrs := []any{slog.String("route", route), slog.String("reason", reason)}
	if reason == ReasonPrincipal {
		attrs = append(attrs, slog.String("workload_subject", subject))
	}
	logger.WarnContext(ctx, "acr internal: workload token refused", attrs...)
}

func countWorkloadRefused(ctx context.Context, route, reason string) {
	workloadRefused.Add(ctx, 1, metric.WithAttributes(attribute.String("route", route), attribute.String("reason", reason)))
}

func countWorkloadAccepted(ctx context.Context, route string) {
	workloadAccepted.Add(ctx, 1, metric.WithAttributes(attribute.String("route", route)))
}

func countSvcAcrCompat(ctx context.Context, route string) {
	svcAcrCompat.Add(ctx, 1, metric.WithAttributes(attribute.String("route", route)))
}

// isWorkloadToken reads the unverified header and reports typ workload+jwt on
// a three-part compact token. It only picks the verification path; the
// verifier re-checks everything.
func isWorkloadToken(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var header struct {
		Typ string `json:"typ"`
	}
	return json.Unmarshal(raw, &header) == nil && header.Typ == workloadTokenTyp
}

// decide verifies token and returns the workload subject and the refusal
// reason (empty reason = accepted). The subject is set only for a verified
// token: on accept, and on a principal refusal.
func (w *WorkloadAuth) decide(token string) (subject, reason string) {
	if w == nil || w.Verifier == nil || w.Subject == "" {
		return "", ReasonNotConfigured
	}
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}
	claims, err := w.Verifier.Verify(token, now())
	if err != nil {
		return "", workloadReason(err)
	}
	if claims.Subject != w.Subject {
		return claims.Subject, ReasonPrincipal
	}
	return claims.Subject, ""
}

func workloadReason(err error) string {
	switch authverify.ReasonOf(err) {
	case authverify.ReasonUnknownKid:
		return ReasonUnknownKid
	case authverify.ReasonSignature:
		return ReasonSignature
	case authverify.ReasonIssuer:
		return ReasonIssuer
	case authverify.ReasonAudience:
		return ReasonAudience
	case authverify.ReasonNotYetValid:
		return ReasonNotYetValid
	case authverify.ReasonExpired:
		return ReasonExpired
	default:
		return ReasonContract
	}
}
