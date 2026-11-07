package acr

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/ratelimit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// AuthAttemptLimit is INTERNAL_ACR_AUTH_ATTEMPT_IP_LIMIT ("100/minute"): one
// bucket per forwarded client address, shared by both routes. Python keyed
// the bucket on the address alone, so the limiter path is a constant.
var AuthAttemptLimit = httpapi.Limit{ID: "internal_acr_auth_attempt", Count: 100, Window: time.Minute}

const authAttemptPath = "/api/v1/internal/acr"

// Audit outcomes, byte-for-byte api/internal/acr.py's.
const (
	outcomeMissingToken = "missing_or_malformed_token"
	outcomeOrgNotFound  = "organization_not_found"
	outcomeLookupFailed = "entitlement_lookup_failed"
	outcomeSuccess      = "success"
	routeHealth         = "health"
	routeEntitlement    = "entitlement"
)

var internalRequests = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/acr").Int64Counter(
		"devhealth_acr_internal_requests_total",
		metric.WithDescription("Requests to the internal acr health and entitlement routes, by route and outcome."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_acr_internal_requests_total")
	}
	return counter
}()

func countInternal(ctx context.Context, route, outcome string) {
	internalRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("route", route), attribute.String("outcome", outcome)))
}

// routeAuth is the bearer check the internal acr routes run: the attempt
// limiter, then the acr-api workload token, the only credential they accept.
// Every other bearer, a svc_acr_ service credential included, gets the
// uniform 401 before any credential read. A nil store never serves
// unauthenticated: it is 503.
type routeAuth struct {
	store   CredentialStore
	limiter *httpapi.KeyedLimiter
	logger  *slog.Logger
	route   string
	// audited routes write an audit row per outcome (the entitlement route);
	// the health route does not.
	audited bool
	// workload verifies the acr-api workload token; nil refuses it as
	// not_configured.
	workload *WorkloadAuth
	// limitPath is the attempt limiter's path key; empty means
	// authAttemptPath, the bucket the health and entitlement routes share.
	limitPath string
}

func (a routeAuth) attemptPath() string {
	if a.limitPath == "" {
		return authAttemptPath
	}
	return a.limitPath
}

func (a routeAuth) audit(ctx context.Context, principalID, orgID, outcome string) bool {
	if !a.audited {
		return true
	}
	if err := a.store.Audit(ctx, "", principalID, orgID, outcome); err != nil {
		a.logger.ErrorContext(ctx, "acr internal: audit write failed",
			slog.String("route", a.route), slog.String("outcome", outcome), slog.String("error_type", errorKind(err)))
		countInternal(ctx, a.route, "audit_failed")
		return false
	}
	return true
}

func (a routeAuth) unavailable(w http.ResponseWriter, r *http.Request, outcome string) {
	countInternal(r.Context(), a.route, outcome)
	writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
}

// authenticate returns the credential when the request may proceed. When it
// returns false the response is already written. orgID is only the audit
// row's requested_org_id.
func (a routeAuth) authenticate(w http.ResponseWriter, r *http.Request, orgID string) (Credential, bool) {
	ctx := r.Context()
	if a.store == nil {
		a.logger.ErrorContext(ctx, "acr internal: credential store not configured", slog.String("route", a.route))
		a.unavailable(w, r, "unconfigured")
		return Credential{}, false
	}
	allowed, err := a.limiter.AllowCounted(ctx, ratelimit.ForwardedIP(r), a.attemptPath())
	if err != nil {
		countInternal(ctx, a.route, "rate_limit_store_failed")
		policy.WriteInternal(w)
		return Credential{}, false
	}
	if !allowed {
		countInternal(ctx, a.route, "rate_limited")
		a.logger.WarnContext(ctx, "acr internal: auth attempt budget exhausted", slog.String("route", a.route))
		writeDetail(w, http.StatusTooManyRequests, "Too many authentication attempts")
		return Credential{}, false
	}
	if bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok && isWorkloadToken(bearer) {
		return a.authenticateWorkload(w, r, orgID, bearer)
	}
	if !a.audit(ctx, "", orgID, outcomeMissingToken) {
		a.unavailable(w, r, "audit_failed")
		return Credential{}, false
	}
	countInternal(ctx, a.route, outcomeMissingToken)
	a.logger.WarnContext(ctx, "acr internal: request refused",
		slog.String("route", a.route), slog.String("outcome", outcomeMissingToken), slog.Int("status", http.StatusUnauthorized))
	writeDetail(w, http.StatusUnauthorized, "Unauthorized")
	return Credential{}, false
}

// authenticateWorkload is the workload-token branch of authenticate: no
// credential row exists, so nothing is looked up. A refusal is
// counted and logged by reason, audited with a NULL principal (an audited
// route), and answered 401, or 403 for a valid token of another workload. An
// accepted token's subject is the Credential's PrincipalID, which the audit
// row of the answer carries.
func (a routeAuth) authenticateWorkload(w http.ResponseWriter, r *http.Request, orgID, token string) (Credential, bool) {
	ctx := r.Context()
	subject, reason := a.workload.decide(token)
	if reason == "" {
		countWorkloadAccepted(ctx, a.route)
		a.logger.DebugContext(ctx, "acr internal: workload token accepted", slog.String("route", a.route))
		return Credential{PrincipalID: subject}, true
	}
	if reason == ReasonUnavailable {
		countWorkloadRefused(ctx, a.route, reason)
		logWorkloadRefused(ctx, a.logger, a.route, reason)
		writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
		return Credential{}, false
	}
	status, outcome, detail := http.StatusUnauthorized, outcomeWorkloadRefused, "Unauthorized"
	if reason == ReasonPrincipal {
		status, outcome, detail = http.StatusForbidden, outcomeWorkloadForbidden, "Forbidden"
	}
	countWorkloadRefused(ctx, a.route, reason)
	logWorkloadRefused(ctx, a.logger, a.route, reason)
	if !a.audit(ctx, "", orgID, outcome) {
		a.unavailable(w, r, "audit_failed")
		return Credential{}, false
	}
	countInternal(ctx, a.route, outcome)
	writeDetail(w, status, detail)
	return Credential{}, false
}
