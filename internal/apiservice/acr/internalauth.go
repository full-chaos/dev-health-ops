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

// EntitlementScope is the scope the health and entitlement routes require
// (api/internal/acr.py _REQUIRED_SCOPE).
const EntitlementScope = "entitlements:read"

// AuthAttemptLimit is INTERNAL_ACR_AUTH_ATTEMPT_IP_LIMIT ("100/minute"): one
// bucket per forwarded client address, shared by both routes. Python keyed
// the bucket on the address alone, so the limiter path is a constant.
var AuthAttemptLimit = httpapi.Limit{ID: "internal_acr_auth_attempt", Count: 100, Window: time.Minute}

const authAttemptPath = "/api/v1/internal/acr"

// Audit outcomes, byte-for-byte api/internal/acr.py's.
const (
	outcomeMissingToken  = "missing_or_malformed_token"
	outcomeUnknownToken  = "unknown_token"
	outcomeInactiveToken = "inactive_token"
	outcomeInsufficient  = "insufficient_scope"
	outcomeOrgNotFound   = "organization_not_found"
	outcomeLookupFailed  = "entitlement_lookup_failed"
	outcomeSuccess       = "success"
	routeHealth          = "health"
	routeEntitlement     = "entitlement"
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

// routeAuth is the svc_acr_ bearer check both internal acr routes run, the
// port of api/internal/acr.py's _reserve_auth_attempt_or_429, _extract_token
// and credential lookup. A nil store never serves unauthenticated: it is 503.
type routeAuth struct {
	store   CredentialStore
	limiter *httpapi.KeyedLimiter
	logger  *slog.Logger
	route   string
	// audited routes write an audit row per outcome and stamp last_used_at
	// (the entitlement route); the health route does neither.
	audited bool
}

// extractToken is _extract_token: a case-sensitive "Bearer " header whose
// token starts with svc_acr_ and is longer than that prefix.
func extractToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(header, "Bearer ")
	if !strings.HasPrefix(token, acrTokenPrefix) || len(token) <= len(acrTokenPrefix) {
		return "", false
	}
	return token, true
}

func (a routeAuth) audit(ctx context.Context, credentialID, orgID, outcome string) bool {
	if !a.audited {
		return true
	}
	if err := a.store.Audit(ctx, credentialID, orgID, outcome); err != nil {
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
	allowed, err := a.limiter.AllowCounted(ctx, ratelimit.ForwardedIP(r), authAttemptPath)
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
	deny := func(credentialID, outcome string, status int, detail string) (Credential, bool) {
		if !a.audit(ctx, credentialID, orgID, outcome) {
			a.unavailable(w, r, "audit_failed")
			return Credential{}, false
		}
		countInternal(ctx, a.route, outcome)
		a.logger.WarnContext(ctx, "acr internal: request refused",
			slog.String("route", a.route), slog.String("outcome", outcome), slog.Int("status", status),
			slog.String("credential_id", credentialID))
		writeDetail(w, status, detail)
		return Credential{}, false
	}
	token, ok := extractToken(r)
	if !ok {
		return deny("", outcomeMissingToken, http.StatusUnauthorized, "Unauthorized")
	}
	cred, found, err := a.store.Find(ctx, token)
	if err != nil {
		a.logger.ErrorContext(ctx, "acr internal: credential lookup failed",
			slog.String("route", a.route), slog.String("error_type", errorKind(err)))
		a.unavailable(w, r, "credential_unavailable")
		return Credential{}, false
	}
	if !found {
		return deny("", outcomeUnknownToken, http.StatusUnauthorized, "Unauthorized")
	}
	if cred.ServiceName != "acr" || !cred.Valid(time.Now().UTC()) {
		return deny(cred.ID, outcomeInactiveToken, http.StatusUnauthorized, "Unauthorized")
	}
	if a.audited {
		if err := a.store.MarkUsed(ctx, cred.ID, time.Now().UTC()); err != nil {
			a.logger.ErrorContext(ctx, "acr internal: last_used_at write failed",
				slog.String("route", a.route), slog.String("credential_id", cred.ID), slog.String("error_type", errorKind(err)))
			a.unavailable(w, r, "credential_unavailable")
			return Credential{}, false
		}
	}
	if !cred.Has(EntitlementScope) {
		return deny(cred.ID, outcomeInsufficient, http.StatusForbidden, "Forbidden")
	}
	a.logger.DebugContext(ctx, "acr internal: credential accepted",
		slog.String("route", a.route), slog.String("credential_id", cred.ID))
	return cred, true
}
