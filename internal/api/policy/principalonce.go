package policy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
)

// BearerRow is what a route's endpoint-profile row declares about the
// Authorization bearer.
type BearerRow struct {
	// AccessToken: the row accepts user_access_token or legacy HS256.
	AccessToken bool
	// Domain: the row accepts a prefixed Bearer domain credential (a class
	// in the refusal table).
	Domain bool
	// Webhook: the row accepts a provider webhook signature.
	Webhook bool
}

// EarlyRefusal is one row of the refusal table: a Bearer value that starts
// with Prefix is a domain credential (or a webhook signing secret), never an
// access token, so the Principal step refuses it before any validator or
// store read. Classes are the credential-classes.json classes it stands for.
type EarlyRefusal struct {
	Prefix  string
	Classes []string
}

// earlyRefusals has one row per prefix. No value with one of these prefixes
// can be a JWS: a JWS starts with the base64url of '{' ("ey").
var earlyRefusals = []EarlyRefusal{
	{Prefix: "svc_", Classes: []string{"internal_svc_acr_token", "internal_svc_worker_token", "internal_svc_authsess_token"}},
	{Prefix: "fcpush_", Classes: []string{"external_push_ingest_token"}},
	{Prefix: "fcacr_", Classes: []string{"acr_client_credential"}},
	{Prefix: "whsec_", Classes: nil},
	// The Compose bootstrap credential is accepted only at auth-service's
	// workload-token exchange, never as a Bearer.
	{Prefix: "fcwlb_", Classes: nil},
}

const (
	classUserAccessToken = "user_access_token"
	webhookClassSuffix   = "_webhook_signature"
)

// BearerRowFor is the BearerRow of a row that accepts classes, with
// legacyHS256 its access_token_validation.legacy_hs256.
func BearerRowFor(classes []string, legacyHS256 bool) BearerRow {
	row := BearerRow{AccessToken: legacyHS256 || slices.Contains(classes, classUserAccessToken)}
	for _, class := range classes {
		if strings.HasSuffix(class, webhookClassSuffix) {
			row.Webhook = true
		}
		for _, refusal := range earlyRefusals {
			if slices.Contains(refusal.Classes, class) {
				row.Domain = true
			}
		}
	}
	return row
}

// DeclaredRows gives the Principal step the request's endpoint-profile row by
// route template. Call it once, before the Scope serves.
func (s *Scope) DeclaredRows(rows func(profile string) (BearerRow, bool)) { s.rows = rows }

// DeclaresRows reports whether DeclaredRows was applied.
func (s *Scope) DeclaresRows() bool { return s.rows != nil }

// Layers is the scope in request order: Principal, OrgScope, Impersonation.
func (s *Scope) Layers() []func(http.Handler) http.Handler {
	return []func(http.Handler) http.Handler{s.Principal, s.OrgScope, s.Impersonation}
}

// principalKey is its own type, so no contextKey value can collide with it.
type principalKey struct{}

// principalDecision is the request's caller as the Principal step decided
// it: the user, or nil (anonymous, with the reason), or the store failure.
type principalDecision struct {
	user   *User
	err    error
	reason string
}

// Reasons a caller is anonymous to the scope.
const (
	reasonNoCredential = "no_credential"
	reasonWebhookRow   = "webhook_row"
	reasonPrefix       = "domain_prefix"
	reasonDomainRow    = "domain_row"
	reasonNoAccessRow  = "no_access_row"
	reasonRefused      = "refused"
)

// Principal is the one step of the scope that reads the Authorization header.
// It decides the caller once, from the route's endpoint-profile row and the
// refusal table, and stores the decision for OrgScope and Impersonation,
// which never read the header. A value the refusal table or the row rules
// out reaches no validator and no store read. A request that already carries
// a decision keeps it.
func (s *Scope) Principal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, decided := decisionFrom(r.Context()); !decided {
			decision := s.decide(r)
			if decision.reason != "" && decision.reason != reasonNoCredential {
				s.logger.DebugContext(r.Context(), "api scope caller is anonymous", slog.String("reason", decision.reason))
			}
			r = r.WithContext(context.WithValue(r.Context(), principalKey{}, decision))
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Scope) decide(r *http.Request) *principalDecision {
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		return &principalDecision{reason: reasonNoCredential}
	}
	var row BearerRow
	found := false
	if s.rows != nil {
		row, found = s.rows(RouteProfileFrom(r.Context()))
	}
	if found && row.Webhook {
		return &principalDecision{reason: reasonWebhookRow}
	}
	token, ok := BearerToken(values[0])
	if !ok {
		return &principalDecision{reason: reasonNoCredential}
	}
	for _, refusal := range earlyRefusals {
		if strings.HasPrefix(token, refusal.Prefix) {
			return &principalDecision{reason: reasonPrefix}
		}
	}
	if found && row.Domain && !row.AccessToken {
		return &principalDecision{reason: reasonDomainRow}
	}
	if s.rows != nil && s.auth != nil && s.auth.Routed() && (!found || !row.AccessToken) {
		return &principalDecision{reason: reasonNoAccessRow}
	}
	user, err := s.authenticateToken(r.Context(), token)
	switch {
	case err == nil:
		return &principalDecision{user: user}
	case isRefusal(err):
		return &principalDecision{reason: reasonRefused}
	default:
		return &principalDecision{err: err}
	}
}

// decisionFrom is the request's Principal decision, if one was made.
func decisionFrom(ctx context.Context) (*principalDecision, bool) {
	decision, ok := ctx.Value(principalKey{}).(*principalDecision)
	return decision, ok
}

var errNoPrincipal = errors.New("policy: no principal decision for the request")

// scopeReason is the reason code a scope refusal logs.
func scopeReason(err error) string {
	if errors.Is(err, errNoPrincipal) {
		return "principal_undecided"
	}
	return "caller_lookup_failed"
}

func (s *Scope) authenticateToken(ctx context.Context, token string) (*User, error) {
	if s.authenticate != nil {
		return s.authenticate(ctx, token)
	}
	return s.auth.Authenticate(ctx, token)
}
