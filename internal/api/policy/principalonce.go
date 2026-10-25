package policy

import (
	"context"
	"errors"
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

// Principal is the one step that reads the Authorization header.
func (s *Scope) Principal(next http.Handler) http.Handler {
	return next
}

var errNoPrincipal = errors.New("policy: no principal decision for the request")

// ScopeReason is the reason code a scope refusal logs.
func ScopeReason(err error) string {
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
