package oidc

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

// go-api's OIDC provider failure texts (internal/api/sso oidc.go, provision.go), the record's error_message. A record
// carries one of these fixed texts, never an error's text, a provider body, a URL or a claim value.
const (
	msgIssuerRequired       = "OIDC issuer is required"
	msgIssuerInvalid        = "Invalid OIDC issuer URL"
	msgIssuerNotHTTPS       = "OIDC issuer must use HTTPS"
	msgIssuerNoHost         = "OIDC issuer must be a valid URL"
	msgDiscoveryFetch       = "Failed to fetch OIDC discovery document"
	msgDiscoveryEndpoints   = "OIDC discovery missing required endpoints"
	msgOIDCExchange         = "OIDC token exchange failed"
	msgNoAccessToken        = "OIDC token response missing access_token"
	msgNoIDToken            = "OIDC token response missing id_token"
	msgOIDCConfig           = "OIDC configuration missing issuer/client_id/jwks"
	msgIDTokenInvalid       = "OIDC id_token validation failed"
	msgNonceMismatch        = "OIDC nonce mismatch"
	msgOIDCUserinfo         = "OIDC userinfo request failed"
	msgClaimsNoEmail        = "OIDC claims missing email"
	msgAutoProvisionOff     = "User not found and auto-provisioning disabled"
	detailProvisioningFails = "OIDC user provisioning failed"

	protocolOAuthRecord = "oauth"
	stageNone           = ""
)

// recordedError marks a refusal go-api records as a provider failure, with go-api's text. An error without the mark
// records nothing: a check go-api does not make must not write to the provider row.
type recordedError struct {
	text string
	err  error
}

func (e *recordedError) Error() string { return e.err.Error() }
func (e *recordedError) Unwrap() error { return e.err }

func recordAs(text string, err error) error { return &recordedError{text: text, err: err} }

// goAPIText is the text go-api records for err; false means go-api records nothing there.
func goAPIText(err error) (string, bool) {
	var recorded *recordedError
	if errors.As(err, &recorded) {
		return recorded.text, true
	}
	return "", false
}

// recordedProviderFailure records a provider error as go-api's recordSSOFailure does (the protocol, the stage if any)
// before failure answers; a write error is the 500 go-api answers.
func (s *Service) recordedProviderFailure(ctx context.Context, row Provider, protocol, stage, message string, failure *Error) *Error {
	if s.cfg.ProviderFailure == nil {
		return failure
	}
	if err := s.cfg.ProviderFailure.Failed(ctx, ssoaudit.Row{OrgID: row.OrgID, ProviderID: row.ID, Protocol: protocol, Stage: stage,
		ErrorMessage: message}); err != nil {
		return internal(ReasonStoreUnavailable, err)
	}
	return failure
}

// recordedIfGoAPIRecords records an OIDC refusal when err carries go-api's text.
func (s *Service) recordedIfGoAPIRecords(ctx context.Context, row Provider, err error, failure *Error) *Error {
	if text, ok := goAPIText(err); ok {
		return s.recordedProviderFailure(ctx, row, ProviderKind, stageNone, text, failure)
	}
	return failure
}

// missingTokenText is go-api's text for a token response without a token, in go-api's order.
func missingTokenText(accessToken, idToken string) string {
	if accessToken == "" {
		return msgNoAccessToken
	}
	if idToken == "" {
		return msgNoIDToken
	}
	return ""
}
