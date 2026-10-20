package oidc

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// Reason is a stable, secret-free outcome code: a metric label and a log
// field, never caller input.
type Reason string

const (
	ReasonAccepted             Reason = "accepted"
	ReasonMalformed            Reason = "malformed"
	ReasonBadSignature         Reason = "bad_signature"
	ReasonAudience             Reason = "audience"
	ReasonIssuer               Reason = "issuer"
	ReasonExpired              Reason = "expired"
	ReasonNotYetValid          Reason = "not_yet_valid"
	ReasonMissingSub           Reason = "missing_sub"
	ReasonEmailUnverified      Reason = "email_unverified"
	ReasonEmailCollision       Reason = "email_collision"
	ReasonPrincipalDeactivated Reason = "principal_deactivated"
	ReasonStoreUnavailable     Reason = "store_unavailable"
	ReasonDependency           Reason = "dependency_unavailable"
	ReasonReplayed             Reason = "replayed"
	ReasonNonceMismatch        Reason = "nonce_mismatch"
	ReasonSignupUnavailable    Reason = "signup_unavailable"
	ReasonProvisioningDisabled Reason = "provisioning_disabled"
	ReasonDomainNotAllowed     Reason = "domain_not_allowed"
	ReasonNotEntitled          Reason = "not_entitled"
)

var allReasons = []Reason{
	ReasonAccepted, ReasonMalformed, ReasonBadSignature, ReasonAudience, ReasonIssuer, ReasonExpired,
	ReasonNotYetValid, ReasonMissingSub, ReasonEmailUnverified, ReasonEmailCollision, ReasonPrincipalDeactivated,
	ReasonStoreUnavailable, ReasonDependency, ReasonReplayed, ReasonNonceMismatch, ReasonSignupUnavailable,
	ReasonProvisioningDisabled, ReasonDomainNotAllowed, ReasonNotEntitled,
}

// Errors of the IdentityStore seam. StoreIdentities maps the identity store's
// own sentinels onto them.
var (
	ErrIdentityNotFound   = errors.New("oidc: no principal holds this external identity")
	ErrSignupNotAvailable = identity.ErrSignupNotAvailable
	ErrIdentityConflict   = errors.New("oidc: the external identity or e-mail belongs to another account")
	ErrInvalidEmail       = identity.ErrInvalidEmail
	ErrInvalidKey         = identity.ErrInvalidExternalKey
)

// Error is a refused or failed flow: the HTTP status and body detail the
// caller sees, and the reason the metrics and logs record. err, when set, is
// the internal cause; it is logged, never returned to the caller.
type Error struct {
	Status int
	Detail any
	Reason Reason
	err    error
}

// Error names the reason only: the cause can hold IdP, URL or database text,
// so a caller that logs the error never logs the cause.
func (e *Error) Error() string { return "oidc: " + string(e.Reason) }

func (e *Error) Unwrap() error { return e.err }

const (
	detailAuthFailed     = "OIDC authentication failed"
	detailProviderAbsent = "SSO provider not found"
)

func deny(status int, reason Reason, detail any) *Error {
	return &Error{Status: status, Detail: detail, Reason: reason}
}

func authFailed(reason Reason, cause error) *Error {
	return &Error{Status: http.StatusBadRequest, Detail: detailAuthFailed, Reason: reason, err: cause}
}

func internal(reason Reason, cause error) *Error {
	return &Error{Status: http.StatusInternalServerError, Detail: "Internal Server Error", Reason: reason, err: cause}
}
