// Package internalidentity is the query-api side of the internal identity
// carrier (CHAOS-6144, chris R340/D1807): an in-cluster caller (the Python
// edge's GraphQL dispatcher) states the identity it already authenticated
// as four plain request headers, with no bearer, no signature and no
// token to mint, refresh or verify.
//
// Boundary (_records/6144/invariant.md): these headers are trusted because
// the caller is in the cluster. That holds ONLY for paths no Ingress can
// reach (/query, /buildinfo), and only while the Ingress removes these four
// names from every inbound request. A route an Ingress does reach (every
// /api/v1 REST route) must never call this package: a browser could set the
// headers itself.
//
// The four headers carry exactly the fields the effective-principal
// envelope's consumers read (authctx.Claims): org, role, superuser flag,
// impersonation flag. Strings pass through unchanged, as the envelope's do.
// A fifth, optional header names the principal behind the caller's own
// credential (CHAOS-13945), for the decision point to judge.
package internalidentity

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// The header names are a cross-repo contract (the Python edge sends them,
// the Ingress strips them). Do not rename without the deploy PR.
const (
	HeaderOrgID               = "X-DH-Internal-Org-Id"
	HeaderRole                = "X-DH-Internal-Role"
	HeaderSuperuser           = "X-DH-Internal-Superuser"
	HeaderImpersonationActive = "X-DH-Internal-Impersonation-Active"
	// HeaderPrincipalID is optional: the principal's wire id (prn_ and 22
	// base64url characters), at most once.
	HeaderPrincipalID = "X-DH-Internal-Principal-Id"
)

// Headers lists the four internal identity header names every carrier sends exactly once.
var Headers = []string{HeaderOrgID, HeaderRole, HeaderSuperuser, HeaderImpersonationActive}

// AllHeaders is Headers and the optional principal header: every name the public listener deletes and any one of
// which makes a carrier present.
var AllHeaders = []string{HeaderOrgID, HeaderRole, HeaderSuperuser, HeaderImpersonationActive, HeaderPrincipalID}

// Rejection reasons. They name the class of failure, never a value.
const (
	ReasonMissing   = "missing_header"
	ReasonDuplicate = "duplicate_header"
	ReasonBoolean   = "not_a_boolean"
	ReasonPrincipal = "not_a_principal"
)

// Rejection is the error FromHeader returns for a request that carries at
// least one internal identity header but not a well-formed set.
type Rejection struct {
	Reason string
	Header string
}

func (r *Rejection) Error() string {
	return "internalidentity: " + r.Reason + ": " + r.Header
}

// Present reports whether the request carries ANY internal identity header.
// A partial set is present: it is refused, never read as "no identity".
func Present(h http.Header) bool {
	for _, name := range AllHeaders {
		if len(h.Values(name)) > 0 {
			return true
		}
	}
	return false
}

// FromHeader reads the identity. All four headers must be present exactly
// once; the two flags must be exactly "true" or "false". A missing header
// is never defaulted: a partial set could otherwise mean "no role, not a
// superuser" by accident, and the fail-closed reading is a refusal. The
// principal header is optional; present, it is exactly one canonical wire id.
func FromHeader(h http.Header) (authctx.Claims, error) {
	var claims authctx.Claims
	values := make(map[string]string, len(Headers))
	for _, name := range Headers {
		got := h.Values(name)
		switch len(got) {
		case 0:
			return claims, &Rejection{Reason: ReasonMissing, Header: name}
		case 1:
			values[name] = got[0]
		default:
			return claims, &Rejection{Reason: ReasonDuplicate, Header: name}
		}
	}
	superuser, err := parseFlag(values[HeaderSuperuser], HeaderSuperuser)
	if err != nil {
		return claims, err
	}
	impersonating, err := parseFlag(values[HeaderImpersonationActive], HeaderImpersonationActive)
	if err != nil {
		return claims, err
	}
	principal, err := principalFrom(h)
	if err != nil {
		return claims, err
	}
	claims.OrgID = values[HeaderOrgID]
	claims.Role = values[HeaderRole]
	claims.IsSuperuser = superuser
	claims.ImpersonationActive = impersonating
	claims.HeaderPrincipal = principal
	return claims, nil
}

// principalFrom is the optional principal header: "" when absent, else its one
// value, which must be a canonical principal wire id.
func principalFrom(h http.Header) (string, error) {
	got := h.Values(HeaderPrincipalID)
	switch len(got) {
	case 0:
		return "", nil
	case 1:
	default:
		return "", &Rejection{Reason: ReasonDuplicate, Header: HeaderPrincipalID}
	}
	encoded, ok := strings.CutPrefix(got[0], "prn_")
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if !ok || err != nil || len(raw) != 16 || isZero(raw) || "prn_"+base64.RawURLEncoding.EncodeToString(raw) != got[0] {
		return "", &Rejection{Reason: ReasonPrincipal, Header: HeaderPrincipalID}
	}
	return got[0], nil
}

func isZero(raw []byte) bool {
	for _, b := range raw {
		if b != 0 {
			return false
		}
	}
	return true
}

func parseFlag(value, header string) (bool, error) {
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, &Rejection{Reason: ReasonBoolean, Header: header}
}

// ReasonOf returns the rejection reason carried by err.
func ReasonOf(err error) string {
	var rejection *Rejection
	if errors.As(err, &rejection) {
		return rejection.Reason
	}
	return "unknown"
}
