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
package internalidentity

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// The header names are a cross-repo contract (the Python edge sends them,
// the Ingress strips them). Do not rename without the deploy PR.
const (
	HeaderOrgID               = "X-DH-Internal-Org-Id"
	HeaderRole                = "X-DH-Internal-Role"
	HeaderSuperuser           = "X-DH-Internal-Superuser"
	HeaderImpersonationActive = "X-DH-Internal-Impersonation-Active"
)

// Headers lists every internal identity header name.
var Headers = []string{HeaderOrgID, HeaderRole, HeaderSuperuser, HeaderImpersonationActive}

// Rejection reasons. They name the class of failure, never a value.
const (
	ReasonMissing   = "missing_header"
	ReasonDuplicate = "duplicate_header"
	ReasonBoolean   = "not_a_boolean"
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
	for _, name := range Headers {
		if len(h.Values(name)) > 0 {
			return true
		}
	}
	return false
}

// FromHeader reads the identity. All four headers must be present exactly
// once; the two flags must be exactly "true" or "false". A missing header
// is never defaulted: a partial set could otherwise mean "no role, not a
// superuser" by accident, and the fail-closed reading is a refusal.
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
	claims.OrgID = values[HeaderOrgID]
	claims.Role = values[HeaderRole]
	claims.IsSuperuser = superuser
	claims.ImpersonationActive = impersonating
	return claims, nil
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
