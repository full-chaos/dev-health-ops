package authapi

import (
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// The named principal reference (CHAOS-13945): query-api's header carrier names the principal behind an acr
// credential by its wire id (prn_...), and query-api forwards it as the principal_reference. Only the one bound
// caller may present it; it has no session (acr-api's per-call access read checks liveness), and it is decided
// without the principal's platform role (acp Q5), so a platform admin named on the carrier gets its member answer.

// NamedPrincipalReferences resolves the named principal reference kind for its bound caller.
type NamedPrincipalReferences struct {
	caller string
}

// NewNamedPrincipalReferences binds the kind to caller, a workload principal id (prn_).
func NewNamedPrincipalReferences(caller string) (*NamedPrincipalReferences, error) {
	if id, ok := parseWirePrincipal(caller); !ok || session.WirePrincipalID(id) != caller {
		return nil, errors.New("authapi: the named principal reference caller is not a principal id")
	}
	return &NamedPrincipalReferences{caller: caller}, nil
}

// Caller is the workload principal that may present the kind.
func (n *NamedPrincipalReferences) Caller() string { return n.caller }

func (n *NamedPrincipalReferences) resolve(reference string) (ReferencedPrincipal, string, error) {
	id, ok := parseWirePrincipal(reference)
	if !ok || session.WirePrincipalID(id) != reference {
		return ReferencedPrincipal{}, "named_malformed", nil
	}
	return ReferencedPrincipal{ID: id, Kind: ReferenceNamedPrincipal}, "", nil
}

// namedSnapshot is the principal's snapshot without its platform role.
type namedSnapshot struct {
	authorization.GrantSnapshot
}

func (namedSnapshot) PlatformRole() string { return "" }

func (n namedSnapshot) RepositoryID(orgID, path string) (string, bool) {
	if ri, ok := n.GrantSnapshot.(authorization.RepositoryIdentities); ok {
		return ri.RepositoryID(orgID, path)
	}
	return "", false
}

func (n namedSnapshot) PathForID(orgID, id string) (string, bool) {
	if ri, ok := n.GrantSnapshot.(authorization.RepositoryIdentities); ok {
		return ri.PathForID(orgID, id)
	}
	return "", false
}

func (n namedSnapshot) HasLiveRepositoryIdentity(orgID string) bool {
	c, ok := n.GrantSnapshot.(authorization.RepositoryIdentityCopy)
	return ok && c.HasLiveRepositoryIdentity(orgID)
}
