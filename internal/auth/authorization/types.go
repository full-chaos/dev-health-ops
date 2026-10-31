// Package authorization is the policy decision core: it answers whether a
// principal may perform a catalog action, from platform and organization
// roles over a state snapshot. It is a pure function: no HTTP, no SQL, no
// clock. Every unknown input denies. The wire request, response and reason
// codes belong to the callers, not here.
package authorization

// Status is a lifecycle state. The zero value and every unrecognised value
// are not active; only StatusActive permits a decision to proceed.
type Status uint8

const (
	StatusUnknown Status = iota
	StatusActive
	StatusInvited
	StatusSuspended
	StatusRemoved
)

// ParseStatus maps a stored status string to a Status. Only the exact
// lowercase names are recognised; anything else is StatusUnknown.
func ParseStatus(s string) Status {
	switch s {
	case "active":
		return StatusActive
	case "invited":
		return StatusInvited
	case "suspended":
		return StatusSuspended
	case "removed":
		return StatusRemoved
	}
	return StatusUnknown
}

// Membership is one organization membership row as read from state.
type Membership struct {
	OrgID  string
	Role   string
	Status Status
}

// Snapshot is the read-only state the decision is made over. An
// implementation reports absence as the zero value, which denies.
type Snapshot interface {
	PrincipalStatus() Status
	// PlatformRole is the principal's platform role, or "" when none.
	PlatformRole() string
	OrganizationStatus(orgID string) Status
	Membership(orgID string) (Membership, bool)
}

// Request is one authorization question. It carries no authority.
type Request struct {
	Action         string
	ResourceKind   string
	OrganizationID string
}

// Reason is a closed, internal token for a decision.
type Reason string

const (
	ReasonAllowedByRole        Reason = "allowed_by_role"
	ReasonInvalidRequest       Reason = "invalid_request"
	ReasonUnknownAction        Reason = "unknown_action"
	ReasonResourceMismatch     Reason = "resource_mismatch"
	ReasonPrincipalInactive    Reason = "principal_inactive"
	ReasonOrganizationInactive Reason = "organization_inactive"
	ReasonNoMembership         Reason = "no_membership"
	ReasonOrgMismatch          Reason = "org_mismatch"
	ReasonMembershipInactive   Reason = "membership_inactive"
	ReasonPlatformRoleAbsent   Reason = "platform_role_absent"
	ReasonRoleUnknown          Reason = "role_unknown"
	ReasonRoleScopeMismatch    Reason = "role_scope_mismatch"
	ReasonRoleLacksAction      Reason = "role_lacks_action"

	ReasonImpersonationDenied Reason = "impersonation_denied"
	ReasonDelegationInvalid   Reason = "delegation_invalid"
)

// AllReasons lists every Reason the package can return.
func AllReasons() []Reason {
	return []Reason{
		ReasonAllowedByRole, ReasonInvalidRequest, ReasonUnknownAction, ReasonResourceMismatch,
		ReasonPrincipalInactive, ReasonOrganizationInactive, ReasonNoMembership, ReasonOrgMismatch,
		ReasonMembershipInactive, ReasonPlatformRoleAbsent, ReasonRoleUnknown, ReasonRoleScopeMismatch,
		ReasonRoleLacksAction, ReasonImpersonationDenied, ReasonDelegationInvalid,
	}
}

// Decision is the outcome. The zero value denies.
type Decision struct {
	Allowed bool
	Reason  Reason
	// PolicyRevision is the catalog version the decision was made under; 0
	// when no catalog was available.
	PolicyRevision int
	// Obligations is set only on an allow of a delegated session.
	Obligations *Obligations
	// Audit is set only on an allow that the owner exemption made possible.
	Audit *DelegationAudit
}
