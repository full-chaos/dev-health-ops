// Package authorization is the policy decision core: it answers whether a
// principal may perform a catalog action, from platform and organization
// roles over a state snapshot. It is a pure function: no HTTP, no SQL, no
// clock. Every unknown input denies.
package authorization

// Status is a lifecycle state. The zero value and every unrecognised value
// are not active.
type Status uint8

const (
	StatusNotActive Status = iota
	StatusActive
)

// StatusFromString maps a stored status to a Status. Only the exact string
// "active" is active.
func StatusFromString(s string) Status {
	if s == "active" {
		return StatusActive
	}
	return StatusNotActive
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
	ResourceType   string
	OrganizationID string
}

// Reason is a closed, stable token for a decision.
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
)

// AllReasons lists every Reason the package can return.
func AllReasons() []Reason {
	return []Reason{
		ReasonAllowedByRole, ReasonInvalidRequest, ReasonUnknownAction, ReasonResourceMismatch,
		ReasonPrincipalInactive, ReasonOrganizationInactive, ReasonNoMembership, ReasonOrgMismatch,
		ReasonMembershipInactive, ReasonPlatformRoleAbsent, ReasonRoleUnknown, ReasonRoleScopeMismatch,
		ReasonRoleLacksAction,
	}
}

// Decision is the outcome. The zero value denies.
type Decision struct {
	Allowed bool
	Reason  Reason
	// PolicyRevision is the catalog version the decision was made under; 0
	// when no catalog was available.
	PolicyRevision int
}
