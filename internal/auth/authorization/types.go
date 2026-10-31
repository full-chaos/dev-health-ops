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
	// ResourceKey is the canonical repository key ("owner/repo", or "*" for
	// the whole organization); empty for every other resource kind.
	ResourceKey string
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

	ReasonAllowedByRepositoryGrant Reason = "allowed_by_repository_grant"
	ReasonGrantAbsent              Reason = "grant_absent"
	ReasonRepositoryNotGranted     Reason = "repository_not_granted"
	ReasonGrantExpired             Reason = "grant_expired"
	ReasonGrantRevoked             Reason = "grant_revoked"
	ReasonWorkloadAudienceDenied   Reason = "workload_audience_denied"
	ReasonWorkloadActionDenied     Reason = "workload_action_denied"
	ReasonClockUnavailable         Reason = "clock_unavailable"
	ReasonServiceAccountInactive   Reason = "service_account_inactive"

	ReasonAllowedByMembershipDefault Reason = "allowed_by_membership_default"
	ReasonGrantReadFailed            Reason = "grant_read_failed"
)

// AllReasons lists every Reason the package can return.
func AllReasons() []Reason {
	return []Reason{
		ReasonAllowedByRole, ReasonInvalidRequest, ReasonUnknownAction, ReasonResourceMismatch,
		ReasonPrincipalInactive, ReasonOrganizationInactive, ReasonNoMembership, ReasonOrgMismatch,
		ReasonMembershipInactive, ReasonPlatformRoleAbsent, ReasonRoleUnknown, ReasonRoleScopeMismatch,
		ReasonRoleLacksAction, ReasonAllowedByRepositoryGrant, ReasonGrantAbsent, ReasonRepositoryNotGranted,
		ReasonGrantExpired, ReasonGrantRevoked, ReasonWorkloadAudienceDenied,
		ReasonWorkloadActionDenied, ReasonClockUnavailable, ReasonServiceAccountInactive,
		ReasonAllowedByMembershipDefault, ReasonGrantReadFailed,
	}
}

// Decision is the outcome. The zero value denies.
type Decision struct {
	Allowed bool
	Reason  Reason
	// PolicyRevision is the catalog version the decision was made under; 0
	// when no catalog was available.
	PolicyRevision int
	// CacheClass says how long a consumer may reuse the decision. A deny is
	// CacheNone. A repository allow is CacheResourceRevisionBound when a live
	// grant allowed it and CacheDecisionBound when the membership default did,
	// unless the action's catalog class is none. Allows of organization and
	// platform actions leave it empty: the catalog class applies.
	CacheClass CacheClass
	// ExpiresAtUnixNano is the instant a repository allow stops being usable:
	// clock plus RepositoryAllowBoundNanos (30 s), for a live-grant allow and a
	// membership-default allow alike. 0 for every other decision, and for a
	// repository action whose catalog cache class is none.
	ExpiresAtUnixNano int64
}

// CacheClass is the closed allow-cache class of a decision (TRD section 14).
type CacheClass string

const (
	CacheNone                  CacheClass = "none"
	CacheDecisionBound         CacheClass = "decision_bound"
	CacheResourceRevisionBound CacheClass = "resource_revision_bound"
)
