package membership

import "errors"

var (
	ErrNotOwned          = errors.New("membership: auth-service does not own users, so go-api writes memberships")
	ErrRoleInvalid       = errors.New("membership: the role is not an organization role")
	ErrNotMember         = errors.New("membership: the principal is not a member of the organization")
	ErrAlreadyMember     = errors.New("membership: the principal already is a member of the organization")
	ErrSuspended         = errors.New("membership: the membership is suspended")
	ErrLastOwner         = errors.New("membership: the organization would have no owner")
	ErrNotOwner          = errors.New("membership: the source principal is not an owner")
	ErrTargetNotMember   = errors.New("membership: the target principal is not a member")
	ErrRoleMissing       = errors.New("membership: the membership has no role to reactivate with")
	ErrRevisionConflict  = errors.New("membership: the membership changed since the given revision")
	ErrOrgNotFound       = errors.New("membership: the organization is not in the auth schema")
	ErrOrgSuspended      = errors.New("membership: the organization is suspended")
	ErrPrincipalNotFound = errors.New("membership: the principal is not in the auth schema or in public.users")
	ErrDenied            = errors.New("membership: the actor may not change this membership")
	ErrStore             = errors.New("membership: store unavailable")
)
