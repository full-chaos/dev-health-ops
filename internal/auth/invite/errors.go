package invite

import "errors"

var (
	ErrNotOwned = errors.New("invite: auth-service does not own users, so go-api writes invites")
	// ErrInviteInvalid is the one refusal of a token that is unknown, badly
	// signed, expired, accepted or revoked: a caller learns nothing else.
	ErrInviteInvalid   = errors.New("invite: the invite is invalid or expired")
	ErrEmailUnverified = errors.New("invite: the account's email address is not verified")
	ErrAccountInactive = errors.New("invite: the account is not active")
	ErrEmailMismatch   = errors.New("invite: the invite is for another email address")
	ErrEmailInvalid    = errors.New("invite: the email address is not a normalized address")
	ErrRoleInvalid     = errors.New("invite: the role is not an organization role")
	ErrTTLInvalid      = errors.New("invite: the lifetime is outside 1 hour to 30 days")
	ErrPendingExists   = errors.New("invite: a pending invite for this address exists")
	ErrNotFound        = errors.New("invite: invite not found")
	ErrNotPending      = errors.New("invite: the invite is no longer pending")
	ErrAlreadyMember   = errors.New("invite: the account already is a member of the organization")
	ErrOrgNotFound     = errors.New("invite: the organization is not in the auth schema")
	ErrOrgSuspended    = errors.New("invite: the organization is suspended")
	ErrInviterNotFound = errors.New("invite: the inviter is not in the auth schema or in public.users")
	ErrDenied          = errors.New("invite: the actor may not change this invite")
	ErrStore           = errors.New("invite: store unavailable")
)
