package serviceaccount

import "errors"

var (
	ErrDenied         = errors.New("serviceaccount: the actor may not change service accounts of the organization")
	ErrNotOwned       = errors.New("serviceaccount: auth-service does not own users, so the verb is not served")
	ErrNotFound       = errors.New("serviceaccount: service account not found")
	ErrNameInvalid    = errors.New("serviceaccount: the name is outside the service account name pattern")
	ErrNameTaken      = errors.New("serviceaccount: the organization has a service account with that name")
	ErrOrgNotFound    = errors.New("serviceaccount: the organization is not in the auth schema")
	ErrOrgNotActive   = errors.New("serviceaccount: the organization is suspended")
	ErrOwnerNotUser   = errors.New("serviceaccount: the owner is not a user principal")
	ErrOwnerDisabled  = errors.New("serviceaccount: the owner principal is disabled")
	ErrOwnerNotMember = errors.New("serviceaccount: the owner is not an active member of the organization")
	ErrAlreadyInState = errors.New("serviceaccount: the service account already is in that state")
	ErrNotAccount     = errors.New("serviceaccount: the principal is not a service account")
	ErrStateChanged   = errors.New("serviceaccount: the service account changed between the read and the locks")
	ErrStore          = errors.New("serviceaccount: store unavailable")

	ErrAccountDisabled = errors.New("serviceaccount: the service account or its principal is disabled")
	ErrKeyExpiry       = errors.New("serviceaccount: a key expiry must be above zero and at most 365 days")
	ErrKeyOverlap      = errors.New("serviceaccount: a rotation overlap must be from zero to 24 hours")
	ErrKeyReason       = errors.New("serviceaccount: the revoke reason is outside compromised and owner_request")
	ErrKeyLive         = errors.New("serviceaccount: the service account has a live key; rotate it")
	ErrKeyRotating     = errors.New("serviceaccount: the service account's rotation is still in its overlap")
	ErrKeyNone         = errors.New("serviceaccount: the service account has no live key; issue one")
	ErrKeyNotFound     = errors.New("serviceaccount: key not found")
)

var refusals = []error{ErrDenied, ErrNotOwned, ErrNotFound, ErrNameInvalid, ErrNameTaken, ErrOrgNotFound, ErrOrgNotActive, ErrOwnerNotUser,
	ErrOwnerDisabled, ErrOwnerNotMember, ErrAlreadyInState, ErrNotAccount, ErrStateChanged}

// keyRefusals are the refusals of the key verbs.
var keyRefusals = append(append([]error(nil), refusals...), ErrAccountDisabled, ErrKeyExpiry, ErrKeyOverlap, ErrKeyReason, ErrKeyLive,
	ErrKeyRotating, ErrKeyNone, ErrKeyNotFound)
