package serviceaccount

import "errors"

var (
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
)

var refusals = []error{ErrNotOwned, ErrNotFound, ErrNameInvalid, ErrNameTaken, ErrOrgNotFound, ErrOrgNotActive, ErrOwnerNotUser,
	ErrOwnerDisabled, ErrOwnerNotMember, ErrAlreadyInState, ErrNotAccount, ErrStateChanged}
