package authschema

// MembershipWriteGrant is the switch-gated access beyond
// RegisterOrganizationGrants that the auth runtime role needs to keep go-api's
// public.memberships equal to auth.memberships while it owns users
// (AUTH_USERS_OWNED): DELETE a row whose membership left active, UPDATE its
// role, and SELECT its id (a removal reads the row it deletes). Off, it is
// revoked.
type MembershipWriteGrant struct {
	Table  string
	Delete bool
	Select []string
	Update []string
}

func MembershipWriteGrants() MembershipWriteGrant {
	return MembershipWriteGrant{}
}
