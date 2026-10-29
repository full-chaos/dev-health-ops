package authschema

// InviteWriteGrant is one switch-gated column grant set on a go-api table the
// invite aggregate keeps equal to its own rows while auth-service owns users
// (AUTH_USERS_OWNED; internal/auth/invite).
type InviteWriteGrant struct {
	Table  string
	Select []string
	Insert []string
	Update []string
}

func InviteWriteGrants() []InviteWriteGrant { return nil }
