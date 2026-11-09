//go:build integration

package platformrole

// allowDisabledTarget is the grant input the user-admin routes send.
func allowDisabledTarget(in GrantInput) GrantInput {
	in.AllowDisabledTarget = true
	return in
}
