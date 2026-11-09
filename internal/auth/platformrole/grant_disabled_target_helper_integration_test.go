//go:build integration

package platformrole

// allowDisabledTarget is the grant input the user-admin routes send. Before GrantInput.AllowDisabledTarget exists
// it is the input unchanged: the grant those routes made.
func allowDisabledTarget(in GrantInput) GrantInput { return in }
