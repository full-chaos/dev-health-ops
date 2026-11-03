package policy

// DelegatedRows names the rows a delegated session's bearer may reach:
// allowed reports the delegated_bearer value of the row of a route template
// ("METHOD /pattern"). Call it once, before the Authenticator serves. Without
// it no row admits that bearer.
func (a *Authenticator) DelegatedRows(allowed func(profile string) bool) { a.delegated = allowed }

// DelegatedAllowed reports whether the row of profile admits a delegated
// session's bearer.
func (a *Authenticator) DelegatedAllowed(profile string) bool {
	return a.delegated != nil && a.delegated(profile)
}
