package identity

// StoredLoginKey is the auth.users.email_lower a public.users address projects to.
func StoredLoginKey(email string) string { return storedLoginKey(email) }
