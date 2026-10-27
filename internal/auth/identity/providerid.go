package identity

import "errors"

// ErrInvalidProviderID refuses a public.users row of a non-local provider whose provider id is missing or that
// ValidSubject refuses. The text names no value.
var ErrInvalidProviderID = errors.New("identity: a non-local provider user needs a valid provider id")

// RefuseInvalidProviderID is the one check every writer of public.users.auth_provider_id runs before it inserts: nil
// for a local or password user (provider "" or "local") and for a service account (IsServiceProvider); for any other
// provider, ErrInvalidProviderID unless providerID is set and passes ValidSubject, the rule the users-owned readiness
// guard and the users backfill apply.
func RefuseInvalidProviderID(provider string, providerID *string) error {
	if provider == "" || provider == "local" || IsServiceProvider(&provider) {
		return nil
	}
	if providerID == nil || !ValidSubject(*providerID) {
		return ErrInvalidProviderID
	}
	return nil
}
