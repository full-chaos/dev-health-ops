package datahealth

// The two functions below are the comparison rules this package already
// applies to the org's identities (identityKeys, decodeProviderIdentities),
// exported so that another reader of the `identities` table (reviewEdges'
// display names, CHAOS-8485) compares identity values the same way and does
// not carry a second copy of the rules.

// IdentityKey is the form under which an identity value (an e-mail address, a
// provider login, a display name, a canonical id) is compared: stripped,
// lower-cased, runs of whitespace collapsed. Two values that are the same
// identity have the same key.
func IdentityKey(value string) string { return norm(value) }

// ProviderIdentityValues decodes the `provider_identities` column of an
// identity (a JSON object of lists) into its values, in the column's own
// order. Anything that is not such an object decodes to no values.
func ProviderIdentityValues(text string) []string {
	var values []string
	for _, group := range decodeProviderIdentities(text) {
		values = append(values, group...)
	}
	return values
}
