package datahealth

import "strings"

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

// ProviderIdentityValuesOf is ProviderIdentityValues for the providers whose
// key (lower case) is in the set: the values of the other providers are left out.
func ProviderIdentityValuesOf(text string, include func(provider string) bool) []string {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	decoded, ok := decodeOrdered(text)
	if !ok {
		return nil
	}
	object, ok := decoded.(*orderedMap)
	if !ok {
		return nil
	}
	var values []string
	for i, key := range object.keys {
		if include(strings.ToLower(strings.TrimSpace(key))) {
			values = append(values, providerValues(object.vals[i])...)
		}
	}
	return values
}
