package goapiproof

import (
	"fmt"
	"strings"
)

// PushTokenPrefix is what every external-ingest (customer push) bearer starts
// with. The token is a customer-script credential, not an identity the mint
// helpers can produce, so the prover reads it from a file the operator
// stages; it is never taken from argv and never printed.
const PushTokenPrefix = "fcpush_"

// PushTokenFileCredential builds the credential for corpus entries that
// authenticate with an external-ingest push token (see TokenFileCredential:
// read on every use, shape-checked, never printed).
func PushTokenFileCredential(path string) *Credential {
	kind, _ := TokenFileKindFor(RESTCredentialPushToken)
	return TokenFileCredential(kind, path)
}

// ValidatePushTokenShape rejects a value that cannot be an external-ingest
// push bearer: it must carry the fcpush_ prefix and one non-empty token with
// no whitespace. The value is never included in the error.
func ValidatePushTokenShape(value string) error {
	token := strings.TrimPrefix(strings.TrimSpace(value), "Bearer ")
	if !strings.HasPrefix(token, PushTokenPrefix) || len(token) == len(PushTokenPrefix) {
		return fmt.Errorf("expected a %s-prefixed token", PushTokenPrefix)
	}
	if strings.ContainsAny(token, " \t\r\n") {
		return fmt.Errorf("the token contains whitespace, so this is not a token")
	}
	return nil
}

// CheckPushTokenFile reads the push token file once and checks its shape (see
// CheckTokenFile).
func CheckPushTokenFile(path string) error {
	kind, _ := TokenFileKindFor(RESTCredentialPushToken)
	return CheckTokenFile(kind, path)
}
