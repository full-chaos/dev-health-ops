package goapiproof

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// PushTokenPrefix is what every external-ingest (customer push) bearer starts
// with. The token is a customer-script credential, not an identity the mint
// helpers can produce, so the prover reads it from a file the operator
// stages; it is never taken from argv and never printed.
const PushTokenPrefix = "fcpush_"

// PushTokenFileCredential builds the credential for corpus entries that
// authenticate with an external-ingest push token. The token is read from
// path on EVERY use (a zero freshness window: nothing is cached, so a token
// rotated or revoked during a run is picked up by the next request), trimmed,
// and checked for shape; the file's content never appears in an error, a log
// or a receipt, only what is wrong with it.
func PushTokenFileCredential(path string) *Credential {
	return MintedCredential("Authorization", "push bearer", 0, func(context.Context) (string, error) {
		raw, err := os.ReadFile(path) // #nosec G304 -- operator-supplied token file path, by design
		if err != nil {
			return "", fmt.Errorf("read the push token file: %w", err)
		}
		return strings.TrimSpace(string(raw)), nil
	}).WithShapeValidator(ValidatePushTokenShape)
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
