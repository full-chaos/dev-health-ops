package goapiproof

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// TokenFileKind describes one credential kind whose value comes from a file
// the operator stages (never argv, never a corpus file, never printed): what
// it is, which prover flag names the file, and how its shape is checked. The
// prover reads the file on EVERY use (a zero freshness window), so a token
// rotated or revoked during a run is picked up by the next request.
type TokenFileKind struct {
	Kind RESTCredentialKind
	// Flag is the prover flag that names the file, with its leading dash.
	Flag string
	// What names the credential in errors, for example "push token".
	What string
	// Label is the credential's label in reports and errors.
	Label string
	// Validate checks a token's shape; the value is never in its error.
	Validate func(string) error
}

// TokenFileKinds returns every credential kind fed from a file, in a fixed
// order. RESTCredentialRun is not one of them: its bearers are minted.
func TokenFileKinds() []TokenFileKind {
	return []TokenFileKind{
		{Kind: RESTCredentialPushToken, Flag: "-push-token-file", What: "external-ingest push token", Label: "push bearer", Validate: ValidatePushTokenShape},
		{Kind: RESTCredentialOrgAdmin, Flag: "-org-admin-token-file", What: "org-admin access token", Label: "org-admin bearer", Validate: ValidateAccessTokenShape},
		{Kind: RESTCredentialPlatformSuperadmin, Flag: "-platform-token-file", What: "platform-superadmin access token", Label: "platform-superadmin bearer", Validate: ValidateAccessTokenShape},
	}
}

// TokenFileKindFor returns the descriptor of a file-fed kind.
func TokenFileKindFor(kind RESTCredentialKind) (TokenFileKind, bool) {
	for _, candidate := range TokenFileKinds() {
		if candidate.Kind == kind {
			return candidate, true
		}
	}
	return TokenFileKind{}, false
}

// ValidateAccessTokenShape rejects a value that cannot be an edge access
// token: a JWT, three non-empty base64url segments. The value is never
// included in the error.
func ValidateAccessTokenShape(value string) error { return ValidateEnvelopeShape(value) }

// TokenFileCredential builds the credential for a file-fed kind: the file is
// read on every use, trimmed, and shape-checked; its content never appears in
// an error, a log or a receipt.
func TokenFileCredential(kind TokenFileKind, path string) *Credential {
	return MintedCredential("Authorization", kind.Label, 0, func(context.Context) (string, error) {
		raw, err := os.ReadFile(path) // #nosec G304 -- operator-supplied token file path, by design
		if err != nil {
			return "", fmt.Errorf("read the %s file: %w", kind.What, err)
		}
		return strings.TrimSpace(string(raw)), nil
	}).WithShapeValidator(kind.Validate)
}

// CheckTokenFile reads a token file once and checks its shape, so a run can
// refuse before it sends anything when the file is missing, unreadable or does
// not hold a token of this kind. The value is never included in the error.
func CheckTokenFile(kind TokenFileKind, path string) error {
	raw, err := os.ReadFile(path) // #nosec G304 -- operator-supplied token file path, by design
	if err != nil {
		return fmt.Errorf("read the %s file: %w", kind.What, err)
	}
	if err := kind.Validate(strings.TrimSpace(string(raw))); err != nil {
		return fmt.Errorf("the %s file does not hold a %s: %w", kind.What, kind.What, err)
	}
	return nil
}
