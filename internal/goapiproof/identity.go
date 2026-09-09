package goapiproof

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// AuthContext is the SHAPE of the caller identity a request was made
// with -- never the credential itself.
//
// This exists because go_api_proof_run.request_identity is documented as
// "digest of variables + auth-context shape + org_id", and a digest over
// the raw token would (a) put credential material into a durable table's
// input and (b) change on every token rotation, making two otherwise
// identical proof runs incomparable. The SHAPE is what actually
// distinguishes one request from another: which principal kind, which
// audience, which key id signed it.
type AuthContext struct {
	// PrincipalKind is e.g. "stored_account" or "service".
	PrincipalKind string
	// Audience is the envelope audience the edge minted for.
	Audience string
	// KeyID is the signing key id (`kid`) -- a public identifier, and the
	// thing that silently broke routing three times on 2026-09-07 when it
	// disagreed with the JWKS (Trap #84), so it belongs in the identity.
	KeyID string
	// Scopes are the granted scopes, order-insensitive.
	Scopes []string
}

// RequestIdentity is the digest go_api_proof_run.request_identity stores:
// sha256 over a canonical JSON encoding of (org_id, auth-context shape,
// variables).
//
// Canonical by construction: encoding/json emits map keys in sorted
// order, and Scopes is sorted here, so two runs of the same logical
// request always produce the same bytes. No credential value is ever an
// input -- see AuthContext.
func RequestIdentity(orgID string, auth AuthContext, variables map[string]any) (string, error) {
	scopes := append([]string(nil), auth.Scopes...)
	sort.Strings(scopes)

	payload := map[string]any{
		"org_id": orgID,
		"auth": map[string]any{
			"principal_kind": auth.PrincipalKind,
			"audience":       auth.Audience,
			"key_id":         auth.KeyID,
			"scopes":         scopes,
		},
		"variables": variables,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("goapiproof: encode request identity: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
