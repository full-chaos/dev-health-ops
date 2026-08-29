package providerfoundation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// credentialIdentifierKeys/credentialSecretKeys/CredentialFingerprint port
// src/dev_health_ops/credentials/fingerprint.py's safe_credential_scope /
// credential_fingerprint byte-for-byte (CHAOS-2755, surfaced for Go by
// CHAOS-4431 codex review P1 + team-lead ruling 2026-08-28: "not accepted as
// a cut... it is the whole point of CHAOS-2755"). The SAME field lists, the
// SAME safe-scope shape, hashed the SAME way, so a stamped
// sync_runs.credential_fingerprint computed by Python at plan time verifies
// correctly against a credential resolved here in Go. Getting any field
// list, key name, or serialization byte wrong would make every run fail
// closed on a FALSE mismatch -- worse than no check at all -- so this is a
// direct line-for-line port, not a reinterpretation. Lives in
// providerfoundation (not a single worker's package) because it operates
// only on Credential's own Config/Secret accessors and belongs next to them;
// any future claimed-unit run-auth-freeze verification reuses it unchanged.
var credentialIdentifierKeys = []string{
	"app_id", "installation_id", "email", "cloud_id", "cloudId",
	"client_id", "clientId", "user_id", "username", "group_id",
	"project_id", "project_key", "environment", "schema_version",
	"organization_id", "workspace_id", "team_id", "oauth_binding_id",
}

var credentialSecretKeys = []string{
	"token", "private_token", "access_token", "accessToken",
	"refresh_token", "refreshToken", "api_token", "apiToken",
	"api_key", "apiKey", "private_key", "privateKey",
	"client_secret", "clientSecret",
}

// CredentialFingerprint returns the SAME SHA-256 hex digest Python's
// credential_fingerprint() would, given the same decrypted credential
// content. credential.Config carries non-secret identifiers/base_url (plain
// strings, matching Python's identifier fields exactly -- no int/float JSON-
// formatting ambiguity is possible here); credential.Secret(key) carries the
// secret-bearing fields Python reads from the same decrypted mapping.
func CredentialFingerprint(credential Credential, credentialID, integrationID string) string {
	scope := safeCredentialScope(credential, credentialID, integrationID)
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	// Python's json.dumps never HTML-escapes '<','>','&'; Go's encoder does
	// by default. Without disabling it, a credential value containing any of
	// those characters would serialize to DIFFERENT bytes than Python wrote
	// at plan time, hashing to a fingerprint that can never match -- a
	// silent false-mismatch bug, not a cosmetic one.
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(scope); err != nil {
		// Unreachable for a map[string]string, but fail to an impossible
		// digest rather than panic -- this can never equal a real stamped
		// fingerprint, so it fails closed exactly like a genuine mismatch.
		return ""
	}
	// json.Encoder.Encode appends a trailing newline Python's json.dumps
	// never emits; strip it so the hashed bytes match exactly.
	payload := bytes.TrimRight(buffer.Bytes(), "\n")
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// safeCredentialScope ports safe_credential_scope() exactly, including its
// two Python-specific edge cases:
//   - "is not None" (an IDENTIFIER key copies even a PRESENT empty string;
//     only an ABSENT key is skipped) -- modeled by Config's two-value lookup.
//   - "A or B" for base_url/baseUrl (an empty-but-PRESENT "base_url" still
//     falls through to "baseUrl", exactly like Python's falsy-string `or`).
func safeCredentialScope(credential Credential, credentialID, integrationID string) map[string]string {
	scope := make(map[string]string)
	for _, key := range credentialIdentifierKeys {
		if value, ok := credential.Config[key]; ok {
			scope[key] = value
		}
	}
	baseURLA, okA := credential.Config["base_url"]
	baseURLB, okB := credential.Config["baseUrl"]
	resolvedBaseURL, resolvedPresent := baseURLB, okB
	if okA && baseURLA != "" {
		resolvedBaseURL, resolvedPresent = baseURLA, true
	}
	if resolvedPresent {
		scope["base_url"] = strings.TrimRight(strings.TrimSpace(resolvedBaseURL), "/")
	}
	for _, key := range credentialSecretKeys {
		secret, ok := credential.Secret(key)
		if !ok {
			continue
		}
		revealed := secret.Reveal()
		if revealed == "" {
			continue
		}
		sum := sha256.Sum256([]byte(revealed))
		scope[key+"_sha256"] = hex.EncodeToString(sum[:])
	}
	if len(scope) == 0 {
		fallbackCredentialID := credentialID
		if fallbackCredentialID == "" {
			fallbackCredentialID = "env"
		}
		return map[string]string{"credential_id": fallbackCredentialID, "integration_id": integrationID}
	}
	return scope
}
