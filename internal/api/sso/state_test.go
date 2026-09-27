package sso

import (
	"encoding/base64"
	"testing"
	"time"
)

// These are white-box unit tests of the AEAD primitive itself
// (mintOIDCState/verifyOIDCState/deriveStateKey), independent of any HTTP
// route or database -- fast, no build tag, no fixtures beyond a secret
// string. oidc_integration_test.go covers the same mechanism through the
// real handler chain (tampered state, expired state, provider mismatch,
// wrong nonce); this file is the direct, minimal version D2727-amended's
// review asked for: happy, tampered ciphertext, tampered AAD/provider
// mismatch, expired, wrong key.

const (
	stateTestSecretA = "state-test-secret-a-0123456789abcdef"
	stateTestSecretB = "state-test-secret-b-fedcba9876543210"
	stateTestOrg     = "22222222-2222-4222-8222-222222222222"
)

func mustMint(t *testing.T, secret, providerID, orgID string, now time.Time) string {
	t.Helper()
	token, err := mintOIDCState(secret, oidcState{ProviderID: providerID, OrgID: orgID, Nonce: "n1", CodeVerifier: "v1",
		NonceHash: hashOIDCLoginNonce("test-login-nonce")}, now)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	return token
}

func TestOIDCStateHappyRoundTrip(t *testing.T) {
	now := time.Now()
	providerID := "11111111-1111-4111-8111-111111111111"
	token := mustMint(t, stateTestSecretA, providerID, stateTestOrg, now)
	state, err := verifyOIDCState(stateTestSecretA, token, providerID, now.Add(1*time.Minute))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if state.ProviderID != providerID || state.OrgID != stateTestOrg || state.Nonce != "n1" || state.CodeVerifier != "v1" {
		t.Fatalf("state = %+v, want the minted fields back", state)
	}
	if state.ID == "" {
		t.Fatal("state.ID: want a random id, got empty")
	}
}

func TestOIDCStateRefusesATamperedCiphertext(t *testing.T) {
	now := time.Now()
	providerID := "11111111-1111-4111-8111-111111111111"
	token := mustMint(t, stateTestSecretA, providerID, stateTestOrg, now)
	tampered := flipOneChar(token)
	if _, err := verifyOIDCState(stateTestSecretA, tampered, providerID, now); err == nil {
		t.Fatal("verify: want an error for a tampered ciphertext, got nil")
	}
}

func TestOIDCStateRefusesAMismatchedProviderAAD(t *testing.T) {
	now := time.Now()
	mintedFor := "11111111-1111-4111-8111-111111111111"
	checkedAgainst := "22222222-2222-4222-8222-222222222299"
	token := mustMint(t, stateTestSecretA, mintedFor, stateTestOrg, now)
	// The token authenticates fine against the provider it was minted
	// for...
	if _, err := verifyOIDCState(stateTestSecretA, token, mintedFor, now); err != nil {
		t.Fatalf("verify against the minting provider: %v", err)
	}
	// ...and is refused, at the AEAD layer, before any JSON parsing,
	// against any other provider id -- not merely detected afterward by
	// comparing a decrypted field.
	if _, err := verifyOIDCState(stateTestSecretA, token, checkedAgainst, now); err == nil {
		t.Fatal("verify against a different provider: want an error, got nil")
	}
}

func TestOIDCStateRefusesAnExpiredToken(t *testing.T) {
	now := time.Now()
	providerID := "11111111-1111-4111-8111-111111111111"
	token := mustMint(t, stateTestSecretA, providerID, stateTestOrg, now)
	_, err := verifyOIDCState(stateTestSecretA, token, providerID, now.Add(oidcStateTTL+time.Minute))
	if err == nil {
		t.Fatal("verify past expiry: want an error, got nil")
	}
	if err != errOIDCStateExpired {
		t.Fatalf("verify past expiry: err = %v, want errOIDCStateExpired (the tag still authenticated; only expiry should distinguish this case)", err)
	}
}

func TestOIDCStateRefusesTheWrongKey(t *testing.T) {
	now := time.Now()
	providerID := "11111111-1111-4111-8111-111111111111"
	token := mustMint(t, stateTestSecretA, providerID, stateTestOrg, now)
	if _, err := verifyOIDCState(stateTestSecretB, token, providerID, now); err == nil {
		t.Fatal("verify under a different secret: want an error, got nil")
	}
}

func TestDeriveStateKeyRefusesAnEmptySecret(t *testing.T) {
	if _, err := deriveStateKey("", oidcStateHKDFInfo); err == nil {
		t.Fatal("deriveStateKey(\"\"): want an error, got nil")
	}
}

func TestDeriveStateKeyIsDeterministicAndSecretDependent(t *testing.T) {
	a1, err := deriveStateKey(stateTestSecretA, oidcStateHKDFInfo)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := deriveStateKey(stateTestSecretA, oidcStateHKDFInfo)
	if err != nil {
		t.Fatal(err)
	}
	if a1 != a2 {
		t.Fatal("deriveStateKey: not deterministic for the same secret")
	}
	b, err := deriveStateKey(stateTestSecretB, oidcStateHKDFInfo)
	if err != nil {
		t.Fatal(err)
	}
	if a1 == b {
		t.Fatal("deriveStateKey: two different secrets derived the same key")
	}
}

// TestDeriveStateKeyIsInfoDependent is D2744's own coverage note (an r1
// reviewer flagged this as untested when reviewing CHAOS-6986's OAuth
// HKDF separation from OIDC's): the SAME secret through a DIFFERENT info
// string must derive a DIFFERENT key, or samlStateHKDFInfo/
// oauthStateHKDFInfo collapsing to the same value silently would not be
// caught by anything.
func TestDeriveStateKeyIsInfoDependent(t *testing.T) {
	oidcKey, err := deriveStateKey(stateTestSecretA, oidcStateHKDFInfo)
	if err != nil {
		t.Fatal(err)
	}
	samlKey, err := deriveStateKey(stateTestSecretA, samlStateHKDFInfo)
	if err != nil {
		t.Fatal(err)
	}
	if oidcKey == samlKey {
		t.Fatal("deriveStateKey: oidcStateHKDFInfo and samlStateHKDFInfo derived the same key from the same secret")
	}
}

// flipOneChar flips one bit in the ciphertext's MIDDLE byte (decode,
// mutate, re-encode) and always changes the decoded bytes: flipping a
// base64 CHARACTER can silently leave the decoded bytes unchanged when
// the flip lands on a padding-only bit range of the final character
// group, which a middle byte never has.
func flipOneChar(s string) string {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) == 0 {
		return s + "x"
	}
	raw[len(raw)/2] ^= 0xFF
	return base64.RawURLEncoding.EncodeToString(raw)
}
