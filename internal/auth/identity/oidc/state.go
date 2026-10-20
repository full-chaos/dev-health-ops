package oidc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"time"

	"golang.org/x/crypto/hkdf"
)

// stateTTL bounds the round trip at the IdP (go-api oidcStateTTL).
const stateTTL = 10 * time.Minute

// stateHKDFInfo derives this service's state key. It differs from go-api's
// info string, so a state minted by one service never opens in the other.
const stateHKDFInfo = "dev-health-ops:auth-service:oidc-state-aead-v1"

// maxStateBytes bounds the encoded state before any decoding work.
const maxStateBytes = 4096

// flowState is sealed into the opaque state value. Its secrets (the protocol
// nonce and the PKCE verifier) are never stored server side: the database
// holds only a hash of ID, which makes the flow single-use.
type flowState struct {
	ProviderID   string `json:"provider_id"`
	OrgID        string `json:"org_id"`
	Nonce        string `json:"nonce"`
	CodeVerifier string `json:"code_verifier"`
	RedirectURI  string `json:"redirect_uri"`
	NonceHash    string `json:"nonce_hash"`
	ID           string `json:"id"`
	IssuedAt     int64  `json:"issued_at"`
	ExpiresAt    int64  `json:"expires_at"`
}

var (
	errStateInvalid = errors.New("oidc: state does not authenticate")
	errStateExpired = errors.New("oidc: state expired")
)

func randomURLSafe(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func newStateAEAD(secret, info string) (cipher.AEAD, error) {
	if secret == "" {
		return nil, errors.New("oidc: no state secret")
	}
	var key [32]byte
	if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(secret), nil, []byte(info)), key[:]); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// sealWith seals v under the key derived with info, with aad (the provider
// id) as additional data.
func sealWith(secret, info, aad string, v any) (string, error) {
	aead, err := newStateAEAD(secret, info)
	if err != nil {
		return "", err
	}
	plaintext, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(aead.Seal(nonce, nonce, plaintext, []byte(aad))), nil
}

// openWith authenticates token under the key derived with info and aad
// before parsing it into v; any failure is errStateInvalid.
func openWith(secret, info, token, aad string, v any) error {
	if token == "" || len(token) > maxStateBytes {
		return errStateInvalid
	}
	aead, err := newStateAEAD(secret, info)
	if err != nil {
		return errStateInvalid
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) < aead.NonceSize()+aead.Overhead() {
		return errStateInvalid
	}
	plaintext, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte(aad))
	if err != nil {
		return errStateInvalid
	}
	if err := json.Unmarshal(plaintext, v); err != nil {
		return errStateInvalid
	}
	return nil
}

// sealState stamps ID, IssuedAt and ExpiresAt and seals the state with the
// provider id as additional data.
func sealState(secret string, state flowState, now time.Time) (string, flowState, error) {
	var err error
	if state.ID, err = randomURLSafe(16); err != nil {
		return "", flowState{}, err
	}
	state.IssuedAt = now.Unix()
	state.ExpiresAt = now.Add(stateTTL).Unix()
	token, err := sealWith(secret, stateHKDFInfo, state.ProviderID, state)
	if err != nil {
		return "", flowState{}, err
	}
	return token, state, nil
}

// openState authenticates the state for providerID before parsing it. A
// tampered value, a value for another provider and a malformed value are all
// errStateInvalid; an authentic but expired one is errStateExpired.
func openState(secret, token, providerID string, now time.Time) (flowState, error) {
	var state flowState
	if err := openWith(secret, stateHKDFInfo, token, providerID, &state); err != nil {
		return flowState{}, err
	}
	if state.ProviderID != providerID || state.OrgID == "" || state.Nonce == "" || state.CodeVerifier == "" ||
		state.ID == "" || state.NonceHash == "" || state.RedirectURI == "" {
		return flowState{}, errStateInvalid
	}
	if now.Unix() > state.ExpiresAt {
		return flowState{}, errStateExpired
	}
	return state, nil
}

// oauthStateHKDFInfo derives the OAuth state key: a key of its own, so an
// OIDC state never opens as an OAuth state and the reverse.
const oauthStateHKDFInfo = stateHKDFInfo

// oauthFlowState is the sealed OAuth state. OAuth validates no ID token, so it
// carries no nonce; CodeVerifier is empty for a provider without PKCE.
type oauthFlowState struct {
	ProviderID   string `json:"provider_id"`
	OrgID        string `json:"org_id"`
	CodeVerifier string `json:"code_verifier,omitempty"`
	RedirectURI  string `json:"redirect_uri"`
	NonceHash    string `json:"nonce_hash"`
	ID           string `json:"id"`
	IssuedAt     int64  `json:"issued_at"`
	ExpiresAt    int64  `json:"expires_at"`
}

func sealOAuthState(secret string, state oauthFlowState, now time.Time) (string, oauthFlowState, error) {
	var err error
	if state.ID, err = randomURLSafe(16); err != nil {
		return "", oauthFlowState{}, err
	}
	state.IssuedAt = now.Unix()
	state.ExpiresAt = now.Add(stateTTL).Unix()
	token, err := sealWith(secret, oauthStateHKDFInfo, state.ProviderID, state)
	if err != nil {
		return "", oauthFlowState{}, err
	}
	return token, state, nil
}

func openOAuthState(secret, token, providerID string, now time.Time) (oauthFlowState, error) {
	var state oauthFlowState
	if err := openWith(secret, oauthStateHKDFInfo, token, providerID, &state); err != nil {
		return oauthFlowState{}, err
	}
	if state.ProviderID != providerID || state.OrgID == "" || state.ID == "" || state.NonceHash == "" || state.RedirectURI == "" {
		return oauthFlowState{}, errStateInvalid
	}
	if now.Unix() > state.ExpiresAt {
		return oauthFlowState{}, errStateExpired
	}
	return state, nil
}

// flowHash is what the flow store keeps for a state: SHA-256 of its ID.
func flowHash(id string) [32]byte { return sha256.Sum256([]byte("oidc-flow\x00" + id)) }

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func hashLoginNonce(nonce string) string {
	sum := sha256.Sum256([]byte(nonce))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
