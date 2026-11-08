package server

import (
	"log"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The envelope key is optional. Without it (one of these unset, or a JWKS document with no usable key at start)
// every route still mounts, the envelope class is refused like any unaccepted bearer, and /readyz has no query_jwks
// dependency. The start decision is made once per builder from the same env, so every route agrees.
const (
	envelopeJWKSPathEnv = "GO_API_ENVELOPE_JWKS_PATH"
	envelopeIssuerEnv   = "GO_API_ENVELOPE_ISSUER"
	envelopeAudienceEnv = "GO_API_ENVELOPE_AUDIENCE"
)

var envelopeKeyEnv = [3]string{envelopeJWKSPathEnv, envelopeIssuerEnv, envelopeAudienceEnv}

const envelopeRetiredLine = "query-api: envelope class retired: no envelope key configured"

// envelopeKey is the start-time answer: a verifier, or a nil one and why.
type envelopeKey struct {
	verifier *principal.Verifier
	unset    []string
	unusable bool
}

// loadEnvelopeKey decides from the three values. An error is a set key that NewVerifier refuses (a refused
// audience), which fails the build as before.
func loadEnvelopeKey(jwksPath, issuer, audience string) (envelopeKey, error) {
	var unset []string
	for i, value := range [3]string{jwksPath, issuer, audience} {
		if value == "" {
			unset = append(unset, envelopeKeyEnv[i])
		}
	}
	if len(unset) > 0 {
		return envelopeKey{unset: unset}, nil
	}
	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return envelopeKey{}, err
	}
	if verifier.CheckJWKS() != nil {
		return envelopeKey{unusable: true}, nil
	}
	return envelopeKey{verifier: verifier}, nil
}

func envelopeKeyFromEnv(getenv getenvFunc) (envelopeKey, error) {
	return loadEnvelopeKey(getenv(envelopeJWKSPathEnv), getenv(envelopeIssuerEnv), getenv(envelopeAudienceEnv))
}

// envelopeVerifierFromEnv is the verifier a REST builder hands authenticateRESTRequest: nil without a key.
func envelopeVerifierFromEnv(getenv getenvFunc) (*principal.Verifier, error) {
	key, err := envelopeKeyFromEnv(getenv)
	return key.verifier, err
}

// jwksProbe is the readiness dependency on the key: an untyped nil without one, so readinessProbes drops query_jwks.
func (k envelopeKey) jwksProbe() jwksChecker {
	if k.verifier == nil {
		return nil
	}
	return k.verifier
}

// logRetired writes the one start line when there is no key. It names keys, never values.
func (k envelopeKey) logRetired() {
	switch {
	case len(k.unset) > 0:
		log.Printf("%s (unset: %s)", envelopeRetiredLine, strings.Join(k.unset, ", "))
	case k.unusable:
		log.Printf("%s (%s holds no usable key)", envelopeRetiredLine, envelopeKeyEnv[0])
	}
}
