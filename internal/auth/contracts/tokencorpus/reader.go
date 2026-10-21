package tokencorpus

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Decision is what a verifier answers for one vector: Reason is empty on accept.
type Decision struct {
	Reason string
	Stage  string
}

// Check is the reference decision for one vector. It runs the steps in the
// manifest's decision order: the access-token.v1 contract (a repeated member,
// the schema, the client rules), the key by kid, the signature, the issuer, the
// audience, then the clock. root is the repo root that owns the contract
// schemas.
func Check(root string, c *Corpus, v Vector, now int64, audience string) (Decision, error) {
	return check(root, c, v, now, audience, "")
}

// check is Check with one decision skipped (skip is a Reason, "" for none): a
// deliberately broken reader, so a test can prove the corpus fails it.
func check(root string, c *Corpus, v Vector, now int64, audience, skip string) (Decision, error) {
	document := `{"header":` + v.HeaderJSON + `,"claims":` + v.PayloadJSON + `}`
	if _, err := contracts.ParseAccessToken(root, []byte(document)); err != nil && skip != ReasonContract {
		var refusal *contracts.CredentialRefusal
		if errors.As(err, &refusal) {
			return Decision{Reason: ReasonContract, Stage: refusal.Stage}, nil
		}
		return Decision{}, err
	}
	var header struct {
		Kid string `json:"kid"`
	}
	var claims struct {
		Iss string `json:"iss"`
		Aud string `json:"aud"`
		Nbf int64  `json:"nbf"`
		Exp int64  `json:"exp"`
	}
	if err := json.Unmarshal([]byte(v.HeaderJSON), &header); err != nil {
		return Decision{}, err
	}
	if err := json.Unmarshal([]byte(v.PayloadJSON), &claims); err != nil {
		return Decision{}, err
	}
	keys, err := c.Jwks.PublicKeys()
	if err != nil {
		return Decision{}, err
	}
	key, ok := keys[header.Kid]
	if !ok && skip != ReasonUnknownKid {
		return Decision{Reason: ReasonUnknownKid}, nil
	}
	signature, err := DecodeSignature(v)
	if skip != ReasonSignature && (err != nil || !ok || len(signature) != ed25519.SignatureSize || !ed25519.Verify(key, v.SigningInput(), signature)) {
		return Decision{Reason: ReasonSignature}, nil
	}
	if claims.Iss != c.Manifest.Issuer && skip != ReasonIssuer {
		return Decision{Reason: ReasonIssuer}, nil
	}
	if claims.Aud != audience && skip != ReasonAudience {
		return Decision{Reason: ReasonAudience}, nil
	}
	leeway := c.Manifest.LeewaySeconds
	if now+leeway < claims.Nbf && skip != ReasonNotYetValid {
		return Decision{Reason: ReasonNotYetValid}, nil
	}
	if now-leeway >= claims.Exp && skip != ReasonExpired {
		return Decision{Reason: ReasonExpired}, nil
	}
	return Decision{}, nil
}

// VerifyAll decides every manifest row with the reference reader and reports
// each row whose decision differs from the manifest's.
func VerifyAll(root string, c *Corpus) error { return verifyAll(root, c, "") }

func verifyAll(root string, c *Corpus, skip string) error {
	var wrong []string
	for _, e := range c.Manifest.Vectors {
		v, ok := c.Vectors[e.File]
		if !ok {
			return fmt.Errorf("tokencorpus: %s has no vector", e.File)
		}
		got, err := check(root, c, v, e.Now, e.VerifyAs, skip)
		if err != nil {
			return fmt.Errorf("tokencorpus: %s: %w", e.File, err)
		}
		if got.Reason != e.Reason || got.Stage != e.Stage {
			wrong = append(wrong, fmt.Sprintf("%s: decided %q/%q, manifest says %q/%q", e.File, got.Reason, got.Stage, e.Reason, e.Stage))
		}
	}
	if len(wrong) > 0 {
		return fmt.Errorf("tokencorpus: %d rows disagree:\n%s", len(wrong), strings.Join(wrong, "\n"))
	}
	return nil
}
