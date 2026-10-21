package tokencorpus

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// DecisionOrder is the order a verifier makes its decisions in: the first
// refusal wins, so a token that fails two of them is refused for the earlier.
var DecisionOrder = []string{
	ReasonContract, ReasonUnknownKid, ReasonSignature, ReasonIssuer,
	ReasonAudience, ReasonNotYetValid, ReasonExpired,
}

// Decision is what a verifier answers for one vector: Reason is empty on accept.
type Decision struct {
	Reason string
	Stage  string
}

// skipRemoved names the broken reader that ignores the manifest's removed kids.
const skipRemoved = "removed_kids"

// Check is the reference decision for one vector, in the manifest's decision
// order: the access-token.v1 contract (a repeated member, the schema, the
// client rules), the key by kid, the signature, the issuer, the audience, then
// the clock. root is the repo root that owns the contract schemas.
func Check(root string, c *Corpus, v Vector, now int64, audience string) (Decision, error) {
	return check(root, c, v, now, audience, c.Manifest.ReasonOrder, "")
}

// check is Check with the decision order given and one decision skipped (skip
// is a Reason, or skipRemoved; "" for none): a deliberately broken reader, so a
// test can prove the corpus fails it.
func check(root string, c *Corpus, v Vector, now int64, audience string, order []string, skip string) (Decision, error) {
	var header struct {
		Kid string `json:"kid"`
	}
	var claims struct {
		Iss string `json:"iss"`
		Aud string `json:"aud"`
		Nbf int64  `json:"nbf"`
		Exp int64  `json:"exp"`
	}
	// A vector the contract refuses may not decode into these shapes; its
	// fields then stay zero, which is what a later step would read.
	_ = json.Unmarshal([]byte(v.HeaderJSON), &header)
	_ = json.Unmarshal([]byte(v.PayloadJSON), &claims)
	keys, err := c.Jwks.PublicKeys()
	if err != nil {
		return Decision{}, err
	}
	key, known := keys[header.Kid]
	leeway := c.Manifest.LeewaySeconds

	for _, step := range order {
		if step == skip {
			continue
		}
		switch step {
		case ReasonContract:
			document := `{"header":` + v.HeaderJSON + `,"claims":` + v.PayloadJSON + `}`
			if _, err := contracts.ParseAccessToken(root, []byte(document)); err != nil {
				var refusal *contracts.CredentialRefusal
				if errors.As(err, &refusal) {
					return Decision{Reason: ReasonContract, Stage: refusal.Stage}, nil
				}
				return Decision{}, err
			}
		case ReasonUnknownKid:
			if skip != skipRemoved {
				for _, removed := range c.Manifest.RemovedKids {
					if removed == header.Kid {
						return Decision{Reason: ReasonUnknownKid}, nil
					}
				}
			}
			if !known {
				return Decision{Reason: ReasonUnknownKid}, nil
			}
		case ReasonSignature:
			signature, err := DecodeSignature(v)
			if err != nil || !known || len(signature) != ed25519.SignatureSize || !ed25519.Verify(key, v.SigningInput(), signature) {
				return Decision{Reason: ReasonSignature}, nil
			}
		case ReasonIssuer:
			if claims.Iss != c.Manifest.Issuer {
				return Decision{Reason: ReasonIssuer}, nil
			}
		case ReasonAudience:
			if claims.Aud != audience {
				return Decision{Reason: ReasonAudience}, nil
			}
		case ReasonNotYetValid:
			if now+leeway < claims.Nbf {
				return Decision{Reason: ReasonNotYetValid}, nil
			}
		case ReasonExpired:
			if now-leeway >= claims.Exp {
				return Decision{Reason: ReasonExpired}, nil
			}
		default:
			return Decision{}, fmt.Errorf("tokencorpus: unknown decision %q in the decision order", step)
		}
	}
	return Decision{}, nil
}

// VerifyAll decides every manifest row with the reference reader and reports
// each row whose decision differs from the manifest's.
func VerifyAll(root string, c *Corpus) error {
	return verifyAllIn(root, c, c.Manifest.ReasonOrder, "")
}

func verifyAll(root string, c *Corpus, skip string) error {
	return verifyAllIn(root, c, c.Manifest.ReasonOrder, skip)
}

func verifyAllIn(root string, c *Corpus, order []string, skip string) error {
	var wrong []string
	for _, e := range c.Manifest.Vectors {
		v, ok := c.Vectors[e.File]
		if !ok {
			return fmt.Errorf("tokencorpus: %s has no vector", e.File)
		}
		got, err := check(root, c, v, e.Now, e.VerifyAs, order, skip)
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
