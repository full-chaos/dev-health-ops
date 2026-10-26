package workloadexchange

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/tokencorpus"
)

// repoRoot is the module root, where the contract schemas and the golden
// corpus live.
const repoRoot = "../../.."

// corpusVerifier is the verifier the workload-token.v1 golden corpus pins,
// built the one way this file builds it for the corpus rows and for a minted
// token alike.
func corpusVerifier(t *testing.T, issuer, audience string, keys authverify.StaticKeys, denied []string, leeway int64) *authverify.WorkloadVerifier {
	t.Helper()
	v, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{
		Issuer: issuer, Audience: audience, Keys: keys, DeniedKids: denied, Leeway: time.Duration(leeway) * time.Second,
	})
	if err != nil {
		t.Fatalf("corpus verifier for %s: %v", audience, err)
	}
	return v
}

func loadWorkloadCorpus(t *testing.T) *tokencorpus.Corpus {
	t.Helper()
	c, err := tokencorpus.Load(filepath.Join(repoRoot, tokencorpus.WorkloadDir))
	if err != nil {
		t.Fatalf("load the workload-token corpus: %v", err)
	}
	return c
}

// The verifier that checks minted tokens below is the corpus verifier: built
// this way, it reaches every row's decision of the CHAOS-10648 golden corpus.
func TestTheCorpusVerifierReachesEveryGoldenDecision(t *testing.T) {
	c := loadWorkloadCorpus(t)
	if len(c.Manifest.Vectors) == 0 {
		t.Fatal("the corpus has no vectors")
	}
	for _, e := range c.Manifest.Vectors {
		raw, err := c.KeysFor(e).PublicKeys()
		if err != nil {
			t.Fatal(err)
		}
		vector := c.Vectors[e.File]
		signature, err := hex.DecodeString(vector.SignatureHex)
		if err != nil {
			t.Fatalf("%s: signature hex: %v", e.File, err)
		}
		compact := string(vector.SigningInput()) + "." + base64.RawURLEncoding.EncodeToString(signature)
		verifier := corpusVerifier(t, c.Manifest.Issuer, e.VerifyAs, authverify.StaticKeys(raw), c.Manifest.RemovedKids, c.Manifest.LeewaySeconds)
		_, err = verifier.Verify(compact, time.Unix(e.Now, 0))
		switch e.Expect {
		case "accept":
			if err != nil {
				t.Errorf("%s: refused %v, the corpus accepts it", e.File, err)
			}
		default:
			if got := string(authverify.ReasonOf(err)); err == nil || got != e.Reason {
				t.Errorf("%s: decision %q (%v), the corpus wants %s %q", e.File, got, err, e.Expect, e.Reason)
			}
		}
	}
}

// A token minted through the real exchange path is a workload-token.v1
// document (the 10648 schema at the module root) and passes the corpus
// verifier; a planted forbidden claim and a missing claim are refused by both.
func TestAMintedTokenIsAWorkloadTokenV1Document(t *testing.T) {
	h := newHarness(t)
	minted := decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", testAction).Encode())).AccessToken
	header, claims := tokenParts(t, minted)
	if _, err := contracts.ParseWorkloadToken(repoRoot, document(t, header, claims)); err != nil {
		t.Fatalf("the minted token is not a workload-token.v1 document: %v", err)
	}
	set, err := h.signer.KeySet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	keys := authverify.StaticKeys{}
	for _, key := range set.Published() {
		keys[key.KeyID] = key.PublicKey
	}
	c := loadWorkloadCorpus(t)
	if c.Manifest.Issuer != testIssuerURL {
		t.Fatalf("the corpus issuer %q is not the test issuer", c.Manifest.Issuer)
	}
	verifier := corpusVerifier(t, testIssuerURL, "auth-service", keys, nil, c.Manifest.LeewaySeconds)
	if _, err := verifier.Verify(minted, h.now); err != nil {
		t.Fatalf("the corpus verifier refuses the minted token: %v", err)
	}

	plants := map[string]func(map[string]any){
		"a scope claim":   func(c map[string]any) { c["scope"] = testAction },
		"an org_id claim": func(c map[string]any) { c["org_id"] = "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e" },
		"no jti":          func(c map[string]any) { delete(c, "jti") },
		"no sub":          func(c map[string]any) { delete(c, "sub") },
	}
	for name, plant := range plants {
		t.Run(name, func(t *testing.T) {
			planted := map[string]any{}
			for key, value := range claims {
				planted[key] = value
			}
			plant(planted)
			if _, err := contracts.ParseWorkloadToken(repoRoot, document(t, header, planted)); err == nil {
				t.Fatal("the 10648 schema accepted the planted claims")
			}
			payload, err := json.Marshal(planted)
			if err != nil {
				t.Fatal(err)
			}
			resigned, _, err := h.signer.Sign(context.Background(), contracts.WorkloadTokenTyp, payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := verifier.Verify(resigned, h.now); authverify.ReasonOf(err) != authverify.ReasonContract {
				t.Fatalf("the corpus verifier decided %v for the planted token, want contract", err)
			}
		})
	}
}

func document(t *testing.T, header, claims map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"header": header, "claims": claims})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
