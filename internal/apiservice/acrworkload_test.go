package apiservice

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
)

const (
	acrWorkloadIssuer = "https://auth.example.test"
	// acrWorkloadSubject is the wire principal id of the acr-api workload in the registry the tests stand in for.
	acrWorkloadSubject = "prn_AAAAAAAAAAAAAAAAAAAAAA"
)

func acrWorkloadLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) { value, ok := values[name]; return value, ok }
}

func acrWorkloadToken(t *testing.T, priv ed25519.PrivateKey, audience string, now time.Time) string {
	t.Helper()
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": "workload+jwt", "kid": "k1"})
	payload, _ := json.Marshal(map[string]any{
		"iss": acrWorkloadIssuer, "aud": audience, "sub": acrWorkloadSubject, "principal_type": "workload",
		"jti": "000000000000000000000001", "iat": now.Unix() - 5, "nbf": now.Unix() - 5, "exp": now.Unix() + 200,
	})
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	return input + "." + enc.EncodeToString(ed25519.Sign(priv, []byte(input)))
}

func TestWorkloadVerifierBuiltForDevHealthAPIAudience(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := authverify.StaticKeys{"k1": pub}
	lookup := acrWorkloadLookup(map[string]string{envAccessTokenIssuer: acrWorkloadIssuer})
	workload, err := buildACRWorkload(lookup, keys, acr.StaticSubject(acrWorkloadSubject))
	if err != nil || workload == nil {
		t.Fatalf("workload %v err %v", workload, err)
	}
	if got, err := workload.Source.Subject(t.Context()); err != nil || got != acrWorkloadSubject {
		t.Fatalf("subject %q err %v", got, err)
	}
	now := time.Now()
	if _, err := workload.Verifier.Verify(acrWorkloadToken(t, priv, "dev-health-api", now), now); err != nil {
		t.Fatalf("a dev-health-api workload token was refused: %v", err)
	}
	_, err = workload.Verifier.Verify(acrWorkloadToken(t, priv, "auth-service", now), now)
	if got := authverify.ReasonOf(err); got != authverify.ReasonAudience {
		t.Fatalf("an auth-service workload token: reason %q, want audience", got)
	}
}

func TestBuildACRWorkloadIsOffUnlessIssuerKeysAndSourceAreSet(t *testing.T) {
	keys := authverify.StaticKeys{}
	source := acr.StaticSubject(acrWorkloadSubject)
	for name, tc := range map[string]struct {
		env    map[string]string
		keys   authverify.KeySource
		source acr.SubjectSource
	}{
		"nothing set":   {map[string]string{}, keys, nil},
		"no source":     {map[string]string{envAccessTokenIssuer: acrWorkloadIssuer}, keys, nil},
		"no issuer":     {map[string]string{}, keys, source},
		"blank issuer":  {map[string]string{envAccessTokenIssuer: "  "}, keys, source},
		"no key source": {map[string]string{envAccessTokenIssuer: acrWorkloadIssuer}, nil, source},
	} {
		t.Run(name, func(t *testing.T) {
			workload, err := buildACRWorkload(acrWorkloadLookup(tc.env), tc.keys, tc.source)
			if workload != nil || err != nil {
				t.Fatalf("workload %v err %v, want the path off", workload, err)
			}
		})
	}
}

// The retired AUTH_ACR_WORKLOAD_SUBJECT pin is not read: with it set to another principal the registry's is the one
// the routes accept, and the variable is no longer a name the package knows.
func TestTheRetiredPinIsNotRead(t *testing.T) {
	lookup := acrWorkloadLookup(map[string]string{
		envAccessTokenIssuer:        acrWorkloadIssuer,
		"AUTH_ACR_WORKLOAD_SUBJECT": "prn_BBBBBBBBBBBBBBBBBBBBBB",
	})
	workload, err := buildACRWorkload(lookup, authverify.StaticKeys{}, acr.StaticSubject(acrWorkloadSubject))
	if err != nil || workload == nil {
		t.Fatalf("workload %v err %v", workload, err)
	}
	if got, _ := workload.Source.Subject(t.Context()); got != acrWorkloadSubject {
		t.Fatalf("subject %q: the retired pin was read", got)
	}
}
