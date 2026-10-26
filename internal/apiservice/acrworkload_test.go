package apiservice

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
)

const (
	acrWorkloadIssuer  = "https://auth.example.test"
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
	lookup := acrWorkloadLookup(map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: acrWorkloadSubject})
	workload, err := buildACRWorkload(lookup, keys)
	if err != nil || workload == nil {
		t.Fatalf("workload %v err %v", workload, err)
	}
	if workload.Subject != acrWorkloadSubject {
		t.Fatalf("subject %q", workload.Subject)
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

func TestBuildACRWorkloadIsOffUnlessIssuerAndSubjectAreSet(t *testing.T) {
	keys := authverify.StaticKeys{}
	for name, tc := range map[string]struct {
		env  map[string]string
		keys authverify.KeySource
	}{
		"nothing set":   {map[string]string{}, keys},
		"no subject":    {map[string]string{envAccessTokenIssuer: acrWorkloadIssuer}, keys},
		"no issuer":     {map[string]string{envACRWorkloadSubject: acrWorkloadSubject}, keys},
		"blank subject": {map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: "  "}, keys},
		"no key source": {map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: acrWorkloadSubject}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			workload, err := buildACRWorkload(acrWorkloadLookup(tc.env), tc.keys)
			if workload != nil || err != nil {
				t.Fatalf("workload %v err %v, want the path off", workload, err)
			}
		})
	}
}

func TestBuildACRWorkloadRefusesASubjectThatIsNotAPrincipalID(t *testing.T) {
	for _, subject := range []string{"acr-api", "ses_AAAAAAAAAAAAAAAAAAAAAA", "prn_" + string(make([]byte, 300))} {
		lookup := acrWorkloadLookup(map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: subject})
		if workload, err := buildACRWorkload(lookup, authverify.StaticKeys{}); err == nil || workload != nil {
			t.Fatalf("subject %.12q accepted", subject)
		}
	}
}

func TestBuildACRWorkloadNamesTheVariableNeverTheValue(t *testing.T) {
	for name, subject := range map[string]string{
		"invalid character": "prn_invalid!",
		"empty suffix":      "prn_",
		"suffix too long":   "prn_" + strings.Repeat("A", 65),
		"space inside":      "prn_AAAA BBBB",
	} {
		t.Run(name, func(t *testing.T) {
			lookup := acrWorkloadLookup(map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: subject})
			workload, err := buildACRWorkload(lookup, authverify.StaticKeys{})
			if err == nil || workload != nil {
				t.Fatalf("subject %q accepted", subject)
			}
			if !strings.Contains(err.Error(), envACRWorkloadSubject) || strings.Contains(err.Error(), subject) {
				t.Fatalf("error %q must name the variable and never its value", err)
			}
		})
	}
	lookup := acrWorkloadLookup(map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: "prn_" + strings.Repeat("A", 64)})
	if workload, err := buildACRWorkload(lookup, authverify.StaticKeys{}); err != nil || workload == nil {
		t.Fatalf("a 64 character suffix: %v %v", workload, err)
	}
}
