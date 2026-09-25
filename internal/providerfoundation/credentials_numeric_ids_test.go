package providerfoundation

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

const numericIDsKey = `"private_key":"-----BEGIN KEY-----"`

func decodeGitHub(t *testing.T, body string) (Credential, error) {
	t.Helper()
	return decodeCredential(EncryptedCredential{Provider: "github", ID: "cred-1", Name: "default"}, []byte(body))
}

// TestDecodeCredentialAcceptsNumericGitHubAppIdentifiers is CHAOS-6737's
// reproduction: github_credentials_from_mapping passes a JSON number through
// and the client renders it with str(), so a row stored with numeric
// app_id / installation_id authenticates in Python. The decoded credential
// must equal the string-id one and build the same App auth.
func TestDecodeCredentialAcceptsNumericGitHubAppIdentifiers(t *testing.T) {
	numeric, err := decodeGitHub(t, `{"app_id": 12, "installation_id": 34, `+numericIDsKey+`}`)
	if err != nil {
		t.Fatalf("decodeCredential refused numeric identifiers: %v", err)
	}
	stringed, err := decodeGitHub(t, `{"app_id": "12", "installation_id": "34", `+numericIDsKey+`}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"app_id", "installation_id", "private_key"} {
		got, gotOK := numeric.Secret(name)
		want, wantOK := stringed.Secret(name)
		if !gotOK || !wantOK || got.Reveal() != want.Reveal() {
			t.Errorf("Secret(%q): numeric %q,%v vs string %q,%v", name, got.Reveal(), gotOK, want.Reveal(), wantOK)
		}
	}
	if err := ValidateCredentialShape(numeric); err != nil {
		t.Errorf("ValidateCredentialShape refused a numeric-id App credential: %v", err)
	}
	numericAuth, err := NewGitHubAppAuth(numeric, githubAPIBase, &githubAppDoer{})
	if err != nil {
		t.Fatalf("NewGitHubAppAuth: %v", err)
	}
	stringAuth, err := NewGitHubAppAuth(stringed, githubAPIBase, &githubAppDoer{})
	if err != nil {
		t.Fatal(err)
	}
	if numericAuth.appID != stringAuth.appID || numericAuth.installationID != stringAuth.installationID ||
		numericAuth.appID != "12" || numericAuth.installationID != "34" {
		t.Errorf("auth ids = %q/%q, want the string credential's 12/34", numericAuth.appID, numericAuth.installationID)
	}
}

// TestDecodeCredentialRendersNumbersLikePythonStr pins the text form: an
// integer keeps every digit (past 2**63), a float is repr() ("12.0"), the
// camelCase aliases take the same path, and a zero -- Python-falsy, absent to
// the App-auth shape check -- is a present-but-empty field.
func TestDecodeCredentialRendersNumbersLikePythonStr(t *testing.T) {
	cases := []struct {
		name, body, appID, installationID string
	}{
		{"big integer", `{"app_id": 9223372036854775808123, "installation_id": 34}`, "9223372036854775808123", "34"},
		{"float", `{"app_id": 12.0, "installation_id": 34.5}`, "12.0", "34.5"},
		{"exponent float", `{"app_id": 1e22, "installation_id": 1}`, "1e+22", "1"},
		{"camelCase aliases", `{"appId": 12, "installationId": 34}`, "12", "34"},
		{"zero is not configured", `{"app_id": 0, "installation_id": 0.0}`, "", ""},
	}
	for _, tc := range cases {
		credential, err := decodeGitHub(t, tc.body)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got, _ := credential.Secret("app_id"); got.Reveal() != tc.appID {
			t.Errorf("%s: app_id = %q, want %q", tc.name, got.Reveal(), tc.appID)
		}
		if got, _ := credential.Secret("installation_id"); got.Reveal() != tc.installationID {
			t.Errorf("%s: installation_id = %q, want %q", tc.name, got.Reveal(), tc.installationID)
		}
	}
}

// TestDecodeCredentialStillRefusesEveryOtherNonString: only the two App
// identifiers, only for github, only JSON numbers.
func TestDecodeCredentialStillRefusesEveryOtherNonString(t *testing.T) {
	cases := []struct{ name, provider, body string }{
		{"a numeric token", "github", `{"token": 12}`},
		{"a numeric private key", "github", `{"app_id": 1, "private_key": 5, "installation_id": 2}`},
		{"a boolean identifier", "github", `{"app_id": true, "installation_id": 2}`},
		{"a null identifier", "github", `{"app_id": null, "installation_id": 2}`},
		{"an object identifier", "github", `{"app_id": {"n": 1}}`},
		{"a list identifier", "github", `{"app_id": [1]}`},
		{"a numeric base url", "github", `{"base_url": 3}`},
		{"a numeric app_id for another provider", "gitlab", `{"app_id": 12, "token": "t"}`},
		{"a numeric api_key", "linear", `{"api_key": 12}`},
	}
	for _, tc := range cases {
		_, err := decodeCredential(EncryptedCredential{Provider: tc.provider}, []byte(tc.body))
		if !errors.Is(err, ErrCredentialInvalid) {
			t.Errorf("%s: err = %v, want ErrCredentialInvalid", tc.name, err)
		}
	}
}

// A zero-value pyjson.Int (nil big.Int) is not a number: never dereferenced.
func TestPythonNumberTextRefusesAnUnsetInteger(t *testing.T) {
	if text, ok := pythonNumberText(pyjson.Int{}); ok || text != "" {
		t.Errorf("pythonNumberText(pyjson.Int{}) = %q, %v; want \"\", false", text, ok)
	}
}
