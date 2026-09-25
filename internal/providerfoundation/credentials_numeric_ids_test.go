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

// TestDecodeCredentialReadsNonStringFieldsLikePython (CHAOS-6770): Python
// reads the fields it wants with `str(cred.get(k) or "")` and ignores the
// rest, so a number is its str(), a falsy value is present-but-empty, a null is
// absent, and a field nobody asks for never fails the credential.
func TestDecodeCredentialReadsNonStringFieldsLikePython(t *testing.T) {
	cases := []struct {
		name, provider, body, field string
		want                        string
		wantPresent                 bool
	}{
		{"github numeric token", "github", `{"token": 12}`, "token", "12", true},
		{"gitlab numeric token", "gitlab", `{"token": 12}`, "token", "12", true},
		{"jira numeric api_token", "jira", `{"api_token": 12, "email": "e@x.com"}`, "api_token", "12", true},
		{"linear numeric api_key", "linear", `{"api_key": 12.5}`, "api_key", "12.5", true},
		{"true is str(True)", "gitlab", `{"token": true}`, "token", "True", true},
		{"false is present and empty", "gitlab", `{"token": false}`, "token", "", true},
		{"zero is present and empty", "linear", `{"api_key": 0}`, "api_key", "", true},
		{"null is absent", "github", `{"app_id": null}`, "app_id", "", false},
		{"a non-empty list has no faithful text", "gitlab", `{"token": [1]}`, "token", "", false},
		{"an empty list is falsy", "gitlab", `{"token": []}`, "token", "", false},
		{"an object has no faithful text", "gitlab", `{"token": {"a": 1}}`, "token", "", false},
		{"a later canonical number outranks its alias", "github", `{"appId": "alias", "app_id": 12}`, "app_id", "12", true},
		{"a later alias string outranks its canonical number", "github", `{"app_id": 12, "appId": "alias"}`, "app_id", "alias", true},
	}
	for _, tc := range cases {
		credential, err := decodeCredential(EncryptedCredential{Provider: tc.provider}, []byte(tc.body))
		if err != nil {
			t.Errorf("%s: decode refused: %v", tc.name, err)
			continue
		}
		got, ok := credential.Secret(tc.field)
		if ok != tc.wantPresent || got.Reveal() != tc.want {
			t.Errorf("%s: Secret(%q) = %q, %v; want %q, %v", tc.name, tc.field, got.Reveal(), ok, tc.want, tc.wantPresent)
		}
	}
}

// TestDecodeCredentialIgnoresFieldsNobodyReads: the whole credential no longer
// fails because ONE field held a value the reader did not want (Python's
// allow-list of kwargs never looks at it), and the shape check still judges
// only the fields the provider needs.
func TestDecodeCredentialIgnoresFieldsNobodyReads(t *testing.T) {
	for _, tc := range []struct{ provider, body string }{
		{"gitlab", `{"token": "glpat-x", "project_id": 7, "tags": [1, {"a": 2}], "meta": {"deep": [true, null]}}`},
		{"github", `{"token": "ghp_abc", "note": null, "extra": 1.5}`},
		{"jira", `{"api_token": "t", "email": "e@x.com", "site_id": 42}`},
		{"linear", `{"api_key": "lin_api_x", "workspace_id": 9}`},
		{"gitlab", `{"token": 12}`},
	} {
		credential, err := decodeCredential(EncryptedCredential{Provider: tc.provider}, []byte(tc.body))
		if err != nil {
			t.Errorf("%s %s: decode refused: %v", tc.provider, tc.body, err)
			continue
		}
		if err := ValidateCredentialShape(credential); err != nil {
			t.Errorf("%s %s: shape refused: %v", tc.provider, tc.body, err)
		}
	}
	// What is still refused: not an object, and a blank key.
	for _, body := range []string{`[1]`, `"x"`, `12`, `{"": "v"}`, `{`} {
		if _, err := decodeCredential(EncryptedCredential{Provider: "gitlab"}, []byte(body)); !errors.Is(err, ErrCredentialInvalid) {
			t.Errorf("%s: err = %v, want ErrCredentialInvalid", body, err)
		}
	}
	// A wanted field that has no faithful text leaves the credential incomplete.
	credential, err := decodeCredential(EncryptedCredential{Provider: "gitlab"}, []byte(`{"token": [1]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCredentialShape(credential); !errors.Is(err, ErrCredentialInvalid) {
		t.Errorf("a list token: shape err = %v, want ErrCredentialInvalid", err)
	}
}

// A zero-value pyjson.Int (nil big.Int) is not a number: never dereferenced.
func TestPythonSecretTextRefusesAnUnsetInteger(t *testing.T) {
	if text, ok := pythonSecretText(pyjson.Int{}); ok || text.Reveal() != "" {
		t.Errorf("pythonSecretText(pyjson.Int{}) = %q, %v; want \"\", false", text.Reveal(), ok)
	}
}

// The safe field count covers every stored field, string or not.
func TestCredentialSafeAttributesCountsNonStringFields(t *testing.T) {
	credential, err := decodeCredential(EncryptedCredential{Provider: "gitlab"}, []byte(`{"token": "t", "project_id": 7, "note": null}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := credential.SafeAttributes()["credential_field_count"]; got != 3 {
		t.Errorf("credential_field_count = %v, want 3", got)
	}
}
