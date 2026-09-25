package providerfoundation

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
		{"a non-empty list is its repr", "gitlab", `{"token": [1]}`, "token", "[1]", true},
		{"a list repr quotes like Python", "gitlab", `{"token": ["it's", "x", null, true, 1.5]}`, "token", `["it's", 'x', None, True, 1.5]`, true},
		{"an empty list is falsy", "gitlab", `{"token": []}`, "token", "", true},
		{"an object is its repr", "gitlab", `{"token": {"a": 1, "b": [2]}}`, "token", "{'a': 1, 'b': [2]}", true},
		{"an empty object is falsy", "gitlab", `{"token": {}}`, "token", "", true},
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
		{"gitlab", `{"token": "t", " ": 7, "": "v"}`},
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
	// What is still refused: not an object and invalid JSON.
	for _, body := range []string{`[1]`, `"x"`, `12`, `{`} {
		if _, err := decodeCredential(EncryptedCredential{Provider: "gitlab"}, []byte(body)); !errors.Is(err, ErrCredentialInvalid) {
			t.Errorf("%s: err = %v, want ErrCredentialInvalid", body, err)
		}
	}
	// A wanted field holding a container is its Python repr, so the shape check
	// accepts it exactly as Python's builder does.
	credential, err := decodeCredential(EncryptedCredential{Provider: "gitlab"}, []byte(`{"token": [1]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCredentialShape(credential); err != nil {
		t.Errorf("a list token: shape err = %v, want it built like Python", err)
	}
}

// A zero-value pyjson.Int (nil big.Int) is falsy and never dereferenced.
func TestPythonSecretTextTreatsAnUnsetIntegerAsFalsy(t *testing.T) {
	if text, ok := pythonSecretText(pyjson.Int{}); !ok || text.Reveal() != "" {
		t.Errorf("pythonSecretText(pyjson.Int{}) = %q, %v; want \"\", true", text.Reveal(), ok)
	}
}

// The safe field count covers every stored field (a null is no field), and an
// alias pair counts once whichever order it is written in.
func TestCredentialSafeAttributesCountsNonStringFields(t *testing.T) {
	for _, tc := range []struct {
		provider, body string
		want           int
	}{
		{"gitlab", `{"token": "t", "project_id": 7, "note": null}`, 2},
		{"github", `{"app_id": 12, "appId": "a"}`, 1},
		{"github", `{"appId": "a", "app_id": 12}`, 1},
		{"github", `{"app_id": "a", "appId": 12}`, 1},
		{"github", `{"appId": 12, "app_id": "a"}`, 1},
	} {
		credential, err := decodeCredential(EncryptedCredential{Provider: tc.provider}, []byte(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		if got := credential.SafeAttributes()["credential_field_count"]; got != tc.want {
			t.Errorf("%s: credential_field_count = %v, want %d", tc.body, got, tc.want)
		}
	}
}

// TestGitHubNullNeverErasesAnEarlierField (r3): github_credentials_from_mapping
// drops None BEFORE resolving aliases, so a null spelling never replaces a
// value stored under the same canonical name.
func TestGitHubNullNeverErasesAnEarlierField(t *testing.T) {
	credential, err := decodeGitHub(t, `{"token": "t", "appId": 12, "app_id": null}`)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := credential.Secret("app_id"); !ok || got.Reveal() != "12" {
		t.Errorf("app_id = %q, %v; want the earlier 12 kept", got.Reveal(), ok)
	}
	if err := ValidateCredentialShape(credential); err == nil {
		t.Error("a token beside an App id must be refused (Python builds nothing)")
	}
	app, err := decodeGitHub(t, `{"appId": 12, "app_id": null, "installation_id": 34, "private_key": "k"}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCredentialShape(app); err != nil {
		t.Errorf("the App credential lost its id to a later null: %v", err)
	}
	for _, alias := range []string{"appId", "baseUrl", "installationId", "privateKey", "privateKeyPath"} {
		canonical := githubFieldAliases[alias]
		c, err := decodeGitHub(t, `{"`+alias+`": "kept", "`+canonical+`": null}`)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := c.Secret(canonical); !ok || got.Reveal() != "kept" {
			t.Errorf("%s then null %s: %q, %v", alias, canonical, got.Reveal(), ok)
		}
	}
}

// TestGitHubShapeReadsThePrivateKeyFileLikePython (r3): with no private_key
// entry the builder reads private_key_path into it, so the file's CONTENT
// decides whether a token beside it conflicts: an empty file is no key; a
// whitespace-only one is a (truthy) key; a missing one makes the builder
// return None.
func TestGitHubShapeReadsThePrivateKeyFileLikePython(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	empty, blank, key := write("empty.pem", ""), write("blank.pem", " \n"), write("key.pem", "-----BEGIN KEY-----")
	for _, tc := range []struct {
		name, body string
		refused    bool
	}{
		{"a token beside an empty key file", `{"token": "t", "private_key_path": ` + strconv.Quote(empty) + `}`, false},
		{"a token beside a whitespace-only key file", `{"token": "t", "private_key_path": ` + strconv.Quote(blank) + `}`, true},
		{"a token beside a key file", `{"token": "t", "privateKeyPath": ` + strconv.Quote(key) + `}`, true},
		{"a token beside a missing key file", `{"token": "t", "private_key_path": ` + strconv.Quote(filepath.Join(dir, "missing.pem")) + `}`, true},
		{"a token beside an empty inline key and a key file", `{"token": "t", "private_key": "", "private_key_path": ` + strconv.Quote(key) + `}`, false},
	} {
		credential, err := decodeGitHub(t, tc.body)
		if err != nil {
			t.Fatal(err)
		}
		if refused := errors.Is(ValidateCredentialShape(credential), ErrCredentialInvalid); refused != tc.refused {
			t.Errorf("%s: refused = %v, want %v", tc.name, refused, tc.refused)
		}
	}
}

// TestCredentialNeverPrintsItsSecrets: fmt and slog reflect over a struct's
// private fields, so a Credential prints only metadata, whether a field was a
// string, a number or a container.
func TestCredentialNeverPrintsItsSecrets(t *testing.T) {
	for _, body := range []string{
		`{"token": "secret-fixture"}`,
		`{"token": ["secret-fixture"]}`,
		`{"token": {"k": "secret-fixture"}}`,
		`{"token": 987654321}`,
		`{"token": "x", "other": ["secret-fixture"]}`,
	} {
		credential, err := decodeCredential(EncryptedCredential{Provider: "gitlab", ID: "cred-1"}, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		var text, jsonOut bytes.Buffer
		slog.New(slog.NewTextHandler(&text, nil)).Info("x", "credential", credential)
		slog.New(slog.NewJSONHandler(&jsonOut, nil)).Info("x", "credential", credential)
		// slog resolves LogValue for JSON as well as text: the group carries the count.
		count := len(credential.fields) + len(credential.deferred)
		if !strings.Contains(jsonOut.String(), fmt.Sprintf(`"credential_field_count":%d`, count)) || !strings.Contains(text.String(), fmt.Sprintf("credential_field_count=%d", count)) {
			t.Errorf("%s: slog output lost the safe attributes: %s %s", body, text.String(), jsonOut.String())
		}
		if want := fmt.Sprintf("fields=%d}", count); !strings.Contains(fmt.Sprintf("%v", credential), want) {
			t.Errorf("%s: String() = %v, want it to count every stored field (%s)", body, credential, want)
		}
		// Every verb, not only the string-compatible ones.
		var printed []string
		for _, verb := range []string{"v", "+v", "#v", "s", "d", "+d", "f", "x", "X", "#x", "q", "t", "e", "o", "b", "c", "U", "g", "10.3v"} {
			printed = append(printed, fmt.Sprintf("%"+verb, credential))
		}
		printed = append(printed,
			text.String(), jsonOut.String(),
			fmt.Sprintf("%v", &credential), fmt.Sprintf("%+v", []Credential{credential}), fmt.Sprintf("%+v", map[string]Credential{"c": credential}),
			fmt.Sprintf("%d", []Credential{credential}), fmt.Sprintf("%x", map[string]Credential{"c": credential}),
		)
		for _, out := range printed {
			for _, leaked := range []string{"secret-fixture", "987654321", "raw:", "0x"} {
				if strings.Contains(out, leaked) {
					t.Errorf("%s printed %q in %q", body, leaked, out)
				}
			}
			if !strings.Contains(out, "gitlab") {
				t.Errorf("%s: %q lost its metadata", body, out)
			}
		}
	}
}
