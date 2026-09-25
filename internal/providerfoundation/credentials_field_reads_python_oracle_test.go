package providerfoundation

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// fieldReadCase is one decrypted credential payload and the fields whose text
// the two planes are compared on.
type fieldReadCase struct {
	Provider string   `json:"provider"`
	Body     string   `json:"body"`
	Fields   []string `json:"fields"`
}

// fieldReadResult is the REAL Python builder's answer: whether it built a
// credential, and each named field as `str(value)` (None -> null) after
// normalising a falsy value to "" -- the text every Python consumer sees.
type fieldReadResult struct {
	Built  bool               `json:"built"`
	Fields map[string]*string `json:"fields"`
}

const fieldReadProgram = `
import json, sys
from dev_health_ops.credentials import resolver
builders = {
    "github": resolver.github_credentials_from_mapping,
    "gitlab": resolver.gitlab_credentials_from_mapping,
    "jira": resolver.jira_credentials_from_mapping,
    "linear": resolver.linear_credentials_from_mapping,
}
out = []
for case in json.load(sys.stdin):
    credential = builders[case["provider"]](json.loads(case["body"]))
    fields = {}
    for name in case["fields"]:
        value = getattr(credential, name, None) if credential is not None else None
        fields[name] = None if value is None else (str(value) if value else "")
    out.append({"built": credential is not None, "fields": fields})
print(json.dumps(out))
`

// TestCredentialFieldReadsMatchLivePython (CHAOS-6770) feeds every payload
// shape to the real Python resolver builders and to decodeCredential +
// ValidateCredentialShape, and compares whether a credential is built and the
// text of each field the provider reads, containers included.
func TestCredentialFieldReadsMatchLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve providerfoundation package path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	python := pyoracle.Resolve(t, repositoryRoot)

	github := []string{"token", "app_id", "installation_id", "private_key", "base_url"}
	gitlab := []string{"token"}
	jira := []string{"api_token", "email", "base_url"}
	linear := []string{"api_key"}
	cases := []fieldReadCase{
		{"github", `{"token": "ghp_abc"}`, github},
		{"github", `{"token": 12}`, github},
		{"github", `{"token": 0}`, github},
		{"github", `{"token": true}`, github},
		{"github", `{"token": "ghp", "base_url": 5}`, github},
		{"github", `{"app_id": 12, "installation_id": 34, "private_key": "k"}`, github},
		{"github", `{"app_id": 12.0, "installation_id": 34.5, "private_key": "k"}`, github},
		{"github", `{"app_id": 1e22, "installation_id": 9223372036854775808123, "private_key": "k"}`, github},
		{"github", `{"appId": 12, "installationId": 34, "privateKey": "k"}`, github},
		{"github", `{"appId": "alias", "app_id": 12, "installation_id": 1, "private_key": "k"}`, github},
		{"github", `{"app_id": 12, "appId": "alias", "installation_id": 1, "private_key": "k"}`, github},
		{"github", `{"token": "ghp", "app_id": 0}`, github},
		{"github", `{"app_id": 0, "installation_id": 34, "private_key": "k"}`, github},
		{"github", `{"app_id": true, "installation_id": 34, "private_key": "k"}`, github},
		{"github", `{"app_id": false, "installation_id": 34, "private_key": "k"}`, github},
		{"github", `{"app_id": null, "installation_id": 34, "private_key": "k"}`, github},
		{"github", `{"token": "ghp", "note": null, "extra": [1, {"a": 2}], "n": 1.5}`, github},
		// CHAOS-6781: a token beside ANY App field is refused by Python.
		{"github", `{"token": "ghp", "app_id": "12"}`, github},
		{"github", `{"token": "ghp", "app_id": 12}`, github},
		{"github", `{"token": "ghp", "installation_id": "1"}`, github},
		{"github", `{"token": "ghp", "private_key": "k"}`, github},
		{"github", `{"token": "ghp", "privateKey": "k"}`, github},
		{"github", `{"token": "ghp", "private_key": ""}`, github},
		{"github", `{"token": "ghp", "private_key_path": "/nonexistent/key.pem"}`, github},
		{"github", `{"token": "ghp", "private_key": "", "private_key_path": "/nonexistent/key.pem"}`, github},
		{"github", `{"token": "ghp", "installation_id": 0, "app_id": false}`, github},
		{"github", `{"token": "ghp", "app_id": "12", "installation_id": "1", "private_key": "k"}`, github},
		{"gitlab", `{"token": "glpat-x"}`, gitlab},
		{"gitlab", `{"token": 12}`, gitlab},
		{"gitlab", `{"token": 0}`, gitlab},
		{"gitlab", `{"token": false}`, gitlab},
		{"gitlab", `{"token": null}`, gitlab},
		{"gitlab", `{"token": []}`, gitlab},
		{"gitlab", `{"token": "t", "project_id": 7, "tags": [1, {"a": 2}]}`, gitlab},
		{"gitlab", `{"private_token": "p", "project_id": 7}`, gitlab},
		// A blank key is a field nobody reads.
		{"gitlab", `{"token": "fixture", " ": 7}`, gitlab},
		{"gitlab", `{"token": "fixture", "": "v"}`, gitlab},
		{"github", `{"token": "ghp", "\t": [1]}`, github},
		// A container in a field the provider reads is its Python str().
		{"gitlab", `{"token": [1]}`, gitlab},
		{"gitlab", `{"token": {"a": 1, "b": [2, {"c": null}]}}`, gitlab},
		{"gitlab", `{"token": ["it's", "x", null, true, 1.5, "é"]}`, gitlab},
		{"gitlab", `{"token": []}`, gitlab},
		{"gitlab", `{"token": {}}`, gitlab},
		{"github", `{"token": [1]}`, github},
		{"github", `{"app_id": [1, 2], "installation_id": {"n": 3}, "private_key": "k"}`, github},
		{"jira", `{"email": "e@x.com", "api_token": ["t"], "base_url": "https://x"}`, jira},
		{"linear", `{"api_key": [1]}`, linear},
		{"jira", `{"email": "e@x.com", "api_token": "t", "base_url": "https://x"}`, jira},
		{"jira", `{"email": "e@x.com", "api_token": 12, "base_url": "https://x"}`, jira},
		{"jira", `{"email": "e@x.com", "apiToken": 12, "baseUrl": "https://x"}`, jira},
		{"jira", `{"email": "e@x.com", "token": 12, "url": "https://x"}`, jira},
		{"jira", `{"email": 5, "api_token": "t", "base_url": "https://x"}`, jira},
		{"jira", `{"email": "e@x.com", "api_token": "t", "base_url": 7}`, jira},
		{"jira", `{"email": "e@x.com", "api_token": "t", "base_url": "https://x", "site_id": 42}`, jira},
		{"linear", `{"api_key": "lin_api_x"}`, linear},
		{"linear", `{"api_key": 12}`, linear},
		{"linear", `{"api_key": 0}`, linear},
		{"linear", `{"api_key": "k", "workspace_id": 9}`, linear},
		// CHAOS-6782: the apiKey alias, canonical first when truthy.
		{"linear", `{"apiKey": "k"}`, linear},
		{"linear", `{"apiKey": 12.5}`, linear},
		{"linear", `{"api_key": "canonical", "apiKey": "alias"}`, linear},
		{"linear", `{"apiKey": "alias", "api_key": "canonical"}`, linear},
		{"linear", `{"api_key": "", "apiKey": "alias"}`, linear},
		{"linear", `{"api_key": 0, "apiKey": "alias"}`, linear},
		{"linear", `{"api_key": null, "apiKey": "alias"}`, linear},
		{"linear", `{"api_key": "canonical", "apiKey": 0}`, linear},
		{"linear", `{"apiKey": ""}`, linear},
	}
	input, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", fieldReadProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repositoryRoot, "src"))
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python credential builders: %v", pyoracle.RunError(python, err, output))
	}
	var results []fieldReadResult
	if err := json.Unmarshal(bytes.TrimSpace(output), &results); err != nil || len(results) != len(cases) {
		t.Fatalf("decode the Python answer (%d results for %d cases): %v\n%s", len(results), len(cases), err, output)
	}

	agreed := 0
	for index, tc := range cases {
		want := results[index]
		credential, err := decodeCredential(EncryptedCredential{Provider: tc.Provider}, []byte(tc.Body))
		if err != nil {
			t.Errorf("case %d %s %s: decode refused: %v", index, tc.Provider, tc.Body, err)
			continue
		}
		built := ValidateCredentialShape(credential) == nil
		if built != want.Built {
			t.Errorf("case %d %s %s: Go built=%v, Python built=%v", index, tc.Provider, tc.Body, built, want.Built)
			continue
		}
		if !want.Built {
			// Python builds nothing, so it has no fields to compare.
			agreed++
			continue
		}
		for _, field := range tc.Fields {
			pythonText := want.Fields[field]
			value, present := goFieldRead(credential, tc.Provider, field)
			goText := value.Reveal()
			// An absent field and an empty one read the same everywhere; only a
			// configured text is compared against Python's.
			pythonConfigured := pythonText != nil && *pythonText != ""
			if pythonConfigured != (present && value.Configured()) || (pythonConfigured && *pythonText != goText) {
				t.Errorf("case %d %s %s: field %q Go=%q (present %v), Python=%v", index, tc.Provider, tc.Body, field, goText, present, deref(pythonText))
			}
		}
		agreed++
	}
	t.Logf("%d payload shapes: Go and the real Python builders agree on %d", len(cases), agreed)

	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if !t.Failed() {
		if err := os.WriteFile(filepath.Join(proofDir, "providerfoundation-credential-field-reads"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func deref(text *string) any {
	if text == nil {
		return nil
	}
	return *text
}

// goFieldRead is the text Go's consumers get for a field: Jira's token and URL
// take the first configured spelling in Python's precedence order
// (`a or b or c`), every other field is read by its own name.
func goFieldRead(credential Credential, provider, field string) (secrets.Value, bool) {
	names := []string{field}
	if provider == "jira" {
		switch field {
		case "api_token":
			names = jiraAPITokenAliases
		case "base_url":
			names = jiraBaseURLAliases
		}
	}
	for _, name := range names {
		if value, ok := credential.Secret(name); ok && value.Configured() {
			return value, true
		}
	}
	return secrets.Value{}, false
}
