package providerfoundation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The grid oracle (CHAOS-6770). The hand-written table beside it lists shapes
// somebody thought of; three review rounds each found a cell it did not list.
// This test GENERATES the cells: the field set of each provider comes from the
// real Python builder (its AST: every `cred_dict.get("k")`, every alias dict,
// every allowed-key set, every `"k" in cred_dict`; and its dataclass fields),
// the alias groups come from the same AST (an `a or b or c` chain, an alias
// dict entry), and every field is crossed with every value class through both
// planes.

const gridDeriveProgram = `
import ast, dataclasses, inspect, json, textwrap
from dev_health_ops.credentials import resolver, types
builders = {
    "github": (resolver.github_credentials_from_mapping, types.GitHubCredentials),
    "gitlab": (resolver.gitlab_credentials_from_mapping, types.GitLabCredentials),
    "jira": (resolver.jira_credentials_from_mapping, types.JiraCredentials),
    "linear": (resolver.linear_credentials_from_mapping, types.LinearCredentials),
}
def const(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None
def get_key(node):
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "get" and node.args:
        return const(node.args[0])
    return None
out = {}
for provider, (fn, cls) in builders.items():
    tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    keys, groups = set(), []
    for node in ast.walk(tree):
        key = get_key(node)
        if key: keys.add(key)
        if isinstance(node, ast.Set):
            keys |= {const(e) for e in node.elts if const(e)}
        if isinstance(node, ast.Compare) and const(node.left):
            keys.add(const(node.left))
        if isinstance(node, ast.Dict) and node.keys and all(const(k) and const(v) for k, v in zip(node.keys, node.values)):
            for k, v in zip(node.keys, node.values):
                keys |= {const(k), const(v)}
                groups.append([const(v), const(k)])
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            chain = [get_key(v) for v in node.values]
            chain = [c for c in chain if c]
            if len(chain) >= 2:
                groups.append(chain)
    skip = {"provider", "source", "credential_name", "extra", "private_key_path"}
    fields = [f.name for f in dataclasses.fields(cls) if f.name not in skip]
    out[provider] = {"keys": sorted(keys), "groups": groups, "fields": fields}
print(json.dumps(out))
`

const gridRunProgram = `
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
    raised = False
    try:
        credential = builders[case["provider"]](json.loads(case["body"]))
    except Exception:
        credential, raised = None, True
    fields = {}
    for name in case["fields"]:
        value = getattr(credential, name, None) if credential is not None else None
        fields[name] = None if value is None else (str(value) if value else "")
    out.append({"built": credential is not None, "raised": raised, "fields": fields})
print(json.dumps(out))
`

type gridDerived struct {
	Keys   []string   `json:"keys"`
	Groups [][]string `json:"groups"`
	Fields []string   `json:"fields"`
}

type gridCase struct {
	Provider string   `json:"provider"`
	Body     string   `json:"body"`
	Fields   []string `json:"fields"`
	Family   string   `json:"-"`
}

type gridResult struct {
	Built  bool               `json:"built"`
	Raised bool               `json:"raised"`
	Fields map[string]*string `json:"fields"`
}

type gridPair struct{ key, raw string }

// gridValueClasses are the JSON value classes crossed with every field.
var gridValueClasses = []string{
	`null`, `""`, `" "`, `"secret-fixture-value"`, `0`, `12`, `-1`, `12.5`, `1e22`, `9223372036854775808123`,
	`true`, `false`, `[]`, `[1]`, `{}`, `{"a":1}`, `["it's","é",null,true,1.5]`, `{"k":["nested",{"d":null}]}`,
}

// gridPairClasses are the classes crossed within an alias group (both orders).
var gridPairClasses = []string{`null`, `""`, `"secret-fixture-a"`, `0`, `12`, `[1]`}

var gridBases = map[string][][]gridPair{
	"github": {
		{},
		{{"token", `"base-token-value"`}},
		{{"app_id", `"1"`}, {"installation_id", `"2"`}, {"private_key", `"base-key-value"`}},
	},
	"gitlab": {{}, {{"token", `"base-token-value"`}}},
	"jira":   {{}, {{"email", `"e@example.com"`}, {"api_token", `"base-token-value"`}, {"base_url", `"https://x.example"`}}},
	"linear": {{}, {{"api_key", `"base-key-value"`}}},
}

func gridPayload(pairs []gridPair) string {
	var out strings.Builder
	out.WriteByte('{')
	for index, pair := range pairs {
		if index > 0 {
			out.WriteByte(',')
		}
		quoted, _ := json.Marshal(pair.key)
		out.Write(quoted)
		out.WriteByte(':')
		out.WriteString(pair.raw)
	}
	out.WriteByte('}')
	return out.String()
}

// gridWithout copies base minus the named keys.
func gridWithout(base []gridPair, keys ...string) []gridPair {
	drop := map[string]bool{}
	for _, key := range keys {
		drop[key] = true
	}
	var out []gridPair
	for _, pair := range base {
		if !drop[pair.key] {
			out = append(out, pair)
		}
	}
	return out
}

func TestCredentialFieldGridMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve providerfoundation package path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	python := pyoracle.Resolve(t, root)

	derived := map[string]gridDerived{}
	deriveOutput := runGridPython(t, python, root, gridDeriveProgram, nil)
	if err := json.Unmarshal(deriveOutput, &derived); err != nil || len(derived) != 4 {
		t.Fatalf("decode the derived field sets: %v\n%s", err, deriveOutput)
	}
	// The derivation must actually find the builders' fields; a parse that
	// returned nothing would make the grid empty and pass.
	for provider, want := range map[string][]string{
		"github": {"token", "app_id", "installation_id", "private_key", "base_url", "private_key_path", "appId", "privateKeyPath"},
		"gitlab": {"token", "gitlab_url", "url", "base_url"},
		"jira":   {"api_token", "apiToken", "token", "email", "base_url", "baseUrl", "url", "server_url"},
		"linear": {"api_key", "apiKey"},
	} {
		have := map[string]bool{}
		for _, key := range derived[provider].Keys {
			have[key] = true
		}
		for _, key := range want {
			if !have[key] {
				t.Fatalf("%s: the AST derivation did not find key %q (found %v)", provider, key, derived[provider].Keys)
			}
		}
	}

	dir := t.TempDir()
	files := map[string]string{}
	for name, content := range map[string][]byte{"empty": {}, "blank": []byte(" \n"), "key": []byte("-----BEGIN KEY-----"), "nonutf8": {0xff, 0xfe, 0x41}} {
		path := filepath.Join(dir, name+".pem")
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
		files[name] = path
	}
	files["missing"] = filepath.Join(dir, "missing.pem")
	files["directory"] = dir

	var cases []gridCase
	add := func(family, provider string, pairs []gridPair) {
		cases = append(cases, gridCase{Provider: provider, Body: gridPayload(pairs), Fields: derived[provider].Fields, Family: family})
	}
	providers := []string{"github", "gitlab", "jira", "linear"}
	for _, provider := range providers {
		info := derived[provider]
		for _, base := range gridBases[provider] {
			// field x value class (and absent), the field appended after the base.
			for _, key := range info.Keys {
				add("absent-field", provider, gridWithout(base, key))
				for _, class := range gridValueClasses {
					add("field", provider, append(gridWithout(base, key), gridPair{key, class}))
				}
			}
			// alias groups: every ordered pair of spellings x every class pair.
			for _, group := range info.Groups {
				for _, first := range group {
					for _, second := range group {
						if first == second {
							continue
						}
						for _, c1 := range gridPairClasses {
							for _, c2 := range gridPairClasses {
								add("alias-pair", provider, append(gridWithout(base, group...), gridPair{first, c1}, gridPair{second, c2}))
							}
						}
					}
				}
			}
			// fields nobody reads, blank keys included, x every class.
			for _, extra := range []string{" ", "", "\t", "zz_unrelated"} {
				for _, class := range gridValueClasses {
					add("unread-field", provider, append(gridWithout(base), gridPair{extra, class}))
				}
			}
		}
	}
	// private_key_path variants: every spelling x file state x companions.
	for _, spelling := range []string{"private_key_path", "privateKeyPath"} {
		for name, path := range files {
			quotedPath, _ := json.Marshal(path)
			for _, token := range []bool{false, true} {
				for _, key := range []string{"", `null`, `""`, `"inline-key-value"`} {
					for _, app := range []bool{false, true} {
						var pairs []gridPair
						if token {
							pairs = append(pairs, gridPair{"token", `"base-token-value"`})
						}
						if app {
							pairs = append(pairs, gridPair{"app_id", `"1"`}, gridPair{"installation_id", `"2"`})
						}
						if key != "" {
							pairs = append(pairs, gridPair{"private_key", key})
						}
						pairs = append(pairs, gridPair{spelling, string(quotedPath)})
						add("key-file-"+name, "github", pairs)
					}
				}
			}
		}
	}

	input, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	var results []gridResult
	if err := json.Unmarshal(runGridPython(t, python, root, gridRunProgram, input), &results); err != nil || len(results) != len(cases) {
		t.Fatalf("decode the Python answers (%d for %d cases): %v", len(results), len(cases), err)
	}

	diffs, builtBoth, refusedBoth := 0, 0, 0
	buckets := map[string]int{}
	families := map[string]int{}
	for index, tc := range cases {
		families[tc.Family]++
		want := results[index]
		report := func(format string, args ...any) {
			diffs++
			bucket := tc.Provider + " " + tc.Family + " " + format
			buckets[bucket]++
			if buckets[bucket] <= 2 {
				t.Errorf("%s %s %s: %s", tc.Family, tc.Provider, tc.Body, fmt.Sprintf(format, args...))
			}
		}
		credential, err := decodeCredential(EncryptedCredential{Provider: tc.Provider}, []byte(tc.Body))
		if err != nil {
			report("decode refused: %v", err)
			continue
		}
		built := ValidateCredentialShape(credential) == nil
		if built != want.Built {
			report("Go built=%v, Python built=%v (raised=%v)", built, want.Built, want.Raised)
			continue
		}
		if !want.Built {
			refusedBoth++
			continue
		}
		builtBoth++
		for _, field := range tc.Fields {
			goText := gridGoField(credential, tc.Provider, field)
			if goText == nil {
				report("no Go reader for the Python field %q: classify it", field)
				continue
			}
			pythonText := want.Fields[field]
			pythonConfigured := pythonText != nil && *pythonText != ""
			if pythonConfigured != (*goText != "") || (pythonConfigured && *pythonText != *goText) {
				report("field %q Go=%q Python=%v", field, *goText, deref(pythonText))
			}
		}
	}

	// Redaction: no verb, handler or container prints a secret the grid put in.
	leaked := 0
	for _, tc := range cases {
		credential, err := decodeCredential(EncryptedCredential{Provider: tc.Provider}, []byte(tc.Body))
		if err != nil {
			continue
		}
		var leaves []string
		var payload any
		if json.Unmarshal([]byte(tc.Body), &payload) == nil {
			leaves = gridLeaves(payload, nil)
		}
		for _, verb := range []string{"v", "+v", "#v", "s", "d", "+d", "f", "x", "q", "t", "e", "o", "b", "c", "U", "g"} {
			out := fmt.Sprintf("%"+verb, credential) + fmt.Sprintf("%"+verb, []Credential{credential}) + fmt.Sprintf("%"+verb, map[string]Credential{"c": credential})
			for _, leaf := range leaves {
				if strings.Contains(out, leaf) {
					leaked++
					if leaked <= 10 {
						t.Errorf("%%%s printed %q for %s %s", verb, leaf, tc.Provider, tc.Body)
					}
				}
			}
		}
	}

	names := make([]string, 0, len(families))
	for family := range families {
		names = append(names, family)
	}
	sort.Strings(names)
	var summary []string
	for _, family := range names {
		summary = append(summary, fmt.Sprintf("%s=%d", family, families[family]))
	}
	for bucket, count := range buckets {
		t.Logf("DIFF bucket %q: %d cells", bucket, count)
	}
	t.Logf("GRID %d cells (%s): %d built in both planes, %d refused in both, %d DIFF, %d redaction leaks", len(cases), strings.Join(summary, " "), builtBoth, refusedBoth, diffs, leaked)
	if len(cases) < 2000 {
		t.Fatalf("the grid has only %d cells; the generation is broken", len(cases))
	}
	if diffs == 0 && leaked == 0 {
		proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
		if proofDir == "" {
			t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
		}
		if err := os.WriteFile(filepath.Join(proofDir, "providerfoundation-credential-field-grid"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func runGridPython(t *testing.T, python, root, program string, stdin []byte) []byte {
	t.Helper()
	command := exec.Command(python, "-c", program)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python: %v", pyoracle.RunError(python, err, output))
	}
	return bytes.TrimSpace(output)
}

// gridLeaves are the secret-shaped leaves of a payload: strings of 8+
// characters and integers of 8+ digits, which cannot occur in the safe text.
func gridLeaves(value any, into []string) []string {
	switch typed := value.(type) {
	case string:
		if len(typed) >= 8 {
			into = append(into, typed)
		}
	case []any:
		for _, item := range typed {
			into = gridLeaves(item, into)
		}
	case map[string]any:
		for _, item := range typed {
			into = gridLeaves(item, into)
		}
	}
	return into
}

// gridGoField is what Go's consumers read for a Python dataclass field, or nil
// when no reader exists (the test then fails: a new Python field needs a
// decision). Empty means not configured.
func gridGoField(credential Credential, provider, field string) *string {
	text := func(names ...string) *string {
		out := ""
		for _, name := range names {
			if value, ok := credential.Secret(name); ok && value.Configured() {
				out = value.Reveal()
				break
			}
		}
		return &out
	}
	switch provider + "." + field {
	case "github.token", "github.app_id", "github.installation_id", "github.base_url":
		return text(field)
	case "github.private_key":
		if _, present := credential.Secret("private_key"); present {
			return text("private_key")
		}
		out := ""
		if path, ok := credential.Secret("private_key_path"); ok && path.Configured() {
			if content, err := readGitHubAppPrivateKeyFile(path.Reveal()); err == nil {
				out = content.Reveal()
			}
		}
		return &out
	case "gitlab.token":
		return text("token")
	case "gitlab.base_url":
		out := gitLabCredentialBaseURL(credential)
		return &out
	case "jira.api_token":
		return text(jiraAPITokenAliases...)
	case "jira.email":
		return text("email")
	case "jira.base_url":
		out := jiraCredentialBaseURL(credential)
		return &out
	case "linear.api_key":
		return text("api_key")
	}
	return nil
}
