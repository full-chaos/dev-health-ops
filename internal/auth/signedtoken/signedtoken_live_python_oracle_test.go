package signedtoken

import (
	"encoding/json"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The program runs the three Python modules' own helpers. Each module keeps
// its own copy, so every case is checked against all three: a drift in any
// one of them is a mismatch here.
const pythonSignedTokenProgram = `
import json, os, sys
from dev_health_ops.api.services import email_verification, invites, password_reset
modules = [invites, email_verification, password_reset]
req = json.loads(sys.stdin.read())
out = {"secret": [], "build": [], "valid": []}
for jwt, settings in req["secrets"]:
    for name in ("JWT_SECRET_KEY", "SETTINGS_ENCRYPTION_KEY"):
        os.environ.pop(name, None)
    if jwt is not None:
        os.environ["JWT_SECRET_KEY"] = jwt
    if settings is not None:
        os.environ["SETTINGS_ENCRYPTION_KEY"] = settings
    out["secret"].append([m._token_secret() for m in modules])
os.environ.pop("SETTINGS_ENCRYPTION_KEY", None)
os.environ["JWT_SECRET_KEY"] = req["secret"]
import uuid
for text in req["ids"]:
    token_id = uuid.UUID(text)
    out["build"].append([[m._build_token(token_id), m._hash_token(m._build_token(token_id))] for m in modules])
for token in req["tokens"]:
    row = []
    for m in modules:
        try:
            row.append("valid" if m._validate_signed_token(token) is not None else "invalid")
        except TypeError as exc:
            row.append("TypeError")
    out["valid"].append(row)
print(json.dumps(out))
`

func signedTokenCorpus(secret string) []string {
	id := uuid.MustParse("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")
	good, _ := Build(id, secret)
	idText, sig, _ := strings.Cut(good, ".")
	upper := strings.ToUpper(idText)
	hyphened := id.String()
	corpus := []string{
		good, "", ".", idText, idText + ".", "." + sig, idText + "." + sig + ".", idText + "." + sig + "x",
		idText + "." + strings.ToUpper(sig), upper + "." + sig, hyphened + "." + sig, "{" + hyphened + "}." + sig,
		"urn:uuid:" + hyphened + "." + sig, " " + idText + "." + sig, idText + " ." + sig,
		idText + "." + sig[:63] + "é", idText + ".é", "zz." + sig + "é", idText[:31] + "." + sig,
		idText + "." + sig[:10], "not-a-uuid." + sig, idText + "..", idText + ".\u0000",
		"+" + idText[1:] + "." + sig, idText[:8] + "_" + idText[8:31] + "." + sig,
	}
	random := rand.New(rand.NewSource(6260))
	alphabet := []string{"0", "a", "f", "F", "-", "{", "}", ".", "é", " ", "_", "g", "urn:", "uuid:", idText[:4], sig[:4]}
	for range 3000 {
		var b strings.Builder
		for range 1 + random.Intn(12) {
			b.WriteString(alphabet[random.Intn(len(alphabet))])
		}
		corpus = append(corpus, b.String())
		mutated := []byte(good)
		mutated[random.Intn(len(mutated))] = "0af.-{ "[random.Intn(7)]
		corpus = append(corpus, string(mutated))
	}
	return corpus
}

// TestSignedTokenMatchesLivePython compares Secret, Build, Hash and Valid
// with the invite, e-mail verification and password reset modules' own
// helpers: the secret chain, the token and hash for a set of ids, and the
// verdict (valid, invalid or TypeError) for a hand-picked and fuzzed corpus.
func TestSignedTokenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)

	const secret = "oracle-signing-text"
	str := func(s string) *string { return &s }
	secrets := [][2]*string{{nil, nil}, {str(""), nil}, {str(""), str("")}, {nil, str("settings")}, {str(""), str("settings")}, {str("jwt"), str("settings")}, {str("jwt"), nil}}
	var ids []string
	random := rand.New(rand.NewSource(62601))
	for range 50 {
		var raw [16]byte
		random.Read(raw[:])
		ids = append(ids, uuid.UUID(raw).String())
	}
	tokens := signedTokenCorpus(secret)
	input, _ := json.Marshal(map[string]any{"secrets": secrets, "secret": secret, "ids": ids, "tokens": tokens})
	command := exec.Command(python, "-c", pythonSignedTokenProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct {
		Secret [][]string   `json:"secret"`
		Build  [][][]string `json:"build"`
		Valid  [][]string   `json:"valid"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want.Secret) != len(secrets) || len(want.Build) != len(ids) || len(want.Valid) != len(tokens) {
		t.Fatalf("python returned %d/%d/%d results for %d/%d/%d cases", len(want.Secret), len(want.Build), len(want.Valid), len(secrets), len(ids), len(tokens))
	}
	deref := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	mismatches := 0
	report := func(format string, args ...any) {
		mismatches++
		if mismatches <= 20 {
			t.Errorf(format, args...)
		}
	}
	for index, pair := range secrets {
		got := Secret(deref(pair[0]), deref(pair[1]))
		for module, expected := range want.Secret[index] {
			if got != expected {
				report("secret %d module %d: go %q python %q", index, module, got, expected)
			}
		}
	}
	for index, text := range ids {
		token, hash := Build(uuid.MustParse(text), secret)
		for module, expected := range want.Build[index] {
			if token != expected[0] || hash != expected[1] || Hash(token) != expected[1] {
				report("build %s module %d: go (%s, %s) python %v", text, module, token, hash, expected)
			}
		}
	}
	verdicts := map[string]int{}
	for index, token := range tokens {
		ok, err := Valid(token, secret)
		got := "invalid"
		switch {
		case err != nil:
			got = "TypeError"
		case ok:
			got = "valid"
		}
		verdicts[got]++
		for module, expected := range want.Valid[index] {
			if got != expected {
				report("valid %q module %d: go %s python %s", token, module, got, expected)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d mismatches", mismatches)
	}
	for _, verdict := range []string{"valid", "invalid", "TypeError"} {
		if verdicts[verdict] == 0 {
			t.Fatalf("the corpus reached no %s verdict; it cannot show that branch agrees", verdict)
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "auth-signedtoken"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d secrets, %d ids, %d tokens compared (verdicts %v); 0 mismatches", len(secrets), len(ids), len(tokens), verdicts)
}
