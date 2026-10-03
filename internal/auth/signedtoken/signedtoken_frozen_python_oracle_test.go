package signedtoken

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }

// signedTokenGoldens is the set of this package's frozen Python answers. The
// producers are three service modules of the pinned build, over the standard
// library only. A golden recorded by another producer is refused.
var signedTokenGoldens = programoracle.Set{
	Package:  "./internal/auth/signedtoken/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"link-signatures.golden.json": "37e4bf4488b8f883730668b1cbc10c9b0dc4422c073478376e0abbda48187147",
	},
}

// textDigest is how the golden holds a secret or a token: the SHA-256 of its
// text and the length of the text in bytes. No golden holds a token or a
// secret.
type textDigest struct {
	SHA256 string `json:"sha256"`
	Length int    `json:"length"`
}

func digestOf(text string) textDigest {
	sum := sha256.Sum256([]byte(text))
	return textDigest{SHA256: hex.EncodeToString(sum[:]), Length: len(text)}
}

// The program runs the three Python modules' own helpers. Each module keeps
// its own copy, so every case is checked against all three: a drift in any
// one of them is a mismatch here. A secret and a token are answered as their
// digest; the hash of a token is the value the services store, and is
// answered as it is.
const pythonSignedTokenProgram = `
import hashlib, json, os, sys
from dev_health_ops.api.services import email_verification, invites, password_reset
modules = [invites, email_verification, password_reset]
def digest(text):
    raw = text.encode("utf-8")
    return {"sha256": hashlib.sha256(raw).hexdigest(), "length": len(raw)}
req = json.loads(sys.stdin.read())
out = {"secret": [], "build": [], "valid": []}
for jwt, settings in req["secrets"]:
    for name in ("JWT_SECRET_KEY", "SETTINGS_ENCRYPTION_KEY"):
        os.environ.pop(name, None)
    if jwt is not None:
        os.environ["JWT_SECRET_KEY"] = jwt
    if settings is not None:
        os.environ["SETTINGS_ENCRYPTION_KEY"] = settings
    out["secret"].append([digest(m._token_secret()) for m in modules])
os.environ.pop("SETTINGS_ENCRYPTION_KEY", None)
os.environ["JWT_SECRET_KEY"] = req["secret"]
import uuid
for text in req["ids"]:
    token_id = uuid.UUID(text)
    out["build"].append([[digest(m._build_token(token_id)), m._hash_token(m._build_token(token_id))] for m in modules])
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

// TestSignedTokenMatchesFrozenPython compares Secret, Build, Hash and Valid
// with the frozen answers of the invite, e-mail verification and password
// reset modules' own helpers: the secret chain and the token for a set of ids
// (each by its digest), the stored hash, and the verdict (valid, invalid or
// TypeError) for a hand-picked and fuzzed corpus.
func TestSignedTokenMatchesFrozenPython(t *testing.T) {
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
	input, err := json.Marshal(map[string]any{"secrets": secrets, "secret": secret, "ids": ids, "tokens": tokens})
	if err != nil {
		t.Fatal(err)
	}
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	output := signedTokenGoldens.Outputs(t, root, "link-signatures.golden.json", programoracle.Program{Name: "signed token", Text: pythonSignedTokenProgram, Stdin: input})[0]
	var want struct {
		Secret [][]textDigest        `json:"secret"`
		Build  [][][]json.RawMessage `json:"build"`
		Valid  [][]string            `json:"valid"`
	}
	if err := json.Unmarshal([]byte(output), &want); err != nil {
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
		got := digestOf(Secret(deref(pair[0]), deref(pair[1])))
		for module, expected := range want.Secret[index] {
			if got != expected {
				report("secret %d module %d: the Go secret is not the Python secret: go %+v python %+v", index, module, got, expected)
			}
		}
	}
	for index, text := range ids {
		token, hash := Build(uuid.MustParse(text), secret)
		for module, expected := range want.Build[index] {
			var pythonToken textDigest
			var pythonHash string
			if len(expected) != 2 || json.Unmarshal(expected[0], &pythonToken) != nil || json.Unmarshal(expected[1], &pythonHash) != nil {
				t.Fatalf("build %s module %d: the answer is not [token digest, hash]: %s", text, module, expected)
			}
			if digestOf(token) != pythonToken || hash != pythonHash || Hash(token) != pythonHash {
				report("build %s module %d: go (token %+v, hash %s) python (token %+v, hash %s)", text, module, digestOf(token), hash, pythonToken, pythonHash)
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
	t.Logf("%d secrets, %d ids, %d tokens compared (verdicts %v); 0 mismatches", len(secrets), len(ids), len(tokens), verdicts)
}
