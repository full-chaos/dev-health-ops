package tokencorpus

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func testRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// seededReader is a deterministic key source built at run time, so no key
// material is a literal anywhere in the tree.
type seededReader struct {
	state [32]byte
	buf   []byte
}

func newSeededReader(seed string) *seededReader {
	return &seededReader{state: sha256.Sum256([]byte("tokencorpus-test-" + seed))}
}

func (r *seededReader) Read(p []byte) (int, error) {
	for len(r.buf) < len(p) {
		r.state = sha256.Sum256(r.state[:])
		r.buf = append(r.buf, r.state[:]...)
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func mustGenerate(t *testing.T, seed string) *Corpus {
	t.Helper()
	c, err := Generate(newSeededReader(seed))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return c
}

func render(t *testing.T, c *Corpus) map[string][]byte {
	t.Helper()
	dir := t.TempDir()
	if err := Write(dir, c); err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		out[rel], err = os.ReadFile(path)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEveryGeneratedVectorGetsItsManifestDecision(t *testing.T) {
	if err := VerifyAll(testRoot(t), mustGenerate(t, "a")); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedCorpusGetsItsManifestDecision(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, Dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyAll(root, c); err != nil {
		t.Fatal(err)
	}
}

func TestGeneratorIsDeterministicExceptForKeys(t *testing.T) {
	first, again := render(t, mustGenerate(t, "a")), render(t, mustGenerate(t, "a"))
	if len(first) != len(again) {
		t.Fatalf("file sets differ: %d vs %d", len(first), len(again))
	}
	for name, body := range first {
		if !bytes.Equal(body, again[name]) {
			t.Errorf("%s differs between two runs on the same keys", name)
		}
	}
	other := render(t, mustGenerate(t, "b"))
	for name, body := range first {
		same := bytes.Equal(body, other[name])
		keyDependent := name == "jwks.json" || (strings.HasPrefix(name, "vectors"+string(filepath.Separator)) && !strings.Contains(string(body), `"signature_hex": ""`))
		if same && name == "jwks.json" {
			t.Errorf("%s did not change with the keys", name)
		}
		if !same && !keyDependent {
			t.Errorf("%s changed with the keys but is not key material", name)
		}
	}
	for name := range first {
		if !strings.HasPrefix(name, "vectors"+string(filepath.Separator)) {
			continue
		}
		var a, b Vector
		if err := json.Unmarshal(first[name], &a); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(other[name], &b); err != nil {
			t.Fatal(err)
		}
		if a.HeaderJSON != b.HeaderJSON || a.PayloadJSON != b.PayloadJSON {
			t.Errorf("%s: header or payload text changed with the keys", name)
		}
	}
}

func TestCommittedCorpusIsTheInventoryOfItsDirectory(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, Dir))
	if err != nil {
		t.Fatal(err)
	}
	claimed := map[string]bool{}
	for _, e := range c.Manifest.Vectors {
		if claimed[e.File] {
			t.Errorf("%s is listed twice", e.File)
		}
		claimed[e.File] = true
	}
	entries, err := os.ReadDir(filepath.Join(root, Dir, "vectors"))
	if err != nil {
		t.Fatal(err)
	}
	var unclaimed []string
	for _, e := range entries {
		if !claimed[e.Name()] {
			unclaimed = append(unclaimed, e.Name())
		}
		delete(claimed, e.Name())
	}
	var missing []string
	for name := range claimed {
		missing = append(missing, name)
	}
	sort.Strings(unclaimed)
	sort.Strings(missing)
	if len(unclaimed)+len(missing) > 0 {
		t.Fatalf("on disk but unlisted: %v; listed but absent: %v", unclaimed, missing)
	}
	top, err := os.ReadDir(filepath.Join(root, Dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range top {
		switch e.Name() {
		case "manifest.json", "jwks.json", "README.md", "vectors":
		default:
			t.Errorf("unexpected entry %s in the corpus directory", e.Name())
		}
	}
}

func TestManifestRowsAreWellFormed(t *testing.T) {
	c, err := Load(filepath.Join(testRoot(t), Dir))
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[string]bool{}
	for _, r := range Reasons {
		reasons[r] = true
	}
	seen := map[string]int{}
	for _, e := range c.Manifest.Vectors {
		switch e.Expect {
		case Accept:
			if e.Reason != "" || e.Stage != "" {
				t.Errorf("%s: an accept row names a reason", e.File)
			}
		case Reject:
			if !reasons[e.Reason] {
				t.Errorf("%s: reason %q is outside the vocabulary", e.File, e.Reason)
			}
			if (e.Reason == ReasonContract) != (e.Stage != "") {
				t.Errorf("%s: contract_stage must be set exactly for the contract reason", e.File)
			}
			seen[e.Reason]++
		default:
			t.Errorf("%s: expect %q", e.File, e.Expect)
		}
		if e.Why == "" || e.VerifyAs == "" || e.Now == 0 {
			t.Errorf("%s: why, verify_as_audience and now are required", e.File)
		}
	}
	for _, r := range Reasons {
		if seen[r] == 0 {
			t.Errorf("no reject row exercises the reason %q", r)
		}
	}
}

func TestCommittedJwksIsPublicOnlyAndValidatesAgainstTheContract(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, Dir, "jwks.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if err := contracts.Validate(root, "jwks.v1", document); err != nil {
		t.Fatalf("jwks.json does not satisfy jwks.v1: %v", err)
	}
	for _, private := range []string{`"d"`, `"p"`, `"q"`, `"k"`} {
		if strings.Contains(string(raw), private+":") {
			t.Errorf("jwks.json carries the private member %s", private)
		}
	}
}

func TestNoPrivateKeyOrCompactTokenIsCommitted(t *testing.T) {
	root := testRoot(t)
	err := filepath.WalkDir(filepath.Join(root, Dir), func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if strings.Contains(text, "PRIVATE KEY") || strings.Contains(text, `"d":`) {
			t.Errorf("%s carries private key material", path)
		}
		if strings.Contains(text, `"eyJ`) || strings.Contains(text, "eyJhbGci") {
			t.Errorf("%s carries a compact JWS shape", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Each broken reader below skips one decision; the corpus must catch it. A
// corpus no mutant can fail proves nothing.
func TestTheCorpusFailsAReaderThatSkipsADecision(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, Dir))
	if err != nil {
		t.Fatal(err)
	}
	for name, skip := range map[string]string{
		"signature":    ReasonSignature,
		"kid":          ReasonUnknownKid,
		"issuer":       ReasonIssuer,
		"audience":     ReasonAudience,
		"not before":   ReasonNotYetValid,
		"expiry":       ReasonExpired,
		"the contract": ReasonContract,
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyAll(root, c, skip); err == nil {
				t.Fatalf("a reader that skips %s passed every vector", skip)
			}
		})
	}
}
