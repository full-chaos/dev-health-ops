package contracts

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The golden token trees hold public Ed25519 verification keys (JWK members
// x) in jwks.json and manifest.json. The scanner reads them as high-entropy
// values. This test scans both trees with the fixture scanner and accepts
// only a closed list of those public keys, each named by file and JSON
// member pointer and proven to be a public OKP Ed25519 JWK.

var goldenTokenTrees = []string{"access-token", "workload-token"}

type goldenAllowedKey struct {
	file    string
	pointer string
}

var goldenAllowedPublicKeys = []goldenAllowedKey{
	{"access-token/jwks.json", "/keys/0/x"},
	{"access-token/jwks.json", "/keys/1/x"},
	{"access-token/manifest.json", "/removed_public_keys/0/x"},
	{"workload-token/jwks.json", "/keys/0/x"},
	{"workload-token/jwks.json", "/keys/1/x"},
	{"workload-token/manifest.json", "/removed_public_keys/0/x"},
}

var goldenLowerHex = regexp.MustCompile(`^[0-9a-f]*$`)

// goldenShortSignatures lists the deliberately malformed negative vectors; every
// other vector carries a 128-character (64-byte) signature.
var goldenShortSignatures = map[string]int{
	"invalid-alg-hs256-public-key-as-secret.json": 64,
	"invalid-alg-hs256-unrelated-secret.json":     64,
	"invalid-alg-none.json":                       0,
	"invalid-signature-empty.json":                0,
	"invalid-signature-truncated.json":            126,
}

func goldenPointerParent(document any, pointer string) (map[string]any, string, bool) {
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	node := document
	for _, part := range parts[:len(parts)-1] {
		switch typed := node.(type) {
		case map[string]any:
			node = typed[part]
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, "", false
			}
			node = typed[index]
		default:
			return nil, "", false
		}
	}
	object, ok := node.(map[string]any)
	return object, parts[len(parts)-1], ok
}

func goldenHasMember(node any, name string) bool {
	switch typed := node.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == name || goldenHasMember(child, name) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if goldenHasMember(child, name) {
				return true
			}
		}
	}
	return false
}

// goldenAllowedKeyValue returns the 43-character x of an allowed entry after
// proving the enclosing object is a public Ed25519 JWK.
func goldenAllowedKeyValue(document any, entry goldenAllowedKey) (string, error) {
	object, member, ok := goldenPointerParent(document, entry.pointer)
	if !ok || member != "x" {
		return "", fmt.Errorf("allowed entry %s %s does not name an x member of a JSON object", entry.file, entry.pointer)
	}
	if object["kty"] != "OKP" || object["crv"] != "Ed25519" {
		return "", fmt.Errorf("allowed entry %s %s is not an OKP Ed25519 JWK", entry.file, entry.pointer)
	}
	if _, private := object["d"]; private {
		return "", fmt.Errorf("allowed entry %s %s carries a private d member", entry.file, entry.pointer)
	}
	value, _ := object["x"].(string)
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) != 32 {
		return "", fmt.Errorf("allowed entry %s %s x is not a 32-byte base64url value", entry.file, entry.pointer)
	}
	return value, nil
}

// scanGoldenTokenTrees returns every problem found under root/golden.
func scanGoldenTokenTrees(root string, allowed []goldenAllowedKey) []string {
	var problems []string
	documents := map[string]any{}
	findingsByFile := map[string][]string{}
	scanned := 0
	for _, tree := range goldenTokenTrees {
		dir := filepath.Join(root, "golden", tree)
		err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			scanned++
			relative, _ := filepath.Rel(filepath.Join(root, "golden"), path)
			relative = filepath.ToSlash(relative)
			findingsByFile[relative] = scanFixtureContent("golden/"+relative, string(raw))
			if jwsShape.Match(raw) {
				problems = append(problems, "compact JWS in "+relative)
			}
			var document any
			parsed := json.Unmarshal(raw, &document) == nil
			if parsed {
				documents[relative] = document
			}
			base := filepath.Base(relative)
			if (base == "jwks.json" || base == "manifest.json") && goldenHasMember(document, "d") {
				problems = append(problems, "private d member in "+relative)
			}
			if filepath.Base(filepath.Dir(relative)) == "vectors" {
				problems = append(problems, goldenVectorProblems(relative, document, parsed)...)
			}
			return nil
		})
		if err != nil {
			problems = append(problems, fmt.Sprintf("walking %s: %v", dir, err))
		}
	}
	if scanned == 0 {
		problems = append(problems, "scanned zero golden token files; the walk is vacuous")
	}

	claimed := map[string]bool{}
	for _, entry := range allowed {
		value, err := goldenAllowedKeyValue(documents[entry.file], entry)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		hit := false
		for _, finding := range findingsByFile[entry.file] {
			if strings.Contains(finding, ": "+value[:16]+"...") {
				claimed[finding] = true
				hit = true
			}
		}
		if !hit {
			problems = append(problems, fmt.Sprintf("stale allowed entry %s %s: the scanner no longer reports it", entry.file, entry.pointer))
		}
	}
	for _, findings := range findingsByFile {
		for _, finding := range findings {
			if !claimed[finding] {
				problems = append(problems, "unlisted finding: "+finding)
			}
		}
	}
	sort.Strings(problems)
	return problems
}

func goldenVectorProblems(relative string, document any, parsed bool) []string {
	members, ok := document.(map[string]any)
	if !parsed || !ok {
		return []string{"vector is not a JSON object: " + relative}
	}
	var problems []string
	if len(members) != 3 {
		problems = append(problems, fmt.Sprintf("vector %s has %d members, want header_json, payload_json, signature_hex", relative, len(members)))
	}
	for _, name := range []string{"header_json", "payload_json", "signature_hex"} {
		if _, ok := members[name].(string); !ok {
			problems = append(problems, fmt.Sprintf("vector %s lacks string member %s", relative, name))
		}
	}
	signature, _ := members["signature_hex"].(string)
	want, short := goldenShortSignatures[filepath.Base(relative)]
	if !short {
		want = 128
	}
	if !goldenLowerHex.MatchString(signature) || len(signature) != want {
		problems = append(problems, fmt.Sprintf("vector signature_hex is not %d lowercase hex characters: %s", want, relative))
	}
	return problems
}

func copyGoldenTokenTrees(t *testing.T, source, destination string) {
	t.Helper()
	for _, tree := range goldenTokenTrees {
		base := filepath.Join(source, "golden", tree)
		err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			relative, _ := filepath.Rel(source, path)
			target := filepath.Join(destination, relative)
			if entry.IsDir() {
				return os.MkdirAll(target, 0o755)
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			return os.WriteFile(target, raw, 0o644)
		})
		if err != nil {
			t.Fatalf("copying %s: %v", base, err)
		}
	}
}

func contractsGoldenSource(t *testing.T) string {
	t.Helper()
	return filepath.Join(ContractsDir(testRoot(t)))
}

func TestGoldenTokenTreesCarryOnlyListedPublicKeys(t *testing.T) {
	for _, problem := range scanGoldenTokenTrees(contractsGoldenSource(t), goldenAllowedPublicKeys) {
		t.Error(problem)
	}
}

func TestGoldenTokenTreeScanFailsOnPlantedSecrets(t *testing.T) {
	source := contractsGoldenSource(t)
	fresh := func(t *testing.T) string {
		root := t.TempDir()
		copyGoldenTokenTrees(t, source, root)
		return root
	}
	edit := func(t *testing.T, root, relative string, change func(map[string]any)) {
		path := filepath.Join(root, "golden", relative)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		change(document)
		out, _ := json.MarshalIndent(document, "", "  ")
		if err := os.WriteFile(path, out, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wantProblem := func(t *testing.T, root string, allowed []goldenAllowedKey, fragment string) {
		t.Helper()
		problems := scanGoldenTokenTrees(root, allowed)
		for _, problem := range problems {
			if strings.Contains(problem, fragment) {
				return
			}
		}
		t.Fatalf("no problem contains %q; got %v", fragment, problems)
	}

	t.Run("untouched copy is clean", func(t *testing.T) {
		if problems := scanGoldenTokenTrees(fresh(t), goldenAllowedPublicKeys); len(problems) != 0 {
			t.Fatalf("unexpected problems: %v", problems)
		}
	})
	t.Run("empty allow list reports the public keys", func(t *testing.T) {
		wantProblem(t, fresh(t), nil, "unlisted finding")
	})
	t.Run("planted private d member", func(t *testing.T) {
		root := fresh(t)
		edit(t, root, "access-token/jwks.json", func(document map[string]any) {
			keys := document["keys"].([]any)
			keys[0].(map[string]any)["d"] = "EXAMPLE"
		})
		wantProblem(t, root, goldenAllowedPublicKeys, "private d member in access-token/jwks.json")
	})
	t.Run("planted compact JWS", func(t *testing.T) {
		root := fresh(t)
		segment := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
		token := segment(`{"alg":"EdDSA"}`) + "." + segment(`{"sub":"x"}`) + "." + segment("signature")
		path := filepath.Join(root, "golden", "workload-token", "README.md")
		if err := os.WriteFile(path, []byte(token+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		wantProblem(t, root, goldenAllowedPublicKeys, "compact JWS in workload-token/README.md")
	})
	t.Run("new 43-character value", func(t *testing.T) {
		root := fresh(t)
		sum := sha256.Sum256([]byte("golden token scan control"))
		value := base64.RawURLEncoding.EncodeToString(sum[:])
		edit(t, root, "workload-token/jwks.json", func(document map[string]any) {
			document["note"] = value
		})
		wantProblem(t, root, goldenAllowedPublicKeys, "unlisted finding: golden/workload-token/jwks.json")
	})
	t.Run("changed allow list entry", func(t *testing.T) {
		root := fresh(t)
		for _, mutated := range []goldenAllowedKey{
			{"access-token/jwks.json", "/keys/2/x"},
			{"access-token/jwks.json", "/keys/0/kid"},
			{"access-token/missing.json", "/keys/0/x"},
		} {
			allowed := append([]goldenAllowedKey{mutated}, goldenAllowedPublicKeys[1:]...)
			wantProblem(t, root, allowed, "allowed entry")
			wantProblem(t, root, allowed, "unlisted finding")
		}
	})
	t.Run("allowed entry that is not a public Ed25519 key", func(t *testing.T) {
		root := fresh(t)
		edit(t, root, "access-token/jwks.json", func(document map[string]any) {
			document["keys"].([]any)[0].(map[string]any)["crv"] = "X25519"
		})
		wantProblem(t, root, goldenAllowedPublicKeys, "is not an OKP Ed25519 JWK")
	})
	t.Run("vector structure", func(t *testing.T) {
		root := fresh(t)
		vectors := filepath.Join("access-token", "vectors")
		entries, err := os.ReadDir(filepath.Join(root, "golden", vectors))
		if err != nil || len(entries) == 0 {
			t.Fatalf("no vectors in the copy: %v", err)
		}
		edit(t, root, filepath.Join(vectors, entries[0].Name()), func(document map[string]any) {
			document["signature_hex"] = strings.ToUpper(document["signature_hex"].(string))
			document["extra"] = "EXAMPLE"
		})
		wantProblem(t, root, goldenAllowedPublicKeys, "signature_hex is not 128 lowercase hex")
		wantProblem(t, root, goldenAllowedPublicKeys, "has 4 members")
	})
	t.Run("zero files", func(t *testing.T) {
		wantProblem(t, t.TempDir(), goldenAllowedPublicKeys, "scanned zero golden token files")
	})
}
