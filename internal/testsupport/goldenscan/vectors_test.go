package goldenscan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// scanner_vectors.tsv holds the verdict of the REAL scanner (gitleaks v8.21.2, default config) on a set of vectors, recorded by the
// verb below; the port must report a hit on every vector the scanner does (Go-hit is a superset of gitleaks-hit, so it may be stricter, never looser), so its parity is executed, not argued. A vector is a key and a value spec; the
// value is made at run time, so no secret-shaped text is in the file.

const (
	vectorsPath = "testdata/scanner_vectors.tsv"
	recordEnv   = "GOLDENSCAN_RECORD_VECTORS"
)

type vector struct{ name, key, spec, verdict string }

func readVectors(t *testing.T) (comments []string, vectors []vector) {
	t.Helper()
	raw, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			comments = append(comments, line)
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 4 {
			t.Fatalf("vector line %q: want 4 fields", line)
		}
		vectors = append(vectors, vector{f[0], f[1], f[2], f[3]})
	}
	return comments, vectors
}

var alphabets = map[string]string{
	"alnum":   "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
	"hex":     "0123456789abcdef",
	"letters": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
	"lower":   "abcdefghijklmnopqrstuvwxyz",
	"digits":  "0123456789",
	"url":     "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.=-",
}

// valueOf makes the value of a spec: parts joined by +; r:<alphabet>:<n>:<seed>, u:uuid:<seed>, or a literal.
func valueOf(t *testing.T, spec string) string {
	t.Helper()
	var b strings.Builder
	for _, part := range strings.Split(spec, "+") {
		fields := strings.Split(part, ":")
		switch {
		case len(fields) == 4 && fields[0] == "r":
			alphabet, ok := alphabets[fields[1]]
			if !ok && strings.HasPrefix(fields[1], "s") {
				alphabet, ok = fields[1][1:], true
			}
			n, err := strconv.Atoi(fields[2])
			if !ok || err != nil {
				t.Fatalf("bad vector part %q", part)
			}
			for i := 0; i < n; i++ {
				sum := sha256.Sum256([]byte(fields[3] + ":" + strconv.Itoa(i)))
				b.WriteByte(alphabet[int(sum[0])%len(alphabet)])
			}
		case len(fields) == 3 && fields[0] == "u" && fields[1] == "uuid":
			b.WriteString(derivedUUID("vector-" + fields[2]))
		default:
			b.WriteString(part)
		}
	}
	return b.String()
}

func unitOf(v vector, value string) string { return `"` + v.key + `": "` + value + `"` + "\n" }

func goVerdict(v vector, value string) string {
	if len(SecretsIn(`"`+v.key+`": "`+value+`"`)) > 0 {
		return "hit"
	}
	return "none"
}

// The port reports a hit on every vector the real scanner does (Go-hit is a superset of gitleaks-hit): a miss is a fail-open and fails
// the test; a vector the port is stricter on is allowed and counted.
func TestThePortNeverMissesWhatTheRealScannerFinds(t *testing.T) {
	_, vectors := readVectors(t)
	if len(vectors) < 60 {
		t.Fatalf("only %d vectors", len(vectors))
	}
	seen := map[string]int{}
	stricter := 0
	for _, v := range vectors {
		if v.verdict != "hit" && v.verdict != "none" {
			t.Errorf("vector %s has no recorded verdict (%q): record it with %s", v.name, v.verdict, recordEnv)
			continue
		}
		seen[v.verdict]++
		got := goVerdict(v, valueOf(t, v.spec))
		switch {
		case v.verdict == "hit" && got != "hit":
			t.Errorf("vector %s (key %s): the scanner flags it and the port does not: the port is looser than the scanner", v.name, v.key)
		case v.verdict == "none" && got == "hit":
			stricter++
			t.Logf("vector %s (key %s): the port is stricter than the scanner", v.name, v.key)
		}
	}
	if seen["hit"] < 10 || seen["none"] < 10 {
		t.Errorf("the vectors do not hold both verdicts: %v", seen)
	}
	if stricter != 0 {
		t.Logf("%d vector(s) where the port is stricter", stricter)
	}
}

// The port is also exactly as strict as the scanner on today's vectors: a stricter port is allowed by the contract above, but it is a
// change to look at, so it is pinned here (to accept a stricter vector on purpose, say so in the vector's name and relax this test).
func TestThePortIsAsStrictAsTheRealScannerOnTheVectors(t *testing.T) {
	_, vectors := readVectors(t)
	for _, v := range vectors {
		if got := goVerdict(v, valueOf(t, v.spec)); v.verdict == "none" && got == "hit" {
			t.Errorf("vector %s (key %s): the port is stricter than the scanner (allowed by the contract, but a change to review)", v.name, v.key)
		}
	}
}

// TestRecordVectors is the recording verb: with GOLDENSCAN_RECORD_VECTORS set to a gitleaks binary it writes one unit file per vector,
// runs the binary over them and rewrites the verdict column. Without the variable it does nothing.
func TestRecordVectors(t *testing.T) {
	binary := os.Getenv(recordEnv)
	if binary == "" {
		t.Skip(recordEnv + " is not set")
	}
	comments, vectors := readVectors(t)
	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	dir := t.TempDir()
	for i, v := range vectors {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("v%03d.txt", i)), []byte(unitOf(v, valueOf(t, v.spec))), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	empty := filepath.Join(t.TempDir(), "empty.gitleaksignore")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(t.TempDir(), "report.json")
	out, err := exec.Command(binary, "dir", "--no-banner", "--redact", "--exit-code", "0", "--gitleaks-ignore-path", empty, "--report-format", "json", "--report-path", report, dir).CombinedOutput()
	if err != nil {
		t.Fatalf("the scanner failed: %v\n%s", err, out)
	}
	var findings []struct {
		File   string
		RuleID string
	}
	if body, err := os.ReadFile(report); err == nil {
		if err := json.Unmarshal(body, &findings); err != nil {
			t.Fatal(err)
		}
	}
	hit := map[string]bool{}
	for _, f := range findings {
		if f.RuleID != "generic-api-key" {
			t.Fatalf("a vector made another rule (%s) fire in %s: the vectors are generic-api-key vectors", f.RuleID, f.File)
		}
		hit[filepath.Base(f.File)] = true
	}
	var b strings.Builder
	for _, line := range comments {
		if !strings.HasPrefix(line, "# binary sha256:") {
			b.WriteString(line + "\n")
		}
	}
	b.WriteString("# binary sha256: " + hex.EncodeToString(sum[:]) + " (gitleaks v8.21.2, default config, empty ignore file, `dir` over one unit file per vector)\n")
	for i, v := range vectors {
		verdict := "none"
		if hit[fmt.Sprintf("v%03d.txt", i)] {
			verdict = "hit"
		}
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\n", v.name, v.key, v.spec, verdict)
	}
	if err := os.WriteFile(vectorsPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
