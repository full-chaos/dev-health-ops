package renderdiff

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// goldensManifestSHA256 pins testdata/goldens-1/MANIFEST.tsv: the 67 cases of the 11788 capture (its case names,
// order and argv), recorded by the kit tool (sha256 ecf6771eb509...) on SYNTHETIC renders: the prod renders of the
// first capture are replaced by same-shape synthetic ones (example.invalid hosts, fake digests, every other string
// value, names and namespaces too, a fresh random string; the same diff and redaction features). One row per case:
// name, exit code, sha256 of the captured stdout+stderr, argv, crash token (the last column is added here; see TestTheCrashTokensAreTheKitsOwn).
const goldensManifestSHA256 = "5b950abcd5e0039f9e7798cc369dc0f921d6f06af6207dd6788875b96a0f3193"

// goldens2ManifestSHA256 pins testdata/goldens-2/MANIFEST.tsv: 84 more kit cases on synthetic fixtures (the
// redaction forms, the empty forms, keyed and index lists, Python scalar types and YAML 1.1 resolution, the dump
// styles, the rows-file value checks, JSON errors, the names files and every usage refusal), captured with stdout
// and stderr apart: name, exit code, sha256(stdout), sha256(stderr), argv, crash token.
const goldens2ManifestSHA256 = "4892fccbcc57f4b4e5101b5aa1575dffb06f5d5715c6e24fc47199838e6243c2"

// goldens3ManifestSHA256 pins testdata/goldens-3/MANIFEST.tsv: 3 kit cases (same form as goldens-2) for what the
// first mutant run showed goldens-1/-2 do not reach: a float at exponent 16, dict keys of mixed types in the dump
// (insertion order kept), a value check that names "<none>" for a missing value, an args entry with :// and @
// that the userinfo rule does not match.
const goldens3ManifestSHA256 = "b7710e41570e205851b895bac71e14e1f1446fefae6ba6fd2165c381333ce79b"

// goldens4ManifestSHA256 pins testdata/goldens-4/MANIFEST.tsv: 1 kit case (same form as goldens-2), a TypeError in
// the kit's nested sig() (a rows-file "old" that is an int), so the crash token names "sig".
const goldens4ManifestSHA256 = "e68d0e273cf0998f4810fe43c761871bd3da2194b6533089a3c72e50da8a3e0d"

// goldens5ManifestSHA256 pins testdata/goldens-5/MANIFEST.tsv: 29 kit cases (same form as goldens-2) for the
// classes the first r1 log showed: Python re.IGNORECASE on secret-like names (U+0130, U+0131, U+017F, U+212A), the
// 4300-digit int<->str limit at every site, int()/float() text under an explicit tag, a bare sign, base64 padding.
const goldens5ManifestSHA256 = "bec4f83df092b025e887fc9d2f007a1788201036cff552f7a1e940a02661382d"

// goldens6ManifestSHA256 pins testdata/goldens-6/MANIFEST.tsv: 29 kit cases (same form as goldens-2) on the
// verify-6720 repro inputs: every `.nan` key is ONE key (PyYAML's shared nan_value), and a mapping that merges
// itself (v: &m {<<: *m} and its variants) is what flatten_mapping leaves, or the kit's RecursionError in redact;
// a list that holds itself is one list (names lists it, redact dies).
const goldens6ManifestSHA256 = "1017e2a92f0eac9343a77fc3c2c9e1719e6cf5af4263e1958f7b747aa5a319ab"

// depthKitSHA256 pins testdata/goldens-6/depth-kit.tsv: the kit's rc, sha256(stdout) and crash token for generated
// deep renders (the verify-6720 depth_sweep generator) in filter, names and the diff.
const depthKitSHA256 = "7fb953b5dbad7b4caffae68a4b318d44c121f4ba2b26770eb4a39d019a5a2c6b"

const tracebackHead = "Traceback (most recent call last):\n"

type goldenCase struct {
	name   string
	rc     int
	sha    string
	errSHA string // goldens-2/-3 only: the kit's stdout and stderr were captured apart
	args   []string
	stdin  string
	crash  string // the crash-token column: "-", or the kit traceback's class token
}

func readManifest(t *testing.T, dir string) []goldenCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "MANIFEST.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		// goldens-1: name rc sha(stdout+stderr) argv crash; goldens-2/-3: name rc sha(stdout) sha(stderr) argv crash.
		f := strings.Split(line, "\t")
		if len(f) != 5 && len(f) != 6 {
			t.Fatalf("manifest row %q: want 5 or 6 fields", line)
		}
		rc, err := strconv.Atoi(f[1])
		if err != nil {
			t.Fatal(err)
		}
		c := goldenCase{name: f[0], rc: rc, sha: f[2], crash: f[len(f)-1]}
		argv := f[len(f)-2]
		if len(f) == 6 {
			c.errSHA = f[3]
		}
		if mode, file, ok := strings.Cut(argv, " < "); ok {
			c.args, c.stdin = []string{mode}, file
		} else if argv != "" {
			c.args = strings.Split(argv, " ")
		}
		cases = append(cases, c)
	}
	return cases
}

// runGoldens runs every case of a goldens dir through Main in its in/ dir (the kit ran with cwd = in/, so the paths
// it prints are the relative argv paths) and returns the count of byte-equal cases and of traceback cases.
func runGoldens(t *testing.T, dir string) (byteEqual, tracebacks int) {
	t.Helper()
	dir, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	cases := readManifest(t, dir)
	fixed := readFixed(t, filepath.Join(filepath.Dir(dir), fixedDir), filepath.Base(dir))
	t.Chdir(kitInputs(t, filepath.Join(dir, "in")))
	for _, c := range cases {
		want, err := os.ReadFile(filepath.Join(dir, "out", c.name+".out"))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(want)
		if hex.EncodeToString(sum[:]) != c.sha {
			t.Fatalf("%s: the golden file does not match its manifest sha", c.name)
		}
		if f, ok := fixed[c.name]; ok {
			want, c.rc = f.out, f.rc
		}
		var stdin []byte
		if c.stdin != "" {
			if stdin, err = os.ReadFile(c.stdin); err != nil {
				t.Fatal(err)
			}
		}
		var stdout, stderr bytes.Buffer
		rc := Main(c.args, bytes.NewReader(stdin), &stdout, &stderr)
		if rc != c.rc {
			t.Errorf("%s: rc %d, the kit gave %d", c.name, rc, c.rc)
			continue
		}
		if c.errSHA != "" {
			wantErr, err := os.ReadFile(filepath.Join(dir, "out", c.name+".err"))
			if err != nil {
				t.Fatal(err)
			}
			if sum := sha256.Sum256(wantErr); hex.EncodeToString(sum[:]) != c.errSHA {
				t.Fatalf("%s: the golden stderr file does not match its manifest sha", c.name)
			}
			if !bytes.Equal(stdout.Bytes(), want) {
				t.Errorf("%s: stdout differs from the kit golden at byte %d\n got: %q\nwant: %q", c.name, firstDiff(stdout.Bytes(), want), clip(stdout.Bytes(), want), clip(want, stdout.Bytes()))
				continue
			}
			if c.crash != "-" {
				if checkCrash(t, c, stderr.String()) {
					tracebacks++
				}
				continue
			}
			if len(wantErr) != 0 || stderr.Len() != 0 {
				t.Errorf("%s: stderr %q, the kit's stderr %q", c.name, stderr.String(), string(wantErr))
				continue
			}
			byteEqual++
			continue
		}
		if c.crash != "-" {
			// The kit's stdout is empty here: the captured stream is the traceback alone (its stderr came first).
			if stdout.Len() != 0 {
				t.Errorf("%s: stdout %q, the kit's stdout is empty", c.name, stdout.String())
				continue
			}
			if checkCrash(t, c, stderr.String()) {
				tracebacks++
			}
			continue
		}
		if stderr.Len() != 0 {
			t.Errorf("%s: unexpected stderr %q", c.name, stderr.String())
			continue
		}
		if !bytes.Equal(stdout.Bytes(), want) {
			t.Errorf("%s: stdout differs from the kit golden at byte %d\n got: %q\nwant: %q", c.name, firstDiff(stdout.Bytes(), want), clip(stdout.Bytes(), want), clip(want, stdout.Bytes()))
			continue
		}
		byteEqual++
	}
	return byteEqual, tracebacks
}

// checkCrash is the kit-crash parity form (ACP-WORD-14332-1): rc = crashRC and the stderr is exactly one line
// that carries the case's fixed class token (string equality).
func checkCrash(t *testing.T, c goldenCase, stderr string) bool {
	t.Helper()
	if c.rc != crashRC {
		t.Errorf("%s: a crash case with kit rc %d, crashRC is %d", c.name, c.rc, crashRC)
		return false
	}
	if want := "full-render-diff: crash " + c.crash + "\n"; stderr != want {
		t.Errorf("%s: stderr %q, want %q", c.name, stderr, want)
		return false
	}
	return true
}

// kitCrashToken derives the class token from a kit traceback with plain string steps. In the LAST traceback block
// (a chained exception prints one block per exception): the exception type of the first unindented line after the
// header (every yaml.* type as its base class YAMLError) and the function of the last frame in the kit file.
func kitCrashToken(kitStream string) string {
	if !strings.HasPrefix(kitStream, tracebackHead) {
		return "-"
	}
	lines := strings.Split(kitStream, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if lines[i]+"\n" == tracebackHead {
			lines = lines[i:]
			break
		}
	}
	fn, typ := "", ""
	for _, l := range lines[1:] {
		if strings.HasPrefix(l, "  File \"") && strings.Contains(l, "full-render-diff.py\", line ") {
			_, fn, _ = strings.Cut(l, ", in ")
		}
		if typ == "" && l != "" && !strings.HasPrefix(l, " ") {
			typ, _, _ = strings.Cut(l, ":")
		}
	}
	if strings.HasPrefix(typ, "yaml.") {
		typ = "YAMLError"
	}
	return typ + "@" + fn
}

// TestTheCrashTokensAreTheKitsOwn: the crash column of every manifest row is what the kit's own captured stream
// gives (a "-" row has no traceback), so no token is chosen by hand.
func TestTheCrashTokensAreTheKitsOwn(t *testing.T) {
	crashes := 0
	for _, dir := range []string{"testdata/goldens-1", "testdata/goldens-2", "testdata/goldens-3", "testdata/goldens-4", "testdata/goldens-5", "testdata/goldens-6"} {
		for _, c := range readManifest(t, dir) {
			stream := filepath.Join(dir, "out", c.name+".out")
			if c.errSHA != "" {
				stream = filepath.Join(dir, "out", c.name+".err")
			}
			raw, err := os.ReadFile(stream)
			if err != nil {
				t.Fatal(err)
			}
			if got := kitCrashToken(string(raw)); got != c.crash {
				t.Errorf("%s/%s: manifest crash token %q, the kit traceback gives %q", dir, c.name, c.crash, got)
			}
			if c.crash != "-" {
				crashes++
			}
		}
	}
	if crashes != 45 {
		t.Fatalf("%d crash rows, want 45 (2 in goldens-1, 12 in goldens-2, 1 in goldens-4, 20 in goldens-5, 10 in goldens-6)", crashes)
	}
}

// TestACrashLineHoldsNoInputBytes: the crash line is the fixed token even when the failing value, key, anchor or
// tag is a planted canary.
func TestACrashLineHoldsNoInputBytes(t *testing.T) {
	const canary = "CANARYq7x"
	both := []string{"filter", "names"}
	for _, tc := range []struct {
		in    string
		modes []string
	}{
		{"kind: ConfigMap\nmetadata: {name: x}\ndata: {a: !!bool " + canary + "}\n", both},
		{"kind: ConfigMap\nmetadata: {name: x}\ndata: {a: !!int " + canary + "}\n", both},
		{"kind: ConfigMap\nmetadata: {name: x}\ndata: {a: !" + canary + " 1}\n", both},
		{"kind: ConfigMap\nmetadata: {name: x}\ndata: {a: *" + canary + "}\n", both},
		{"kind: ConfigMap\nmetadata: {name: x, " + canary + ": [\n", both},
		{"kind: ConfigMap\nmetadata: " + canary + "\n", []string{"names"}},
	} {
		in := tc.in
		for _, mode := range tc.modes {
			var stdout, stderr bytes.Buffer
			rc := Main([]string{mode}, strings.NewReader(in), &stdout, &stderr)
			if rc != crashRC || !strings.HasPrefix(stderr.String(), "full-render-diff: crash ") || strings.Count(stderr.String(), "\n") != 1 {
				t.Errorf("%s %q: rc %d stderr %q, want rc %d and one crash line", mode, in, rc, stderr.String(), crashRC)
			}
			if strings.Contains(stdout.String()+stderr.String(), canary) {
				t.Errorf("%s %q: the canary reached the output: %q", mode, in, stderr.String())
			}
		}
	}
}

// invalidJSONSuffix marks a rows file the kit reads that is NOT JSON on purpose (a JSON error case). Every *.json
// under a testdata dir must parse (the golden secret scan reads them all), so such a file is stored with this
// suffix and gets its kit name back in the run copy.
const invalidJSONSuffix = ".json.invalid"

// kitInputs copies an in/ dir to a temp dir with the kit's own file names (the kit prints them) and returns it.
func kitInputs(t *testing.T, in string) string {
	t.Helper()
	tmp := t.TempDir()
	entries, err := os.ReadDir(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(in, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		name := e.Name()
		if strings.HasSuffix(name, invalidJSONSuffix) {
			name = strings.TrimSuffix(name, invalidJSONSuffix) + ".json"
		}
		if err := os.WriteFile(filepath.Join(tmp, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return tmp
}

// fixedDir holds the cases whose expected stdout is NOT the kit's because a fix of a kit gap changes it (CHAOS-14332:
// a changed redacted value is a row, an over-redacted ConfigMap key too, --live counts Secret docs). Each row names
// its golden dir, case, rc, sha256 and the gap classes of its difference. Each was checked against the kit: every
// kit-to-port line is in one of the three gap classes.
const fixedDir = "fixed-3b"

type fixedCase struct {
	rc      int
	out     []byte
	classes string
}

func readFixed(t *testing.T, root, golden string) map[string]fixedCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "MANIFEST.tsv"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]fixedCase{}
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 5 || f[4] == "" {
			t.Fatalf("fixed row %q: want golden, case, rc, sha256, classes", line)
		}
		if f[0] != golden {
			continue
		}
		rc, err := strconv.Atoi(f[2])
		if err != nil {
			t.Fatal(err)
		}
		body, err := os.ReadFile(filepath.Join(root, f[0], f[1]+".out"))
		if err != nil {
			t.Fatal(err)
		}
		if sum := sha256.Sum256(body); hex.EncodeToString(sum[:]) != f[3] {
			t.Fatalf("fixed %s/%s does not match its sha", f[0], f[1])
		}
		out[f[1]] = fixedCase{rc: rc, out: body, classes: f[4]}
	}
	return out
}

func firstDiff(a, b []byte) int {
	for i := range a {
		if i >= len(b) || a[i] != b[i] {
			return i
		}
	}
	return len(a)
}

func clip(a, b []byte) string {
	i := firstDiff(a, b)
	lo, hi := max(0, i-120), min(len(a), i+120)
	return string(a[lo:hi])
}

func TestTheGoldensManifestsArePinned(t *testing.T) {
	for path, pin := range map[string]string{
		"testdata/goldens-1/MANIFEST.tsv":  goldensManifestSHA256,
		"testdata/goldens-2/MANIFEST.tsv":  goldens2ManifestSHA256,
		"testdata/goldens-3/MANIFEST.tsv":  goldens3ManifestSHA256,
		"testdata/goldens-4/MANIFEST.tsv":  goldens4ManifestSHA256,
		"testdata/goldens-5/MANIFEST.tsv":  goldens5ManifestSHA256,
		"testdata/goldens-6/MANIFEST.tsv":  goldens6ManifestSHA256,
		"testdata/goldens-6/depth-kit.tsv": depthKitSHA256,
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != pin {
			t.Fatalf("%s sha256 = %s, pinned %s", path, got, pin)
		}
	}
}

// TestEveryKitGoldenIsReproduced is the parity oracle: 65 of the 67 kit cases are byte-equal on rc, stdout and
// stderr. The two cases where the kit dies in a Python traceback (filter and names on a render that is not YAML)
// give crashRC, the kit's (empty) stdout and one stderr line with the case's crash token.
func TestEveryKitGoldenIsReproduced(t *testing.T) {
	byteEqual, tracebacks := runGoldens(t, "testdata/goldens-1")
	t.Logf("byte-equal %d, traceback cases %d", byteEqual, tracebacks)
	if byteEqual != 65 || tracebacks != 2 {
		t.Fatalf("byte-equal %d (want 65), traceback cases %d (want 2)", byteEqual, tracebacks)
	}
}

// TestTheEdgeGoldensAreReproduced: every goldens-2 case gives the kit's exit code and stdout bytes; the kit's stderr
// is empty there too, except in the 12 cases where the kit dies in a traceback (crashRC and the crash-token line).
func TestTheEdgeGoldensAreReproduced(t *testing.T) {
	byteEqual, tracebacks := runGoldens(t, "testdata/goldens-2")
	t.Logf("byte-equal %d, traceback cases %d", byteEqual, tracebacks)
	if byteEqual != 72 || tracebacks != 12 {
		t.Fatalf("byte-equal %d (want 72), traceback cases %d (want 12)", byteEqual, tracebacks)
	}
}

func TestTheMutantGapGoldensAreReproduced(t *testing.T) {
	byteEqual, tracebacks := runGoldens(t, "testdata/goldens-3")
	if byteEqual != 3 || tracebacks != 0 {
		t.Fatalf("byte-equal %d (want 3), traceback cases %d (want 0)", byteEqual, tracebacks)
	}
}

func TestTheSigCrashGoldenIsReproduced(t *testing.T) {
	byteEqual, tracebacks := runGoldens(t, "testdata/goldens-4")
	if byteEqual != 0 || tracebacks != 1 {
		t.Fatalf("byte-equal %d (want 0), crash-token cases %d (want 1)", byteEqual, tracebacks)
	}
}

func TestTheRoundOneClassGoldensAreReproduced(t *testing.T) {
	byteEqual, tracebacks := runGoldens(t, "testdata/goldens-5")
	if byteEqual != 9 || tracebacks != 20 {
		t.Fatalf("byte-equal %d (want 9), crash-token cases %d (want 20)", byteEqual, tracebacks)
	}
}

func TestTheVerifyReproGoldensAreReproduced(t *testing.T) {
	byteEqual, tracebacks := runGoldens(t, "testdata/goldens-6")
	if byteEqual != 19 || tracebacks != 10 {
		t.Fatalf("byte-equal %d (want 19), crash-token cases %d (want 10)", byteEqual, tracebacks)
	}
}
