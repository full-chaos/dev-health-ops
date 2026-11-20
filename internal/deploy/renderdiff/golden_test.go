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

// goldensManifestSHA256 pins testdata/goldens-1/MANIFEST.tsv: the 67 cases the kit tool (sha256 ecf6771eb509...)
// produced, one row per case: name, exit code, sha256 of the captured stdout+stderr, argv. The source capture's
// MANIFEST sha12 is bc7e47ef923a; this copy differs only in the sha of the two filter outputs whose secret-derived
// hashes (four ClickHouse password_sha256_hex values, one checksum/secret annotation) are replaced by same-length
// letter-only placeholders, in the inputs and the outputs alike.
const goldensManifestSHA256 = "c317a11176c17f386966618f6e7ca04239853c7e656909a4c779366d520bc37b"

// goldens2ManifestSHA256 pins testdata/goldens-2/MANIFEST.tsv: 84 more kit cases on synthetic fixtures (the
// redaction forms, the empty forms, keyed and index lists, Python scalar types and YAML 1.1 resolution, the dump
// styles, the rows-file value checks, JSON errors, the names files and every usage refusal), captured with stdout
// and stderr apart: name, exit code, sha256(stdout), sha256(stderr), argv.
const goldens2ManifestSHA256 = "1616cf392f7034196bb2eeb17121475f389aeffc252b8576c69859f9193d484e"

// goldens3ManifestSHA256 pins testdata/goldens-3/MANIFEST.tsv: 3 kit cases (same form as goldens-2) for what the
// first mutant run showed goldens-1/-2 do not reach: a float at exponent 16, dict keys of mixed types in the dump
// (insertion order kept), a value check that names "<none>" for a missing value, an args entry with :// and @
// that the userinfo rule does not match.
const goldens3ManifestSHA256 = "ff8c4c77e536d0bb7c9e3c39a654e54ac4b0d2819c58bc9a09ca8bed7d267938"

const tracebackHead = "Traceback (most recent call last):\n"

type goldenCase struct {
	name   string
	rc     int
	sha    string
	errSHA string // goldens-2 only: the kit's stdout and stderr were captured apart
	args   []string
	stdin  string
}

func readManifest(t *testing.T, dir string) []goldenCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "MANIFEST.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 4 && len(f) != 5 {
			t.Fatalf("manifest row %q: want 4 or 5 fields", line)
		}
		rc, err := strconv.Atoi(f[1])
		if err != nil {
			t.Fatal(err)
		}
		c := goldenCase{name: f[0], rc: rc, sha: f[2]}
		argv := f[len(f)-1]
		if len(f) == 5 {
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
	t.Chdir(filepath.Join(dir, "in"))
	for _, c := range cases {
		want, err := os.ReadFile(filepath.Join(dir, "out", c.name+".out"))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(want)
		if hex.EncodeToString(sum[:]) != c.sha {
			t.Fatalf("%s: the golden file does not match its manifest sha", c.name)
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
			switch {
			case len(wantErr) == 0 && stderr.Len() == 0:
				byteEqual++
			case strings.HasPrefix(string(wantErr), tracebackHead) && stderr.Len() > 0:
				tracebacks++
			default:
				t.Errorf("%s: stderr %q, the kit's stderr %q", c.name, stderr.String(), string(wantErr))
			}
			continue
		}
		if strings.HasPrefix(string(want), tracebackHead) {
			if stdout.Len() != 0 || stderr.Len() == 0 {
				t.Errorf("%s: a kit traceback case must give empty stdout and one stderr line; stdout %q stderr %q", c.name, stdout.String(), stderr.String())
				continue
			}
			tracebacks++
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
		"testdata/goldens-1/MANIFEST.tsv": goldensManifestSHA256,
		"testdata/goldens-2/MANIFEST.tsv": goldens2ManifestSHA256,
		"testdata/goldens-3/MANIFEST.tsv": goldens3ManifestSHA256,
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

// TestEveryKitGoldenIsReproduced is the parity oracle: every one of the 67 kit cases gives the kit's exit code, and
// its exact stdout bytes. The two cases where the kit dies in a Python traceback (filter and names on a render that
// is not YAML) give rc 1, empty stdout and one stderr line.
func TestEveryKitGoldenIsReproduced(t *testing.T) {
	byteEqual, tracebacks := runGoldens(t, "testdata/goldens-1")
	t.Logf("byte-equal %d, traceback cases %d", byteEqual, tracebacks)
	if byteEqual != 65 || tracebacks != 2 {
		t.Fatalf("byte-equal %d (want 65), traceback cases %d (want 2)", byteEqual, tracebacks)
	}
}

// TestTheEdgeGoldensAreReproduced: every goldens-2 case gives the kit's exit code and stdout bytes; the kit's stderr
// is empty there too, except in the 12 cases where the kit dies in a traceback (the port prints one stderr line).
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
