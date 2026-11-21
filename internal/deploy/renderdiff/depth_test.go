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

// deepRender is the verify-6720 depth_sweep generator: a value nested d levels under the key v, in one of four
// YAML styles.
func deepRender(style string, d int) string {
	var v strings.Builder
	switch style {
	case "flowlist":
		v.WriteString(strings.Repeat("[", d) + "SNTDEEP" + strings.Repeat("]", d))
	case "flowmap":
		v.WriteString(strings.Repeat("{a: ", d) + "SNTDEEP" + strings.Repeat("}", d))
	case "blocklist":
		v.WriteString("\n")
		for i := 0; i < d-1; i++ {
			v.WriteString(strings.Repeat(" ", 2*i) + "-\n")
		}
		v.WriteString(strings.Repeat(" ", 2*(d-1)) + "- SNTDEEP")
	default:
		v.WriteString("\n")
		for i := 0; i < d-1; i++ {
			v.WriteString(strings.Repeat(" ", 2*i+2) + "a:\n")
		}
		v.WriteString(strings.Repeat(" ", 2*d) + "a: SNTDEEP")
	}
	return "kind: X\nmetadata: {name: deep}\nv: " + v.String() + "\n"
}

// TestTheKitDepthBoundsAreKept: at every row of depth-kit.tsv (the kit's own run) the port gives the kit's rc, the
// same stdout bytes, and the kit's crash token where the kit dies in a RecursionError: the dump dies 330 levels
// under a key (main), composing a block-style render dies at 489 (a render file) or 490 (stdin) levels (docs_of).
func TestTheKitDepthBoundsAreKept(t *testing.T) {
	raw, err := os.ReadFile("testdata/goldens-6/depth-kit.tsv")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	rows := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	crashes := 0
	for _, row := range rows {
		f := strings.Split(row, "\t")
		if len(f) != 6 {
			t.Fatalf("depth row %q: want 6 fields", row)
		}
		d, _ := strconv.Atoi(f[1])
		wantRC, _ := strconv.Atoi(f[3])
		text := deepRender(f[0], d)
		path := filepath.Join(dir, "deep.yaml")
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		args := []string{f[2]}
		stdin := text
		if f[2] == "info" {
			args, stdin = []string{path, path, "--rc-a", "0", "--rc-b", "0", "--info"}, ""
		}
		var stdout, stderr bytes.Buffer
		rc := Main(args, strings.NewReader(stdin), &stdout, &stderr)
		sum := sha256.Sum256(stdout.Bytes())
		wantErr := ""
		if f[5] != "-" {
			wantErr = "full-render-diff: crash " + f[5] + "\n"
			crashes++
		}
		if rc != wantRC || hex.EncodeToString(sum[:]) != f[4] || stderr.String() != wantErr {
			t.Errorf("%s %d %s: rc %d stderr %q, the kit: rc %d %s", f[0], d, f[2], rc, stderr.String(), wantRC, f[5])
		}
	}
	if len(rows) != 51 || crashes != 25 {
		t.Fatalf("%d rows, %d crash rows: want 51 and 25", len(rows), crashes)
	}
}

// TestEachDepthBoundIsPinnedByTheKit: each bound of the port is a pair of depth-kit.tsv rows, the deepest level that
// passes and the first that the kit refuses, with the kit's token: the dump 329/330 levels (RecursionError@main, all
// four styles), composing a render file 488/489 and stdin 489/490 levels (RecursionError@docs_of, block styles). The
// flow styles reach 2 more levels on the kit; the port refuses them 2 levels earlier (fails closed), not pinned.
func TestEachDepthBoundIsPinnedByTheKit(t *testing.T) {
	raw, err := os.ReadFile("testdata/goldens-6/depth-kit.tsv")
	if err != nil {
		t.Fatal(err)
	}
	kit := map[string]string{}
	for _, row := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		f := strings.Split(row, "\t")
		kit[f[0]+" "+f[1]+" "+f[2]] = f[3] + " " + f[5]
	}
	type pin struct{ style, mode, pass, refuse, token string }
	var pins []pin
	for _, s := range []string{"flowlist", "flowmap", "blocklist", "blockmap"} {
		pins = append(pins, pin{s, "filter", "329", "330", "RecursionError@main"})
	}
	for _, s := range []string{"blocklist", "blockmap"} {
		pins = append(pins, pin{s, "info", "488", "489", "RecursionError@docs_of"}, pin{s, "names", "489", "490", "RecursionError@docs_of"})
	}
	for _, p := range pins {
		if got := kit[p.style+" "+p.pass+" "+p.mode]; got != "0 -" {
			t.Errorf("%s %s %s: the kit row is %q, want a pass", p.style, p.pass, p.mode, got)
		}
		if got := kit[p.style+" "+p.refuse+" "+p.mode]; got != "1 "+p.token {
			t.Errorf("%s %s %s: the kit row is %q, want 1 %s", p.style, p.refuse, p.mode, got, p.token)
		}
		for _, d := range []string{p.pass, p.refuse} {
			n, _ := strconv.Atoi(d)
			text := deepRender(p.style, n)
			path := filepath.Join(t.TempDir(), "deep.yaml")
			if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
			args, stdin := []string{p.mode}, text
			if p.mode == "info" {
				args, stdin = []string{path, path, "--rc-a", "0", "--rc-b", "0", "--info"}, ""
			}
			var stdout, stderr bytes.Buffer
			rc := Main(args, strings.NewReader(stdin), &stdout, &stderr)
			want := "0 "
			if d == p.refuse {
				want = "1 full-render-diff: crash " + p.token + "\n"
			}
			if got := strconv.Itoa(rc) + " " + stderr.String(); got != want {
				t.Errorf("%s %s %s: port %q, the kit %q", p.style, d, p.mode, got, want)
			}
		}
	}
}
