package docsprepare

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goldenRevision = "rev\U0001F600\t\""

func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func treeBytes(t *testing.T, root string) map[string]string {
	t.Helper()
	got := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, rerr := os.ReadFile(p)
		rel, _ := filepath.Rel(root, p)
		got[rel] = string(b)
		return rerr
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// The golden trees were produced once by scripts/prepare_docs_cloudflare.py over testdata/site (NFC and NFD names,
// non-ASCII, quote and backslash names, a non-ASCII redirect row, a quoted TSV field). The Go port must match every
// byte, including the JSON \u escapes and the path order.
func TestPrepareMatchesPythonGolden(t *testing.T) {
	for _, mode := range []string{"preview", "production"} {
		t.Run(mode, func(t *testing.T) {
			outDir := filepath.Join(t.TempDir(), "out")
			code, stdout, stderr := run(t, "--source", "testdata/site", "--output", outDir, "--mode", mode,
				"--redirects", "testdata/redirects.tsv", "--source-revision", goldenRevision)
			if code != 0 || stderr != "" {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
			if !strings.HasPrefix(stdout, "Prepared Cloudflare documentation assets: mode="+mode+" files=") {
				t.Errorf("stdout = %q", stdout)
			}
			want, got := treeBytes(t, "testdata/golden-"+mode), treeBytes(t, outDir)
			for name, w := range want {
				g, ok := got[name]
				if !ok {
					t.Errorf("missing %q", name)
				} else if g != w {
					t.Errorf("%q differs:\n got %q\nwant %q", name, g, w)
				}
			}
			for name := range got {
				if _, ok := want[name]; !ok {
					t.Errorf("unexpected %q", name)
				}
			}
		})
	}
}

// Python names an undecodable file byte with a lone surrogate (\udcXX) in the manifest.
func TestInvalidUTF8NameMatchesPythonEscape(t *testing.T) {
	src := t.TempDir()
	for name, body := range map[string]string{"index.html": "<html>\n", "404.html": "404\n", "bad\xff.bin": "k", "ctl\x01\x7f.t": "c"} {
		if err := os.WriteFile(filepath.Join(src, name), []byte(body), 0o644); err != nil {
			t.Skipf("filesystem refuses the name: %v", err)
		}
	}
	outDir := filepath.Join(t.TempDir(), "out")
	if code, _, stderr := run(t, "--source", src, "--output", outDir, "--mode", "preview", "--redirects", "testdata/redirects.tsv"); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	m, _ := os.ReadFile(filepath.Join(outDir, "cloudflare-build-manifest.json"))
	if !strings.Contains(string(m), `"path": "bad\udcff.bin"`) {
		t.Errorf("manifest = %s", m)
	}
	if !strings.Contains(string(m), `"path": "ctl\u0001\u007f.t"`) {
		t.Errorf("control bytes not escaped like Python: %s", m)
	}
}

func TestPrepareRejects(t *testing.T) {
	tsv := func(body string) string {
		p := filepath.Join(t.TempDir(), "r.tsv")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	site := t.TempDir()
	for _, n := range []string{"index.html", "404.html"} {
		os.WriteFile(filepath.Join(site, n), nil, 0o644)
	}
	hdr := "source_path\ttarget_path\n"
	cases := []struct{ name, redirects, origin, want string }{
		{"duplicate", tsv(hdr + "/a\t/b\n/a\t/c\n"), "", "duplicate redirect source at"},
		{"self", tsv(hdr + "/a\t/a\n"), "", "self redirect at"},
		{"relative", tsv(hdr + "a\t/a\n"), "", "redirect paths must be site-relative at"},
		{"short row", tsv(hdr + "/a\n"), "", "empty redirect at"},
		{"header", tsv("a\tb\n/a\t/b\n"), "", "redirect manifest is missing ['source_path', 'target_path']"},
		{"empty file", tsv(""), "", "redirect manifest is missing"},
		{"http origin", tsv(hdr), "http://x.com", "canonical origin must be an HTTPS origin: http://x.com"},
		{"origin path", tsv(hdr), "https://x.com/p", "canonical origin must be an HTTPS origin"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := []string{"--source", site, "--output", filepath.Join(t.TempDir(), "o"), "--mode", "preview", "--redirects", c.redirects}
			if c.origin != "" {
				args = append(args, "--canonical-origin", c.origin)
			}
			code, _, stderr := run(t, args...)
			if code != 1 || !strings.HasPrefix(stderr, "ERROR: ") || !strings.Contains(stderr, c.want) {
				t.Errorf("code=%d stderr=%q, want %q", code, stderr, c.want)
			}
		})
	}
}

func TestPrepareRejectsMarkdownAndMissingInputs(t *testing.T) {
	site := t.TempDir()
	for _, n := range []string{"index.html", "404.html", "x.md"} {
		os.WriteFile(filepath.Join(site, n), nil, 0o644)
	}
	args := func(src string) []string {
		return []string{"--source", src, "--output", filepath.Join(t.TempDir(), "o"), "--mode", "production", "--redirects", "testdata/redirects.tsv"}
	}
	if code, _, e := run(t, args(site)...); code != 1 || !strings.Contains(e, "built output unexpectedly contains Markdown source: x.md") {
		t.Errorf("markdown: code=%d %q", code, e)
	}
	if code, _, e := run(t, args(filepath.Join(site, "nope"))...); code != 1 || !strings.Contains(e, "built documentation directory not found") {
		t.Errorf("missing source: code=%d %q", code, e)
	}
	empty := t.TempDir()
	if code, _, e := run(t, args(empty)...); code != 1 || !strings.Contains(e, "has no index.html") {
		t.Errorf("no index: code=%d %q", code, e)
	}
}

func TestUsage(t *testing.T) {
	if code, _, _ := run(t, "--mode", "preview"); code != 2 {
		t.Errorf("missing flags: code=%d", code)
	}
	if code, _, _ := run(t, "--source", "a", "--output", "b", "--mode", "staging", "--redirects", "c"); code != 2 {
		t.Errorf("bad mode: code=%d", code)
	}
}
