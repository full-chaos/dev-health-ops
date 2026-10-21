package docsprepare

import (
	"bytes"
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

type fixtureFile struct{ name, body string }

// fixtureFiles is the tree the golden files were captured from: the former Python helper ran once over exactly these
// names (NFC and NFD, emoji, quote and backslash, a private-use name, control bytes, invalid UTF-8) and its manifest,
// _redirects, _headers and robots.txt were saved in testdata. The names are built here, not committed, because
// non-ASCII and non-UTF-8 file names break git and the repository's path guards.
var fixtureFiles = []fixtureFile{
	{"index.html", "<html>\n"}, {"404.html", "404\n"}, {"assets/x.css", "a{}"}, {"é/sub/n.html", "x"},
	{"é.html", "y"}, {"a-b.txt", "z"}, {"a/b", "q"}, {"a b", "q"}, {"\U0001F600.txt", "\U0001F600"},
	{"empty", ""}, {"q\"\\.t", "q\"\\"}, {"bad\xff.bin", "k"}, {"\xff.txt", "i"}, {"ctl\x01\x7f.t", "c"},
	{"\xee\x80\x80.t", "p"}, {"\xc3(", "m"},
}

const fixtureRedirects = "source_path\ttarget_path\tx\n/é/\t/ü/\tz\n\n\"/q a/\"\t /w/ \n/x\t/y\textra\textra2\n"

func writeFixture(t *testing.T) (site, redirects string) {
	t.Helper()
	dir := t.TempDir()
	site = filepath.Join(dir, "site")
	for _, f := range fixtureFiles {
		p := filepath.Join(site, f.name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(f.body), 0o644); err != nil {
			t.Skipf("filesystem refuses the name %q: %v", f.name, err)
		}
	}
	return site, redirectsFile(t)
}

func redirectsFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "r.tsv")
	if err := os.WriteFile(p, []byte(fixtureRedirects), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// The Go port must match the Python output byte for byte: the manifest lists every file with its size, sha256 and path
// order, so a matching manifest proves the copied tree too.
func TestPrepareMatchesPythonGolden(t *testing.T) {
	site, redirects := writeFixture(t)
	for _, mode := range []string{"preview", "production"} {
		t.Run(mode, func(t *testing.T) {
			outDir := filepath.Join(t.TempDir(), "out")
			code, stdout, stderr := run(t, "--source", site, "--output", outDir, "--mode", mode,
				"--redirects", redirects, "--source-revision", goldenRevision)
			if code != 0 || stderr != "" {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
			if want := "Prepared Cloudflare documentation assets: mode=" + mode + " files=19 redirects=3\n"; stdout != want {
				t.Errorf("stdout = %q, want %q", stdout, want)
			}
			for out, golden := range map[string]string{
				"cloudflare-build-manifest.json": "manifest", "_redirects": "redirects", "_headers": "headers", "robots.txt": "robots",
			} {
				got, err := os.ReadFile(filepath.Join(outDir, out))
				if err != nil {
					t.Fatal(err)
				}
				want, err := os.ReadFile(filepath.Join("testdata", mode+"."+golden))
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != string(want) {
					t.Errorf("%s differs from the Python golden:\n got %q\nwant %q", out, got, want)
				}
			}
		})
	}
}

func TestAcceptedBracketedOrigins(t *testing.T) {
	for _, o := range []string{"https://[::1]", "https://[::1]:8080", "https://[v1.x]", "https://[fe80::1%25eth0]"} {
		if _, err := validOrigin(o); err != nil {
			t.Errorf("%s: %v", o, err)
		}
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
		{"unclosed bracket", tsv(hdr), "https://[not-ipv6", "Invalid IPv6 URL"},
		{"bad IPv6", tsv(hdr), "https://[:::]", "':::' does not appear to be an IPv4 or IPv6 address"},
		{"IPv4 in brackets", tsv(hdr), "https://[1.2.3.4]", "An IPv4 address cannot be in brackets"},
		{"empty brackets", tsv(hdr), "https://[]", "'' does not appear to be an IPv4 or IPv6 address"},
		{"text after bracket", tsv(hdr), "https://[::1]x", "Invalid IPv6 URL"},
		{"bad IPvFuture", tsv(hdr), "https://[vz.x]", "IPvFuture address is invalid"},
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
		return []string{"--source", src, "--output", filepath.Join(t.TempDir(), "o"), "--mode", "production", "--redirects", redirectsFile(t)}
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
