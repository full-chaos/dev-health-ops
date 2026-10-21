package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// pythonRefusals is the checked-in refusal inventory: each row names a trigger exercised by the table below and the
// line of scripts/prepare_docs_cloudflare.py at the merge base (9127c3728d51) that refuses it (STRICTER = Go refuses what
// Python accepted, allowed by the port rule). Removing a row, or a refusal from the table, fails the test.
var pythonRefusals = map[string]string{
	"missing source":                           "165 add_argument(--source, required=True): argparse exit 2",
	"missing output":                           "166 add_argument(--output, required=True): argparse exit 2",
	"missing mode":                             "167 add_argument(--mode, required=True): argparse exit 2",
	"missing redirects":                        "168 add_argument(--redirects, required=True): argparse exit 2",
	"mode staging":                             "167 choices=(preview, production): argparse exit 2",
	"trailing positional":                      "174 parse_args: unrecognized arguments (junk after valid options)",
	"trailing positional then flag":            "174 parse_args: unrecognized arguments (junk, then a later --mode)",
	"unknown option":                           "174 parse_args: unrecognized arguments (--nope)",
	"abbreviated option":                       "174 STRICTER: argparse accepted --mod as --mode",
	"single dash option":                       "174 parse_args: -mode is no option of the parser",
	"double dash":                              "174 parse_args: -- ends options, leaving positional junk",
	"option without value":                     "174 parse_args: expected one argument",
	"value that looks like an option":          "174 parse_args: expected one argument (-abc taken as an option)",
	"empty required value":                     "165 STRICTER: Path('') is '.' in Python",
	"origin http":                              "26 raise ValueError: scheme != https",
	"origin no scheme":                         "26 raise ValueError: no scheme",
	"origin empty netloc":                      "26 raise ValueError: empty netloc",
	"origin with path":                         "26 raise ValueError: path not empty or /",
	"origin unclosed bracket":                  "24 urlparse raises ValueError: Invalid IPv6 URL",
	"origin bad IPv6":                          "24 urlparse raises ValueError: bracketed host is no IPv6 address",
	"origin IPv4 in brackets":                  "24 urlparse raises ValueError: An IPv4 address cannot be in brackets",
	"origin text before bracket":               "24 urlparse raises ValueError: Invalid IPv6 URL (text before the bracket)",
	"origin fullwidth slash":                   "24 urlparse _checknetloc: NFKC netloc gains /",
	"origin fullwidth colon":                   "24 urlparse _checknetloc: NFKC netloc gains :",
	"origin fullwidth question":                "24 urlparse _checknetloc: NFKC netloc gains ?",
	"origin fullwidth hash":                    "24 urlparse _checknetloc: NFKC netloc gains #",
	"origin fullwidth at":                      "24 urlparse _checknetloc: NFKC netloc gains @",
	"production origin that is not UTF-8":      "88 write_text(robots.txt): UnicodeEncodeError on a surrogate (production only)",
	"source missing":                           "121-122 raise ValueError: built documentation directory not found",
	"source without index":                     "123-124 raise ValueError: no index.html",
	"source without 404":                       "125-126 raise ValueError: no 404.html",
	"redirects missing":                        "127-128 raise ValueError: redirect manifest not found",
	"redirects not UTF-8":                      "33-34 open(encoding=utf-8) + csv read: UnicodeDecodeError (ValueError)",
	"redirects empty file":                     "36-37 raise ValueError: manifest is missing columns (no fieldnames)",
	"redirects wrong header":                   "36-37 raise ValueError: manifest is missing columns",
	"redirects empty first line":               "36-37 DictReader takes the empty first line as the header",
	"redirects source over the csv limit":      "34 csv.Error: field larger than field limit (131072), source field",
	"redirects target over the csv limit":      "34 csv.Error: field larger than field limit (131072), target field",
	"redirects extra field over the csv limit": "34 csv.Error: field larger than field limit (131072), unused extra field",
	"redirects header over the csv limit":      "34 csv.Error: field larger than field limit (131072), header field",
	"redirects empty target":                   "41-42 raise ValueError: empty redirect",
	"redirects short row":                      "41-42 raise ValueError: empty redirect (row with one field)",
	"redirects relative":                       "43-47 raise ValueError: redirect paths must be site-relative",
	"redirects self":                           "48-49 raise ValueError: self redirect",
	"redirects duplicate":                      "50-53 raise ValueError: duplicate redirect source",
	"redirects CRLF in quotes":                 "65 STRICTER: Python keeps CRLF in _redirects; Go csv would fold it to LF",
	"redirects LF in quotes":                   "65 STRICTER: a line break inside a _redirects field corrupts the file",
	"redirects CR in quotes":                   "65 STRICTER: same, bare CR",
	"redirects tab in quotes":                  "65 STRICTER: a tab inside a _redirects field corrupts the file",
	"redirects NUL":                            "65 STRICTER: control character in a redirect field",
	"output is a file":                         "132 shutil.rmtree(file): NotADirectoryError (OSError)",
	"output is a symlink to a directory":       "132 shutil.rmtree(symlink): Cannot call rmtree on a symbolic link",
	"output is a dangling symlink":             "133 shutil.copytree: makedirs File exists (OSError)",
	"markdown file in the output":              "139-142 raise ValueError: output contains Markdown source (file)",
	"directory named .md in the output":        "139-142 raise ValueError: rglob('*.md') also matches directories",
	"dangling symlink in the source":           "133 shutil.copytree: copy2 of a dangling symlink (shutil.Error)",
}

const pythonRefusalRows = 56

// Every input the former Python helper refused (its argparse errors, its ValueError and OSError paths, and the stdlib
// calls that raise inside it) must be refused by `cigate docs-prepare`: exit 2 for a usage error as argparse did, exit 1
// with "ERROR:" for everything else. lanes/lane-10431/refusals.md maps each row to the Python raise and the Go line.
func TestDocsPrepareRefusesEverythingTheFormerHelperRefused(t *testing.T) {
	dir := t.TempDir()
	write := func(t *testing.T, dir, name, text string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	files := 0
	site := filepath.Join(dir, "site")
	for _, f := range []string{"index.html", "404.html"} {
		write(t, site, f, "x")
	}
	hdr := "source_path\ttarget_path\n"
	tsv := func(body string) string {
		files++
		return write(t, dir, "tsv/"+strconv.Itoa(files)+".tsv", body)
	}
	ok := tsv(hdr + "/a\t/b\n")
	long := func(n int) string { return strings.Repeat("a", n) }
	out := func(name string) string { return filepath.Join(dir, "out-"+name) }

	type row struct {
		name   string
		args   []string
		want   int
		stderr string
	}
	base := func(name string, extra ...string) []string {
		return append([]string{"--source", site, "--redirects", ok, "--output", out(name), "--mode", "preview"}, extra...)
	}
	with := func(name string, redirects string, extra ...string) []string {
		return append([]string{"--source", site, "--redirects", redirects, "--output", out(name), "--mode", "preview"}, extra...)
	}
	rows := []row{
		// argparse
		{"missing source", []string{"--output", out("a"), "--mode", "preview", "--redirects", ok}, 2, "required: --source"},
		{"missing output", []string{"--source", site, "--mode", "preview", "--redirects", ok}, 2, "required: --output"},
		{"missing mode", []string{"--source", site, "--output", out("b"), "--redirects", ok}, 2, "required: --mode"},
		{"missing redirects", []string{"--source", site, "--output", out("c"), "--mode", "preview"}, 2, "required: --redirects"},
		{"mode staging", []string{"--source", site, "--redirects", ok, "--output", out("e"), "--mode", "staging"}, 2, "invalid choice"},
		{"trailing positional", base("f", "junk"), 2, "unrecognized arguments: junk"},
		{"trailing positional then flag", base("g", "junk", "--mode", "production"), 2, "unrecognized arguments: junk"},
		{"unknown option", base("h", "--nope", "1"), 2, "unrecognized arguments: --nope"},
		{"abbreviated option", []string{"--source", site, "--redirects", ok, "--output", out("i"), "--mod", "preview"}, 2, "unrecognized"},
		{"single dash option", []string{"--source", site, "--redirects", ok, "--output", out("j"), "-mode", "preview"}, 2, "unrecognized"},
		{"double dash", base("k", "--"), 2, "unrecognized"},
		{"option without value", []string{"--source", site, "--redirects", ok, "--output", out("l"), "--mode"}, 2, "expected one argument"},
		{"value that looks like an option", base("m", "--source-revision", "-abc"), 2, "expected one argument"},
		{"empty required value", []string{"--source", "", "--redirects", ok, "--output", out("n"), "--mode", "preview"}, 2, "required: --source"},
		// the canonical origin (urlsplit and its NFKC netloc check)
		{"origin http", base("o", "--canonical-origin", "http://x.com"), 1, "must be an HTTPS origin"},
		{"origin no scheme", base("p", "--canonical-origin", "x.com"), 1, "must be an HTTPS origin"},
		{"origin empty netloc", base("q", "--canonical-origin", "https:///p"), 1, "must be an HTTPS origin"},
		{"origin with path", base("r", "--canonical-origin", "https://x.com/p"), 1, "must be an HTTPS origin"},
		{"origin unclosed bracket", base("s", "--canonical-origin", "https://[::1"), 1, "Invalid IPv6 URL"},
		{"origin bad IPv6", base("t", "--canonical-origin", "https://[:::]"), 1, "does not appear to be an IPv4 or IPv6 address"},
		{"origin IPv4 in brackets", base("u", "--canonical-origin", "https://[1.2.3.4]"), 1, "cannot be in brackets"},
		{"origin text before bracket", base("v", "--canonical-origin", "https://a[::1]"), 1, "Invalid IPv6 URL"},
		{"origin fullwidth slash", base("w", "--canonical-origin", "https://docs.example／evil"), 1, "NFKC"},
		{"origin fullwidth colon", base("x", "--canonical-origin", "https://docs.example：80"), 1, "NFKC"},
		{"origin fullwidth question", base("y", "--canonical-origin", "https://docs.example？x"), 1, "NFKC"},
		{"origin fullwidth hash", base("z", "--canonical-origin", "https://docs.example＃x"), 1, "NFKC"},
		{"origin fullwidth at", base("aa", "--canonical-origin", "https://u＠docs.example"), 1, "NFKC"},
		{"production origin that is not UTF-8", []string{"--source", site, "--redirects", ok, "--output", out("ab"), "--mode", "production", "--canonical-origin", "https://d\xffcs.example"}, 1, "surrogates not allowed"},
		// inputs
		{"source missing", []string{"--source", filepath.Join(dir, "nope"), "--redirects", ok, "--output", out("ac"), "--mode", "preview"}, 1, "directory not found"},
		{"source without index", []string{"--source", filepath.Dir(write(t, dir, "s2/404.html", "x")), "--redirects", ok, "--output", out("ad"), "--mode", "preview"}, 1, "no index.html"},
		{"source without 404", []string{"--source", filepath.Dir(write(t, dir, "s3/index.html", "x")), "--redirects", ok, "--output", out("ae"), "--mode", "preview"}, 1, "no 404.html"},
		{"redirects missing", with("af", filepath.Join(dir, "nope.tsv")), 1, "redirect manifest not found"},
		{"redirects not UTF-8", with("ag", tsv(hdr+"/a\t/b\xff\n")), 1, "utf-8"},
		{"redirects empty file", with("ah", tsv("")), 1, "is missing"},
		{"redirects wrong header", with("ai", tsv("a\tb\n/a\t/b\n")), 1, "is missing"},
		{"redirects empty first line", with("aj", tsv("\n"+hdr+"/a\t/b\n")), 1, "is missing"},
		{"redirects source over the csv limit", with("ak", tsv(hdr+"/"+long(131072)+"\t/b\n")), 1, "field larger than field limit"},
		{"redirects target over the csv limit", with("al", tsv(hdr+"/a\t/"+long(131072)+"\n")), 1, "field larger than field limit"},
		{"redirects extra field over the csv limit", with("am", tsv("source_path\ttarget_path\tx\n/a\t/b\t"+long(131073)+"\n")), 1, "field larger than field limit"},
		{"redirects header over the csv limit", with("an", tsv("source_path\ttarget_path\t"+long(131073)+"\n/a\t/b\n")), 1, "field larger than field limit"},
		{"redirects empty target", with("ao", tsv(hdr+"/a\t\n")), 1, "empty redirect"},
		{"redirects short row", with("ap", tsv(hdr+"/a\n")), 1, "empty redirect"},
		{"redirects relative", with("aq", tsv(hdr+"a\t/b\n")), 1, "site-relative"},
		{"redirects self", with("ar", tsv(hdr+"/a\t/a\n")), 1, "self redirect"},
		{"redirects duplicate", with("as", tsv(hdr+"/a\t/b\n/a\t/c\n")), 1, "duplicate redirect source"},
		{"redirects CRLF in quotes", with("at", tsv(hdr+"\"/a\r\nb\"\t\"/c\r\nd\"\n")), 1, "control character"},
		{"redirects LF in quotes", with("au", tsv(hdr+"\"/a\nb\"\t/c\n")), 1, "control character"},
		{"redirects CR in quotes", with("av", tsv(hdr+"/a\t\"/c\rd\"\n")), 1, "control character"},
		{"redirects tab in quotes", with("aw", tsv(hdr+"/a\t\"/c\td\"\n")), 1, "control character"},
		{"redirects NUL", with("ax", tsv(hdr+"/a\x00b\t/c\n")), 1, "control character"},
	}
	outFile := write(t, dir, "outfile", "x")
	target := filepath.Join(dir, "target")
	_ = os.MkdirAll(target, 0o755)
	link := filepath.Join(dir, "link")
	_ = os.Symlink(target, link)
	broken := filepath.Join(dir, "broken")
	_ = os.Symlink(filepath.Join(dir, "gone"), broken)
	mdSite := filepath.Join(dir, "mdsite")
	for _, f := range []string{"index.html", "404.html", "x.md"} {
		write(t, mdSite, f, "x")
	}
	mdDir := filepath.Join(dir, "mddir")
	for _, f := range []string{"index.html", "404.html", "d.md/y.html"} {
		write(t, mdDir, f, "x")
	}
	dangling := filepath.Join(dir, "dangling")
	for _, f := range []string{"index.html", "404.html"} {
		write(t, dangling, f, "x")
	}
	_ = os.Symlink(filepath.Join(dir, "gone"), filepath.Join(dangling, "bad"))
	mk := func(src, output string) []string {
		return []string{"--source", src, "--redirects", ok, "--output", output, "--mode", "preview"}
	}
	rows = append(rows,
		row{"output is a file", mk(site, outFile), 1, "Not a directory"},
		row{"output is a symlink to a directory", mk(site, link), 1, "symbolic link"},
		row{"output is a dangling symlink", mk(site, broken), 1, ""},
		row{"markdown file in the output", mk(mdSite, out("md1")), 1, "Markdown source"},
		row{"directory named .md in the output", mk(mdDir, out("md2")), 1, "Markdown source"},
		row{"dangling symlink in the source", mk(dangling, out("dl")), 1, ""},
	)
	if len(rows) != pythonRefusalRows || len(pythonRefusals) != pythonRefusalRows {
		t.Fatalf("refusal table has %d rows and %d Python references, want %d each: a refusal was removed or added without its reference", len(rows), len(pythonRefusals), pythonRefusalRows)
	}
	for _, r := range rows {
		if pythonRefusals[r.name] == "" {
			t.Fatalf("row %q has no Python reference", r.name)
		}
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			code, stdout, stderr := runCaptured(t, append([]string{"docs-prepare"}, r.args...)...)
			if code != r.want {
				t.Fatalf("exit %d, want %d\nstdout %q\nstderr %q", code, r.want, stdout, stderr)
			}
			if !strings.Contains(stderr, r.stderr) {
				t.Errorf("stderr %q does not contain %q", stderr, r.stderr)
			}
			if r.want == 1 && !strings.HasPrefix(stderr, "ERROR: ") {
				t.Errorf("stderr %q does not start with ERROR:", stderr)
			}
			if stdout != "" {
				t.Errorf("a refusal wrote to stdout: %q", stdout)
			}
		})
	}
	t.Run("the same command line without a defect succeeds", func(t *testing.T) {
		if code, _, stderr := runCaptured(t, append([]string{"docs-prepare"}, base("okay")...)...); code != 0 {
			t.Fatalf("exit %d: %s", code, stderr)
		}
	})
	t.Run("a symlinked output directory is left untouched", func(t *testing.T) {
		if _, err := os.Lstat(link); err != nil {
			t.Fatalf("the link was removed: %v", err)
		}
		if _, err := os.Stat(target); err != nil {
			t.Fatalf("the link target was removed: %v", err)
		}
	})
}
