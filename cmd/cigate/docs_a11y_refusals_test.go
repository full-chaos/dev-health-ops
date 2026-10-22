package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// The checked-in refusal inventory of the accessibility check ported from Python (CHAOS-10500, part 2). Each row names
// one trigger and the line of the former script, scripts/check_docs_candidate_accessibility.py at the merge base
// (testdata/refusals/check_docs_candidate_accessibility.py.frozen), that refuses it. "STRICTER" marks a place where
// Python accepted and Go refuses (D6129); the other rows are places where both refuse. The rows run through the live
// entry point, so a refusal removed from Go, or a row removed from the table, fails the test.
const acRefusalRows = 76

const acCSS = ":focus-visible { outline: 2px solid; }\n@media (prefers-reduced-motion: reduce) {}\n"

func acPage(body string) string {
	return `<html lang="en"><head><title>T</title></head><body><a class="md-skip" href="#c">s</a><main><article class="md-content__inner"><h1>H</h1>` + body + `</article></main></body></html>` + "\n"
}

func acFiles(page string) map[string]string {
	return map[string]string{"site/index.html": page, "extra.css": acCSS}
}

func acArgs(extra ...string) []string {
	return append([]string{"docs", "check-docs-accessibility"}, extra...)
}

var acSA = []string{"--site-dir", "site", "--css", "extra.css", "--report", "r.tsv"}

func acRun(extra ...string) []string {
	return acArgs(append(append([]string(nil), acSA...), extra...)...)
}

func acFinding(name, ref, body, out string) dcRow {
	return dcRow{name: name, ref: ref, files: acFiles(acPage(body)), args: acRun(), rc: 1, out: out}
}

func acRefusalTable() []dcRow {
	longDigits := strings.Repeat("9", 4301)
	rows := []dcRow{
		// ---- argparse (A218-225)
		{name: "a11y missing --site-dir", ref: "A218 add_argument(--site-dir, required=True): argparse exit 2", args: acArgs("--css", "extra.css"), rc: 2, err: "required: --site-dir"},
		{name: "a11y missing --css", ref: "A219 add_argument(--css, required=True): argparse exit 2", args: acArgs("--site-dir", "site"), rc: 2, err: "required: --css"},
		{name: "a11y unknown option", ref: "A225 parse_args: unrecognized arguments", args: acRun("--nope", "x"), rc: 2, err: "unrecognized arguments: --nope"},
		{name: "a11y trailing positional", ref: "A225 parse_args: unrecognized arguments", args: acRun("junk"), rc: 2, err: "unrecognized arguments: junk"},
		{name: "a11y option without value", ref: "A225 parse_args: expected one argument", args: acArgs("--site-dir", "site", "--css"), rc: 2, err: "expected one argument"},
		{name: "a11y value that looks like an option", ref: "A225 parse_args: expected one argument (-abc is an option)", args: acArgs("--site-dir", "-abc", "--css", "extra.css"), rc: 2, err: "expected one argument"},
		{name: "a11y abbreviated option", ref: "A225 STRICTER: argparse accepted --site as --site-dir", args: acArgs("--site", "site", "--css", "extra.css"), rc: 2, err: "unrecognized"},
		{name: "a11y abbreviated report", ref: "A225 STRICTER: argparse accepted --rep as --report", args: acArgs("--site-dir", "site", "--css", "extra.css", "--rep", "r.tsv"), rc: 2, err: "unrecognized"},
		{name: "a11y help flag", ref: "A225 STRICTER: argparse printed help and exited 0", args: acArgs("-h"), rc: 2, err: "unrecognized"},
		{name: "a11y long help flag", ref: "A225 STRICTER: argparse printed help and exited 0", args: acArgs("--help"), rc: 2, err: "unrecognized"},
		{name: "a11y double dash", ref: "A225 STRICTER: argparse took -- and accepted a complete command line", args: acRun("--"), rc: 2, err: "unrecognized"},
		{name: "a11y dash value with a space", ref: "A218 STRICTER: argparse accepted '-a b' as a value", args: acArgs("--site-dir", "-a b", "--css", "extra.css"), rc: 2, err: "expected one argument"},
		{name: "a11y negative number value", ref: "A219 STRICTER: argparse accepted -1 as a value", args: acArgs("--site-dir", "site", "--css", "-1"), rc: 2, err: "expected one argument"},
		{name: "a11y equals form with a dash value", ref: "A218 STRICTER: argparse accepted --site-dir=-x", args: acArgs("--site-dir=-x", "--css", "extra.css"), rc: 2, err: "expected one argument"},
		{name: "a11y empty site dir value", ref: "A218 STRICTER: Path('') is '.' in Python", args: acArgs("--site-dir", "", "--css", "extra.css"), rc: 2, err: "expected one argument"},
		{name: "a11y empty css value", ref: "A219 STRICTER: Path('') is '.' in Python", args: acArgs("--site-dir", "site", "--css", ""), rc: 2, err: "expected one argument"},
		{name: "a11y empty report value", ref: "A220-224 STRICTER: Path('') is '.' in Python", args: acArgs("--site-dir", "site", "--css", "extra.css", "--report", ""), rc: 2, err: "expected one argument"},
		{name: "a11y report dash value", ref: "A220-224 STRICTER: argparse accepted '-a b' as a value", args: acArgs("--site-dir", "site", "--css", "extra.css", "--report", "-a b"), rc: 2, err: "expected one argument"},
		// ---- inputs and stdlib refusals
		{name: "a11y site dir missing", ref: "A227-229 print ERROR built site directory not found, return 1 (stdout)", files: map[string]string{"extra.css": acCSS}, args: acArgs("--site-dir", "nope", "--css", "extra.css"), rc: 1, out: "ERROR: built site directory not found: nope"},
		{name: "a11y site dir is a file", ref: "A227 is_dir() false for a file", files: map[string]string{"site": "x", "extra.css": acCSS}, args: acRun(), rc: 1, out: "built site directory not found"},
		{name: "a11y css missing", ref: "A230-232 print ERROR CSS file not found, return 1 (stdout)", files: map[string]string{"site/index.html": acPage("")}, args: acRun(), rc: 1, out: "ERROR: CSS file not found: extra.css"},
		{name: "a11y css is a directory", ref: "A230 is_file() false for a directory", files: map[string]string{"site/index.html": acPage(""), "extra.css/x": "1"}, args: acRun(), rc: 1, out: "CSS file not found"},
		{name: "a11y css dangling symlink", ref: "A230 is_file() false for a dangling link", files: map[string]string{"site/index.html": acPage("")}, links: [][2]string{{"extra.css", "nowhere"}}, args: acRun(), rc: 1, out: "CSS file not found"},
		{name: "a11y css not UTF-8", ref: "A203 read_text: UnicodeDecodeError (uncaught)", files: map[string]string{"site/index.html": acPage(""), "extra.css": "a{}\xff"}, args: acRun(), rc: 1, err: "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff in position 3: invalid start byte"},
		{name: "a11y css truncated UTF-8", ref: "A203 read_text: UnicodeDecodeError unexpected end of data (uncaught)", files: map[string]string{"site/index.html": acPage(""), "extra.css": "ab\xe2\x82"}, args: acRun(), rc: 1, err: "UnicodeDecodeError: 'utf-8' codec can't decode bytes in position 2-3: unexpected end of data"},
		{name: "a11y page not UTF-8", ref: "A180-182 read_text: UnicodeDecodeError caught, finding could not parse HTML", files: acFiles("<html>\xff</html>"), args: acRun(), rc: 1, out: "index.html: could not parse HTML: 'utf-8' codec can't decode byte 0xff in position 6: invalid start byte"},
		{name: "a11y page truncated UTF-8", ref: "A180-182 read_text: UnicodeDecodeError unexpected end of data caught", files: acFiles("abc\xe2\x82"), args: acRun(), rc: 1, out: "could not parse HTML: 'utf-8' codec can't decode bytes in position 3-4: unexpected end of data"},
		{name: "a11y page bad continuation byte", ref: "A180-182 read_text: UnicodeDecodeError invalid continuation byte caught", files: acFiles("abc\xe2\x82x"), args: acRun(), rc: 1, out: "could not parse HTML: 'utf-8' codec can't decode bytes in position 3-4: invalid continuation byte"},
		{name: "a11y page directory named html", ref: "A180-182 read_text on a directory: IsADirectoryError (an OSError) caught", files: map[string]string{"site/odd.html/x": "1", "extra.css": acCSS}, args: acRun(), rc: 1, out: "odd.html: could not parse HTML: [Errno 21] Is a directory: 'site/odd.html'"},
		{name: "a11y page dangling symlink", ref: "A180-182 read_text on a dangling link: FileNotFoundError caught", files: acFiles(acPage("")), links: [][2]string{{"site/gone.html", "nowhere"}}, args: acRun(), rc: 1, out: "gone.html: could not parse HTML: [Errno 2] No such file or directory: 'site/gone.html'"},
		{name: "a11y page symlink loop", ref: "A180-182 read_text on a link loop: OSError ELOOP caught", files: acFiles(acPage("")), links: [][2]string{{"site/loop.html", "loop.html"}}, args: acRun(), rc: 1, out: "loop.html: could not parse HTML: [Errno 40] Too many levels of symbolic links: 'site/loop.html'"},
		{name: "a11y character reference over the digit limit in text", ref: "A180 feed: html.unescape int() of more than 4300 digits raises ValueError (uncaught)", files: acFiles(acPage("<p>&#" + longDigits + ";</p>")), args: acRun(), rc: 1, err: "ValueError: Exceeds the limit (4300 digits) for integer string conversion: value has 4301 digits"},
		{name: "a11y character reference with leading zeros over the digit limit", ref: "A180 feed: int() counts leading zeros", files: acFiles(acPage("<p>&#" + strings.Repeat("0", 4300) + "1;</p>")), args: acRun(), rc: 1, err: "ValueError: Exceeds the limit (4300 digits)"},
		{name: "a11y character reference over the digit limit in an attribute", ref: "A180 feed: _unescape_attrvalue -> html.unescape ValueError (uncaught)", files: acFiles(acPage("<a href='x' title='&#" + longDigits + ";'>t</a>")), args: acRun(), rc: 1, err: "ValueError: Exceeds the limit (4300 digits)"},
		{name: "a11y character reference over the digit limit in a title", ref: "A180 feed: RCDATA text is unescaped, ValueError (uncaught)", files: acFiles(acPage("") + "<title>&#" + longDigits + ";</title>"), args: acRun(), rc: 1, err: "ValueError: Exceeds the limit (4300 digits)"},
		{name: "a11y page name not UTF-8", ref: "A235-237 STRICTER: Python listed the page and printed its name with surrogateescape (a UnicodeEncodeError on error)", files: map[string]string{"site/bad\xffname.html": acPage(""), "extra.css": acCSS}, args: acRun(), rc: 1, err: "not UTF-8"},
		{name: "a11y named pipe as a page", ref: "A180 STRICTER: read_text on a named pipe blocks forever", files: map[string]string{"extra.css": acCSS}, args: acRun(), rc: 1, err: "special file"},
		{name: "a11y report parent is a file", ref: "A241 Path.mkdir(parents=True, exist_ok=True): FileExistsError (uncaught)", files: map[string]string{"site/index.html": acPage(""), "extra.css": acCSS, "blocker": "x"}, args: acArgs("--site-dir", "site", "--css", "extra.css", "--report", "blocker/r.tsv"), rc: 1, err: "FileExistsError: [Errno 17] File exists: 'blocker'"},
		{name: "a11y report parent below a file", ref: "A241 Path.mkdir: NotADirectoryError (uncaught)", files: map[string]string{"site/index.html": acPage(""), "extra.css": acCSS, "blocker": "x"}, args: acArgs("--site-dir", "site", "--css", "extra.css", "--report", "blocker/sub/r.tsv"), rc: 1, err: "NotADirectoryError: [Errno 20] Not a directory: 'blocker/sub'"},
		{name: "a11y report is a directory", ref: "A242 Path.open('w'): IsADirectoryError (uncaught)", files: map[string]string{"site/index.html": acPage(""), "extra.css": acCSS, "rdir/x": "1"}, args: acArgs("--site-dir", "site", "--css", "extra.css", "--report", "rdir"), rc: 1, err: "IsADirectoryError: [Errno 21] Is a directory: 'rdir'"},
		// ---- tabindex: int() (A120-127)
		acFinding("a11y tabindex positive", "A124-125 int(tabindex) > 0: errors.append positive tabindex", "<div tabindex=1></div>", "positive tabindex is not allowed: 1"),
		acFinding("a11y tabindex not a number", "A126-127 int('abc'): ValueError caught, errors.append invalid tabindex", "<div tabindex=abc></div>", "invalid tabindex value: abc"),
		acFinding("a11y tabindex float text", "A126-127 int('1.5'): ValueError caught", "<div tabindex=1.5></div>", "invalid tabindex value: 1.5"),
		acFinding("a11y tabindex hexadecimal text", "A126-127 int('0x1'): ValueError caught (base 10)", "<div tabindex=0x1></div>", "invalid tabindex value: 0x1"),
		acFinding("a11y tabindex double underscore", "A126-127 int('1__0'): ValueError caught", "<div tabindex=1__0></div>", "invalid tabindex value: 1__0"),
		acFinding("a11y tabindex single underscore", "A124-125 int('1_0') is 10: positive", "<div tabindex=1_0></div>", "positive tabindex is not allowed: 1_0"),
		acFinding("a11y tabindex Arabic-Indic digit", "A124-125 int('\\u0663') is 3 (Unicode decimal digit): positive", "<div tabindex=\u0663></div>", "positive tabindex is not allowed: \u0663"),
		acFinding("a11y tabindex Unicode 16 digit (STRICTER than 3.14.7, equal to the 3.13 Workers Builds ran)", "A126-127 int('\U00010d41'): 3.13 (Unicode 15.1) raises ValueError, caught; 3.14.7 (Unicode 16) reads it as 1", "<div tabindex=\U00010d41></div>", "invalid tabindex value: \U00010d41"),
		acFinding("a11y tabindex no-break space padding", "A124-125 int('\\xa07') strips Unicode space: positive", "<div tabindex=\u00a07></div>", "positive tabindex is not allowed"),
		acFinding("a11y tabindex control character padding", "A126-127 int('\\x1c7'): ValueError caught (only ASCII spaces strip)", "<div tabindex=\x1c7></div>", "invalid tabindex value"),
		acFinding("a11y tabindex over the digit limit", "A126-127 int() of 4301 digits: ValueError caught", "<div tabindex='"+longDigits+"'></div>", "invalid tabindex value: 99"),
		acFinding("a11y tabindex negative over the digit limit", "A126-127 int('-999...') of 4301 digits: ValueError caught", "<div tabindex='-"+longDigits+"'></div>", "invalid tabindex value: -99"),
		// ---- findings: every errors.append of the audit
		acFinding("a11y image without alt", "A79 errors.append content image is missing an alt attribute", "<img src=a>", "content image is missing an alt attribute"),
		acFinding("a11y image with empty alt", "A81-83 errors.append empty alt text without being marked decorative", "<img src=a alt=''>", "content image has empty alt text without being marked decorative"),
		acFinding("a11y input without a name", "A113-115 errors.append content input has no aria-label", "<input type=text>", "content input has no aria-label, aria-labelledby, or title"),
		acFinding("a11y h1 without text", "A140 errors.append content h1 has no accessible text", "<h1> </h1>", "content h1 has no accessible text"),
		acFinding("a11y link without a name", "A149-151 errors.append link has no accessible name", "<a href=x></a>", "link has no accessible name: x"),
		acFinding("a11y link with generic text", "A153-155 errors.append generic link text is not descriptive", "<a href=x> Click Here </a>", "generic link text is not descriptive: 'Click Here'"),
		acFinding("a11y button without a name", "A160 errors.append button has no accessible name", "<button></button>", "button has no accessible name"),
		acFinding("a11y table without header cells", "A164 errors.append content table has no header cells", "<table><tr><td>x</td></tr></table>", "content table has no header cells"),
		{name: "a11y html without lang", ref: "A185-186 errors.append html element has no lang attribute", files: acFiles(strings.Replace(acPage(""), ` lang="en"`, "", 1)), args: acRun(), rc: 1, out: "html element has no lang attribute"},
		{name: "a11y empty title", ref: "A187-188 errors.append document title is empty", files: acFiles(strings.Replace(acPage(""), "<title>T</title>", "<title></title>", 1)), args: acRun(), rc: 1, out: "document title is empty"},
		{name: "a11y no main landmark", ref: "A189-190 errors.append expected one main landmark", files: acFiles(strings.Replace(strings.Replace(acPage(""), "<main>", "", 1), "</main>", "", 1)), args: acRun(), rc: 1, out: "expected one main landmark, found 0"},
		{name: "a11y no content article", ref: "A191-194 errors.append expected one primary content article", files: acFiles(strings.Replace(acPage(""), "md-content__inner", "x", 1)), args: acRun(), rc: 1, out: "expected one primary content article, found 0"},
		{name: "a11y two content h1", ref: "A195-196 errors.append expected one content h1", files: acFiles(acPage("<h1>two</h1>")), args: acRun(), rc: 1, out: "expected one content h1, found 2"},
		{name: "a11y no skip link", ref: "A197-198 errors.append Material skip link was not found", files: acFiles(strings.Replace(acPage(""), "md-skip", "x", 1)), args: acRun(), rc: 1, out: "Material skip link was not found"},
		{name: "a11y css without focus-visible", ref: "A205-206 errors.append custom CSS has no visible-focus rule", files: map[string]string{"site/index.html": acPage(""), "extra.css": "@media (prefers-reduced-motion: reduce) {}"}, args: acRun(), rc: 1, out: "custom CSS has no visible-focus rule"},
		{name: "a11y css without reduced-motion", ref: "A207-208 errors.append custom CSS has no reduced-motion handling", files: map[string]string{"site/index.html": acPage(""), "extra.css": ":focus-visible {}"}, args: acRun(), rc: 1, out: "custom CSS has no reduced-motion handling"},
		{name: "a11y css outline none", ref: "A209-210 errors.append outline is removed without an alternative visible focus style", files: map[string]string{"site/index.html": acPage(""), "extra.css": acCSS + "a { outline : none }"}, args: acRun(), rc: 1, out: "outline is removed without an alternative visible focus style"},
		{name: "a11y css outline none with a Unicode space", ref: "A209 re \\s is Unicode: outline\\u00a0:\\u2003none matches", files: map[string]string{"site/index.html": acPage(""), "extra.css": acCSS + "a { outline\u00a0:\u2003none }"}, args: acRun(), rc: 1, out: "outline is removed without an alternative visible focus style"},
		{name: "a11y failure line and report name", ref: "A249-251 print ERROR lines then the failed summary, return 1", files: acFiles(acPage("<img>")), args: acRun(), rc: 1, out: "Accessibility structural audit failed; report: r.tsv"},
		{name: "a11y findings print page then message", ref: "A249 print ERROR: page: finding", files: acFiles(acPage("<img>")), args: acRun(), rc: 1, out: "ERROR: index.html: content image is missing an alt attribute"},
		{name: "a11y css finding names the css path", ref: "A239 rows.append((str(args.css), error))", files: map[string]string{"site/index.html": acPage(""), "extra.css": ""}, args: acRun(), rc: 1, out: "ERROR: extra.css: custom CSS has no visible-focus rule"},
		{name: "a11y 404 page keeps the other findings", ref: "A197 the skip link is exempt only for a page named 404.html", files: map[string]string{"site/404.html": strings.Replace(strings.Replace(acPage(""), "md-skip", "x", 1), ` lang="en"`, "", 1), "extra.css": acCSS}, args: acRun(), rc: 1, out: "404.html: html element has no lang attribute"},
		{name: "a11y generic link text with no-break space is not generic", ref: "A152 text.lower() in GENERIC_LINK_TEXT is an exact match", files: acFiles(acPage("<a href=x>read\u00a0more</a><a href=y></a>")), args: acRun(), rc: 1, out: "link has no accessible name: y"},
		{name: "a11y valid UTF-8 BOM page keeps the BOM", ref: "A180 read_text(utf-8) does not strip a BOM; the html tag is still found", files: acFiles("\ufeff" + acPage("<img>")), args: acRun(), rc: 1, out: "content image is missing an alt attribute"},
	}
	return rows
}

func acRunRow(t *testing.T, r dcRow) {
	dir := t.TempDir()
	for name, text := range r.files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, l := range r.links {
		p := filepath.Join(dir, l[0])
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(l[1], p); err != nil {
			t.Fatal(err)
		}
	}
	if r.name == "a11y named pipe as a page" {
		if err := os.MkdirAll(filepath.Join(dir, "site"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(filepath.Join(dir, "site", "pipe.html"), 0o644); err != nil {
			t.Skip("no named pipes here:", err)
		}
	}
	t.Chdir(dir)
	code, stdout, stderr := runCaptured(t, r.args...)
	if code != r.rc {
		t.Errorf("exit %d, want %d\nstdout: %q\nstderr: %q", code, r.rc, stdout, stderr)
	}
	if !strings.Contains(stderr, r.err) {
		t.Errorf("stderr %q does not hold %q", stderr, r.err)
	}
	if !strings.Contains(stdout, r.out) {
		t.Errorf("stdout %q does not hold %q", stdout, r.out)
	}
}

// Every input the former Python check refused (argparse errors, a missing directory or file, the stdlib calls that
// raise, every finding the audit reports) is refused by the Go verb with the exit code Python gave (2 for a usage
// error, 1 for the rest), the exit-1 output on the stream Python used.
func TestDocsAccessibilityRefusesEverythingTheFormerScriptRefused(t *testing.T) {
	rows := acRefusalTable()
	if len(rows) != acRefusalRows {
		t.Fatalf("the table has %d rows, pinned %d: a row was added or removed", len(rows), acRefusalRows)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if r.name == "" || r.ref == "" || seen[r.name] {
			t.Fatalf("row %q: empty name, empty Python reference or a repeated name", r.name)
		}
		seen[r.name] = true
		if r.rc == 0 {
			t.Fatalf("row %q expects a success", r.name)
		}
		if !strings.Contains(r.ref, "A") {
			t.Fatalf("row %q has no line reference", r.name)
		}
		t.Run(r.name, func(t *testing.T) { acRunRow(t, r) })
	}
}

func TestDocsAccessibilityUsageErrorsPrintUsageAndExit2(t *testing.T) {
	t.Chdir(t.TempDir())
	code, _, stderr := runCaptured(t, "docs", "check-docs-accessibility")
	if code != 2 || !strings.Contains(stderr, "usage: cigate docs check-docs-accessibility --site-dir SITE_DIR --css CSS [--report REPORT]") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}

// The frozen copy of the Python script is what the line references point at.
func TestDocsAccessibilityFrozenScriptLinesMatchTheTable(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "cigates", "docstools", "testdata", "refusals", "check_docs_candidate_accessibility.py.frozen"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	check := map[int]string{218: `add_argument("--site-dir"`, 225: "parse_args", 227: "is_dir()", 230: "is_file()", 241: "mkdir(parents=True", 242: "report.open", 124: "int(tabindex)", 203: "read_text", 180: "feed(path.read_text"}
	for n, frag := range check {
		if !strings.Contains(lines[n-1], frag) {
			t.Errorf("frozen script line %d is %q, the table refers to %q", n, lines[n-1], frag)
		}
	}
}
