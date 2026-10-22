package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The checked-in refusal inventory of validate-publication, ported from scripts/validate_docs_v2_publication.py
// (CHAOS-10500) at the merge base (17e0a38f4). Each row names one trigger and the line of the former script that refuses
// it. "STRICTER" marks a place where Python accepted and Go refuses (allowed by D6129); every other row is a place where both
// refuse and the text is Python's. The rows run through the live entry point (run), so a refusal removed from Go, or a row
// removed from the table, fails the test. The parity pairs `pub-*` under internal/cigates/docstools/testdata/parity cover
// the same refusals with the whole stdout, stderr and written files; this table adds the classes a pair cannot hold.
//
//	P = scripts/validate_docs_v2_publication.py (frozen beside the row inventory in testdata/refusals)
type pubRow struct {
	name  string
	ref   string
	files map[string]string
	links [][2]string
	rc    int
	err   string
	out   string
}

const pubRefusalRows = 81

const pubMkdocs = "nav:\n  - index.md\n"

func pubBase() map[string]string {
	return map[string]string{
		"mkdocs.yml":         pubMkdocs,
		"docs/index.md":      "# i\n",
		"docs-data/ia/a.tsv": "url\tid\n/\th\n",
	}
}

func pubWith(over map[string]string) map[string]string {
	f := pubBase()
	for k, v := range over {
		f[k] = v
	}
	return f
}

func pubMk(mkdocs string) map[string]string { return pubWith(map[string]string{"mkdocs.yml": mkdocs}) }

func pubFM(front string) map[string]string {
	return pubWith(map[string]string{"docs/index.md": "---\n" + front + "\n---\nbody\n"})
}

func pubPage(body string) map[string]string { return pubWith(map[string]string{"docs/index.md": body}) }

func pubRefusalTable() []pubRow {
	deep := func(n int) string { return strings.Repeat("[", n) + strings.Repeat("]", n) }
	return []pubRow{
		// ---- the mkdocs.yml loader (P27) and the front matter loader (P35): PyYAML errors
		{name: "mkdocs tab before a key", ref: "P27 safe_load: ScannerError while scanning for the next token", files: pubMk("nav:\n\t- index.md\n"), rc: 1, err: "yaml.scanner.ScannerError: while scanning for the next token"},
		{name: "mkdocs mapping value not allowed", ref: "P27 safe_load: ScannerError mapping values are not allowed here", files: pubMk("a: b: c\n"), rc: 1, err: "yaml.scanner.ScannerError: mapping values are not allowed here"},
		{name: "mkdocs unterminated flow sequence", ref: "P27 safe_load: ParserError while parsing a flow sequence", files: pubMk("nav: [a\n"), rc: 1, err: "yaml.parser.ParserError: while parsing a flow sequence"},
		{name: "mkdocs unterminated quote", ref: "P27 safe_load: ScannerError while scanning a quoted scalar", files: pubMk("a: 'x\n"), rc: 1, err: "yaml.scanner.ScannerError: while scanning a quoted scalar"},
		{name: "mkdocs unknown escape", ref: "P27 safe_load: ScannerError found unknown escape character", files: pubMk("a: \"\\q\"\n"), rc: 1, err: "yaml.scanner.ScannerError: while scanning a double-quoted scalar"},
		{name: "mkdocs bad hex escape", ref: "P27 safe_load: ScannerError expected escape sequence of 2 hexadecimal numbers", files: pubMk("a: \"\\x4\"\n"), rc: 1, err: "yaml.scanner.ScannerError: while scanning a double-quoted scalar"},
		{name: "mkdocs escape beyond Unicode", ref: "P27 chr(): ValueError chr() arg not in range(0x110000)", files: pubMk("a: \"\\U00110000\"\n"), rc: 1, err: "ValueError: chr() arg not in range(0x110000)"},
		{name: "mkdocs lone surrogate escape", ref: "P27 STRICTER: Python kept the lone surrogate in the str", files: pubMk("a: \"\\ud800\"\n"), rc: 1, err: "STRICTER"},
		{name: "mkdocs two documents", ref: "P27 safe_load: ComposerError expected a single document in the stream", files: pubMk(pubMkdocs + "---\nb: 1\n"), rc: 1, err: "yaml.composer.ComposerError: expected a single document in the stream"},
		{name: "mkdocs undefined alias", ref: "P27 safe_load: ComposerError found undefined alias", files: pubMk("a: *x\n"), rc: 1, err: "yaml.composer.ComposerError: found undefined alias 'x'"},
		{name: "mkdocs duplicate anchor", ref: "P27 safe_load: ComposerError found duplicate anchor", files: pubMk("a: &x 1\nb: &x 2\n"), rc: 1, err: "yaml.composer.ComposerError: found duplicate anchor 'x'; first occurrence"},
		{name: "mkdocs unknown directive handle", ref: "P27 safe_load: ParserError found undefined tag handle", files: pubMk("a: !e!x 5\n"), rc: 1, err: "yaml.parser.ParserError: while parsing a node"},
		{name: "mkdocs duplicate YAML directive", ref: "P27 safe_load: ParserError found duplicate YAML directive", files: pubMk("%YAML 1.1\n%YAML 1.1\n---\na: 1\n"), rc: 1, err: "yaml.parser.ParserError: found duplicate YAML directive"},
		{name: "mkdocs incompatible YAML version", ref: "P27 safe_load: ParserError found incompatible YAML document", files: pubMk("%YAML 2.0\n---\na: 1\n"), rc: 1, err: "yaml.parser.ParserError: found incompatible YAML document (version 1.* is required)"},
		{name: "mkdocs NUL character", ref: "P27 safe_load: ReaderError unacceptable character #x0000", files: pubMk("a: \"b\x00c\"\n"), rc: 1, err: "yaml.reader.ReaderError: unacceptable character #x0000: special characters are not allowed"},
		{name: "mkdocs control character", ref: "P27 safe_load: ReaderError unacceptable character #x0001", files: pubMk("a: \x01\n"), rc: 1, err: "yaml.reader.ReaderError: unacceptable character #x0001"},
		{name: "mkdocs python tag", ref: "P27 safe_load: ConstructorError could not determine a constructor for the tag", files: pubMk("a: !!python/name:os.system\n"), rc: 1, err: "yaml.constructor.ConstructorError: could not determine a constructor for the tag 'tag:yaml.org,2002:python/name:os.system'"},
		{name: "mkdocs python object tag", ref: "P27 safe_load: ConstructorError (python/object)", files: pubMk("a: !!python/object:os.system {}\n"), rc: 1, err: "yaml.constructor.ConstructorError: could not determine a constructor for the tag"},
		{name: "mkdocs local tag", ref: "P27 safe_load: ConstructorError could not determine a constructor for the tag '!foo'", files: pubMk("a: !foo bar\n"), rc: 1, err: "yaml.constructor.ConstructorError: could not determine a constructor for the tag '!foo'"},
		{name: "mkdocs ENV tag", ref: "P27 safe_load: ConstructorError (the mkdocs !ENV tag is not a safe_load tag)", files: pubMk("a: !ENV [HOME]\n"), rc: 1, err: "could not determine a constructor for the tag '!ENV'"},
		{name: "mkdocs merge of a scalar", ref: "P27 safe_load: ConstructorError expected a mapping or list of mappings for merging", files: pubMk("<<: 5\n"), rc: 1, err: "yaml.constructor.ConstructorError: while constructing a mapping"},
		{name: "mkdocs merge list with a non-mapping", ref: "P27 safe_load: ConstructorError expected a mapping for merging", files: pubMk("<<: [{a: 1}, 5]\n"), rc: 1, err: "yaml.constructor.ConstructorError: while constructing a mapping"},
		{name: "mkdocs unhashable key", ref: "P27 safe_load: ConstructorError found unhashable key", files: pubMk("? [a]\n: b\n"), rc: 1, err: "yaml.constructor.ConstructorError: while constructing a mapping"},
		{name: "mkdocs int tag with text", ref: "P27 safe_load: ValueError invalid literal for int()", files: pubMk("a: !!int abc\n"), rc: 1, err: "ValueError: invalid literal for int() with base 10: 'abc'"},
		{name: "mkdocs int tag empty", ref: "P27 safe_load: IndexError string index out of range", files: pubMk("a: !!int ''\n"), rc: 1, err: "IndexError: string index out of range"},
		{name: "mkdocs int tag with a non-ASCII digit", ref: "P27 STRICTER: int() accepts any Unicode decimal digit", files: pubMk("a: !!int \"\u0663\"\n"), rc: 1, err: "STRICTER"},
		{name: "mkdocs float tag with text", ref: "P27 safe_load: ValueError could not convert string to float", files: pubMk("a: !!float abc\n"), rc: 1, err: "ValueError: could not convert string to float: 'abc'"},
		{name: "mkdocs bool tag with text", ref: "P27 safe_load: KeyError", files: pubMk("a: !!bool maybe\n"), rc: 1, err: "KeyError: 'maybe'"},
		{name: "mkdocs null tag with a sequence", ref: "P27 safe_load: ConstructorError expected a scalar node, but found sequence", files: pubMk("a: !!null [1]\n"), rc: 1, err: "yaml.constructor.ConstructorError: expected a scalar node, but found sequence"},
		{name: "mkdocs timestamp tag with text", ref: "P27 safe_load: AttributeError 'NoneType' object has no attribute 'groupdict'", files: pubMk("a: !!timestamp foo\n"), rc: 1, err: "AttributeError: 'NoneType' object has no attribute 'groupdict'"},
		{name: "mkdocs date with month 13", ref: "P27 safe_load: ValueError month must be in 1..12", files: pubMk("a: 2001-13-14\n"), rc: 1, err: "ValueError: month must be in 1..12, not 13"},
		{name: "mkdocs date with day 30 of February", ref: "P27 safe_load: ValueError day must be in range", files: pubMk("a: 2020-02-30\n"), rc: 1, err: "ValueError: day 30 must be in range 1..29 for month 2 in year 2020"},
		{name: "mkdocs date with year 0", ref: "P27 safe_load: ValueError year must be in 1..9999", files: pubMk("a: 0000-01-01\n"), rc: 1, err: "ValueError: year must be in 1..9999, not 0"},
		{name: "mkdocs timestamp with hour 24", ref: "P27 safe_load: ValueError hour must be in 0..23", files: pubMk("a: 2001-12-14T24:00:00\n"), rc: 1, err: "ValueError: hour must be in 0..23, not 24"},
		{name: "mkdocs timestamp with offset 99:99", ref: "P27 safe_load: ValueError offset must be a timedelta strictly between", files: pubMk("a: 2001-12-14T21:59:43+99:99\n"), rc: 1, err: "ValueError: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not datetime.timedelta(days=4, seconds="},
		{name: "mkdocs binary with a bad length", ref: "P27 safe_load: ConstructorError failed to decode base64 data", files: pubMk("a: !!binary aGVsbG8\n"), rc: 1, err: "yaml.constructor.ConstructorError: failed to decode base64 data: Incorrect padding"},
		{name: "mkdocs binary with a non-ASCII character", ref: "P27 safe_load: ConstructorError failed to convert base64 data into ascii", files: pubMk("a: !!binary \u00e9\n"), rc: 1, err: "failed to convert base64 data into ascii: 'ascii' codec can't encode character '\\xe9' in position 0"},
		{name: "mkdocs omap of a mapping", ref: "P27 safe_load: ConstructorError expected a sequence", files: pubMk("a: !!omap {a: 1}\n"), rc: 1, err: "yaml.constructor.ConstructorError: while constructing an ordered map"},
		{name: "mkdocs pairs entry of two items", ref: "P27 safe_load: ConstructorError expected a single mapping item", files: pubMk("a: !!pairs [{a: 1, b: 2}]\n"), rc: 1, err: "yaml.constructor.ConstructorError: while constructing pairs"},
		{name: "mkdocs set of a sequence", ref: "P27 safe_load: ConstructorError expected a mapping node", files: pubMk("a: !!set [a]\n"), rc: 1, err: "yaml.constructor.ConstructorError: expected a mapping node, but found sequence"},
		{name: "mkdocs seq tag with a mapping", ref: "P27 safe_load: ConstructorError expected a sequence node", files: pubMk("a: !!seq {a: 1}\n"), rc: 1, err: "yaml.constructor.ConstructorError: expected a sequence node, but found mapping"},
		{name: "mkdocs trailing newline after an implicit type", ref: "P27 STRICTER: Python's $ lets a plain match end before one newline", files: pubMk("a: ! \"true\\n\"\n"), rc: 1, err: "STRICTER"},
		{name: "mkdocs nesting beyond the recursion limit", ref: "P27 safe_load: RecursionError (Python stops near 485 levels)", files: pubMk("a: " + deep(700) + "\n"), rc: 1, err: "RecursionError: maximum recursion depth exceeded"},
		{name: "mkdocs nesting between 400 and the limit", ref: "P27 STRICTER: Python composed 450 levels, Go refuses above 400", files: pubMk("a: " + deep(450) + "\n"), rc: 1, err: "RecursionError: maximum recursion depth exceeded"},
		{name: "nav key with 4301 digits", ref: "P58 str(label): ValueError Exceeds the limit (4300 digits)", files: pubMk("nav:\n  - ? 1" + strings.Repeat("0", 4300) + "\n    : index.md\n"), rc: 1, err: "ValueError: Exceeds the limit (4300 digits) for integer string conversion"},
		{name: "mkdocs huge integer", ref: "P27 safe_load: ValueError Exceeds the limit (4300 digits)", files: pubMk("a: 1" + strings.Repeat("0", 4300) + "\n"), rc: 1, err: "Exceeds the limit (4300 digits) for integer string conversion: value has 4301 digits"},
		{name: "front matter YAML error", ref: "P35 safe_load: ScannerError", files: pubFM("a: b: c"), rc: 1, err: "yaml.scanner.ScannerError: mapping values are not allowed here"},
		{name: "front matter python tag", ref: "P35 safe_load: ConstructorError", files: pubFM("page_id: !!python/name:os.system"), rc: 1, err: "yaml.constructor.ConstructorError: could not determine a constructor for the tag"},
		{name: "front matter set with two members", ref: "P154 STRICTER: str() of a set prints in hash order", files: pubFM("page_id: !!set {a, b}"), rc: 1, err: "STRICTER"},
		{name: "front matter page_id with 4301 digits", ref: "P154 str(int): ValueError Exceeds the limit (4300 digits)", files: pubFM("page_id: 1" + strings.Repeat("0", 4300)), rc: 1, err: "ValueError: Exceeds the limit (4300 digits) for integer string conversion"},
		// ---- reading files
		{name: "mkdocs missing", ref: "P27 read_text: FileNotFoundError", files: map[string]string{"docs/index.md": "x"}, rc: 1, err: "FileNotFoundError: [Errno 2] No such file or directory:"},
		{name: "mkdocs is a directory", ref: "P27 read_text: IsADirectoryError", files: map[string]string{"mkdocs.yml" + "/x": "y"}, rc: 1, err: "IsADirectoryError: [Errno 21] Is a directory:"},
		{name: "mkdocs not UTF-8", ref: "P27 read_text(encoding=utf-8): UnicodeDecodeError", files: pubMk("a: \xff\n"), rc: 1, err: "UnicodeDecodeError:"},
		{name: "page not UTF-8", ref: "P32,179 read_text: UnicodeDecodeError", files: pubPage("\xff"), rc: 1, err: "UnicodeDecodeError:"},
		{name: "off-nav page is a directory", ref: "P179 read_text of a directory named *.md: IsADirectoryError", files: pubWith(map[string]string{"docs/dir.md/x": "y"}), rc: 1, err: "IsADirectoryError: [Errno 21] Is a directory:"},
		{name: "off-nav page is a dangling symlink", ref: "P179 read_text of a dangling link: FileNotFoundError", files: pubBase(), links: [][2]string{{"docs/dangling.md", "nowhere"}}, rc: 1, err: "FileNotFoundError: [Errno 2] No such file or directory:"},
		{name: "off-nav page is a symlink loop", ref: "P179 read_text: OSError Too many levels of symbolic links", files: pubBase(), links: [][2]string{{"docs/loop.md", "loop.md"}}, rc: 1, err: "OSError: [Errno 40] Too many levels of symbolic links:"},
		{name: "page name not UTF-8", ref: "P133 STRICTER: Python listed the page and printed its name with surrogateescape", files: pubWith(map[string]string{"docs/bad\xffname.md": "x"}), rc: 1, err: "not UTF-8"},
		// ---- csv (P85-87, P188-189)
		{name: "IA file not UTF-8", ref: "P86-87 open(encoding=utf-8) iteration: UnicodeDecodeError", files: pubWith(map[string]string{"docs-data/ia/a.tsv": "url\tid\n\xff\tx\n"}), rc: 1, err: "UnicodeDecodeError:"},
		{name: "IA file is a directory", ref: "P86 open() of a directory named *.tsv: IsADirectoryError", files: pubWith(map[string]string{"docs-data/ia/sub.tsv/x": "y"}), rc: 1, err: "IsADirectoryError: [Errno 21] Is a directory:"},
		{name: "IA field over the csv limit", ref: "P87 csv.DictReader: _csv.Error field larger than field limit", files: pubWith(map[string]string{"docs-data/ia/a.tsv": "url\tid\n/\t" + strings.Repeat("x", 131073) + "\n"}), rc: 1, err: "_csv.Error: field larger than field limit (131072)"},
		{name: "IA url column left empty by a short row sorts None against str", ref: "P231 sorted(key=item.get): TypeError", files: pubWith(map[string]string{"docs-data/ia/a.tsv": "id\turl\nx\ny\t/\n"}), rc: 1, err: "TypeError: '<' not supported between instances of 'str' and 'NoneType'"},
		{name: "IA url column left empty in every row", ref: "P231 sorted(key=item.get): TypeError NoneType and NoneType", files: pubWith(map[string]string{"docs-data/ia/a.tsv": "id\turl\nx\nz\n"}), rc: 1, err: "TypeError: '<' not supported between instances of 'NoneType' and 'NoneType'"},
		{name: "redirects file not UTF-8", ref: "P188-189 open + DictReader: UnicodeDecodeError", files: pubWith(map[string]string{"docs-data/redirects.tsv": "source_path\ttarget_path\n\xff\t\xfe\n"}), rc: 1, err: "UnicodeDecodeError:"},
		{name: "redirects row without a target cell", ref: "P193 None.strip(): AttributeError", files: pubWith(map[string]string{"docs-data/redirects.tsv": "source_path\ttarget_path\n/old/\n"}), rc: 1, err: "AttributeError: 'NoneType' object has no attribute 'strip'"},
		{name: "redirects field over the csv limit", ref: "P189 csv.DictReader: _csv.Error field larger than field limit", files: pubWith(map[string]string{"docs-data/redirects.tsv": "source_path\ttarget_path\n" + strings.Repeat("x", 131073) + "\t/\n"}), rc: 1, err: "_csv.Error: field larger than field limit (131072)"},
		// ---- links (P104-114, P181)
		{name: "link of blanks", ref: "P181 raw.strip().split()[0]: IndexError", files: pubPage("[a]( )\n"), rc: 1, err: "IndexError: list index out of range"},
		{name: "link with an unclosed bracketed host", ref: "P104 urlparse: ValueError Invalid IPv6 URL", files: pubPage("[a](//[::1/x)\n"), rc: 1, err: "ValueError: Invalid IPv6 URL"},
		{name: "link with a bracketed host", ref: "P104 STRICTER: Python validated the host as an IP address and accepted a valid one", files: pubPage("[a](//[::1]/x)\n"), rc: 1, err: "STRICTER"},
		{name: "link with a NFKC netloc", ref: "P104 urlparse _checknetloc: ValueError", files: pubPage("[a](//a\uff20b.com/x)\n"), rc: 1, err: "ValueError: netloc 'a\uff20b.com' contains invalid characters under NFKC normalization"},
		{name: "link with a NUL byte", ref: "P113 resolve(): ValueError embedded null", files: pubPage("[a](a\x00b.md)\n"), rc: 1, err: "ValueError: lstat: embedded null character in path"},
		{name: "link up to the file system root", ref: "P96 with_suffix on '/': ValueError has an empty name", files: pubPage("[a](" + strings.Repeat("../", 63) + "..)\n"), rc: 1, err: "ValueError: PosixPath('/') has an empty name"},
		{name: "docs_dir with a NUL", ref: "P120 resolve(): ValueError embedded null", files: pubMk("docs_dir: \"a\\0b\"\n"), rc: 1, err: "ValueError: lstat: embedded null character in path"},
		// ---- writing .build (P204-231)
		{name: "build directory is a file", ref: "P204 mkdir(parents=True, exist_ok=True): FileExistsError", files: pubWith(map[string]string{".build": "x"}), rc: 1, err: "FileExistsError: [Errno 17] File exists:"},
		{name: "inventory file is a directory", ref: "P217 open('w'): IsADirectoryError", files: pubWith(map[string]string{".build/docs-v2-publication-inventory.tsv/x": "y"}), rc: 1, err: "IsADirectoryError: [Errno 21] Is a directory:"},
		{name: "coverage file is a directory", ref: "P225 open('w'): IsADirectoryError", files: pubWith(map[string]string{".build/docs-v2-ia-coverage.tsv/x": "y"}), rc: 1, err: "IsADirectoryError: [Errno 21] Is a directory:"},
		{name: "summary file is a directory", ref: "P246 write_text: IsADirectoryError", files: pubWith(map[string]string{".build/docs-v2-publication-summary.md/x": "y"}), rc: 1, err: "IsADirectoryError: [Errno 21] Is a directory:"},
		// ---- the validation errors themselves (P263 print + return 1)
		{name: "navigation target missing", ref: "P128-131 errors.append navigation targets do not exist; P263 return 1", files: pubMk("nav:\n  - gone.md\n"), rc: 1, err: "navigation targets do not exist: ['gone.md']"},
		{name: "markdown page outside the navigation", ref: "P135-138 errors.append unclassified Markdown", files: pubWith(map[string]string{"docs/extra.md": "x"}), rc: 1, err: "unclassified Markdown outside navigation: ['extra.md']"},
		{name: "canonical URL outside the IA", ref: "P142-145 errors.append canonical URLs outside the approved IA", files: pubWith(map[string]string{"docs-data/ia/a.tsv": "url\tid\n/x/\ty\n"}), rc: 1, err: "canonical URLs outside the approved IA: ['/']"},
		{name: "broken local link", ref: "P182-184 errors.append broken local link", files: pubPage("[a](missing.md)\n"), rc: 1, err: "broken local link: index.md -> missing.md"},
	}
}

func pubRunRow(t *testing.T, r pubRow) {
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
	t.Chdir(dir)
	code, stdout, stderr := runCaptured(t, "docs", "validate-publication")
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

// Every input the former Python validator refused (YAML errors of every class, csv errors, OSError and ValueError paths,
// the errors it reported) is refused by the Go verb with exit code 1, and the text is Python's unless the row says STRICTER.
func TestValidatePublicationRefusesEverythingTheFormerScriptRefused(t *testing.T) {
	rows := pubRefusalTable()
	if len(rows) != pubRefusalRows {
		t.Fatalf("the table has %d rows, pinned %d: a row was added or removed", len(rows), pubRefusalRows)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if r.name == "" || r.ref == "" || seen[r.name] {
			t.Fatalf("row %q: empty name, empty Python reference or a repeated name", r.name)
		}
		seen[r.name] = true
		if r.rc == 0 || !strings.HasPrefix(r.ref, "P") {
			t.Fatalf("row %q expects a success or has no line reference", r.name)
		}
		t.Run(r.name, func(t *testing.T) { pubRunRow(t, r) })
	}
}

// The base tree of the table passes: a row that fails does so for the cause it names.
func TestValidatePublicationBaseTreePasses(t *testing.T) {
	dir := t.TempDir()
	for name, text := range pubBase() {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)
	code, stdout, stderr := runCaptured(t, "docs", "validate-publication")
	if code != 0 || stderr != "" || !strings.Contains(stdout, "Validated 1 canonical pages, 0 redirects, and 1 frozen IA nodes.") {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}
