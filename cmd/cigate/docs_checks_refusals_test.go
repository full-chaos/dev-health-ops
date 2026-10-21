package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// The checked-in refusal inventory of the three docs checks ported from Python (CHAOS-10435, part 1). Each row names one
// trigger and the line of the former script at the merge base (a64af6106a44) that refuses it. "STRICTER" marks a
// place where Python accepted and Go refuses (allowed by D6129); "PARITY" a place where both refuse. The rows run
// through the live entry point (run), so a refusal removed from Go, or a row removed from the table, fails the test.
//
//	L = scripts/check_built_site_links.py
//	S = scripts/check_docs_candidate_search.py
//	F = scripts/check_docs_candidate_facts.py
type dcRow struct {
	name  string
	ref   string
	files map[string]string
	links [][2]string
	args  []string
	rc    int
	err   string
	out   string
}

const dcRefusalRows = 101

func dcPage(body string) string { return "<html><body>" + body + "</body></html>\n" }

const dcIndexJSON = `{"docs": [{"location": "a/", "title": "A", "text": "alpha"}]}`

func dcSearchFiles(index, queries string) map[string]string {
	return map[string]string{"site/search/search_index.json": index, "q.json": queries}
}

var dcSearchArgs = []string{"docs", "check-docs-search", "--site-dir", "site", "--queries", "q.json", "--report", "r.tsv"}

func dcWithTail(args []string, tail ...string) []string {
	return append(append([]string(nil), args...), tail...)
}

func dcLinksArgs(extra ...string) []string {
	return append([]string{"docs", "check-built-site-links"}, extra...)
}

var dcOKQuery = `{"queries": [{"query": "alpha", "expected": "/a/"}]}`

func dcCase(name, ref, query, err string) dcRow {
	return dcRow{name: name, ref: ref, files: dcSearchFiles(dcIndexJSON, query), args: dcSearchArgs, rc: 1, err: err}
}

func dcRefusalTable() []dcRow {
	page := func(body string) map[string]string { return map[string]string{"site/index.html": dcPage(body)} }
	rows := []dcRow{
		// ---- links: argparse (L 90-91)
		{name: "links missing --site-dir", ref: "L90 add_argument(--site-dir, required=True): argparse exit 2", args: dcLinksArgs(), rc: 2, err: "required: --site-dir"},
		{name: "links unknown option", ref: "L91 parse_args: unrecognized arguments (--nope)", args: dcLinksArgs("--site-dir", "site", "--nope", "x"), rc: 2, err: "unrecognized arguments: --nope"},
		{name: "links trailing positional", ref: "L91 parse_args: unrecognized arguments (junk)", args: dcLinksArgs("--site-dir", "site", "junk"), rc: 2, err: "unrecognized arguments: junk"},
		{name: "links option without value", ref: "L91 parse_args: expected one argument", args: dcLinksArgs("--site-dir"), rc: 2, err: "expected one argument"},
		{name: "links value that looks like an option", ref: "L91 parse_args: expected one argument (-abc is an option)", args: dcLinksArgs("--site-dir", "-abc"), rc: 2, err: "expected one argument"},
		{name: "links abbreviated option", ref: "L91 STRICTER: argparse accepted --site as --site-dir", args: dcLinksArgs("--site", "site"), rc: 2, err: "unrecognized"},
		{name: "links help flag", ref: "L91 STRICTER: argparse printed help and exited 0", args: dcLinksArgs("-h"), rc: 2, err: "unrecognized"},
		{name: "links long help flag", ref: "L91 STRICTER: argparse printed help and exited 0", args: dcLinksArgs("--help"), rc: 2, err: "unrecognized"},
		{name: "links double dash", ref: "L91 STRICTER: argparse took -- and accepted a complete command line", args: dcLinksArgs("--site-dir", "site", "--"), rc: 2, err: "unrecognized"},
		{name: "links dash value with a space", ref: "L90 STRICTER: argparse accepted '-a b' as a value", args: dcLinksArgs("--site-dir", "-a b"), rc: 2, err: "expected one argument"},
		{name: "links negative number value", ref: "L90 STRICTER: argparse accepted -1 as a value", args: dcLinksArgs("--site-dir", "-1"), rc: 2, err: "expected one argument"},
		{name: "links equals form with a dash value", ref: "L90 STRICTER: argparse accepted --site-dir=-x", args: dcLinksArgs("--site-dir=-x"), rc: 2, err: "expected one argument"},
		{name: "links empty value", ref: "L90 STRICTER: Path('') is '.' in Python", args: dcLinksArgs("--site-dir", ""), rc: 2, err: "expected one argument"},
		// ---- links: inputs and stdlib refusals
		{name: "links site dir missing", ref: "L93-97 print ERROR built site directory not found, return 1", args: dcLinksArgs("--site-dir", "nope"), rc: 1, err: "built site directory not found: nope"},
		{name: "links site dir is a file", ref: "L93 is_dir() false for a file", files: map[string]string{"site": "x"}, args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "built site directory not found"},
		{name: "links page not UTF-8", ref: "L60 read_text(encoding=utf-8): UnicodeDecodeError", files: page("\xff"), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "UnicodeDecodeError"},
		{name: "links directory named html", ref: "L60 read_text on a directory: IsADirectoryError", files: map[string]string{"site/odd.html/x": "1"}, args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "IsADirectoryError"},
		{name: "links dangling symlink page", ref: "L60 read_text on a dangling symlink: FileNotFoundError", files: map[string]string{"site/index.html": dcPage("x")}, links: [][2]string{{"site/x.html", "nowhere"}}, args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "FileNotFoundError"},
		{name: "links target page not UTF-8", ref: "L25 anchors_for read_text: UnicodeDecodeError", files: map[string]string{"site/index.html": dcPage(`<a href="b.html#x">b</a>`), "site/b.html": "<i id=\"\xff\">"}, args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "UnicodeDecodeError"},
		{name: "links invalid IPv6 netloc", ref: "L35 urlsplit raises ValueError: Invalid IPv6 URL (should_skip)", files: page(`<a href="//[x/y">1</a>`), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "Invalid IPv6 URL"},
		{name: "links stray closing bracket", ref: "L35 urlsplit raises ValueError: Invalid IPv6 URL", files: page(`<a href="//x]/y">1</a>`), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "Invalid IPv6 URL"},
		{name: "links valid bracketed netloc", ref: "L35 STRICTER: Python validated the host as an IP address and skipped the link", files: page(`<a href="//[::1]/x">1</a>`), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "STRICTER"},
		{name: "links netloc NFKC slash", ref: "L35 urlsplit _checknetloc: NFKC netloc gains /", files: page("<a href=\"//ex／ample/x\">1</a>"), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "NFKC"},
		{name: "links netloc NFKC colon", ref: "L35 urlsplit _checknetloc: NFKC netloc gains :", files: page("<a href=\"//ex：ample/x\">1</a>"), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "NFKC"},
		{name: "links netloc NFKC at", ref: "L35 urlsplit _checknetloc: NFKC netloc gains @", files: page("<a href=\"//u＠h/x\">1</a>"), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "NFKC"},
		{name: "links symlink loop in a target", ref: "L43,70-75 Path.resolve() (3.14) keeps a loop; exists() is false: errors.append missing built asset", files: page(`<a href="loop/x.html">1</a>`), links: [][2]string{{"site/loop", "loop"}}, args: dcLinksArgs("--site-dir", "site"), rc: 1, out: "loop/x.html: missing built asset"},
		{name: "links NUL in a target", ref: "L43 Path.resolve() (3.14): ValueError lstat: embedded null character in path", files: page(`<a href="a%00b.html">1</a>`), args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "embedded null character"},
		{name: "links broken target reported", ref: "L72-75 errors.append missing built asset, return 1", files: page(`<a href="nope.html">1</a>`), args: dcLinksArgs("--site-dir", "site"), rc: 1, out: "index.html -> nope.html: missing built asset"},
		{name: "links missing anchor reported", ref: "L78-82 errors.append missing anchor, return 1", files: map[string]string{"site/index.html": dcPage(`<a href="b.html#z">b</a>`), "site/b.html": dcPage("x")}, args: dcLinksArgs("--site-dir", "site"), rc: 1, out: "missing anchor 'z'"},
		{name: "links page name not UTF-8", ref: "L59 STRICTER: Python listed the page and printed its name with surrogateescape (a UnicodeEncodeError on error)", files: map[string]string{"site/bad\xffname.html": dcPage("x")}, args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "not UTF-8"},
		{name: "links named pipe as a page", ref: "L60 STRICTER: read_text on a named pipe blocks forever", args: dcLinksArgs("--site-dir", "site"), rc: 1, err: "special file"},
		// ---- search: argparse (S 154-157)
		{name: "search missing --site-dir", ref: "S154 add_argument(--site-dir, required=True): argparse exit 2", args: []string{"docs", "check-docs-search", "--queries", "q.json"}, rc: 2, err: "required: --site-dir"},
		{name: "search missing --queries", ref: "S155 add_argument(--queries, required=True): argparse exit 2", args: []string{"docs", "check-docs-search", "--site-dir", "site"}, rc: 2, err: "required: --queries"},
		{name: "search unknown option", ref: "S160 parse_args: unrecognized arguments", args: dcWithTail(dcSearchArgs, "--nope", "1"), rc: 2, err: "unrecognized arguments: --nope"},
		{name: "search trailing positional", ref: "S160 parse_args: unrecognized arguments", args: dcWithTail(dcSearchArgs, "junk"), rc: 2, err: "unrecognized arguments: junk"},
		{name: "search abbreviated option", ref: "S160 STRICTER: argparse accepted --rep as --report", args: []string{"docs", "check-docs-search", "--site-dir", "site", "--queries", "q.json", "--rep", "r.tsv"}, rc: 2, err: "unrecognized"},
		{name: "search help flag", ref: "S160 STRICTER: argparse printed help and exited 0", args: []string{"docs", "check-docs-search", "-h"}, rc: 2, err: "unrecognized"},
		{name: "search option without value", ref: "S160 parse_args: expected one argument", args: []string{"docs", "check-docs-search", "--site-dir", "site", "--queries", "q.json", "--report"}, rc: 2, err: "expected one argument"},
		{name: "search report dash value", ref: "S156 STRICTER: argparse accepted '-a b' as a value", args: dcWithTail(dcSearchArgs[:6], "--report", "-a b"), rc: 2, err: "expected one argument"},
		{name: "search empty report value", ref: "S156 STRICTER: Path('') is '.' in Python", args: dcWithTail(dcSearchArgs[:6], "--report", ""), rc: 2, err: "expected one argument"},
		{name: "search double dash", ref: "S160 STRICTER: argparse took -- and accepted a complete command line", args: dcWithTail(dcSearchArgs, "--"), rc: 2, err: "unrecognized"},
		// ---- search: inputs and stdlib refusals
		{name: "search index missing", ref: "S164-166 print ERROR search index not found, return 1", files: map[string]string{"q.json": dcOKQuery}, args: dcSearchArgs, rc: 1, err: "search index not found: site/search/search_index.json"},
		{name: "search index is a directory", ref: "S164 is_file() false for a directory", files: map[string]string{"site/search/search_index.json/x": "1", "q.json": dcOKQuery}, args: dcSearchArgs, rc: 1, err: "search index not found"},
		{name: "search index not UTF-8", ref: "S105,171 read_text: UnicodeDecodeError (a ValueError) -> ERROR, return 1", files: dcSearchFiles("{\"docs\": \"\xff\"}", dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR:"},
		{name: "search index is not JSON", ref: "S105,171 json.loads: JSONDecodeError -> ERROR, return 1", files: dcSearchFiles("not json", dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR: Expecting value"},
		{name: "search index empty file", ref: "S105,171 json.loads: JSONDecodeError (empty)", files: dcSearchFiles("", dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR: Expecting value"},
		{name: "search index BOM", ref: "S105,171 json.loads: JSONDecodeError (a BOM is not whitespace)", files: dcSearchFiles("\ufeff"+dcIndexJSON, dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR:"},
		{name: "search index extra data", ref: "S105,171 json.loads: JSONDecodeError Extra data", files: dcSearchFiles(dcIndexJSON+" x", dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR: Extra data"},
		{name: "search index raw control character", ref: "S105,171 json.loads: JSONDecodeError Invalid control character", files: dcSearchFiles("{\"docs\": [{\"location\": \"a\tb\"}]}", dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR: Invalid control character"},
		{name: "search index trailing comma", ref: "S105,171 json.loads: JSONDecodeError", files: dcSearchFiles(`{"docs": [],}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR:"},
		{name: "search index single quotes", ref: "S105,171 json.loads: JSONDecodeError", files: dcSearchFiles(`{'docs': []}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR:"},
		{name: "search index leading zero number", ref: "S105,171 json.loads: JSONDecodeError (Extra data after 0)", files: dcSearchFiles(`{"docs": [], "n": 01}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "ERROR:"},
		{name: "search index lone surrogate escape", ref: "S105 STRICTER: Python kept a lone surrogate in the str", files: dcSearchFiles(`{"docs": [{"location": "a/", "title": "\ud800"}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "lone surrogate"},
		{name: "search index nested too deep", ref: "S105 STRICTER: Python raised RecursionError near 990 levels, Go refuses above 500", files: dcSearchFiles(`{"docs": [], "x": `+strings.Repeat("[", 600)+strings.Repeat("]", 600)+`}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "nesting deeper"},
		{name: "search index is a list", ref: "S106 payload.get on a list: AttributeError (uncaught)", files: dcSearchFiles(`[1]`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "AttributeError"},
		{name: "search index docs is not a list", ref: "S107-108 raise ValueError: search index has no docs array", files: dcSearchFiles(`{"docs": {}}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "search index has no docs array"},
		{name: "search index without docs", ref: "S107-108 raise ValueError: search index has no docs array", files: dcSearchFiles(`{}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "search index has no docs array"},
		{name: "search document location is a number", ref: "S113 STRICTER: Python took str() of a truthy number", files: dcSearchFiles(`{"docs": [{"location": 5, "title": "t"}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "location must be a string"},
		{name: "search document title is a list", ref: "S114 STRICTER: Python took str() of a truthy list", files: dcSearchFiles(`{"docs": [{"location": "a/", "title": ["x"]}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "title must be a string"},
		{name: "search document text is an object", ref: "S115 STRICTER: Python took str() of a truthy object", files: dcSearchFiles(`{"docs": [{"location": "a/", "text": {"k": 1}}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "text must be a string"},
		{name: "search document location with invalid IPv6", ref: "S41 urlsplit ValueError in _normalize_url (rank phase, uncaught)", files: dcSearchFiles(`{"docs": [{"location": "//[x/y", "title": "t"}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "Invalid IPv6 URL"},
		{name: "search document location with NFKC netloc", ref: "S41 urlsplit _checknetloc ValueError (rank phase, uncaught)", files: dcSearchFiles("{\"docs\": [{\"location\": \"//ex／ample\", \"title\": \"t\"}]}", dcOKQuery), args: dcSearchArgs, rc: 1, err: "NFKC"},
		{name: "search document location with valid brackets", ref: "S41 STRICTER: Python validated the bracketed host as an IP address", files: dcSearchFiles(`{"docs": [{"location": "//[::1]/x", "title": "t"}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "STRICTER"},
		{name: "search queries file missing", ref: "S123,171 read_text: FileNotFoundError (an OSError) -> ERROR, return 1", files: map[string]string{"site/search/search_index.json": dcIndexJSON}, args: dcSearchArgs, rc: 1, err: "ERROR: [Errno 2] No such file or directory: 'q.json'"},
		{name: "search queries file is a directory", ref: "S123,171 read_text: IsADirectoryError -> ERROR, return 1", files: map[string]string{"site/search/search_index.json": dcIndexJSON, "q.json/x": "1"}, args: dcSearchArgs, rc: 1, err: "ERROR: [Errno 21] Is a directory"},
		{name: "search queries file not UTF-8", ref: "S123,171 read_text: UnicodeDecodeError (a ValueError) -> ERROR, return 1", files: dcSearchFiles(dcIndexJSON, "{\"queries\": \"\xff\"}"), args: dcSearchArgs, rc: 1, err: "ERROR:"},
		{name: "search queries not JSON", ref: "S123,171 json.loads: JSONDecodeError", files: dcSearchFiles(dcIndexJSON, "{"), args: dcSearchArgs, rc: 1, err: "ERROR:"},
		{name: "search queries is null", ref: "S124 payload.get on None: AttributeError (uncaught)", files: dcSearchFiles(dcIndexJSON, "null"), args: dcSearchArgs, rc: 1, err: "AttributeError"},
		{name: "search queries not a list", ref: "S125-126 raise ValueError: query file has no queries array", files: dcSearchFiles(dcIndexJSON, `{"queries": "x"}`), args: dcSearchArgs, rc: 1, err: "query file has no queries array"},
		{name: "search queries key missing", ref: "S125-126 raise ValueError: query file has no queries array", files: dcSearchFiles(dcIndexJSON, `{}`), args: dcSearchArgs, rc: 1, err: "query file has no queries array"},
		dcCase("search empty query", "S135-136 raise ValueError: invalid search case (empty query)", `{"queries": [{"query": "  ", "expected": "/a/"}]}`, "invalid search case"),
		dcCase("search missing query", "S131,135-136 raise ValueError: invalid search case (no query)", `{"queries": [{"expected": "/a/"}]}`, "invalid search case"),
		dcCase("search missing expected", "S132,135-136 raise ValueError: invalid search case (expected '' normalises to /)", `{"queries": [{"query": "alpha"}]}`, "invalid search case"),
		dcCase("search expected is the root", "S135-136 raise ValueError: invalid search case (expected /)", `{"queries": [{"query": "alpha", "expected": "/"}]}`, "invalid search case"),
		dcCase("search expected is index.html", "S42-45,136 raise ValueError: invalid search case (index.html normalises to /)", `{"queries": [{"query": "alpha", "expected": "index.html"}]}`, "invalid search case"),
		dcCase("search expected is a fragment", "S41-42,136 raise ValueError: invalid search case (no path)", `{"queries": [{"query": "alpha", "expected": "#top"}]}`, "invalid search case"),
		dcCase("search expected with invalid IPv6", "S41 urlsplit ValueError in _normalize_url inside the try -> ERROR, return 1", `{"queries": [{"query": "alpha", "expected": "//[x/y"}]}`, "ERROR: Invalid IPv6 URL"),
		dcCase("search expected with NFKC netloc", "S41 urlsplit _checknetloc ValueError -> ERROR, return 1", "{\"queries\": [{\"query\": \"alpha\", \"expected\": \"//ex／ample\"}]}", "NFKC"),
		dcCase("search expected with valid brackets", "S41 STRICTER: Python validated the bracketed host as an IP address", `{"queries": [{"query": "alpha", "expected": "//[::1]/x"}]}`, "STRICTER"),
		dcCase("search query is a number", "S131 STRICTER: Python took str() of a truthy number", `{"queries": [{"query": 7, "expected": "/a/"}]}`, "query must be a string"),
		dcCase("search expected is a number", "S132 STRICTER: Python took str() of a truthy number", `{"queries": [{"query": "alpha", "expected": 7}]}`, "expected must be a string"),
		{name: "search max_rank integer over the digit limit", ref: "S123,134,171 json.loads: ValueError Exceeds the limit (4300 digits) -> ERROR, return 1", files: dcSearchFiles(dcIndexJSON, `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": `+strings.Repeat("9", 4301)+`}]}`), args: dcSearchArgs, rc: 1, err: "4300"},
		{name: "search max_rank negative integer over the digit limit", ref: "S123,134,171 json.loads: ValueError Exceeds the limit (4300 digits); the sign does not count", files: dcSearchFiles(dcIndexJSON, `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": -`+strings.Repeat("9", 4301)+`}]}`), args: dcSearchArgs, rc: 1, err: "4300"},
		{name: "search character reference with leading zeros over the digit limit", ref: "S52 html.unescape: int() counts leading zeros (4300 digits) -> ValueError", files: dcSearchFiles(`{"docs": [{"location": "a/", "title": "&#`+strings.Repeat("0", 4299)+`65;"}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "4300"},
		dcCase("search max_rank is a numeric string", "S134 STRICTER: int('3') parsed the string", `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": "3"}]}`, "max_rank must be a number"),
		dcCase("search max_rank is a word", "S134,171 int('abc') raises ValueError -> ERROR, return 1", `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": "abc"}]}`, "max_rank must be a number"),
		dcCase("search max_rank is a list", "S134 int([1]) raises TypeError (uncaught)", `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": [1]}]}`, "TypeError"),
		dcCase("search max_rank is an object", "S134 int({...}) raises TypeError (uncaught)", `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": {"a": 1}}]}`, "TypeError"),
		dcCase("search max_rank is NaN", "S134,171 int(nan) raises ValueError -> ERROR, return 1", `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": NaN}]}`, "cannot convert float NaN"),
		dcCase("search max_rank is infinity", "S134 int(inf) raises OverflowError (uncaught)", `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": Infinity}]}`, "OverflowError"),
		dcCase("search max_rank is negative infinity", "S134 int(-inf) raises OverflowError (uncaught)", `{"queries": [{"query": "alpha", "expected": "/a/", "max_rank": -Infinity}]}`, "OverflowError"),
		{name: "search failing query reported", ref: "S205-209 print ERROR failures, return 1", files: dcSearchFiles(`{"docs": [{"location": "a/", "title": "A", "text": "alpha"}, {"location": "b/", "title": "B", "text": "beta"}]}`, `{"queries": [{"query": "alpha", "expected": "/b/", "max_rank": 1}]}`), args: dcSearchArgs, rc: 1, out: "expected /b/ at rank <= 1, found 2; top=/a/"},
		{name: "search report parent is a file", ref: "S175 Path.mkdir(parents=True, exist_ok=True): FileExistsError", files: map[string]string{"site/search/search_index.json": dcIndexJSON, "q.json": dcOKQuery, "blocked": "x"}, args: []string{"docs", "check-docs-search", "--site-dir", "site", "--queries", "q.json", "--report", "blocked/r.tsv"}, rc: 1, err: "Error"},
		{name: "search report is a directory", ref: "S177 Path.open('w'): IsADirectoryError", files: map[string]string{"site/search/search_index.json": dcIndexJSON, "q.json": dcOKQuery, "rdir/x": "1"}, args: []string{"docs", "check-docs-search", "--site-dir", "site", "--queries", "q.json", "--report", "rdir"}, rc: 1, err: "IsADirectoryError"},
		{name: "search character reference with too many digits", ref: "S52 html.unescape: int() of more than 4300 digits raises ValueError", files: dcSearchFiles(`{"docs": [{"location": "a/", "title": "&#`+strings.Repeat("1", 4301)+`;"}]}`, dcOKQuery), args: dcSearchArgs, rc: 1, err: "4300"},
		// ---- facts (F 47-83); the script took no arguments
		{name: "facts page missing", ref: "F48-50 print ERROR canonical taxonomy source or page is missing, return 1", args: []string{"docs", "check-docs-facts"}, rc: 1, out: "canonical taxonomy source or page is missing"},
		{name: "facts page is a directory", ref: "F48 is_file() false for a directory", files: map[string]string{"docs/reference/taxonomies/investment.md/x": "1"}, args: []string{"docs", "check-docs-facts"}, rc: 1, out: "canonical taxonomy source or page is missing"},
		{name: "facts page not UTF-8", ref: "F59 read_text: UnicodeDecodeError (uncaught)", files: map[string]string{"docs/reference/taxonomies/investment.md": "`risk` \xff"}, args: []string{"docs", "check-docs-facts"}, rc: 1, err: "UnicodeDecodeError"},
		{name: "facts page without keys", ref: "F63-73 errors.append missing canonical taxonomy keys, return 1", files: map[string]string{"docs/reference/taxonomies/investment.md": "nothing\n"}, args: []string{"docs", "check-docs-facts"}, rc: 1, out: "missing canonical taxonomy keys: ['feature_delivery'"},
		{name: "facts page with an unknown dotted key", ref: "F64-65 unknown keys with a dot, return 1", files: map[string]string{"docs/reference/taxonomies/investment.md": "`risk.nope`\n"}, args: []string{"docs", "check-docs-facts"}, rc: 1, out: "unknown taxonomy keys: ['risk.nope']"},
		{name: "facts page with the placeholder key", ref: "F64-65 unknown keys with a dot (theme.subcategory is not a taxonomy key here)", files: map[string]string{"docs/reference/taxonomies/investment.md": "`theme.subcategory`\n"}, args: []string{"docs", "check-docs-facts"}, rc: 1, out: "unknown taxonomy keys: ['theme.subcategory']"},
	}
	return rows
}

func dcRunRow(t *testing.T, r dcRow) {
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
	if r.name == "links named pipe as a page" {
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
	if r.rc != 0 && r.rc != 2 && r.err == "" && stderr != "" && !strings.Contains(r.name, "reported") {
		t.Errorf("unexpected stderr %q", stderr)
	}
}

// Every input the former Python checks refused (argparse errors, ValueError and OSError paths, the stdlib calls that
// raise inside them) is refused by the Go verbs, with the exit code Python gave (2 for a usage error, 1 for the rest).
func TestDocsChecksRefuseEverythingTheFormerScriptsRefused(t *testing.T) {
	rows := dcRefusalTable()
	if len(rows) != dcRefusalRows {
		t.Fatalf("the table has %d rows, pinned %d: a row was added or removed", len(rows), dcRefusalRows)
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
		if !strings.Contains(r.ref, "L") && !strings.Contains(r.ref, "S") && !strings.Contains(r.ref, "F") {
			t.Fatalf("row %q has no line reference", r.name)
		}
		t.Run(r.name, func(t *testing.T) { dcRunRow(t, r) })
	}
}

// The docs checks accept the real docs page, and a page the Python script accepted with the registry file absent.
func TestDocsFactsAcceptsTheRepositoryPage(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	if code, stdout, stderr := runCaptured(t, "docs", "check-docs-facts"); code != 0 || !strings.HasPrefix(stdout, "Canonical fact drift check passed for 5 themes and 15 subcategories") || stderr != "" {
		t.Fatalf("exit %d\nstdout: %q\nstderr: %q", code, stdout, stderr)
	}
}

func TestDocsSearchUsageErrorsPrintUsageAndExit2(t *testing.T) {
	t.Chdir(t.TempDir())
	code, _, stderr := runCaptured(t, "docs", "check-docs-search")
	if code != 2 || !strings.Contains(stderr, "usage: cigate docs check-docs-search --site-dir SITE_DIR --queries QUERIES [--report REPORT]") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}
