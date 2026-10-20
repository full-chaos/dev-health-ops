package routing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// Go port of ci/check_go_api_routing_digest.py, tests/test_go_api_routing_digest_checker.py and
// tests/test_go_api_routing_operator_text.py (CHAOS-10174 E3a). Every check takes a root so a planted defect runs in a temp
// tree; the real-tree tests run them over the repository.

const (
	sdlRelative     = "contracts/graphql/v1/schema.graphql"
	pinRelative     = "contracts/graphql/v1/schema-digest.json"
	historyRelative = "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md"
	historyHeading  = "### Schema-digest history"
)

func guardsRoot() string { return filepath.Join("..", "..", "..") }

// leadingFence returns the fence character, run length and the text after the run of a line that opens or closes a ``` or ~~~
// fence. A closing fence carries nothing after the run (CommonMark), so `~~~text` inside a block is content.
func leadingFence(stripped string) (byte, int, string) {
	for _, c := range []byte{'`', '~'} {
		if strings.HasPrefix(stripped, strings.Repeat(string(c), 3)) {
			rest := strings.TrimLeft(stripped, string(c))
			return c, len(stripped) - len(rest), rest
		}
	}
	return 0, 0, ""
}

// Python's re treats Unicode whitespace as \s and Unicode letters and digits as \w and \b; Go's classes are ASCII only. uniWS is
// \s as Python reads it, and wordRune/boundary stand in for \w and \b, so a non-breaking space, an accented letter or a
// non-ASCII digit in a doc or in Go source is read the way the replaced Python checkers read it.
const (
	uniWS    = `[\s\p{Z}\x{85}]`
	uniNotWS = `[^\s\p{Z}\x{85}]`
)

// pyStrip is Python's str.strip(): Unicode whitespace as unicode.IsSpace reads it plus the four control separators
// U+001C to U+001F, which str.isspace counts and Go's unicode.IsSpace does not.
func pyStrip(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) })
}

func pyRStrip(s string) string {
	return strings.TrimRightFunc(s, func(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) })
}

// pySplitLines is Python's str.splitlines(): it also breaks at \r, \v, \f, U+001C to U+001E, U+0085, U+2028 and U+2029, and
// drops the final empty piece after a trailing break.
func pySplitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch r {
		case '\n', '\r', '\v', '\f', 0x1c, 0x1d, 0x1e, 0x85, 0x2028, 0x2029:
			out = append(out, s[start:i])
			if r == '\r' && i+1 < len(s) && s[i+1] == '\n' {
				size++
			}
			start = i + size
		}
		i += size
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func wordRune(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r) }

// wordBefore and wordAfter report whether the rune just before or after a byte offset is a word rune.
func wordBefore(s string, i int) bool {
	r, _ := utf8.DecodeLastRuneInString(s[:i])
	return i > 0 && wordRune(r)
}

func wordAfter(s string, i int) bool {
	r, _ := utf8.DecodeRuneInString(s[i:])
	return i < len(s) && wordRune(r)
}

var tableSeparator = regexp.MustCompile(`^\|?` + uniWS + `*:?-+:?` + uniWS + `*(?:\|` + uniWS + `*:?-+:?` + uniWS + `*)*\|?$`)

// digestInHistoryRow is true iff the digest appears in a four-cell table row of THIS section: not in a fenced block (tracked
// by character and run length so nested fences cannot smuggle an example row), not after the next heading, not a stub row.
func digestInHistoryRow(section, digest string) bool {
	var fenceChar byte
	fenceLen := 0
	tableActive := false
	prevPipeLine := false
	for _, line := range pySplitLines(section) {
		stripped := pyStrip(line)
		if c, n, rest := leadingFence(stripped); c != 0 && (fenceChar == 0 || (c == fenceChar && n >= fenceLen && pyStrip(rest) == "")) {
			if fenceChar == 0 {
				fenceChar, fenceLen = c, n
			} else {
				fenceChar, fenceLen = 0, 0
			}
			tableActive, prevPipeLine = false, false
			continue
		}
		if fenceChar != 0 {
			continue
		}
		if strings.HasPrefix(stripped, "#") {
			return false
		}
		isPipe := strings.HasPrefix(stripped, "|")
		switch {
		case isPipe && prevPipeLine && !tableActive && tableSeparator.MatchString(stripped):
			tableActive = true
		case !isPipe:
			tableActive = false
		case tableActive && strings.Contains(stripped, digest):
			cells := strings.Split(strings.Trim(stripped, "|"), "|")
			if len(cells) >= 4 {
				filled := true
				for _, cell := range cells[:4] {
					if pyStrip(cell) == "" {
						filled = false
					}
				}
				if filled {
					return true
				}
			}
		}
		prevPipeLine = isPipe
	}
	return false
}

// checkSchemaDigest returns "" when the pin equals the SDL's digest and the digest is a row of the history table, else the
// reason. A missing file is a failure, never a pass.
func checkSchemaDigest(root string) string {
	read := func(rel, label string) ([]byte, string) {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			return nil, fmt.Sprintf("%s not found at %s", label, rel)
		}
		return raw, ""
	}
	sdl, bad := read(sdlRelative, "canonical SDL")
	if bad != "" {
		return bad
	}
	pinRaw, bad := read(pinRelative, "schema-digest pin")
	if bad != "" {
		return bad
	}
	doc, bad := read(historyRelative, "schema-digest history doc")
	if bad != "" {
		return bad
	}
	sum := sha256.Sum256(sdl)
	actual := "sha256:" + hex.EncodeToString(sum[:])
	var pin struct {
		SchemaDigest string `json:"schema_digest"`
	}
	if err := json.Unmarshal(pinRaw, &pin); err != nil {
		return pinRelative + " is not valid JSON: " + err.Error()
	}
	if pin.SchemaDigest == "" {
		return pinRelative + " has no usable schema_digest string"
	}
	if pin.SchemaDigest != actual {
		return fmt.Sprintf("the schema digest MOVED: computed %s, pinned %s", actual, pin.SchemaDigest)
	}
	_, history, found := strings.Cut(string(doc), historyHeading)
	if !found {
		return historyRelative + " has no '" + historyHeading + "' section"
	}
	if !digestInHistoryRow(history, actual) {
		return fmt.Sprintf("schema digest %s is pinned and correct but is not a row of the history table", actual)
	}
	return ""
}

func TestSchemaDigestPinMatchesTheSDLAndIsInTheHistoryTable(t *testing.T) {
	if reason := checkSchemaDigest(guardsRoot()); reason != "" {
		t.Fatal(reason)
	}
}

func digestTree(t *testing.T, sdl, pinned, historyBody string) string {
	t.Helper()
	root := t.TempDir()
	put := func(rel, body string) {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	put(sdlRelative, sdl)
	put(pinRelative, fmt.Sprintf(`{"schema_digest": %q}`, pinned))
	put(historyRelative, "# Doc\n\n"+historyHeading+"\n\n"+historyBody)
	return root
}

func digestOf(sdl string) string {
	sum := sha256.Sum256([]byte(sdl))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Planted defects: one per way the Python gate could be satisfied without recording a digest move.
func TestSchemaDigestGatePlantedDefects(t *testing.T) {
	const sdl = "type Query { a: Int }\n"
	d := digestOf(sdl)
	hdr := "| Digest | In force from | Moved by | Notes |\n"
	sep := "|---|---|---|---|\n"
	row := "| " + d + " | 2026-10-19 | CHAOS-1 | note |\n"
	cases := []struct {
		name, sdl, pinned, history string
		want                       string
	}{
		{"documented and pinned passes", sdl, d, hdr + sep + row, ""},
		{"SDL moved without the pin", sdl + "type X { b: Int }\n", d, row, "MOVED"},
		{"pin bumped without a history row", sdl, d, "| other | a | b | c |\n", "not a row"},
		{"prose mention does not count", sdl, d, "The digest " + d + " was recorded here.\n", "not a row"},
		{"stub row with empty cells does not count", sdl, d, "| " + d + " | | | |\n", "not a row"},
		{"fenced row does not count", sdl, d, "```\n" + hdr + sep + row + "```\n", "not a row"},
		{"nested fences cannot smuggle a row", sdl, d, "````\n```\n```\n" + hdr + sep + row + "````\n", "not a row"},
		{"a longer closing fence still closes the block", sdl, d, "```\nexample\n````\n" + hdr + sep + row, ""},
		{"a lone pipe row without a header and separator does not count", sdl, d, row, "not a row"},
		{"a fence line followed by text does not close the block", sdl, d, "~~~\nexample\n~~~this is content\n" + hdr + sep + row, "not a row"},
		{"a header and separator make the row count", sdl, d, hdr + sep + row, ""},
		{"separator cells padded with U+00A0 make a table", sdl, d, hdr + "|\u00a0---\u00a0|\u00a0---\u00a0|\u00a0---\u00a0|\u00a0---\u00a0|\n" + row, ""},
		{"a row whose other cells hold only U+00A0 is a stub", sdl, d, hdr + sep + "| " + d + " | \u00a0 | \u00a0 | \u00a0 |\n", "not a row"},
		{"a cell holding only U+001C is empty as Python strip reads it", sdl, d, hdr + sep + "| " + d + " | \x1c | \x1f | note |\n", "not a row"},
		{"non-ASCII text in the cells is a real row", sdl, d, hdr + sep + "| " + d + " | café | \u0663 | note |\n", ""},
		{"single-hyphen separator cells make a real table", sdl, d, hdr + "|-|-|-|-|\n" + row, ""},
		{"a whole table inside a fence does not count", sdl, d, "```\n" + hdr + sep + row + "```\n", "not a row"},
		{"row after the next heading does not count", sdl, d, "## Later\n" + hdr + sep + row, "not a row"},
		{"row of a later table under no heading counts only inside the section", sdl, d, "text\n\n## Other table\n\n" + hdr + sep + row, "not a row"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := checkSchemaDigest(digestTree(t, tc.sdl, tc.pinned, tc.history))
			if tc.want == "" && got != "" {
				t.Fatalf("clean tree refused: %s", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("defect not caught (want %q): %q", tc.want, got)
			}
		})
	}
	t.Run("missing files fail instead of passing", func(t *testing.T) {
		if got := checkSchemaDigest(t.TempDir()); got == "" {
			t.Fatal("an empty tree passed the gate")
		}
	})
}

var (
	enableFlagDefinition = regexp.MustCompile(`\.(?:String|Bool|Int|Duration)Var\(&[\p{L}\p{N}_.]+,` + uniWS + `*"([a-z][a-z-]*)"`)
	enableCommand        = regexp.MustCompile(`^` + uniWS + `*["']?` + uniWS + `*(?:\$` + uniWS + `+)?(?:[A-Z_]+=` + uniNotWS + `+` + uniWS + `+)*(?:SDL:` + uniWS + `+)?(dho goapi routing enable)`)
	booleanFlags         = map[string]bool{"dry-run": true}
	shorthandVerb        = regexp.MustCompile(`routing (?:enable|disable|repoint|carry|status)`)
	staleFullName        = regexp.MustCompile(`go-api-routing (?:enable|disable|repoint|carry|status)`)
	imperativeWords      = map[string]bool{"run": true, "use": true, "call": true, "execute": true, "invoke": true, "type": true, "try": true}
)

func flagNames(source string) []string {
	var out []string
	for _, m := range enableFlagDefinition.FindAllStringSubmatch(source, -1) {
		out = append(out, m[1])
	}
	return out
}

func definedFlags(root string) (map[string]bool, error) {
	flags := map[string]bool{}
	for _, name := range []string{"enable.go", "main.go"} {
		raw, err := os.ReadFile(filepath.Join(root, "internal", "goapicli", "routing", name))
		if err != nil {
			return nil, err
		}
		for _, name := range flagNames(string(raw)) {
			flags[name] = true
		}
	}
	return flags, nil
}

// enableCommands returns each `dho goapi routing enable` invocation with its continuation lines joined.
func enableCommands(text string) []string {
	lines := pySplitLines(text)
	var found []string
	for i, line := range lines {
		loc := enableCommand.FindStringSubmatchIndex(line)
		if loc == nil || wordAfter(line, loc[3]) {
			continue
		}
		command := line[loc[2]:]
		cursor := i
		for strings.HasSuffix(pyRStrip(command), "\\") && cursor+1 < len(lines) {
			cursor++
			command = strings.TrimSuffix(pyRStrip(command), "\\") + " " + pyStrip(lines[cursor])
		}
		found = append(found, command)
	}
	return found
}

// shellSplit is Python's shlex.split in POSIX mode: whitespace is space, tab, CR and LF only; a backslash outside quotes escapes
// the next character; single quotes are literal; inside double quotes a backslash escapes only a double quote or a backslash;
// an unterminated quote or a trailing backslash is an error (shlex raises ValueError).
func shellSplit(s string) ([]string, error) {
	var out []string
	var cur strings.Builder
	started := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case ' ', '\t', '\r', '\n':
			if started {
				out = append(out, cur.String())
				cur.Reset()
				started = false
			}
		case '\\':
			if i+1 >= len(s) {
				return nil, fmt.Errorf("no escaped character")
			}
			i++
			cur.WriteByte(s[i])
			started = true
		case '\'':
			end := strings.IndexByte(s[i+1:], '\'')
			if end < 0 {
				return nil, fmt.Errorf("no closing quotation")
			}
			cur.WriteString(s[i+1 : i+1+end])
			i += end + 1
			started = true
		case '"':
			started = true
			i++
			closed := false
			for ; i < len(s); i++ {
				if s[i] == '"' {
					closed = true
					break
				}
				if s[i] == '\\' && i+1 < len(s) && (s[i+1] == '"' || s[i+1] == '\\') {
					i++
				}
				cur.WriteByte(s[i])
			}
			if !closed {
				return nil, fmt.Errorf("no closing quotation")
			}
		default:
			cur.WriteByte(c)
			started = true
		}
	}
	if started {
		out = append(out, cur.String())
	}
	return out, nil
}

// commandProblems returns the undefined flags and stray words of one enable command. `enable` takes no positional operand:
// operations are ONE comma-separated -operations value and Go's flag parser stops at the first stray word.
func commandProblems(command string, defined map[string]bool) []string {
	text := strings.ReplaceAll(strings.ReplaceAll(command, "\\n\"", " "), "\"\n", " ")
	tokens, err := shellSplit(text)
	if err != nil {
		return []string{"unparseable command: " + err.Error()}
	}
	if len(tokens) < 4 {
		return nil
	}
	tokens = tokens[4:]
	var problems []string
	// Every token that starts with a dash must name a defined flag, wherever it sits (the Python checker read them all).
	for _, token := range tokens {
		if strings.HasPrefix(token, "-") {
			name, _, _ := strings.Cut(strings.TrimLeft(token, "-"), "=")
			if !defined[name] {
				problems = append(problems, "undefined flag "+name)
			}
		}
	}
	for i := 0; i < len(tokens); {
		token := tokens[i]
		if !strings.HasPrefix(token, "-") {
			problems = append(problems, fmt.Sprintf("stray word %q", token))
			i++
			continue
		}
		name, _, hasValue := strings.Cut(strings.TrimLeft(token, "-"), "=")
		if hasValue || booleanFlags[name] {
			i++
		} else {
			i += 2
		}
	}
	return problems
}

func markdownDocs(root string) []string {
	var out []string
	_ = filepath.WalkDir(filepath.Join(root, "docs"), func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".md") {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out
}

func TestEveryEnableCommandUsesOnlyFlagsTheVerbDefines(t *testing.T) {
	root := guardsRoot()
	defined, err := definedFlags(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"operations", "mode", "recorded-by", "review-evidence", "dry-run"} {
		if !defined[want] {
			t.Fatalf("the verb no longer defines -%s: the flag scan lost its anchor", want)
		}
	}
	for _, bad := range []string{"apply", "candidate-build"} {
		if defined[bad] {
			t.Fatalf("the verb defines -%s, which the docs must not rely on", bad)
		}
	}
	scanned := map[string]bool{}
	for _, path := range markdownDocs(root) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		commands := enableCommands(string(raw))
		if len(commands) > 0 {
			scanned[filepath.Base(path)] = true
		}
		for _, command := range commands {
			if problems := commandProblems(command, defined); len(problems) > 0 {
				t.Errorf("%s: `%s`: %v", filepath.Base(path), command, problems)
			}
		}
	}
	for _, want := range []string{"query-api-bootstrap.md", "go-api-wave-0-proof-infrastructure.md"} {
		if !scanned[want] {
			t.Errorf("the scan found no enable command in %s: it checks nothing there", want)
		}
	}
}

func TestEnableCommandScanPlantedDefects(t *testing.T) {
	defined := map[string]bool{"operations": true, "mode": true, "dry-run": true, "recorded-by": true}
	cases := []struct {
		name, command string
		want          int
	}{
		{"valid", "dho goapi routing enable -operations a,b --mode on --dry-run", 0},
		{"undefined flag", "dho goapi routing enable --apply", 1},
		{"stray operand", "dho goapi routing enable -operations a b", 1},
		{"equals form is one token", "dho goapi routing enable --mode=on", 0},
		{"an undefined flag in a value position is still undefined", "dho goapi routing enable -operations --apply", 1},
		{"a backslash-escaped space is one argument", "dho goapi routing enable -operations a -mode on -recorded-by Jane\\ Doe", 0},
		{"a quoted argument is one argument", "dho goapi routing enable -operations 'a b' --mode \"on now\"", 0},
		{"an unterminated quote is refused", "dho goapi routing enable -operations 'a", 1},
	}
	for _, tc := range cases {
		if got := commandProblems(tc.command, defined); len(got) != tc.want {
			t.Errorf("%s: %v, want %d problem(s)", tc.name, got, tc.want)
		}
	}
	if got := enableCommands("$ dho goapi routing enable --apply\n"); len(got) != 1 {
		t.Fatalf("a shell-prompted command was not extracted: %q", got)
	}
	if got := enableCommands("  $ SDL=x dho goapi routing enable -operations a\n"); len(got) != 1 {
		t.Fatalf("a shell-prompted command with an env prefix was not extracted: %q", got)
	}
	if got := enableCommands("dho goapi routing enable\u0663 --apply\n"); len(got) != 0 {
		t.Fatalf("a non-ASCII digit right after the verb is not a word boundary (Python \\b): %q", got)
	}
	if got := enableCommands("dho goapi routing enable\u00e9 --apply\n"); len(got) != 0 {
		t.Fatalf("a non-ASCII letter right after the verb is not a word boundary (Python \\b): %q", got)
	}
	if got := pySplitLines("a\u2028b\fc\r\nd\n"); strings.Join(got, "|") != "a|b|c|d" {
		t.Fatalf("pySplitLines differs from Python str.splitlines: %q", got)
	}
	if got := enableCommands("dho goapi routing enable --apply\u2028dho goapi routing enable --mode on\n"); len(got) != 2 {
		t.Fatalf("a U+2028 line break splits two commands (Python splitlines): %q", got)
	}
	if got := enableCommands("$\u00a0dho goapi routing enable --apply\n"); len(got) != 1 {
		t.Fatalf("U+00A0 is whitespace to Python \\s and must separate the prompt from the command: %q", got)
	}
	if got := flagNames("fs.StringVar(&caf\u00e9, \"mode\", \"\", \"x\")\nfs.BoolVar(&o.dry\u0663, \"dry-run\", false, \"\")"); len(got) != 2 {
		t.Fatalf("flag definitions with non-ASCII identifiers were missed: %v", got)
	}
	joined := enableCommands("run:\n  dho goapi routing enable \\\n    -operations a \\\n    --mode on\n")
	if len(joined) != 1 || !strings.Contains(joined[0], "--mode on") {
		t.Fatalf("continuation lines were not joined: %q", joined)
	}
}

// bareHits returns file:line of every non-test source or doc line where a verb is named without its binary.
func bareHits(root string, shorthand bool) []string {
	var hits []string
	for _, top := range []string{"cmd", "internal", "src", "docs", "ci"} {
		_ = filepath.WalkDir(filepath.Join(root, top), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			ext := filepath.Ext(path)
			if ext != ".go" && ext != ".py" && ext != ".md" && ext != ".sh" {
				return nil
			}
			if strings.HasSuffix(path, "_test.go") || strings.Contains(filepath.ToSlash(path), "/testdata/") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			for n, line := range pySplitLines(string(raw)) {
				stripped := strings.TrimLeft(line, " \t")
				if (ext == ".go" || ext == ".py") && (strings.HasPrefix(stripped, "//") || strings.HasPrefix(stripped, "#")) {
					continue
				}
				if lineNamesBareVerb(line, shorthand) {
					rel, _ := filepath.Rel(root, path)
					hits = append(hits, fmt.Sprintf("%s:%d", rel, n+1))
				}
			}
			return nil
		})
	}
	return hits
}

// lineNamesBareVerb finds a verb named without its binary: the shorthand `routing <verb>` (backticked, or after an imperative
// such as run, use or call) must
// not follow `goapi `, `goapi-`, `go-api ` or `go-api-` (the binary's own words); the stale binary go-api-routing <verb> must not follow a word character or dash.
func lineNamesBareVerb(line string, shorthand bool) bool {
	re := staleFullName
	if shorthand {
		re = shorthandVerb
	}
	for _, loc := range re.FindAllStringIndex(line, -1) {
		if wordAfter(line, loc[1]) {
			continue
		}
		before := line[:loc[0]]
		if !shorthand {
			if before != "" && (before[len(before)-1] == '-' || wordBefore(before, len(before))) {
				continue
			}
			return true
		}
		if strings.HasSuffix(before, "`") {
			ctx := before[:len(before)-1]
			if ctx != "" && (ctx[len(ctx)-1] == '-' || wordBefore(ctx, len(ctx))) {
				continue
			}
			if strings.HasSuffix(ctx, "goapi-") || strings.HasSuffix(ctx, "goapi ") || strings.HasSuffix(ctx, "go-api ") || strings.HasSuffix(ctx, "go-api-") {
				continue
			}
			return true
		}
		trimmed := strings.TrimRightFunc(before, unicode.IsSpace)
		if len(trimmed) == len(before) {
			continue
		}
		word := trimmed
		start := len(trimmed)
		for start > 0 {
			r, size := utf8.DecodeLastRuneInString(trimmed[:start])
			if !wordRune(r) {
				break
			}
			start -= size
		}
		word = strings.ToLower(trimmed[start:])
		if imperativeWords[word] {
			return true
		}
	}
	return false
}

func TestNoOperatorTextNamesARoutingVerbWithoutItsBinary(t *testing.T) {
	if hits := bareHits(guardsRoot(), true); len(hits) != 0 {
		t.Fatalf("`routing <verb>` shorthand sends an operator to `command not found`: %v", hits)
	}
}

func TestNoOperatorTextNamesTheDeletedGoAPIRoutingBinary(t *testing.T) {
	if hits := bareHits(guardsRoot(), false); hits != nil && len(hits) != 0 {
		t.Fatalf("go-api-routing was folded into `dho goapi routing`: %v", hits)
	}
}

func TestBareVerbPatternsPlantedDefects(t *testing.T) {
	cases := []struct {
		line      string
		shorthand bool
		want      bool
	}{
		{"run `routing enable -operations a`", true, true},
		{"run `dho goapi routing enable`", true, false},
		{"run `dho goapi-routing status`", true, false},
		{"see go-api-routing enable", false, true},
		{"the dev-health-go-api-routing enable path", false, false},
		{"use dho goapi routing enable", false, false},
		{"Run routing enable with the operation name", true, true},
		{"Run routing status on the host", true, true},
		{"use dho goapi routing status", true, false},
		{"use dev-hops go-api routing status", true, false},
		{"the Go routing status verb", true, false},
		{"caf\u00e9`routing enable`", true, false},
		{"`routing enable\u0663`", true, false},
		{"run\u00a0routing enable now", true, true},
		{"Run \u00a0routing status", true, true},
		{"caf\u00e9go-api-routing enable", false, false},
		{"go-api-routing enable\u0663", false, false},
		{"see caf\u00e9 go-api-routing enable", false, true},
		{"First-time routing enable (canary set only)", true, false},
		{"use routing disable to turn it off", true, true},
	}
	for _, tc := range cases {
		if got := lineNamesBareVerb(tc.line, tc.shorthand); got != tc.want {
			t.Errorf("%q shorthand=%v: got %v want %v", tc.line, tc.shorthand, got, tc.want)
		}
	}
}
