package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	sisyphus = "Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-openagent)"
	sisCo    = "Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>"
)

// stripRows: each row names the pattern index it needs (agentLines order: Ultraworked, Sisyphus, Claude, robot) so a
// pattern that stops matching shows up as a failing row. The non-ASCII rows hold the Unicode semantics Python's re gave
// (\s is Unicode whitespace; U+200B and U+180E are not whitespace).
var stripRows = []struct {
	name string
	in   string
	want string
	uses []int
}{
	{"sisyphus trailer pair", "feat(api): add product telemetry endpoint (CHAOS-1789)\n\nAdds POST /api/v1/product-telemetry/events.\n\n" + sisyphus + "\n\n" + sisCo + "\n",
		"feat(api): add product telemetry endpoint (CHAOS-1789)\n\nAdds POST /api/v1/product-telemetry/events.\n", []int{0, 1}},
	{"claude code trailer", "fix(db): handle BUSYGROUP on consumer group setup\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\nCo-authored-by: Claude <noreply@anthropic.com>\n",
		"fix(db): handle BUSYGROUP on consumer group setup\n", []int{2, 3}},
	{"a real human co-author stays", "feat(metrics): wire DORA dashboard tile (CHAOS-1234)\n\nCo-authored-by: Alex Real-Human <alex@example.com>\n" + sisCo + "\n",
		"feat(metrics): wire DORA dashboard tile (CHAOS-1234)\n\nCo-authored-by: Alex Real-Human <alex@example.com>\n", []int{1}},
	{"signed-off-by, refs and closes stay", "fix(api): drop nullable sort key (CHAOS-450)\n\nRefs CHAOS-450\nCloses CHAOS-786\nSigned-off-by: Real Person <real@example.com>\n" + sisyphus + "\n" + sisCo + "\n",
		"fix(api): drop nullable sort key (CHAOS-450)\n\nRefs CHAOS-450\nCloses CHAOS-786\nSigned-off-by: Real Person <real@example.com>\n", []int{0, 1}},
	{"a clean message is unchanged", "chore(deps): bump ruff to 0.6.5\n\nNo behaviour change.\n", "chore(deps): bump ruff to 0.6.5\n\nNo behaviour change.\n", nil},
	{"runs of blank lines collapse", "subject\n\nbody line\n\n\n\nUltraworked with [Sisyphus](https://example.com/x)\nCo-authored-by: Sisyphus <agent@example.com>\n",
		"subject\n\nbody line\n", []int{0, 1}},
	{"another sisyphus address", "subject\n\nbody\n\nCo-authored-by: Sisyphus <other-agent@example.org>\n", "subject\n\nbody\n", []int{1}},
	{"another ultraworked agent", "subject\n\nbody\n\nUltraworked with [SomeAgent](https://example.com)\n", "subject\n\nbody\n", []int{0}},
	{"claude co-author", "subject\n\nbody\n\nCo-authored-by: Claude <noreply@anthropic.com>\n", "subject\n\nbody\n", []int{2}},
	{"a message with no trailing newline gets one", "subject", "subject\n", nil},
	{"an empty message becomes one newline", "", "\n", nil},
	// Unicode parity with Python's re (\s = str.isspace) and str.rstrip().
	{"no-break space around the name", "subject\n\nCo-authored-by: Sisyphus <a@b.c>\n", "subject\n", []int{1}},
	{"ideographic space after an ultraworked line", "subject\n\nUltraworked with [A](u)　\n", "subject\n", []int{0}},
	{"line separator after a claude co-author", "x\n\nCo-authored-by: Claude <c@d.e> \n", "x\n", []int{2}},
	{"next line (U+0085) between the words", "x\n\nCo-authored-by:\u0085Claude\u0085<z@y.x>\n", "x\n", []int{2}},
	{"unit separator (U+001F) between the words", "x\n\nCo-authored-by:\x1fSisyphus\x1f<a@b.c>\n", "x\n", []int{1}},
	{"vertical tab and form feed around the name", "x\n\nCo-authored-by:\vSisyphus\f<a@b.c>\n", "x\n", []int{1}},
	{"non-ASCII agent name and url", "x\n\nUltraworked with [Ágent✓](https://e.x/ü)\n", "x\n", []int{0}},
	{"a trailing no-break space on the last line is trimmed", "subject \n", "subject\n", nil},
	{"zero width space is not whitespace: line kept", "x\n\nCo-authored-by: Sisyphus​ <a@b.c>\n", "x\n\nCo-authored-by: Sisyphus​ <a@b.c>\n", nil},
	{"Mongolian vowel separator is not whitespace: line kept", "x\n\nCo-authored-by:᠎Claude <a@b.c>\n", "x\n\nCo-authored-by:᠎Claude <a@b.c>\n", nil},
	{"lowercase name is not an agent line", "x\n\nCo-authored-by: sisyphus <a@b.c>\n", "x\n\nCo-authored-by: sisyphus <a@b.c>\n", nil},
	{"an indented agent line is kept", "x\n\n  Ultraworked with [A](u)\n", "x\n\n  Ultraworked with [A](u)\n", nil},
}

func TestStrip(t *testing.T) {
	for _, row := range stripRows {
		t.Run(row.name, func(t *testing.T) {
			if got := Strip(row.in); got != row.want {
				t.Fatalf("Strip(%q) = %q, want %q", row.in, got, row.want)
			}
			if again := Strip(row.want); again != row.want {
				t.Fatalf("not idempotent: Strip(%q) = %q", row.want, again)
			}
		})
	}
}

// stripWith runs Strip with the given line patterns (the trailing blank-run and trim steps as in Strip).
func stripWith(lines []*regexp.Regexp, trim *regexp.Regexp, message string) string {
	cleaned := message
	for _, line := range lines {
		cleaned = line.ReplaceAllString(cleaned, "")
	}
	cleaned = blankRuns.ReplaceAllString(cleaned, "\n\n")
	return trim.ReplaceAllString(cleaned, "") + "\n"
}

// Red proofs, executed: the rows fail when a pattern is dropped, and the Unicode rows fail when \s is ASCII only.
func TestStripRowsCatchAMissingPattern(t *testing.T) {
	for skip := range agentLines {
		var kept []*regexp.Regexp
		for i, line := range agentLines {
			if i != skip {
				kept = append(kept, line)
			}
		}
		failed := false
		for _, row := range stripRows {
			for _, used := range row.uses {
				if used == skip && stripWith(kept, pyTrim, row.in) != row.want {
					failed = true
				}
			}
		}
		if !failed {
			t.Errorf("dropping pattern %d changes no row: the rows do not exercise it", skip)
		}
	}
}

func TestStripRowsCatchAnAsciiOnlyWhitespaceClass(t *testing.T) {
	ascii := strings.NewReplacer(pyWS, `\s`)
	var lines []*regexp.Regexp
	for _, line := range agentLines {
		lines = append(lines, regexp.MustCompile(ascii.Replace(line.String())))
	}
	trim := regexp.MustCompile(ascii.Replace(pyTrim.String()))
	if len(lines[0].String()) >= len(agentLines[0].String()) {
		t.Fatal("the ASCII-only variant was not built")
	}
	failed := 0
	for _, row := range stripRows {
		if stripWith(lines, trim, row.in) != row.want {
			failed++
		}
	}
	if failed < 5 {
		t.Errorf("only %d rows fail with an ASCII-only \\s; the non-ASCII rows are not doing their job", failed)
	}
}

func TestRun(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, content []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, content, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cases := []struct {
		name      string
		content   []byte // nil: the path is not created
		dirPath   bool
		argc      int // 1 normally; other counts test the usage refusal
		wantCode  int
		wantFile  []byte
		wantInErr string
	}{
		{"rewrites the file in place", []byte("feat: thing\n\nUltraworked with [Sisyphus](https://example.com/x)\nCo-authored-by: Sisyphus <agent@example.com>\n"), false, 1, 0, []byte("feat: thing\n"), ""},
		{"a clean file is not rewritten", []byte("feat: thing\n"), false, 1, 0, []byte("feat: thing\n"), ""},
		{"a clean CRLF file keeps its bytes", []byte("feat: thing\r\n\r\nbody\r\n"), false, 1, 0, []byte("feat: thing\r\n\r\nbody\r\n"), ""},
		{"a CRLF file with a trailer is rewritten with LF", []byte("feat: thing\r\n\r\nUltraworked with [A](u)\r\n"), false, 1, 0, []byte("feat: thing\n"), ""},
		{"a lone CR is a newline", []byte("a\rb\n"), false, 1, 0, []byte("a\rb\n"), ""},
		{"a missing file is exit 1", nil, false, 1, 1, nil, "cannot read"},
		{"a directory is exit 1", nil, true, 1, 1, nil, "cannot read"},
		{"invalid UTF-8 is exit 1 and the file is untouched", []byte("subject\n\xff\xfe\nUltraworked with [A](u)\n"), false, 1, 1, []byte("subject\n\xff\xfe\nUltraworked with [A](u)\n"), "not valid UTF-8"},
		{"no argument is exit 2", nil, false, 0, 2, nil, "usage"},
		{"two arguments are exit 2", nil, false, 2, 2, nil, "usage"},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var args []string
			path := filepath.Join(dir, "missing")
			switch {
			case c.dirPath:
				path = dir
			case c.content != nil:
				path = write("msg"+string(rune('a'+i)), c.content)
			}
			switch c.argc {
			case 1:
				args = []string{path}
			case 2:
				args = []string{path, path}
			}
			var stderr bytes.Buffer
			if code := run(args, &stderr); code != c.wantCode {
				t.Fatalf("exit %d, want %d (stderr %q)", code, c.wantCode, stderr.String())
			}
			if c.wantInErr != "" && !strings.Contains(stderr.String(), c.wantInErr) {
				t.Fatalf("stderr %q lacks %q", stderr.String(), c.wantInErr)
			}
			if c.wantCode == 0 || c.wantFile != nil {
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, c.wantFile) {
					t.Fatalf("file is %q, want %q", got, c.wantFile)
				}
			}
		})
	}
}
