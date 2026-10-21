// Command strip-agent-attribution removes agent-attribution trailers from a commit message file. The lefthook
// commit-msg hook runs it on every commit: AGENTS.md says "Never add contribution attribution for agents in commits",
// and agents that inject "Ultraworked with [...]" or "Co-authored-by: <agent>" trailers anyway are stripped before the
// commit lands. Real human Co-authored-by, Signed-off-by, Refs and Closes trailers are kept; runs of blank lines left
// by a removal collapse to one; a clean message is not rewritten.
//
// Exit codes: 0 message rewritten or unchanged; 1 file unreadable (or not valid UTF-8, left untouched); 2 argument count.
package main

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

// pyWS is Python's str.isspace set, which is what Python's re \s and str.rstrip() use for str patterns; Go's \s is
// ASCII only, so a trailing U+00A0 or U+3000 would survive without it.
const pyWS = `[\t\n\v\f\r\x1c-\x1f \x{85}\x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}]`

var agentLines = []*regexp.Regexp{
	regexp.MustCompile(`(?m)^Ultraworked with \[[^\]]+\]\([^)]*\)` + pyWS + `*$`),
	regexp.MustCompile(`(?m)^Co-authored-by:` + pyWS + `+Sisyphus` + pyWS + `+<[^>]*>` + pyWS + `*$`),
	regexp.MustCompile(`(?m)^Co-authored-by:` + pyWS + `+Claude` + pyWS + `+<[^>]*>` + pyWS + `*$`),
	regexp.MustCompile(`(?m)^🤖 Generated with \[?[^\]\n]+\]?\([^)]*\)?` + pyWS + `*$`),
}

var (
	blankRuns = regexp.MustCompile(`\n{3,}`)
	pyTrim    = regexp.MustCompile(pyWS + `+$`)
)

// Strip returns message with the agent-attribution lines removed.
func Strip(message string) string {
	cleaned := message
	for _, line := range agentLines {
		cleaned = line.ReplaceAllString(cleaned, "")
	}
	cleaned = blankRuns.ReplaceAllString(cleaned, "\n\n")
	return pyTrim.ReplaceAllString(cleaned, "") + "\n"
}

// universalNewlines is what reading a text file does: \r\n and a lone \r both become \n.
func universalNewlines(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\r", "\n")
}

func run(args []string, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: strip-agent-attribution <commit-msg-file>")
		return 2
	}
	path := args[0]
	raw, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(stderr, "strip-agent-attribution: cannot read %s: %v\n", path, err)
		return 1
	}
	if !utf8.Valid(raw) {
		fmt.Fprintf(stderr, "strip-agent-attribution: cannot read %s: not valid UTF-8\n", path)
		return 1
	}
	original := universalNewlines(string(raw))
	cleaned := Strip(original)
	if cleaned != original {
		info, err := os.Stat(path)
		if err != nil {
			fmt.Fprintf(stderr, "strip-agent-attribution: cannot stat %s: %v\n", path, err)
			return 1
		}
		if err := os.WriteFile(path, []byte(cleaned), info.Mode().Perm()); err != nil {
			fmt.Fprintf(stderr, "strip-agent-attribution: cannot write %s: %v\n", path, err)
			return 1
		}
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stderr)) }
