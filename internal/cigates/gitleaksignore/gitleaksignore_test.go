package gitleaksignore_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/goldenscan"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedfiles"
)

const genericRule = "generic-api-key"

type row struct {
	file string
	rule string
	line int
	raw  string
}

func parseRows(text string) ([]row, []string) {
	var rows []row
	var bad []string
	for _, raw := range strings.Split(text, "\n") {
		raw = strings.TrimSpace(raw)
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		parts := strings.Split(raw, ":")
		if len(parts) != 3 {
			bad = append(bad, fmt.Sprintf("row %q is not file:rule:line (a commit-pinned row is not allowed)", raw))
			continue
		}
		n, err := strconv.Atoi(parts[2])
		if err != nil || n < 1 {
			bad = append(bad, fmt.Sprintf("row %q has no positive line number", raw))
			continue
		}
		rows = append(rows, row{file: parts[0], rule: parts[1], line: n, raw: raw})
	}
	return rows, bad
}

// violations is one message per row that no longer suppresses a real finding of the pinned line.
func violations(root, text string) []string {
	rows, out := parseRows(text)
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.raw] {
			out = append(out, fmt.Sprintf("row %q is listed twice", r.raw))
			continue
		}
		seen[r.raw] = true
		if r.rule != genericRule {
			out = append(out, fmt.Sprintf("row %q: rule %s is not covered by the guard (only %s)", r.raw, r.rule, genericRule))
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(r.file)))
		if err != nil {
			out = append(out, fmt.Sprintf("row %q: file is not readable: %v", r.raw, err))
			continue
		}
		lines := strings.Split(string(data), "\n")
		if r.line > len(lines) {
			out = append(out, fmt.Sprintf("row %q: the file has %d lines", r.raw, len(lines)))
			continue
		}
		line := lines[r.line-1]
		if strings.Contains(line, "gitleaks:allow") {
			out = append(out, fmt.Sprintf("row %q: the line carries an inline gitleaks:allow, drop the row", r.raw))
			continue
		}
		if len(goldenscan.SecretsIn(line)) == 0 {
			out = append(out, fmt.Sprintf("row %q: line %d no longer holds a %s finding (stale row)", r.raw, r.line, r.rule))
		}
	}
	return out
}

func TestEveryIgnoreRowPointsAtALineThatStillHoldsAFinding(t *testing.T) {
	root, err := recordedfiles.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, ".gitleaksignore"))
	if err != nil {
		t.Fatal(err)
	}
	if got := violations(root, string(data)); len(got) > 0 {
		t.Fatalf("%d stale or invalid .gitleaksignore row(s):\n%s", len(got), strings.Join(got, "\n"))
	}
}

func TestPlantedDefects(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	flagged := `AUTH_ACCESS_TOKEN_CLASS = "hs256_compat"` // gitleaks:allow
	write("hit.txt", "first\n"+flagged+"\n")
	write("marked.txt", flagged+" // gitleaks:allow\n")
	write("plain.txt", "nothing here\n\n")
	cases := []struct {
		name string
		text string
		want string
	}{
		{"flagged line passes", "hit.txt:generic-api-key:2\n", ""},
		{"comments and blanks pass", "# note\n\nhit.txt:generic-api-key:2\n", ""},
		{"unflagged text", "plain.txt:generic-api-key:1\n", "stale row"},
		{"blank line", "plain.txt:generic-api-key:2\n", "stale row"},
		{"line past the end", "hit.txt:generic-api-key:9\n", "the file has"},
		{"missing file", "gone.txt:generic-api-key:1\n", "not readable"},
		{"inline allow on the line", "marked.txt:generic-api-key:1\n", "inline gitleaks:allow"},
		{"duplicate row", "hit.txt:generic-api-key:2\nhit.txt:generic-api-key:2\n", "listed twice"},
		{"other rule", "hit.txt:jwt:2\n", "not covered"},
		{"commit-pinned row", "abc123:hit.txt:generic-api-key:2\n", "not file:rule:line"},
		{"zero line", "hit.txt:generic-api-key:0\n", "no positive line"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := violations(root, c.text)
			if c.want == "" {
				if len(got) != 0 {
					t.Fatalf("want none, got %v", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0], c.want) {
				t.Fatalf("want one violation containing %q, got %v", c.want, got)
			}
		})
	}
}
