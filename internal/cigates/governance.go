// Package cigates holds the CI gate programs that cmd/cigate runs.
package cigates

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

const (
	govSrcPrefix  = "src/"
	govTestPrefix = "tests/"
)

var (
	govTestFilePattern = regexp.MustCompile(`(?i)(^|/)(test_.*|.*_test)\.py$`)
	govMarkerLine      = regexp.MustCompile(`^[A-Z][A-Z-]+:\s*`)
	govHeadingMarks    = regexp.MustCompile(`^#{1,6}\s+`)
	govPlaceholders    = map[string]bool{
		"": true, "n/a": true, "na": true, "none": true, "tbd": true, "todo": true, "pending": true,
		"<commands run and key results>":                                  true,
		"<commands + results>":                                            true,
		"<blast radius, rollback approach, monitoring, follow-up issues>": true,
		"<blast radius + rollback + follow-up>":                           true,
	}
)

type repeatedFlag []string

func (r *repeatedFlag) String() string     { return strings.Join(*r, ",") }
func (r *repeatedFlag) Set(v string) error { *r = append(*r, v); return nil }

// splitLines splits like Python str.splitlines (no trailing empty element).
func splitLines(s string) []string {
	var out []string
	rs := []rune(s)
	cur := make([]rune, 0, len(rs))
	for i := 0; i < len(rs); i++ {
		r := rs[i]
		switch r {
		case '\n', '\v', '\f', 0x1c, 0x1d, 0x1e, 0x85, 0x2028, 0x2029:
			out = append(out, string(cur))
			cur = cur[:0]
		case '\r':
			if i+1 < len(rs) && rs[i+1] == '\n' {
				i++
			}
			out = append(out, string(cur))
			cur = cur[:0]
		default:
			cur = append(cur, r)
		}
	}
	if len(cur) > 0 {
		out = append(out, string(cur))
	}
	return out
}

func extractMarkerValue(body, marker string) string {
	lines := splitLines(body)
	markerUpper := strings.ToUpper(marker)
	markerPrefix := marker + ":"
	for index, line := range lines {
		raw := strings.TrimSpace(line)
		stripped := strings.TrimSpace(govHeadingMarks.ReplaceAllString(raw, ""))
		upper := strings.ToUpper(stripped)
		var remainder string
		switch {
		case strings.HasPrefix(upper, markerPrefix):
			remainder = strings.TrimSpace(stripped[strings.Index(stripped, ":")+1:])
		case upper == markerUpper:
			remainder = ""
		default:
			continue
		}
		var values []string
		if remainder != "" {
			values = append(values, remainder)
		}
		for _, next := range lines[index+1:] {
			nextStripped := strings.TrimSpace(next)
			if nextStripped == "" {
				continue
			}
			nextClean := strings.TrimSpace(govHeadingMarks.ReplaceAllString(nextStripped, ""))
			if govMarkerLine.MatchString(nextClean) {
				break
			}
			if govHeadingMarks.MatchString(nextStripped) {
				break
			}
			if strings.HasPrefix(nextStripped, "<!--") {
				continue
			}
			cleaned := strings.TrimSpace(strings.TrimLeft(nextStripped, "-* "))
			if cleaned != "" {
				values = append(values, cleaned)
			}
		}
		return strings.TrimSpace(strings.Join(values, " "))
	}
	return ""
}

func isMeaningful(value string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(value), " "))
	if govPlaceholders[normalized] {
		return false
	}
	if strings.HasPrefix(normalized, "<") && strings.HasSuffix(normalized, ">") {
		return false
	}
	return normalized != ""
}

func gitChangedFiles(stdout io.Writer, base, head string) ([]string, bool) {
	cmd := exec.Command("git", "diff", "--name-only", "--diff-filter=ACMRTUXB", base, head)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintln(stdout, "ERROR: Unable to compute changed files from git diff.")
		if s := strings.TrimSpace(stderr.String()); s != "" {
			fmt.Fprintln(stdout, s)
		}
		fmt.Fprintln(stdout, "Hint: ensure both base/head commits are fetched (for Actions use fetch-depth: 0).")
		return nil, false
	}
	var files []string
	for _, l := range splitLines(string(out)) {
		if l = strings.TrimSpace(l); l != "" {
			files = append(files, l)
		}
	}
	return files, true
}

// Governance is the PR test-evidence gate. It returns the process exit code.
func Governance(args []string, stdout io.Writer) int {
	fs := flag.NewFlagSet("governance", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	base := fs.String("base-sha", "", "Base commit SHA for diff.")
	head := fs.String("head-sha", "", "Head commit SHA for diff.")
	var changedFlag repeatedFlag
	fs.Var(&changedFlag, "changed-file", "Changed file path (repeatable). Bypasses git diff for local testing.")
	body := fs.String("pr-body", os.Getenv("PR_BODY"), "PR body content (defaults to PR_BODY env var).")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	var changed []string
	for _, f := range changedFlag {
		if f != "" {
			changed = append(changed, f)
		}
	}
	if len(changed) == 0 {
		if *base == "" || *head == "" {
			fmt.Fprintln(stdout, "ERROR: supply --base-sha and --head-sha, or provide --changed-file entries.")
			return 2
		}
		var ok bool
		if changed, ok = gitChangedFiles(stdout, *base, *head); !ok {
			return 2
		}
	}

	var src []string
	for _, p := range changed {
		if strings.HasPrefix(p, govSrcPrefix) {
			src = append(src, p)
		}
	}
	sort.Strings(src)
	if len(src) == 0 {
		fmt.Fprintln(stdout, "Governance gate: no src/ changes detected, skipping strict evidence checks.")
		return 0
	}
	for _, p := range changed {
		if strings.HasPrefix(p, govTestPrefix) || govTestFilePattern.MatchString(p) {
			fmt.Fprintln(stdout, "Governance gate: src/ and test changes detected; policy satisfied without PR markers.")
			return 0
		}
	}

	var missing []string
	if !isMeaningful(extractMarkerValue(*body, "TEST-EVIDENCE")) {
		missing = append(missing, "TEST-EVIDENCE")
	}
	if !isMeaningful(extractMarkerValue(*body, "RISK-NOTES")) {
		missing = append(missing, "RISK-NOTES")
	}
	if len(missing) > 0 {
		fmt.Fprintln(stdout, "Governance gate failed.")
		fmt.Fprintf(stdout, "Detected src/ changes without test file updates (%d files).\n", len(src))
		for i, p := range src {
			if i == 10 {
				break
			}
			fmt.Fprintf(stdout, "  - %s\n", p)
		}
		if len(src) > 10 {
			fmt.Fprintf(stdout, "  ... and %d more\n", len(src)-10)
		}
		fmt.Fprintln(stdout, "Add meaningful PR body markers or modify tests:\n  TEST-EVIDENCE: <commands + results>\n  RISK-NOTES: <blast radius + rollback + follow-up>")
		fmt.Fprintf(stdout, "Missing/invalid markers: %s\n", strings.Join(missing, ", "))
		return 1
	}
	fmt.Fprintln(stdout, "Governance gate: src/ changes detected, PR markers provided; policy satisfied.")
	return 0
}
