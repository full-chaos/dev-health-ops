package operationaledges

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/textrefs"
)

// jiraKeyCore and githubPRURLPattern are the Go port of operational_edges.py's
// own module-level `_JIRA_KEY`/`_GITHUB_PR_URL` constants
// (src/dev_health_ops/work_graph/operational_edges.py:18-19).
//
// # WHY THESE ARE NOT internal/jobs/workgraph/textrefs
//
// textrefs.ExtractJiraKeys exists and looks like the obvious reuse, but its
// underlying regex (jiraKeyCore in extract.go:59, an unfortunate name
// collision with the var below -- different package) is
// `[A-Z][A-Z0-9]{1,9}-\d+`: a project key capped at 10 characters, ported
// from extractors/text_parser.py's OWN separate `JIRA_KEY_PATTERN`.
// operational_edges.py's `_JIRA_KEY` is `[A-Z][A-Z0-9]+-\d+`: no upper bound
// on project-key length at all. These are two DIFFERENT Python regexes in two
// different modules that happen to look alike -- reusing textrefs here would
// silently drop any incident/timeline reference whose project key runs past
// 10 characters. TestJiraKeyCoreDivergesFromTextrefs pins the exact case
// where they disagree, per team-lead's ruling: the reuse decision is a test,
// not a one-time read.
//
// githubPRURLPattern has no textrefs counterpart at all --
// textrefs.ExtractGitHubIssueRefs matches "#123"/"closes #123" reference
// forms, not a literal "github.com/owner/repo/pull/N" URL, which is what
// incident timeline/note bodies actually contain (PagerDuty/incident tooling
// links out with full URLs, it does not write GitHub's own shorthand).
//
// Both patterns deliberately omit `\b`: RE2's is ASCII-only where Python's is
// Unicode-aware (see textrefs.ExtractJiraKeys' own comment on the identical
// gap), so jiraKeyCore's boundary is re-checked explicitly below with
// textrefs.WordBoundaryBefore/After instead of trusting RE2's. `_GITHUB_PR_URL`
// has no `\b` in Python either, so githubPRURLPattern needs no boundary check.
//
// Both also use textrefs.DigitClass instead of a bare `\d`, for the SAME
// reason: RE2's `\d` is ASCII-only, Python's `\d` accepts any Unicode decimal
// digit (Arabic-Indic, Devanagari, etc. -- codex round chaos-4924-pr-a's
// finding 4, confirmed: "ABC-١٢" and ".../pull/١٢" match in Python and
// produced zero edges here before this fix).
var (
	jiraKeyCore        = regexp.MustCompile(`[A-Z][A-Z0-9]+-[` + textrefs.DigitClass + `]+`)
	githubPRURLPattern = regexp.MustCompile(`github\.com/([^/]+)/([^/]+)/pull/([` + textrefs.DigitClass + `]+)`)
)

// jiraKeyMatches returns every distinct Jira-shaped key found in text, in
// FindAllStringSubmatchIndex order -- mirrors `_JIRA_KEY.findall(body)`.
func jiraKeyMatches(text string) []string {
	if text == "" {
		return nil
	}
	var keys []string
	for _, loc := range jiraKeyCore.FindAllStringIndex(text, -1) {
		if !textrefs.WordBoundaryBefore(text, loc[0]) || !textrefs.WordBoundaryAfter(text, loc[1]) {
			continue
		}
		keys = append(keys, text[loc[0]:loc[1]])
	}
	return keys
}

// githubPRURLRef is one github.com/.../pull/N URL match: owner, repo, and the
// PR number as Python leaves it (a string; int() is applied by the caller,
// matching operational_edges.py's `int(number)` at the call site).
type githubPRURLRef struct {
	Owner  string
	Repo   string
	Number string
}

// unicodeDigitRange is one contiguous Unicode decimal-digit block: every
// Nd-category digit script encodes its ten digits as ten consecutive code
// points, digit-zero first, so a code point's VALUE within its own script is
// codepoint-Low.
type unicodeDigitRange struct{ Low, High rune }

// unicodeDigitRanges is parsed once from textrefs.DigitClass (the same
// `\x{XXXX}-\x{YYYY}` pairs used to build the regex character class), so
// there is exactly one pinned table for "what code points are Python `\d`"
// rather than a second one hand-copied here.
var unicodeDigitRanges = parseUnicodeDigitRanges(textrefs.DigitClass)

var unicodeDigitRangePattern = regexp.MustCompile(`\\x\{([0-9A-Fa-f]+)\}-\\x\{([0-9A-Fa-f]+)\}`)

func parseUnicodeDigitRanges(class string) []unicodeDigitRange {
	matches := unicodeDigitRangePattern.FindAllStringSubmatch(class, -1)
	ranges := make([]unicodeDigitRange, 0, len(matches))
	for _, m := range matches {
		low, err := strconv.ParseInt(m[1], 16, 32)
		if err != nil {
			continue
		}
		high, err := strconv.ParseInt(m[2], 16, 32)
		if err != nil {
			continue
		}
		ranges = append(ranges, unicodeDigitRange{Low: rune(low), High: rune(high)})
	}
	return ranges
}

// asciiDigitValue returns r's value (0-9) within its own decimal-digit
// script, mirroring what Python's int() does when parsing a string of
// Unicode decimal digits (e.g. int("١٢") == 12) -- RE2 has no digit-value
// primitive, unlike Python's int(), so this is a from-scratch lookup rather
// than a stdlib call. ok is false for a rune that is not a Unicode decimal
// digit at all (should not happen for text this package's own
// DigitClass-based regexes just matched, but checked rather than assumed).
func asciiDigitValue(r rune) (digit rune, ok bool) {
	for _, rg := range unicodeDigitRanges {
		if r >= rg.Low && r <= rg.High {
			return '0' + (r - rg.Low), true
		}
	}
	return 0, false
}

// normalizeDigitsToASCII converts every rune in a DigitClass-matched string
// to its ASCII '0'-'9' equivalent, so downstream numeric handling never has
// to deal with non-ASCII digit code points -- the conversion Python gets for
// free from int(), and RE2/Go do not.
func normalizeDigitsToASCII(digits string) string {
	var b strings.Builder
	b.Grow(len(digits))
	for _, r := range digits {
		if ascii, ok := asciiDigitValue(r); ok {
			b.WriteRune(ascii)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// githubPRURLMatches mirrors `_GITHUB_PR_URL.findall(body)`.
func githubPRURLMatches(text string) []githubPRURLRef {
	if text == "" {
		return nil
	}
	matches := githubPRURLPattern.FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return nil
	}
	refs := make([]githubPRURLRef, 0, len(matches))
	for _, m := range matches {
		refs = append(refs, githubPRURLRef{Owner: m[1], Repo: m[2], Number: m[3]})
	}
	return refs
}
