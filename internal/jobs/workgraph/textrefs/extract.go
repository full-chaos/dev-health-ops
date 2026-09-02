package textrefs

import (
	"regexp"
	"strings"
)

// RefType mirrors Python's RefType enum.
type RefType string

const (
	RefCloses     RefType = "closes"
	RefReferences RefType = "references"
)

// ParsedIssueRef mirrors Python's frozen dataclass of the same name. ProjectKey
// is empty rather than nil-able because the Python side's None and "" are never
// distinguished by any consumer; the corpus records which it was.
type ParsedIssueRef struct {
	RawMatch   string
	IssueKey   string
	RefType    RefType
	ProjectKey string
}

// The character classes Python's patterns rely on, written out because RE2's
// own `\s`, `\w` and `\d` are ASCII-only. See charclass.go for the measurements
// these encode; these strings and the predicates there must agree, which
// TestRE2ClassesAgreeWithPredicates checks rather than assumes.
const (
	// Python's `\s`: 29 runes, exactly str.isspace(). The 001C-001F span is the
	// part unicode.IsSpace omits and the part no reviewer thinks to test.
	wsClass = `[\x{0009}-\x{000D}\x{001C}-\x{0020}\x{0085}\x{00A0}\x{1680}` +
		`\x{2000}-\x{200A}\x{2028}-\x{2029}\x{202F}\x{205F}\x{3000}]`
	// Python's `\d`: category Nd.
	digitClass = `\p{Nd}`
	// Python's `\w`: alphanumeric-or-underscore, where alphanumeric spans the
	// whole N category and not just Nd.
	wordClass = `\p{L}\p{N}_`
)

var (
	// `(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*#(\d+)` with IGNORECASE.
	githubClosingRefPattern = regexp.MustCompile(
		`(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)` + wsClass + `*#(` + digitClass + `+)`)

	// `(?<!\w)#(\d+)\b` -- the lookbehind and the trailing boundary are applied
	// in code by findPlainRefs, not here. This matches the CORE only.
	githubPlainRefCore = regexp.MustCompile(`#(` + digitClass + `+)`)

	// `\b([A-Z][A-Z0-9]{1,9})-(\d+)\b` -- both boundaries applied in code.
	jiraKeyCore = regexp.MustCompile(`([A-Z][A-Z0-9]{1,9})-(` + digitClass + `+)`)

	// `([\w\-\.]+/[\w\-\.]+)#(\d+)`
	gitlabCrossProjectPattern = regexp.MustCompile(
		`([` + wordClass + `\-\.]+/[` + wordClass + `\-\.]+)#(` + digitClass + `+)`)

	// `merge\s+pull\s+request\s+#(\d+)` with IGNORECASE.
	githubMergePRPattern = regexp.MustCompile(
		`(?i)merge` + wsClass + `+pull` + wsClass + `+request` + wsClass + `+#(` + digitClass + `+)`)

	// `(?:merge\s+request|see\s+merge\s+request)\b[^!\n]*!(\d+)` with IGNORECASE.
	// The `\b` after "request" is a word boundary against ASCII letters only in
	// practice -- "request" ends in a word char, so the boundary holds unless the
	// next rune is also a word char. Applied in code for the same reason as the
	// others: RE2's `\b` is ASCII and would disagree on a non-ASCII follower.
	gitlabMergeMRCore = regexp.MustCompile(
		`(?i)(?:see` + wsClass + `+)?merge` + wsClass + `+request`)
	gitlabMRNumber = regexp.MustCompile(`^[^!\n]*!(` + digitClass + `+)`)

	// `\(#(\d+)\)\s*$`. Go's `$` without (?m) is end-of-text where Python's is
	// end-of-text-or-before-a-final-newline, but the preceding `\s*` is greedy
	// and newlines are in wsClass, so it absorbs the difference. Verified by the
	// corpus rather than left to that argument.
	githubSquashPRPattern = regexp.MustCompile(`\(#(` + digitClass + `+)\)` + wsClass + `*$`)

	// `^\s*revert[\s"']` with IGNORECASE.
	revertSubjectPattern = regexp.MustCompile(`(?i)^` + wsClass + `*revert[` + wsClass[1:len(wsClass)-1] + `"']`)

	// `^\s*this\s+reverts\s+(?:commit|merge\s+request)\b` with IGNORECASE|MULTILINE.
	revertBodyPattern = regexp.MustCompile(
		`(?im)^` + wsClass + `*this` + wsClass + `+reverts` + wsClass +
			`+(?:commit|merge` + wsClass + `+request)`)
)

// isRevertMessage mirrors _is_revert_message.
func isRevertMessage(text string) bool {
	return revertSubjectPattern.MatchString(text) || revertBodyPattern.MatchString(text)
}

// ExtractPRRefs mirrors extract_pr_refs: de-duplicated PR/MR numbers in
// first-seen order, from the explicit merge-keyword forms only.
//
// The numbers go through pythonAtoi rather than strconv.Atoi because the Python
// side ends in int() over a Unicode `\d+` capture. See number.go.
func ExtractPRRefs(text string) []int {
	if text == "" {
		return nil
	}
	if isRevertMessage(text) {
		return nil
	}
	seen := map[int]bool{}
	var ordered []int
	add := func(raw string) {
		n, ok := pythonAtoi(raw)
		if !ok {
			return
		}
		if !seen[n] {
			seen[n] = true
			ordered = append(ordered, n)
		}
	}
	for _, m := range githubMergePRPattern.FindAllStringSubmatch(text, -1) {
		add(m[1])
	}
	for _, loc := range gitlabMergeMRCore.FindAllStringIndex(text, -1) {
		// The `\b` after "request", then `[^!\n]*!(\d+)` from that point.
		if !wordBoundaryAfter(text, loc[1]) {
			continue
		}
		if m := gitlabMRNumber.FindStringSubmatch(text[loc[1]:]); m != nil {
			add(m[1])
		}
	}
	return ordered
}

// ExtractSquashPRRefs mirrors extract_squash_pr_refs.
func ExtractSquashPRRefs(text string) []int {
	if text == "" {
		return nil
	}
	if isRevertMessage(text) {
		return nil
	}
	// Python anchors to the end of the SUBJECT -- the first line -- not the end
	// of the message.
	subject := text
	if i := strings.IndexByte(text, '\n'); i >= 0 {
		subject = text[:i]
	}
	m := githubSquashPRPattern.FindStringSubmatch(subject)
	if m == nil {
		return nil
	}
	n, ok := pythonAtoi(m[1])
	if !ok {
		return nil
	}
	return []int{n}
}

// ExtractJiraKeys mirrors extract_jira_keys.
//
// The `\b` on both sides is applied in code: RE2's is ASCII-only, so it would
// find a boundary before a non-ASCII letter where Python finds none, matching
// PROJ-1 in "éPROJ-1" that Python rejects.
func ExtractJiraKeys(text string) []ParsedIssueRef {
	if text == "" {
		return nil
	}
	var out []ParsedIssueRef
	seen := map[string]bool{}
	for _, loc := range jiraKeyCore.FindAllStringSubmatchIndex(text, -1) {
		if !wordBoundaryBefore(text, loc[0]) || !wordBoundaryAfter(text, loc[1]) {
			continue
		}
		project := text[loc[2]:loc[3]]
		number := text[loc[4]:loc[5]]
		key := project + "-" + number
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, ParsedIssueRef{
			RawMatch:   text[loc[0]:loc[1]],
			IssueKey:   key,
			RefType:    RefReferences,
			ProjectKey: project,
		})
	}
	return out
}

// ExtractGitHubIssueRefs mirrors extract_github_issue_refs: closing references
// first, then plain ones whose number was not already claimed.
//
// IssueKey is the RAW captured text, not a converted number -- Python does not
// call int() here, so '#٣٤' yields the key '٣٤'. Only ExtractPRRefs converts.
func ExtractGitHubIssueRefs(text string) []ParsedIssueRef {
	if text == "" {
		return nil
	}
	var out []ParsedIssueRef
	seen := map[string]bool{}
	for _, loc := range githubClosingRefPattern.FindAllStringSubmatchIndex(text, -1) {
		number := text[loc[2]:loc[3]]
		if seen[number] {
			continue
		}
		seen[number] = true
		out = append(out, ParsedIssueRef{
			RawMatch: text[loc[0]:loc[1]],
			IssueKey: number,
			RefType:  RefCloses,
		})
	}
	for _, loc := range findPlainRefs(text) {
		number := text[loc[2]:loc[3]]
		if seen[number] {
			continue
		}
		seen[number] = true
		out = append(out, ParsedIssueRef{
			RawMatch: text[loc[0]:loc[1]],
			IssueKey: number,
			RefType:  RefReferences,
		})
	}
	return out
}

// findPlainRefs applies `(?<!\w)#(\d+)\b` -- the lookbehind and the trailing
// boundary in code, the core by regexp. Returns submatch index quadruples.
func findPlainRefs(text string) [][]int {
	var out [][]int
	for _, loc := range githubPlainRefCore.FindAllStringSubmatchIndex(text, -1) {
		if !wordBoundaryBefore(text, loc[0]) {
			continue
		}
		if !wordBoundaryAfter(text, loc[1]) {
			continue
		}
		out = append(out, loc)
	}
	return out
}

// ExtractGitLabIssueRefs mirrors extract_gitlab_issue_refs.
//
// Three passes in Python's order, and the order is load-bearing because each
// pass consults the same `seen` set: closing refs, then cross-project refs,
// then plain refs that are not INSIDE a cross-project match.
//
// That last containment test is why this cannot reuse ExtractGitHubIssueRefs:
// in "group/proj#42" the plain pattern also matches "#42", and Python suppresses
// it by checking whether its start falls inside a cross-project span. The
// suppression is positional, not by number -- a plain "#42" elsewhere in the
// same text is still emitted if 42 has not been seen.
func ExtractGitLabIssueRefs(text string) []ParsedIssueRef {
	if text == "" {
		return nil
	}
	var out []ParsedIssueRef
	seen := map[string]bool{}

	for _, loc := range githubClosingRefPattern.FindAllStringSubmatchIndex(text, -1) {
		number := text[loc[2]:loc[3]]
		if seen[number] {
			continue
		}
		seen[number] = true
		out = append(out, ParsedIssueRef{
			RawMatch: text[loc[0]:loc[1]], IssueKey: number, RefType: RefCloses,
		})
	}

	crossSpans := gitlabCrossProjectPattern.FindAllStringSubmatchIndex(text, -1)
	for _, loc := range crossSpans {
		key := text[loc[2]:loc[3]] + "#" + text[loc[4]:loc[5]]
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, ParsedIssueRef{
			RawMatch: text[loc[0]:loc[1]], IssueKey: key, RefType: RefReferences,
		})
	}

	for _, loc := range findPlainRefs(text) {
		number := text[loc[2]:loc[3]]
		if seen[number] {
			continue
		}
		inCrossProject := false
		for _, cp := range crossSpans {
			if cp[0] <= loc[0] && loc[0] < cp[1] {
				inCrossProject = true
				break
			}
		}
		if inCrossProject {
			continue
		}
		seen[number] = true
		out = append(out, ParsedIssueRef{
			RawMatch: text[loc[0]:loc[1]], IssueKey: number, RefType: RefReferences,
		})
	}
	return out
}
