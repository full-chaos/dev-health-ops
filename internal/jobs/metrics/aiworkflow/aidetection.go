// Package aiworkflow ports the ai_workflow metrics.daily family (CHAOS-4280
// part B / CHAOS-4286 part B) -- the OTHER half of ai_workflow.py's
// extraction pair, distinct from work_graph_edges (#2263/#2273), which
// already ports extract_review_deployment_incident_edges. This package
// covers extract_ai_workflow_from_pull_requests only.
package aiworkflow

import (
	"regexp"
	"strings"
	"unicode"
)

// Signal ports providers/_ai_detection.py's AIAttributionSignal, the pure
// detection result every detector function returns.
type Signal struct {
	Source     string
	Kind       string
	Confidence float64
	Actor      *string
	Evidence   map[string]any
}

// AuthorInfo ports providers/_ai_detection.py's AuthorInfo NamedTuple.
//
// UserType and AppSlug are plain strings, not pointers, DELIBERATELY: the
// only production call site (Compute, below, porting _signals_from_pr) reads
// them through Python's `_str(row, key, default="")`, which returns "" for a
// missing/None column, never None itself. An empty string is exactly as
// falsy as None in Python's `if author.user_type and ...` check, and the two
// must produce the SAME evidence JSON byte for byte -- `"user_type":""`, not
// `"user_type":null` -- so a pointer type here would be a fidelity bug, not
// an improvement. Per CHAOS-4280 astra finding 2, the live PR loader never
// populates either field at all today (the production SELECT lacks the
// source columns), so both are always "" in practice; the fields exist so
// the kernel is honestly testable with synthetic data that DOES set them.
type AuthorInfo struct {
	Login    string
	UserType string
	AppSlug  string
}

// Attribution kind/source string constants, ported from
// dev_health_ops.models.ai_attribution's enums (str-valued in Python).
const (
	KindAIAssisted   = "ai_assisted"
	KindAgentCreated = "agent_created"
	KindAIReview     = "ai_review"

	SourcePRLabel    = "pr_label"
	SourceBotAuthor  = "bot_author"
	SourceBranchName = "branch_name"
	SourcePRBody     = "pr_body"
)

// aiLabels ports AI_LABELS: known explicit AI attribution label names.
var aiLabels = map[string]struct{}{
	"ai-assisted":   {},
	"agent-created": {},
	"ai-review":     {},
	"copilot":       {},
	"claude-code":   {},
	"codex":         {},
	"cursor":        {},
	"windsurf":      {},
}

// labelKindMap ports _LABEL_KIND_MAP.
var labelKindMap = map[string]string{
	"ai-assisted":   KindAIAssisted,
	"agent-created": KindAgentCreated,
	"ai-review":     KindAIReview,
	"copilot":       KindAIAssisted,
	"claude-code":   KindAIAssisted,
	"codex":         KindAIAssisted,
	"cursor":        KindAIAssisted,
	"windsurf":      KindAIAssisted,
}

// knownAIBots ports KNOWN_AI_BOTS: exact-match GitHub logins, lowercase.
var knownAIBots = map[string]struct{}{
	"copilot[bot]":       {},
	"claude-code[bot]":   {},
	"cursor-agent[bot]":  {},
	"chatgpt-codex[bot]": {},
	"sweep-ai[bot]":      {},
	"coderabbit[bot]":    {},
	"devin[bot]":         {},
}

// ciBots ports CI_BOTS: automation, NOT AI, excluded from attribution.
var ciBots = map[string]struct{}{
	"github-actions[bot]": {},
	"dependabot[bot]":     {},
	"renovate[bot]":       {},
}

// DetectFromPRLabels ports detect_from_pr_labels: one Signal per matching
// label, source PR_LABEL.
func DetectFromPRLabels(labels []string) []Signal {
	return detectFromLabels(labels, SourcePRLabel)
}

func detectFromLabels(labels []string, source string) []Signal {
	var signals []Signal
	for _, label := range labels {
		normalized := strings.ToLower(strings.TrimSpace(label))
		if _, ok := aiLabels[normalized]; !ok {
			continue
		}
		kind, ok := labelKindMap[normalized]
		if !ok {
			kind = KindAIAssisted
		}
		signals = append(signals, Signal{
			Source:     source,
			Kind:       kind,
			Confidence: 0.95,
			Evidence:   map[string]any{"label": label},
		})
	}
	return signals
}

// DetectFromAuthor ports detect_from_author. Returns nil for CI bots and for
// non-bot authors -- matching Python's None return, not a zero Signal.
func DetectFromAuthor(author AuthorInfo) *Signal {
	loginLower := strings.ToLower(strings.TrimSpace(author.Login))

	if _, ok := ciBots[loginLower]; ok {
		return nil
	}

	if _, ok := knownAIBots[loginLower]; ok {
		return &Signal{
			Source:     SourceBotAuthor,
			Kind:       KindAgentCreated,
			Confidence: 0.90,
			Actor:      strPtr(author.Login),
			Evidence: map[string]any{
				"login":        author.Login,
				"user_type":    author.UserType,
				"app_slug":     author.AppSlug,
				"known_ai_bot": true,
			},
		}
	}

	// Python: `author.user_type and author.user_type.lower() == "bot"` --
	// "" is falsy, so this branch is unreachable for an empty UserType,
	// exactly like the pointer-nil case would be.
	if author.UserType != "" && strings.ToLower(author.UserType) == "bot" &&
		strings.HasSuffix(loginLower, "[bot]") {
		return &Signal{
			Source:     SourceBotAuthor,
			Kind:       KindAgentCreated,
			Confidence: 0.55,
			Actor:      strPtr(author.Login),
			Evidence: map[string]any{
				"login":        author.Login,
				"user_type":    author.UserType,
				"app_slug":     author.AppSlug,
				"known_ai_bot": false,
			},
		}
	}

	return nil
}

// branchPattern is one (delimiter-framed substring, kind, actor) rule for
// DetectFromBranchName. Unlike the PR-body patterns below, these do NOT use
// \b -- the Python source frames each name with `(?:^|[-/])...(?:[-/]|$)`
// instead, which sidesteps the Unicode word-boundary divergence entirely
// (astra finding 4 is about \b/\s patterns specifically; these have neither).
type branchPattern struct {
	name  string // lowercase token, e.g. "copilot"
	kind  string
	actor string
}

// branchPatterns ports _AI_BRANCH_PATTERNS, in order (not that order matters
// here -- DetectFromBranchName returns the FIRST match, and Python iterates
// this same list in this same order via a for/return loop).
var branchPatterns = []branchPattern{
	{"copilot", KindAIAssisted, "copilot"},
	{"claude", KindAIAssisted, "claude"},
	{"cursor", KindAIAssisted, "cursor"},
	{"codex", KindAIAssisted, "codex"},
	{"windsurf", KindAIAssisted, "windsurf"},
	{"devin", KindAgentCreated, "devin"},
	{"agent", KindAgentCreated, "agent"},
	{"ai", KindAIAssisted, "ai"},
}

// DetectFromBranchName ports detect_from_branch_name (weak signal,
// confidence 0.35). Matches `(?:^|[-/])NAME(?:[-/]|$)`, case-insensitive.
func DetectFromBranchName(branch string) *Signal {
	lower := strings.ToLower(branch)
	for _, p := range branchPatterns {
		if branchNameMatches(lower, p.name) {
			actor := p.actor
			return &Signal{
				Source:     SourceBranchName,
				Kind:       p.kind,
				Confidence: 0.35,
				Actor:      &actor,
				Evidence: map[string]any{
					"branch":          branch,
					"matched_pattern": `(?:^|[-/])` + p.name + `(?:[-/]|$)`,
				},
			}
		}
	}
	return nil
}

// branchNameMatches checks `(?:^|[-/])name(?:[-/]|$)` against an
// already-lowercased haystack, scanning every occurrence of name (there is
// no Unicode-boundary ambiguity here: the delimiters are literal '-', '/',
// or start/end of string, not \b).
func branchNameMatches(lowerBranch, name string) bool {
	start := 0
	for {
		idx := strings.Index(lowerBranch[start:], name)
		if idx < 0 {
			return false
		}
		pos := start + idx
		end := pos + len(name)
		beforeOK := pos == 0 || lowerBranch[pos-1] == '-' || lowerBranch[pos-1] == '/'
		afterOK := end == len(lowerBranch) || lowerBranch[end] == '-' || lowerBranch[end] == '/'
		if beforeOK && afterOK {
			return true
		}
		start = pos + 1
		if start >= len(lowerBranch) {
			return false
		}
	}
}

// bodyPattern is one PR-body detection rule. matcher does the actual find;
// kind/actor (actor may be nil, meaning "no actor hint") describe the hit.
type bodyPattern struct {
	kind  string
	actor *string
	find  func(body string) (matchedText string, ok bool)
}

// bodyPatterns ports _PR_BODY_PATTERNS in EXACT order -- DetectFromPRBody
// returns the FIRST match, mirroring Python's for/return loop over this list.
var bodyPatterns = []bodyPattern{
	{
		kind: KindAIAssisted, actor: nil,
		find: unicodeWordBoundaryFind(
			`(?:generated|created|authored|written)\s+(?:by|with|using)\s+(?:copilot|claude|codex|cursor|windsurf|ai|an\s+ai)`,
		),
	},
	{kind: KindAIAssisted, actor: nil, find: unicodeWordBoundaryFind(`ai[\s\-]assisted`)},
	{kind: KindAgentCreated, actor: nil, find: unicodeWordBoundaryFind(`agent[\s\-]created`)},
	{kind: KindAIAssisted, actor: strPtr("copilot"), find: unicodeWordBoundaryFind(`copilot`)},
	{kind: KindAIAssisted, actor: strPtr("claude"), find: unicodeWordBoundaryFind(`claude`)},
	{kind: KindAIAssisted, actor: strPtr("codex"), find: unicodeWordBoundaryFind(`codex`)},
	{kind: KindAIAssisted, actor: strPtr("cursor"), find: unicodeWordBoundaryFind(`cursor`)},
}

// pythonBodyPatternSource records each entry's ORIGINAL Python regex source,
// for the evidence's matched_pattern field -- the fix is applied at match
// time (unicodeWordBoundaryFind), but the reported pattern string must still
// read as the pattern a human authored, matching Python's
// `pattern.pattern` attribute exactly.
var pythonBodyPatternSource = []string{
	`\b(?:generated|created|authored|written)\s+(?:by|with|using)\s+(?:copilot|claude|codex|cursor|windsurf|ai|an\s+ai)\b`,
	`\bai[\s\-]assisted\b`,
	`\bagent[\s\-]created\b`,
	`\bcopilot\b`,
	`\bclaude\b`,
	`\bcodex\b`,
	`\bcursor\b`,
}

// DetectFromPRBody ports detect_from_pr_body (weak signal, confidence 0.25).
// Returns the FIRST matching pattern in source order, exactly like Python's
// for-loop-with-return.
func DetectFromPRBody(body string) *Signal {
	if body == "" {
		return nil
	}
	for i, p := range bodyPatterns {
		matched, ok := p.find(body)
		if !ok {
			continue
		}
		return &Signal{
			Source:     SourcePRBody,
			Kind:       p.kind,
			Confidence: 0.25,
			Actor:      p.actor,
			Evidence: map[string]any{
				"matched_text":    matched,
				"matched_pattern": pythonBodyPatternSource[i],
			},
		}
	}
	return nil
}

// --------------------------------------------------------------------------
// Unicode-aware \b / \s matching (codex round chaos-4280 astra review,
// finding 4).
//
// Go's regexp (RE2) treats \b and \w as ASCII-only ([A-Za-z0-9_]) and \s as
// [\t\n\f\r ] -- Python's `re` module, with no re.ASCII flag (the default,
// and what _ai_detection.py uses), treats both as Unicode-aware: \w matches
// any Unicode letter/digit/underscore, \s matches the full Unicode
// White_Space property, and \b is a boundary defined in terms of that \w.
//
// MEASURED, not assumed (astra ran both engines against real inputs):
//   - Python's `\bai[\s\-]assisted\b` MATCHES "ai\xa0assisted" (U+00A0 NBSP
//     is Python \s); Go's ASCII-only \s would NOT match it.
//   - Python's `\bcopilot\b` does NOT match "écopiloté" (é is a
//     Python \w character, so no boundary forms on either side); Go's
//     ASCII-only \w would treat é as non-word, forming a boundary Go would
//     wrongly accept, over-matching.
//
// The fix: build the CORE pattern with Go's \s already replaced by an
// explicit Unicode White_Space class (RE2 supports \p{Z} plus a handful of
// non-Zs codepoints Unicode's White_Space property also includes but \p{Z}
// alone would miss -- U+0009-000D and U+0085 are control characters, not
// separators), find every candidate match with THAT pattern and no \b at
// all, then verify Unicode-aware word-boundary manually on each side using
// unicode.IsLetter/IsDigit plus '_' -- Python's \w set exactly. Return the
// FIRST candidate (leftmost start position) that passes both boundary
// checks, matching re.search's leftmost-match semantics.
var pythonUnicodeWhitespaceClass = `[\t-\r \x{0085}\x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}]`

func unicodeWordBoundaryFind(corePattern string) func(string) (string, bool) {
	// \s -> the explicit Unicode class; \- stays literal (already is).
	rewritten := strings.ReplaceAll(corePattern, `\s`, pythonUnicodeWhitespaceClass)
	re := regexp.MustCompile(`(?i)` + rewritten)
	return func(text string) (string, bool) {
		for _, loc := range re.FindAllStringIndex(text, -1) {
			start, end := loc[0], loc[1]
			if !isPythonWordBoundary(text, start) {
				continue
			}
			if !isPythonWordBoundary(text, end) {
				continue
			}
			return text[start:end], true
		}
		return "", false
	}
}

// isPythonWordBoundary reports whether byte offset pos in text is a \b
// position under Python's Unicode-aware definition: a transition between a
// \w rune and a non-\w rune (or a string edge adjacent to a \w rune).
func isPythonWordBoundary(text string, pos int) bool {
	before, hasBefore := runeBefore(text, pos)
	after, hasAfter := runeAfter(text, pos)
	beforeIsWord := hasBefore && isPythonWordRune(before)
	afterIsWord := hasAfter && isPythonWordRune(after)
	return beforeIsWord != afterIsWord
}

func isPythonWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func runeBefore(text string, pos int) (rune, bool) {
	if pos <= 0 {
		return 0, false
	}
	prefix := text[:pos]
	r := []rune(prefix)
	if len(r) == 0 {
		return 0, false
	}
	return r[len(r)-1], true
}

func runeAfter(text string, pos int) (rune, bool) {
	if pos >= len(text) {
		return 0, false
	}
	suffix := text[pos:]
	r := []rune(suffix)
	if len(r) == 0 {
		return 0, false
	}
	return r[0], true
}

func strPtr(s string) *string { return &s }
