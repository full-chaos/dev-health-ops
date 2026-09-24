package pythonparity

import (
	"unicode"
)

// SanitizeErrorText is src/dev_health_ops/sync/error_sanitize.py's
// sanitize_error_text over a string (the exception-object branch has no Go
// counterpart): each credential-shaped pattern is replaced by "[REDACTED]" in
// the file's own order, then the text is capped at maxLength code points with
// "...[truncated]" (a maxLength of 0 or less means no cap, as Python's
// max_length=None). Empty input is returned as is.
//
// Why not regexp: the Python patterns are `re` patterns over `str`, and
// three of their tokens are Unicode-aware there while RE2 keeps them ASCII --
// `\s`/`\S` (Unicode whitespace), `\b` (a boundary between a Unicode word
// character and anything else), and IGNORECASE on ASCII letters (which also
// matches U+0130, U+0131, U+017F and U+212A). Each pattern is therefore a
// small matcher over runes that spells those three out; the corpus test in
// errorsanitize_live_python_oracle_test.go compares every one against `re`.
func SanitizeErrorText(text string, maxLength int) string {
	if text == "" {
		return text
	}
	runes := []rune(text)
	for _, matcher := range sanitizeMatchers {
		runes = substitute(runes, matcher)
	}
	if maxLength > 0 && len(runes) > maxLength {
		const suffix = "...[truncated]"
		if maxLength > len([]rune(suffix)) {
			runes = append(runes[:maxLength-len([]rune(suffix))], []rune(suffix)...)
		} else {
			runes = runes[:maxLength]
		}
	}
	return string(runes)
}

const redactionMarker = "[REDACTED]"

// substitute is pattern.sub(marker, text): non-overlapping leftmost matches.
func substitute(text []rune, match func([]rune, int) (int, bool)) []rune {
	var out []rune
	for i := 0; i < len(text); {
		if end, ok := match(text, i); ok && end > i {
			out = append(out, []rune(redactionMarker)...)
			i = end
			continue
		}
		out = append(out, text[i])
		i++
	}
	return out
}

// pySpace is str.isspace() for one code point, which is what `\s` matches in
// a str pattern.
func pySpace(r rune) bool {
	switch {
	case r >= 0x09 && r <= 0x0d, r >= 0x1c && r <= 0x20:
		return true
	case r == 0x85, r == 0xa0, r == 0x1680, r >= 0x2000 && r <= 0x200a,
		r == 0x2028, r == 0x2029, r == 0x202f, r == 0x205f, r == 0x3000:
		return true
	}
	return false
}

// pyWord is `\w` for a str pattern: a letter or number (str.isalnum) or "_".
func pyWord(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r) }

// boundaryAt is `\b` at index i: exactly one side of i is a word character.
func boundaryAt(text []rune, i int) bool {
	before := i > 0 && pyWord(text[i-1])
	after := i < len(text) && pyWord(text[i])
	return before != after
}

// foldEq is an ASCII pattern character under IGNORECASE: the letter in either
// case, plus the four non-ASCII characters `re` folds onto it.
func foldEq(r rune, c byte) bool {
	if r == rune(c) {
		return true
	}
	if c >= 'a' && c <= 'z' {
		if r == rune(c-32) {
			return true
		}
		switch c {
		case 'i':
			return r == 0x130 || r == 0x131
		case 's':
			return r == 0x17f
		case 'k':
			return r == 0x212a
		}
	} else if c >= 'A' && c <= 'Z' {
		return foldEq(r, c+32)
	}
	return false
}

// literalAt matches an ASCII literal, case-insensitively, at i.
func literalAt(text []rune, i int, literal string) (int, bool) {
	if i+len(literal) > len(text) {
		return 0, false
	}
	for k := 0; k < len(literal); k++ {
		if !foldEq(text[i+k], literal[k]) {
			return 0, false
		}
	}
	return i + len(literal), true
}

// classRun consumes the longest run (at least min) of runes for which in
// reports true, returning the end of the run and every valid end index down
// to min (for a backtracking `{min,}` followed by more pattern).
func classRun(text []rune, i, min int, in func(rune) bool) (end int, ok bool) {
	j := i
	for j < len(text) && in(text[j]) {
		j++
	}
	return j, j-i >= min
}

func alnumFold(r rune) bool {
	if r >= '0' && r <= '9' {
		return true
	}
	if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
		return true
	}
	return r == 0x130 || r == 0x131 || r == 0x17f || r == 0x212a
}

func classWith(extra string) func(rune) bool {
	return func(r rune) bool {
		if alnumFold(r) {
			return true
		}
		for _, c := range extra {
			if r == c {
				return true
			}
		}
		return false
	}
}

func skipSpace(text []rune, i int) int {
	for i < len(text) && pySpace(text[i]) {
		i++
	}
	return i
}

func skipNonSpace(text []rune, i int) int {
	for i < len(text) && !pySpace(text[i]) {
		i++
	}
	return i
}

// tokenPattern matches `\b<literal>` then `\s*[:=]\s*\S+`.
func headerValueMatcher(literals []string, trailingToken bool) func([]rune, int) (int, bool) {
	return func(text []rune, i int) (int, bool) {
		if !boundaryAt(text, i) {
			return 0, false
		}
		for _, literal := range literals {
			j, ok := literalAt(text, i, literal)
			if !ok {
				continue
			}
			j = skipSpace(text, j)
			if j >= len(text) || (text[j] != ':' && text[j] != '=') {
				continue
			}
			j = skipSpace(text, j+1)
			end := skipNonSpace(text, j)
			if end == j {
				continue
			}
			if trailingToken {
				// (?:\s+\S+)?
				k := skipSpace(text, end)
				if k > end {
					if k2 := skipNonSpace(text, k); k2 > k {
						end = k2
					}
				}
			}
			return end, true
		}
		return 0, false
	}
}

// bearerMatcher is `\bbearer\s+\S+`.
func bearerMatcher(text []rune, i int) (int, bool) {
	if !boundaryAt(text, i) {
		return 0, false
	}
	j, ok := literalAt(text, i, "bearer")
	if !ok {
		return 0, false
	}
	k := skipSpace(text, j)
	if k == j {
		return 0, false
	}
	end := skipNonSpace(text, k)
	return end, end > k
}

// basicMatcher is `\bbasic\s+[a-z0-9+/=]{8,}\b`, with the backtracking a
// class holding non-word characters allows: the longest run that ends on a
// word boundary wins.
func basicMatcher(text []rune, i int) (int, bool) {
	if !boundaryAt(text, i) {
		return 0, false
	}
	j, ok := literalAt(text, i, "basic")
	if !ok {
		return 0, false
	}
	k := skipSpace(text, j)
	if k == j {
		return 0, false
	}
	runEnd, _ := classRun(text, k, 0, classWith("+/="))
	for end := runEnd; end-k >= 8; end-- {
		if boundaryAt(text, end) {
			return end, true
		}
	}
	return 0, false
}

// prefixedTokenMatcher is `\b<prefix>[class]{min,}\b`.
func prefixedTokenMatcher(prefix string, extra string, min int) func([]rune, int) (int, bool) {
	in := classWith(extra)
	return func(text []rune, i int) (int, bool) {
		if !boundaryAt(text, i) {
			return 0, false
		}
		j, ok := literalAt(text, i, prefix)
		if !ok {
			return 0, false
		}
		runEnd, _ := classRun(text, j, 0, in)
		for end := runEnd; end-j >= min; end-- {
			if boundaryAt(text, end) {
				return end, true
			}
		}
		return 0, false
	}
}

// xoxMatcher is `\bxox[baprs]-[a-z0-9-]{10,}\b`.
func xoxMatcher(text []rune, i int) (int, bool) {
	if !boundaryAt(text, i) {
		return 0, false
	}
	j, ok := literalAt(text, i, "xox")
	if !ok || j >= len(text) {
		return 0, false
	}
	kind := text[j]
	if !(foldEq(kind, 'b') || foldEq(kind, 'a') || foldEq(kind, 'p') || foldEq(kind, 'r') || foldEq(kind, 's')) {
		return 0, false
	}
	j++
	if j >= len(text) || text[j] != '-' {
		return 0, false
	}
	j++
	runEnd, _ := classRun(text, j, 0, classWith("-"))
	for end := runEnd; end-j >= 10; end-- {
		if boundaryAt(text, end) {
			return end, true
		}
	}
	return 0, false
}

// urlUserinfoMatcher is `\b[a-z][a-z0-9+.-]*://[^\s/@]+@`: a scheme, "://",
// a userinfo with no space, slash or at-sign, then the at-sign.
func urlUserinfoMatcher(text []rune, i int) (int, bool) {
	if !boundaryAt(text, i) || i >= len(text) {
		return 0, false
	}
	if r := text[i]; !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == 0x130 || r == 0x131 || r == 0x17f || r == 0x212a) {
		return 0, false
	}
	schemeIn := classWith("+.-")
	runEnd, _ := classRun(text, i+1, 0, schemeIn)
	// [a-z0-9+.-]* is greedy and backtracks: try each end from the longest.
	for end := runEnd; end >= i+1; end-- {
		if end+3 > len(text) || text[end] != ':' || text[end+1] != '/' || text[end+2] != '/' {
			continue
		}
		j := end + 3
		k := j
		for k < len(text) && !pySpace(text[k]) && text[k] != '/' && text[k] != '@' {
			k++
		}
		if k > j && k < len(text) && text[k] == '@' {
			return k + 1, true
		}
	}
	return 0, false
}

var sanitizeMatchers = []func([]rune, int) (int, bool){
	headerValueMatcher([]string{"authorization", "proxy-authorization"}, true),
	bearerMatcher,
	basicMatcher,
	prefixedTokenMatcher("ghp_", "", 20),
	prefixedTokenMatcher("gho_", "", 20),
	prefixedTokenMatcher("ghu_", "", 20),
	prefixedTokenMatcher("ghs_", "", 20),
	prefixedTokenMatcher("ghr_", "", 20),
	prefixedTokenMatcher("github_pat_", "_", 20),
	prefixedTokenMatcher("glpat-", "_-", 20),
	xoxMatcher,
	headerValueMatcher([]string{"private_token", "access_token", "api_key", "apikey", "client_secret", "secret", "token"}, false),
	urlUserinfoMatcher,
}
