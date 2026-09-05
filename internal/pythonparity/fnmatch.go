package pythonparity

import (
	"regexp"
	"strings"
	"sync"
)

// FnMatch reports whether name matches pattern using Python's
// “fnmatch.fnmatch“ semantics.
//
// # WHY NOT path.Match
//
// `_load_repos` (metrics/job_complexity_db.py) filters repository names with
// `fnmatch.fnmatch(repo_name or "", search_pattern)`, and `search_pattern`
// reaches the worker through ComplexityScope
// (api/internal/worker_metrics.py:1806). Go's path.Match is NOT a substitute,
// and the difference is not cosmetic on this input:
//
//   - path.Match's `*` does NOT cross a `/`. fnmatch's does. Repository names
//     here are routinely "org/repo", so `*health*` matches
//     "full-chaos/dev-health-ops" under fnmatch and does NOT under path.Match.
//     A port using path.Match would silently scan FEWER repositories and still
//     report success -- the same silent-undercount shape as a skipped language.
//   - Python negates a character class with `[!...]`; Go's regexp and
//     path.Match use `[^...]`. Under path.Match a `[!abc]` class is read as a
//     literal `!` plus a, b, c.
//   - path.Match returns an error for a malformed pattern; fnmatch never
//     errors. An unterminated `[` is a LITERAL bracket to Python.
//
// This is a port of `fnmatch.translate` followed by a full-string match, which
// is what fnmatch.fnmatchcase does. Case normalisation is deliberately NOT
// applied: `fnmatch.fnmatch` calls os.path.normcase on both arguments, which
// is the identity function on POSIX, and this code only ever runs on Linux
// workers. Applying a Windows-style fold here would make Go match names Python
// would reject.
func FnMatch(name, pattern string) bool {
	re, err := compileFnMatch(pattern)
	if err != nil {
		// translate() cannot produce an invalid expression for any input, so
		// this is unreachable in practice. Returning false rather than
		// panicking keeps a filter failure from taking down a partition, and
		// false is the conservative direction: it scans fewer repos rather
		// than silently widening the scope.
		return false
	}
	return re.MatchString(name)
}

var (
	fnMatchCacheMu sync.RWMutex
	fnMatchCache   = map[string]*regexp.Regexp{}
)

func compileFnMatch(pattern string) (*regexp.Regexp, error) {
	fnMatchCacheMu.RLock()
	cached, ok := fnMatchCache[pattern]
	fnMatchCacheMu.RUnlock()
	if ok {
		return cached, nil
	}

	re, err := regexp.Compile(TranslateFnMatch(pattern))
	if err != nil {
		return nil, err
	}

	fnMatchCacheMu.Lock()
	fnMatchCache[pattern] = re
	fnMatchCacheMu.Unlock()
	return re, nil
}

// TranslateFnMatch ports Python's “fnmatch.translate“: it converts a shell
// pattern into a regular expression matching the WHOLE string.
//
// CPython's translate() emits only a trailing `\Z`, because its output is
// always used with `re.match`, which supplies start anchoring itself. Go has
// no start-anchoring matcher, so the `\A` is added here rather than left to
// every caller to remember.
//
// Exported so the parity test can compare the produced expression against
// CPython's own, rather than only comparing match outcomes -- two different
// expressions can agree on a small sample and diverge on the input nobody
// thought to try.
func TranslateFnMatch(pattern string) string {
	var out strings.Builder
	// `\A` is load-bearing: Python applies translate()'s output with
	// `re.match`, which anchors at the START of the string, while Go's
	// MatchString searches ANYWHERE. Without it, the exclude glob `tests/**`
	// matches "contests/thing.py" -- the pattern finds "tests/thing.py" as a
	// suffix -- and every path whose name merely CONTAINS an excluded
	// directory name is silently dropped from the scan. Found by the
	// should_process oracle, not by the fnmatch cases, because a suffix-only
	// match needs a pattern anchored at neither end to expose it.
	out.WriteString(`\A(?s:`)

	runes := []rune(pattern)
	i := 0
	for i < len(runes) {
		c := runes[i]
		i++
		switch c {
		case '*':
			out.WriteString(".*")
		case '?':
			out.WriteString(".")
		case '[':
			j := i
			// Python: a ']' immediately after '[' or after '[!' is a LITERAL
			// member of the class, not the terminator.
			if j < len(runes) && runes[j] == '!' {
				j++
			}
			if j < len(runes) && runes[j] == ']' {
				j++
			}
			for j < len(runes) && runes[j] != ']' {
				j++
			}
			if j >= len(runes) {
				// Unterminated class: Python emits a LITERAL '['. path.Match
				// would report an error here instead.
				out.WriteString(`\[`)
				break
			}
			stuff := string(runes[i:j])
			// Python replaces an unescaped '\' inside the class so the regex
			// engine cannot reinterpret it.
			stuff = strings.ReplaceAll(stuff, `\`, `\\`)
			i = j + 1
			switch {
			case strings.HasPrefix(stuff, "!"):
				stuff = "^" + stuff[1:]
			case strings.HasPrefix(stuff, "^"):
				// A leading literal '^' must be escaped, or it would negate
				// the class -- the opposite of what the pattern asked for.
				stuff = `\` + stuff
			}
			out.WriteString("[" + stuff + "]")
		default:
			out.WriteString(regexp.QuoteMeta(string(c)))
		}
	}

	out.WriteString(`)\z`)
	return out.String()
}
