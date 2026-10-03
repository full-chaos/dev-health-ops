// Package errortext is the one sanitizer for error text that is persisted or
// logged: credential-shaped substrings are replaced and the text is bounded. It
// is a leaf package (stdlib only) so that every layer, including the job runtime,
// can import it without a cycle (CHAOS-7943). It is the ONE implementation: the Python-parity
// matchers (matchers.go) answer every caller (CHAOS-7947).
package errortext

const redactionMarker = "[REDACTED]"

// defaultMaxErrorTextLength mirrors error_sanitize.py's DEFAULT_MAX_ERROR_TEXT_LENGTH.
const defaultMaxErrorTextLength = 4000

const truncationSuffix = "...[truncated]"

// Redact is error_sanitize.py's pattern pass: every credential-shaped substring is replaced by "[REDACTED]", in the Python
// file's own order, with no cap. Empty input is returned as is.
func Redact(text string) string {
	if text == "" {
		return text
	}
	runes := []rune(text)
	for _, match := range matchers {
		runes = substitute(pythonDialect, runes, match)
	}
	return string(runes)
}

// RedactRE2Reading is Redact with the former RE2 port's reading of every Unicode-sensitive token: `\s` and `\b` are ASCII only (a key
// name glued to a non-ASCII letter is a boundary there and not in Python) and the case fold is RE2's (only the Kelvin sign and the long s
// fold, not U+0130/U+0131). It is the former port's chain reproduced by the one engine; TestRE2ReadingIsTheFormerChain pins it.
func RedactRE2Reading(text string) string {
	if text == "" {
		return text
	}
	runes := []rune(text)
	for _, match := range matchers {
		runes = substitute(asciiDialect, runes, match)
	}
	return string(runes)
}

// Truncate caps text at maxLength code points with "...[truncated]" (a maxLength of 0 or less means no cap, as Python's
// max_length=None); by rune, so a multi-byte rune is never split.
func Truncate(text string, maxLength int) string {
	if maxLength <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxLength {
		return text
	}
	suffix := []rune(truncationSuffix)
	if maxLength > len(suffix) {
		return string(runes[:maxLength-len(suffix)]) + truncationSuffix
	}
	return string(runes[:maxLength])
}

// Cap is Truncate at Python's default of 4000. A caller that edits Sanitize's output (the userinfo pass) cuts again with it, so its
// result never exceeds the cap and a second call over it changes nothing.
func Cap(text string) string { return Truncate(text, defaultMaxErrorTextLength) }

// Sanitize is the former RE2 function of the same name, reproduced by the one engine: the patterns read as the former RE2 port read
// them (RedactRE2Reading, pinned byte-identical to the former pattern list), then the cap of 4000. It is the INNER chain of the
// sync writers' path; the Python-parity pass is appended after it (hardened.SyncWriters).
func Sanitize(text string) string {
	if text == "" {
		return text
	}
	return Cap(RedactRE2Reading(text))
}
