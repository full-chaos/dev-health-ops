// Package errortext is the one sanitizer for error text that is persisted or
// logged: credential-shaped substrings are replaced and the text is bounded. It
// is a leaf package (stdlib only) so that every layer, including the job runtime,
// can import it without a cycle (CHAOS-7943). It is the ONE implementation: the Python-parity
// matchers (matchers.go) answer every caller (CHAOS-7947). It imports the logging package for the credential-shape pass (stdlib
// plus the secrets leaf below it; no import cycle with the job runtime).
package errortext

import "github.com/full-chaos/dev-health-ops/internal/platform/logging"

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

// SanitizeHardened is error_sanitize.py's pattern pass (Python-parity matchers) followed by the credential shapes the Python
// list never had (LLM-provider, Stripe, Google, Slack and JWT keys by prefix, and a long value behind a credential word,
// CHAOS-7937), then the cap, so a cap can never cut a key to a fragment below a shape's minimum length. maxLength <= 0 = no
// cap. It is what pythonparity.SanitizeErrorTextHardened always was; it differs from Sanitize only on text holding such a shape.
func SanitizeHardened(text string, maxLength int) string {
	if text == "" {
		return text
	}
	return Truncate(logging.RedactCredentialShapes(Redact(text)), maxLength)
}

// SanitizeHardenedShapesFirst is the composition of the sync writers' path (CHAOS-7947): the credential shapes FIRST (as that
// path always did), then the patterns with the former RE2 port's ASCII `\s`/`\b`, then the Python-parity patterns, then the
// cap. One engine, two readings: the ASCII pass keeps everything the former RE2 port hid (Python's
// Unicode `\s` ends a token at a no-break space, RE2's did not; Python sees no boundary between a non-ASCII letter and a key
// name, RE2 did), the Python pass adds what RE2 missed (a secret behind a no-break space). Every pass only hides more, and the
// order is the former one: a pass is only ever ADDED after the passes main ran (D4495: a redaction change is monotone). The
// frozen former port is the reference of TestSyncEntryPointNeverHidesLessThanTheFormerPort.
func SanitizeHardenedShapesFirst(text string, maxLength int) string {
	if text == "" {
		return text
	}
	shapes := logging.RedactCredentialShapes
	return Truncate(Redact(RedactRE2Reading(shapes(text))), maxLength)
}

// SanitizeHardenedShapesFirstDefault is SanitizeHardenedShapesFirst with Python's default cap of 4000.
func SanitizeHardenedShapesFirstDefault(text string) string {
	return SanitizeHardenedShapesFirst(text, defaultMaxErrorTextLength)
}
