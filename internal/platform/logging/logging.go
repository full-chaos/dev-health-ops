// Package logging provides the process-wide structured logging policy.
package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	redacted = "[REDACTED]"
	// redactionFailed replaces a value the redactor could not process (a
	// panic inside it, or inside the value's own Error/MarshalJSON). The raw
	// value is never the fallback.
	redactionFailed = "[REDACTION_FAILED]"
	// unloggable replaces a structured value that cannot be marshalled.
	unloggable = "[unloggable]"
	// maxLoggedValueBytes bounds one attribute value after redaction.
	maxLoggedValueBytes = 16 << 10
	truncatedSuffix     = "...[truncated]"
)

var (
	dsnPattern           = regexp.MustCompile(`(?i)\b(?:postgres(?:ql)?|clickhouse|redis|rediss|valkey|https?)://[^\s"'<>]+`)
	credentialURLPattern = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^/@\s"'<>]+:[^@\s"'<>]+@[^\s"'<>]+`)
	// userinfoPattern catches a credential written as `user:secret@` with or
	// without a scheme in front (a DSN or URL without its scheme, a Valkey/Redis
	// `:secret@host` with an empty user, a message that quotes the shape):
	// credentialURLPattern needs `scheme://user:secret@host` whole, so a bare
	// userinfo, or one whose scheme was cut off before it, reached the log
	// (CHAOS-8277). The user part may be empty and cannot hold a quote, a slash, a
	// bracket or `=`, so a JSON `"key":"value@x"` pair and a `key=value` pair never
	// match; the separator may be a colon or its percent-encoded form, and the `@` that
	// ends the userinfo may be `%40`, so the layers that do not decode see it too; the secret
	// runs to the LAST `@` of the run, so a `/`, a `%2F` or an `@` inside the
	// password is covered. A quote or `<`/`>` inside a password ends the run (named
	// limit: a JSON string or a tag around the text must stay intact).
	userinfoPattern = regexp.MustCompile(`[^\s/@:"'<>()\[\]{},;=\\]*(?::|%3[aA])[^\s"'<>]*(?:@|%40)`)
	// bareCredentialPatterns catch credential-shaped substrings OUTSIDE a
	// URL -- an HTTP Authorization header or a bearer/basic credential --
	// that dsnPattern/credentialURLPattern's URL-anchored matching cannot
	// see. A header-shaped match consumes its whole "<scheme> <credential>"
	// pair so no fragment of it survives. Every other protected key/value
	// pair is handled by redactKeyValues, which bounds the value exactly.
	bareCredentialPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(?:proxy-)?authorization\s*[:=]\s*(?:(?:bearer|basic|token|digest|negotiate|ntlm|apikey|sso-key)\s+)?[^\s"'\\,;&]+`),
		regexp.MustCompile(`(?i)\bbearer\s+[^\s"'\\,;&]+`),
		regexp.MustCompile(`(?i)\bbasic\s+[a-z0-9+/=]{8,}\b`),
	}
	// bareCredentialLiterals[i] is the lower-case literal
	// bareCredentialPatterns[i] cannot match without.
	bareCredentialLiterals = []string{"authorization", "bearer", "basic"}
)

// NewJSON returns a logger whose handler passes every attribute through one
// redactor: the message, every string, every error, every structured value
// (maps, structs, slices, types with their own JSON form) at any depth, and
// attributes inside groups. A protected key or group name hides its whole
// value; protected key/value pairs inside text are replaced in place.
func NewJSON(output io.Writer, level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: attrRedactor{}.replace,
	})
	return slog.New(handler)
}

// InstallDefault makes logger the process default, so slog.Default(), the
// package-level slog functions and the standard log package all write through
// it. The returned function restores the previous default and the standard
// log package's output and flags.
func InstallDefault(logger *slog.Logger) func() {
	previous := slog.Default()
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	slog.SetDefault(logger)
	// The secret registry reports (setting names only) through the same logger.
	previousWarner := secrets.SetWarner(func(msg string, args ...any) { logger.Warn(msg, args...) })
	return func() {
		secrets.SetWarner(previousWarner)
		slog.SetDefault(previous)
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	}
}

// RedactText removes supported DSNs, URLs containing userinfo, header and
// bearer credentials, provider tokens recognised by their prefix, and the
// value of every protected key in JSON, escaped-JSON, query-string (plain or
// percent-encoded), header, key=value and Go %v forms, a credential-shaped
// value after a credential word in prose, and protected path segments from
// free-form text before it can reach operator logs. A failure inside the
// redactor returns a fixed marker.
func RedactText(value string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	// Percent-encoded text is read (and logged) decoded, so an encoded key,
	// separator or quote is seen as what it stands for; the key/value scan
	// runs first, while it still knows which bytes were encoded.
	value = secrets.RedactRegistered(value)
	// A userinfo whose password holds a percent-encoded quote, bracket or space
	// (%22 %27 %3C %3E %20 %09) is one run of valid URL characters here; decoded, the
	// raw quote would end the match. So THOSE runs are matched before the decode. Every
	// other userinfo is matched after the key/value scan below, where main matches its
	// URL patterns: a pass before that scan would write its marker in front of a
	// protected key's value and leave the rest of the value readable.
	if mayHoldUserinfo(value) {
		value = redactUserinfoWhere(value, hasEncodedSpecial)
	}
	value, escaped := percentDecoded(value)
	// The decoded text can hold a registered secret its encoded form hid.
	value = secrets.RedactRegistered(value)
	value = redactKeyValues(value, escaped)
	// Each pattern runs only when the literal it cannot match without is in
	// the text: the skip never changes the result, it keeps the common
	// attribute (an id, a provider name) off the regexp engine.
	value = redactURLCredentials(value)
	lower := strings.ToLower(value)
	for index, pattern := range bareCredentialPatterns {
		if strings.Contains(lower, bareCredentialLiterals[index]) {
			value = pattern.ReplaceAllString(value, redacted)
			lower = strings.ToLower(value)
		}
	}
	if mayHoldProviderToken(value) {
		value = providerTokenPattern.ReplaceAllString(value, redacted)
	}
	if mayHoldVendorKey(value) {
		value = vendorKeyPattern.ReplaceAllString(value, redacted)
	}
	value = redactProseCredentials(value)
	return redactPathSegments(value)
}

// redactURLCredentials hides a credential carried in a URL or in a bare userinfo:
// the whole DSN and credential URLs first (so their host goes with them), then
// whatever `user:secret@` is left. The order matters: the bare-userinfo match would
// otherwise take `postgres:` as the user and leave the host of the DSN readable.
func redactURLCredentials(value string) string {
	if strings.Contains(value, "://") {
		value = dsnPattern.ReplaceAllString(value, redacted)
		value = credentialURLPattern.ReplaceAllString(value, redacted)
	}
	if mayHoldUserinfo(value) {
		value = redactUserinfo(value)
	}
	return value
}

// mayHoldUserinfo is the cheap gate in front of the userinfo match: it needs an `@`
// (or its percent-encoded form) to end on.
func mayHoldUserinfo(value string) bool {
	return strings.Contains(value, "@") || strings.Contains(value, "%40")
}

// redactUserinfo replaces the `user:secret` part of every `user:secret@` in value
// with the redaction marker and keeps the `@` (so the host after it stays readable).
func redactUserinfo(value string) string {
	return redactUserinfoWhere(value, nil)
}

// encodedSpecials are the percent-encoded characters that would end a userinfo
// match once decoded: a double and a single quote, the angle brackets, a space, a tab.
var encodedSpecials = []string{"%22", "%27", "%3c", "%3e", "%20", "%09"}

// hasEncodedSpecial reports whether a matched run holds one of encodedSpecials.
func hasEncodedSpecial(run string) bool {
	lower := strings.ToLower(run)
	for _, special := range encodedSpecials {
		if strings.Contains(lower, special) {
			return true
		}
	}
	return false
}

// redactUserinfoWhere is redactUserinfo for the matches keep accepts (nil = all).
// A run whose user part is itself a protected key (`Token:value@host`) is the
// key/value form of a credential, not a userinfo: the key stays and the whole
// run is hidden as the value, as the key/value scan hides it.
func redactUserinfoWhere(value string, keep func(run string) bool) string {
	matches := userinfoPattern.FindAllStringIndex(value, -1)
	if len(matches) == 0 {
		return value
	}
	var out strings.Builder
	written := 0
	for _, match := range matches {
		if match[0] < written {
			continue
		}
		run := value[match[0]:match[1]]
		if keep != nil && !keep(run) {
			continue
		}
		end := match[1]
		// `name:tag@sha256:<digest>` is an image reference, not a credential: the last
		// `@` of the run is the digest's. A credential before it (`user:secret@host/repo@sha256:...`)
		// is still redacted, up to the `@` before the digest; with no other `@` in the run
		// there is nothing to redact.
		if isDigestReference(value[end:]) {
			previous := strings.LastIndexByte(value[match[0]:end-1], '@')
			if previous < 0 {
				continue
			}
			end = match[0] + previous + 1
		}
		if separator := strings.IndexByte(run, ':'); separator > 0 && protectedKeyCached(run[:separator]) {
			stop := end
			for stop < len(value) && !isRunEnd(value[stop]) {
				stop++
			}
			out.WriteString(value[written:match[0]])
			out.WriteString(run[:separator+1] + redacted)
			written = stop
			continue
		}
		out.WriteString(value[written:match[0]])
		out.WriteString(redacted + "@")
		written = end
	}
	if written == 0 {
		return value
	}
	out.WriteString(value[written:])
	return out.String()
}

// isRunEnd reports whether b ends a whitespace-free run or a quoted/bracketed value.
func isRunEnd(b byte) bool {
	return strings.IndexByte(" \t\r\n\"'<>(),;[]{}", b) >= 0
}

// isDigestReference reports whether text, the text right after an `@`, starts an
// image digest (`sha256:`, `sha384:`, `sha512:`).
func isDigestReference(text string) bool {
	for _, algorithm := range []string{"sha256:", "sha384:", "sha512:"} {
		if strings.HasPrefix(text, algorithm) {
			return true
		}
	}
	return false
}

// keyVerdicts caches ProtectedKey for attribute keys and group names, which
// come from a small, fixed set in the code. It stops growing at
// maxCachedKeyVerdicts so a caller that builds keys from data cannot grow it
// without bound.
var (
	keyVerdicts      sync.Map
	keyVerdictsCount atomic.Int64
)

const maxCachedKeyVerdicts = 4096

func protectedKeyCached(key string) bool {
	if verdict, cached := keyVerdicts.Load(key); cached {
		return verdict.(bool)
	}
	verdict := ProtectedKey(key)
	if keyVerdictsCount.Load() < maxCachedKeyVerdicts {
		if _, loaded := keyVerdicts.LoadOrStore(key, verdict); !loaded {
			keyVerdictsCount.Add(1)
		}
	}
	return verdict
}

// attrRedactor is the handler's ReplaceAttr.
type attrRedactor struct{}

func (r attrRedactor) replace(groups []string, attr slog.Attr) (result slog.Attr) {
	defer func() {
		if recover() != nil {
			result = slog.String(attr.Key, redactionFailed)
		}
	}()
	if len(groups) == 0 && builtIn(attr) {
		return attr
	}
	if protectedKeyCached(attr.Key) {
		return slog.String(attr.Key, redacted)
	}
	for _, group := range groups {
		if protectedKeyCached(group) {
			return slog.String(attr.Key, redacted)
		}
	}
	value := attr.Value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(bound(RedactText(value.String())))
	case slog.KindAny:
		attr.Value = r.redactAny(value.Any())
	}
	return attr
}

// builtIn reports whether attr is the handler's own time, level or source
// attribute. A caller's attribute that reuses one of those keys carries a
// different Go type and is redacted like any other.
func builtIn(attr slog.Attr) bool {
	switch attr.Key {
	case slog.TimeKey:
		return attr.Value.Kind() == slog.KindTime
	case slog.LevelKey:
		_, isLevel := attr.Value.Any().(slog.Level)
		return attr.Value.Kind() == slog.KindAny && isLevel
	case slog.SourceKey:
		_, isSource := attr.Value.Any().(*slog.Source)
		return attr.Value.Kind() == slog.KindAny && isSource
	}
	return false
}

func (r attrRedactor) redactAny(value any) slog.Value {
	if err, ok := value.(error); ok {
		return slog.StringValue(bound(RedactText(err.Error())))
	}
	if raw, isBytes := value.([]byte); isBytes {
		// JSON would log bytes as base64, which no key or prefix check can
		// read. Bytes holding a JSON document take the structured path (its
		// keys decoded and classified); any other bytes are logged as
		// redacted text.
		if json.Valid(raw) {
			value = json.RawMessage(raw)
		} else {
			return slog.StringValue(bound(RedactText(string(raw))))
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return slog.StringValue(unloggable)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return slog.StringValue(unloggable)
	}
	cleaned, err := json.Marshal(redactJSONValue(generic))
	if err != nil {
		return slog.StringValue(unloggable)
	}
	return slog.AnyValue(json.RawMessage(boundJSON(cleaned)))
}

// redactJSONValue hides the value of every protected object key at any depth
// and redacts every string.
func redactJSONValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			return typed
		}
		for key, nested := range typed {
			if ProtectedKey(key) {
				typed[key] = redacted
				continue
			}
			typed[key] = redactJSONValue(nested)
		}
		return typed
	case []any:
		for index, nested := range typed {
			typed[index] = redactJSONValue(nested)
		}
		return typed
	case string:
		return RedactText(typed)
	default:
		return value
	}
}

// bound caps text at maxLoggedValueBytes on a rune boundary.
func bound(text string) string {
	if len(text) <= maxLoggedValueBytes {
		return text
	}
	cut := maxLoggedValueBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + truncatedSuffix
}

// boundJSON keeps an encoded value whole when it fits; a larger one is logged
// as a truncated JSON string so the line stays valid JSON.
func boundJSON(encoded []byte) []byte {
	if len(encoded) <= maxLoggedValueBytes {
		return encoded
	}
	truncated, err := json.Marshal(bound(string(encoded)))
	if err != nil {
		return []byte(`"` + unloggable + `"`)
	}
	return truncated
}
