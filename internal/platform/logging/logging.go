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
	return func() {
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
	value, escaped := percentDecoded(value)
	value = redactKeyValues(value, escaped)
	// Each pattern runs only when the literal it cannot match without is in
	// the text: the skip never changes the result, it keeps the common
	// attribute (an id, a provider name) off the regexp engine.
	if strings.Contains(value, "://") {
		value = dsnPattern.ReplaceAllString(value, redacted)
		value = credentialURLPattern.ReplaceAllString(value, redacted)
	}
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
	value = redactProseCredentials(value)
	return redactPathSegments(value)
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
