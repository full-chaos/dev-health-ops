package logging

import (
	"sync"
	"sync/atomic"
)

// RedactKeyedText is the redaction of one flat text attribute: a protected key hides the whole value, any other value
// goes through RedactText and is bounded. The log handler (attrRedactor.replace) and the span exporter's scrub both
// call it, so a rule exists once.
func RedactKeyedText(key, value string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	if protectedKeyCached(key) {
		return redacted
	}
	return bound(RedactText(value))
}

// KeyProtected reports whether key names a protected value (ProtectedKey, with the handler's per-key verdict cache).
func KeyProtected(key string) bool { return protectedKeyCached(key) }

// RedactedKeyName replaces an attribute key whose text the redactor would change (a credential or user data built into
// the key). Its value is hidden. Any number of such keys at one level collapse to one entry, so the set of keys that
// reach a log line or a span stays bounded.
const RedactedKeyName = "redacted.key"

var (
	cleanKeys     sync.Map
	cleanKeyCount atomic.Int64
)

const maxCleanKeys = 4096

// KeyClean reports whether the redactor leaves key text unchanged. A verdict is remembered per distinct clean key (bounded);
// a key that changed is never stored, so a stream of credential keys cannot grow the cache.
func KeyClean(key string) bool {
	if key == "" {
		return true
	}
	if _, ok := cleanKeys.Load(key); ok {
		return true
	}
	if RedactText(key) != key {
		return false
	}
	if cleanKeyCount.Add(1) > maxCleanKeys {
		cleanKeyCount.Add(-1)
	} else if _, loaded := cleanKeys.LoadOrStore(key, struct{}{}); loaded {
		cleanKeyCount.Add(-1)
	}
	return true
}
