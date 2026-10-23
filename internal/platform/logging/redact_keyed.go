package logging

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
