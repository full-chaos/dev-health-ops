package pyjson

import "strings"

// HasLoneSurrogate reports whether text holds a surrogate code point in its WTF-8 form. DecodeString combines a
// valid pair into one code point, so a surrogate left in a decoded string is always a lone one.
func HasLoneSurrogate(text string) bool {
	for index := 0; index < len(text); index++ {
		if _, ok := surrogateAt(text, index); ok {
			return true
		}
	}
	return false
}

// ReplaceLoneSurrogates returns text with each lone surrogate replaced by U+FFFD, the value encoding/json gives the
// same escape, so a name stored by either plane is the same string. Text without one is returned as it is.
func ReplaceLoneSurrogates(text string) string {
	if !HasLoneSurrogate(text) {
		return text
	}
	var builder strings.Builder
	for index := 0; index < len(text); {
		if _, ok := surrogateAt(text, index); ok {
			builder.WriteString("�")
			index += 3
			continue
		}
		builder.WriteByte(text[index])
		index++
	}
	return builder.String()
}
