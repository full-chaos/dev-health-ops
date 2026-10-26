package pyjson

import (
	"encoding/json"
	"strings"
)

// HasLoneSurrogateEscape reports whether a JSON document holds a string or a
// key with a \uD800-\uDFFF escape that is not half of a pair. It reads raw
// bytes because encoding/json turns such an escape into U+FFFD before a
// handler sees the string. A high escape is paired only when a low escape
// follows at once (json.loads in CPython); an escaped backslash is not an
// escape. A document that does not decode is outside the contract: scan a body
// the decoder has already accepted.
func HasLoneSurrogateEscape(data []byte) bool {
	inString := false
	for i := 0; i < len(data); {
		c := data[i]
		if !inString {
			inString = c == '"'
			i++
			continue
		}
		switch c {
		case '"':
			inString = false
			i++
		case '\\':
			if i+1 >= len(data) {
				return false
			}
			if data[i+1] != 'u' {
				i += 2
				continue
			}
			code, ok := hex4(data, i+2)
			if !ok {
				return false
			}
			i += 6
			switch {
			case code >= 0xdc00 && code <= 0xdfff:
				return true
			case code >= 0xd800 && code <= 0xdbff:
				if i+1 < len(data) && data[i] == '\\' && data[i+1] == 'u' {
					if low, ok := hex4(data, i+2); ok && low >= 0xdc00 && low <= 0xdfff {
						i += 6
						continue
					}
				}
				return true
			}
		default:
			i++
		}
	}
	return false
}

func hex4(data []byte, at int) (int, bool) {
	if at+4 > len(data) {
		return 0, false
	}
	value := 0
	for _, c := range data[at : at+4] {
		switch {
		case c >= '0' && c <= '9':
			value = value<<4 | int(c-'0')
		case c >= 'a' && c <= 'f':
			value = value<<4 | int(c-'a'+10)
		case c >= 'A' && c <= 'F':
			value = value<<4 | int(c-'A'+10)
		default:
			return 0, false
		}
	}
	return value, true
}

// MemberHasLoneSurrogateEscape applies HasLoneSurrogateEscape to the value of
// the top-level member name of a JSON object, and to nothing else: a member
// the handler never reads may keep a surrogate, as pydantic ignores an extra
// field. With foldCase every member whose key matches name case-insensitively
// is checked (encoding/json matches a struct tag that way). A document that
// is not an object, or does not decode, reports false.
func MemberHasLoneSurrogateEscape(data []byte, name string, foldCase bool) bool {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return false
	}
	for key, raw := range members {
		if (key == name || (foldCase && strings.EqualFold(key, name))) && HasLoneSurrogateEscape(raw) {
			return true
		}
	}
	return false
}
