package pyjson

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

// HasLoneSurrogateEscape reports whether a JSON document holds a string or a
// key with a lone surrogate: a \uD800-\uDFFF escape that is not half of a
// pair, or the three raw bytes of a surrogate (0xED 0xA0-0xBF 0x80-0xBF),
// which the api plane reads as one (DecodeBody). It reads raw bytes because
// encoding/json turns either form into U+FFFD before a handler sees the
// string. A high escape is paired only when a low escape follows at once
// (json.loads in CPython); raw halves never pair; an escaped backslash is not
// an escape. A document that does not decode is outside the contract: scan a
// body the decoder has already accepted.
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
		case 0xed:
			if i+2 < len(data) && data[i+1] >= 0xa0 && data[i+1] <= 0xbf && data[i+2] >= 0x80 && data[i+2] <= 0xbf {
				return true
			}
			i++
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

// MemberHasLoneSurrogateEscape applies HasLoneSurrogateEscape to the value
// encoding/json keeps for the top-level member name of a JSON object, and to
// nothing else: a member the handler never reads may keep a surrogate, as
// pydantic ignores an extra field. encoding/json takes the last member whose
// key matches (with foldCase: equal ignoring case, as a struct tag matches),
// and a later null replaces an earlier value in a pointer or an interface, so
// the last matching member decides and a null there means no value. Only a
// JSON string is scanned: an object, an array or a number is the type check's
// answer, and the strings nested in them are not this field. A document that
// is not exactly one object, or does not decode, reports false: the callers
// have a decoder that refuses data after the object, and its error is the
// answer, not this one.
func MemberHasLoneSurrogateEscape(data []byte, name string, foldCase bool) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return false
	}
	var kept json.RawMessage
	for decoder.More() {
		token, err := decoder.Token()
		key, isKey := token.(string)
		if err != nil || !isKey {
			return false
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return false
		}
		if key == name || (foldCase && strings.EqualFold(key, name)) {
			kept = value
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return false
	}
	return len(kept) > 0 && kept[0] == '"' && HasLoneSurrogateEscape(kept)
}
