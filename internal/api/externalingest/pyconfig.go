package externalingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
)

// errCredentialConfigNotObject stands for the AttributeError Python raises
// reading `.get` on an integration or credential config that is truthy and not
// a dict.
var errCredentialConfigNotObject = errors.New("config not an object")

// pyConfig is an integration or credential `config` JSONB read the way Python
// reads it: `(config or {})`, then `.get(key)` on the fields it wants, each
// value looked at only when asked for and skipped when it is not the type the
// caller wants. It is a map of the raw JSON of each value and nothing decodes a
// value up front.
//
// This is the structural fix behind CHAOS-6748's second and third review
// findings: Go used to decode the whole config into typed Go values
// (map[string]any, float64 numbers) before reading any field, so ONE value Go
// could not represent (1e400, which Python's json parses to inf) failed the
// whole config and answered 500 for every host, where Python only ever reads
// the string-valued host keys and never looks at the rest. Because no value is
// decoded here, a value the reader does not want cannot fail the read.
//
// Named divergence (D2438): Python raises ValueError while loading a JSONB
// document whose integer literal has more than 4300 digits
// (sys.int_info.default_max_str_digits). That is an interpreter limit, not a
// decision; Go does not port it and reads such a config like any other.
type pyConfig map[string]json.RawMessage

// decodePyConfig is `config or {}` as a mapping: an absent, null or falsy config
// (an empty object, list or string, zero, false) is empty; a JSON object is
// itself; any other value is errCredentialConfigNotObject.
func decodePyConfig(raw []byte) (pyConfig, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] == '{' {
		object, err := topLevelMembers(trimmed)
		if err != nil {
			return nil, err
		}
		return pyConfig(object), nil
	}
	truthy, err := rawTruthy(trimmed)
	if err != nil {
		return nil, err
	}
	if !truthy {
		return nil, nil
	}
	return nil, errCredentialConfigNotObject
}

// truthy is Python's bool(config.get(key)): an absent key is None.
func (c pyConfig) truthy(key string) bool {
	raw, ok := c[key]
	if !ok {
		return false
	}
	truthy, err := rawTruthy(raw)
	return err == nil && truthy
}

// isTrue is `config.get(key) is True`: the JSON literal true, nothing truthy.
func (c pyConfig) isTrue(key string) bool {
	return string(bytes.TrimSpace(c[key])) == "true"
}

// str is config.get(key) when that value is a str, and ("", false) for an
// absent key or any other type: the caller skips it, as Python's
// isinstance(value, str) does.
func (c pyConfig) str(key string) (string, bool) {
	raw, ok := c[key]
	if !ok || len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", false
	}
	return text, true
}

// strOrNil is config.get(key) for a caller that wants a str or None: a str comes
// back as itself and every other value as nil (a candidate that is skipped).
func (c pyConfig) strOrNil(key string) any {
	if text, ok := c.str(key); ok {
		return text
	}
	return nil
}

// rawTruthy is Python's bool() of one JSON value, decided from its raw text.
// A number is truthy unless it is zero as Python's float() reads it: an
// overflow (1e400) is inf, truthy; an underflow (1e-400) is 0.0, falsy.
func rawTruthy(raw []byte) (bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false, errors.New("empty JSON value")
	}
	switch raw[0] {
	case 'n':
		return false, nil
	case 't':
		return true, nil
	case 'f':
		return false, nil
	case '"':
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return false, err
		}
		return text != "", nil
	case '[', '{':
		// Emptiness is the next token being the closer: no descent, so a
		// container nested deeper than encoding/json's limit reads like any other.
		return !bytes.HasPrefix(bytes.TrimSpace(raw[1:]), closerOf(raw[0])), nil
	default:
		// ParseFloat returns +-Inf with ErrRange on overflow and 0 on
		// underflow; the value alone decides, so the range error is ignored.
		number, err := strconv.ParseFloat(string(raw), 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return false, err
		}
		return number != 0, nil
	}
}

func closerOf(opener byte) []byte {
	if opener == '[' {
		return []byte("]")
	}
	return []byte("}")
}

// topLevelMembers splits a JSON object into the raw text of its values without
// looking inside them: an iterative scan that counts brackets and steps over
// strings. json.Unmarshal cannot be used: it validates the whole document and
// refuses nesting deeper than 10000, while Python's json.loads reads such a
// document and the reader only wants a few string keys (CHAOS-6748 r3). The
// input is a stored JSONB document, valid by construction, so the scan finds
// value boundaries and does not re-validate their content. A repeated key keeps
// its last value, as Python's dict does.
func topLevelMembers(raw []byte) (map[string]json.RawMessage, error) {
	members := map[string]json.RawMessage{}
	i := skipSpace(raw, 0)
	if i >= len(raw) || raw[i] != '{' {
		return nil, errors.New("JSON object expected")
	}
	i = skipSpace(raw, i+1)
	if i < len(raw) && raw[i] == '}' {
		return members, nil
	}
	for {
		if i >= len(raw) || raw[i] != '"' {
			return nil, errors.New("JSON object key expected")
		}
		keyEnd, err := scanString(raw, i)
		if err != nil {
			return nil, err
		}
		var key string
		if err := json.Unmarshal(raw[i:keyEnd], &key); err != nil {
			return nil, err
		}
		i = skipSpace(raw, keyEnd)
		if i >= len(raw) || raw[i] != ':' {
			return nil, errors.New("JSON object colon expected")
		}
		i = skipSpace(raw, i+1)
		valueEnd, err := scanValue(raw, i)
		if err != nil {
			return nil, err
		}
		members[key] = json.RawMessage(raw[i:valueEnd])
		i = skipSpace(raw, valueEnd)
		if i >= len(raw) {
			return nil, errors.New("JSON object not closed")
		}
		if raw[i] == '}' {
			return members, nil
		}
		if raw[i] != ',' {
			return nil, errors.New("JSON object comma expected")
		}
		i = skipSpace(raw, i+1)
	}
}

func skipSpace(raw []byte, i int) int {
	for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\n' || raw[i] == '\r') {
		i++
	}
	return i
}

// scanString returns the index just past the string starting at raw[start].
func scanString(raw []byte, start int) (int, error) {
	for i := start + 1; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			i++
		case '"':
			return i + 1, nil
		}
	}
	return 0, errors.New("JSON string not closed")
}

// scanValue returns the index just past the value starting at raw[start]. A
// container is skipped with a depth counter, never recursion.
func scanValue(raw []byte, start int) (int, error) {
	if start >= len(raw) {
		return 0, errors.New("JSON value expected")
	}
	switch raw[start] {
	case '"':
		return scanString(raw, start)
	case '{', '[':
		depth := 0
		for i := start; i < len(raw); i++ {
			switch raw[i] {
			case '"':
				end, err := scanString(raw, i)
				if err != nil {
					return 0, err
				}
				i = end - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1, nil
				}
			}
		}
		return 0, errors.New("JSON container not closed")
	default:
		i := start
		for i < len(raw) && raw[i] != ',' && raw[i] != '}' && raw[i] != ']' &&
			raw[i] != ' ' && raw[i] != '\t' && raw[i] != '\n' && raw[i] != '\r' {
			i++
		}
		return i, nil
	}
}
