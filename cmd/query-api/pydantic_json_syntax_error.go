// This file classifies a malformed JSON request body the way jiter
// (pydantic-core's Rust JSON parser -- the parser FastAPI actually runs
// a request body through, not Python's stdlib json module) reports it:
// a (message, character position) pair. Ported from CPython's own
// json.decoder/json.scanner algorithm (Lib/json/decoder.py, Lib/json/
// scanner.py -- the same grammar every JSON parser implements) with ONE
// deliberate departure jiter's own behaviour required: a trailing comma
// before a closing `}` or `]` is detected as its OWN error class
// ("Illegal trailing comma before end of object"/"...array", at the
// COMMA's own position), where CPython's algorithm has no such special
// case and instead falls through to a later, differently-worded error
// at a different position. Every message and position below was checked
// against a live FastAPI TestClient run (see this repo's PR history for
// the capture script) for: an invalid start token, a missing property
// quote, a missing ':'/',' delimiter, an unterminated string, trailing
// data after a complete value, whitespace-only input, an invalid
// keyword, a leading-zero number, and both trailing-comma shapes -- the
// realistic malformed-body shapes a caller sends. Values, string
// escapes and numbers are walked using the same grammar bounds as
// CPython's algorithm; this file's own job ends at finding the FIRST
// error, matching a JSON parser's natural fail-fast behaviour (jiter
// does not aggregate multiple syntax errors the way pydantic's own
// per-field validation aggregates multiple FIELD errors).
package main

// pyJSONError is one (message, position) pair -- position is a rune
// (Unicode codepoint) index into the decoded body, matching Python's own
// string-indexing semantics (json_invalid's "loc" entry is this index).
type pyJSONError struct {
	Msg string
	Pos int
}

// classifyJSONSyntaxError re-scans a body that already failed
// encoding/json's own Unmarshal to find the message/position jiter would
// report for it. Callers must only invoke this after confirming the body
// does NOT decode -- it assumes a syntax error exists and always returns
// one (falling back to "Expecting value" at the body's own length if the
// scan somehow reaches the end without finding the problem, which should
// not happen for a body Go's own decoder already rejected).
func classifyJSONSyntaxError(body []byte) pyJSONError {
	s := []rune(string(body))
	idx := skipJSONWhitespace(s, 0)
	end, err := scanJSONValue(s, idx)
	if err != nil {
		return *err
	}
	end = skipJSONWhitespace(s, end)
	if end != len(s) {
		return pyJSONError{Msg: "Extra data", Pos: end}
	}
	return pyJSONError{Msg: "Expecting value", Pos: len(s)}
}

func skipJSONWhitespace(s []rune, idx int) int {
	for idx < len(s) {
		switch s[idx] {
		case ' ', '\t', '\n', '\r':
			idx++
		default:
			return idx
		}
	}
	return idx
}

func jsonRunesHavePrefixAt(s []rune, idx int, lit string) bool {
	litRunes := []rune(lit)
	if idx+len(litRunes) > len(s) {
		return false
	}
	for i, r := range litRunes {
		if s[idx+i] != r {
			return false
		}
	}
	return true
}

// scanJSONValue mirrors scanner.py's _scan_once: dispatch on the first
// character, recursing into scanJSONObject/scanJSONArray/scanJSONString
// for the composite/string cases, matching a literal keyword or number
// inline otherwise.
func scanJSONValue(s []rune, idx int) (int, *pyJSONError) {
	if idx >= len(s) {
		return idx, &pyJSONError{Msg: "Expecting value", Pos: idx}
	}
	switch s[idx] {
	case '"':
		return scanJSONString(s, idx+1)
	case '{':
		return scanJSONObject(s, idx+1)
	case '[':
		return scanJSONArray(s, idx+1)
	case 'n':
		if jsonRunesHavePrefixAt(s, idx, "null") {
			return idx + 4, nil
		}
	case 't':
		if jsonRunesHavePrefixAt(s, idx, "true") {
			return idx + 4, nil
		}
	case 'f':
		if jsonRunesHavePrefixAt(s, idx, "false") {
			return idx + 5, nil
		}
	}
	if end := matchJSONNumber(s, idx); end > idx {
		return end, nil
	}
	return idx, &pyJSONError{Msg: "Expecting value", Pos: idx}
}

// matchJSONNumber matches scanner.py's NUMBER_RE
// (-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)?) at idx, returning idx
// (no match) if the position is not a number start.
func matchJSONNumber(s []rune, idx int) int {
	i := idx
	if i < len(s) && s[i] == '-' {
		i++
	}
	digitsStart := i
	if i >= len(s) || s[i] < '0' || s[i] > '9' {
		return idx
	}
	if s[i] == '0' {
		i++
	} else {
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	_ = digitsStart
	if i < len(s) && s[i] == '.' {
		j := i + 1
		k := j
		for k < len(s) && s[k] >= '0' && s[k] <= '9' {
			k++
		}
		if k > j {
			i = k
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		k := j
		for k < len(s) && s[k] >= '0' && s[k] <= '9' {
			k++
		}
		if k > j {
			i = k
		}
	}
	return i
}

// scanJSONObject mirrors JSONObject (decoder.py), plus the trailing-
// comma special case this file's own doc comment describes: jiter
// reports a comma immediately followed (after whitespace) by `}` as its
// own error, at the COMMA's position, instead of continuing to CPython's
// "Expecting property name" error one token later.
func scanJSONObject(s []rune, idx int) (int, *pyJSONError) {
	end := idx
	nextchar := runeAt(s, end)
	if nextchar != '"' {
		if isJSONWhitespace(nextchar) {
			end = skipJSONWhitespace(s, end)
			nextchar = runeAt(s, end)
		}
		if nextchar == '}' {
			return end + 1, nil
		}
		if nextchar != '"' {
			return end, &pyJSONError{Msg: "Expecting property name enclosed in double quotes", Pos: end}
		}
	}
	end++
	for {
		keyEnd, err := scanJSONString(s, end)
		if err != nil {
			return keyEnd, err
		}
		end = keyEnd

		if runeAt(s, end) != ':' {
			end = skipJSONWhitespace(s, end)
			if runeAt(s, end) != ':' {
				return end, &pyJSONError{Msg: "Expecting ':' delimiter", Pos: end}
			}
		}
		end++
		end = skipJSONWhitespace(s, end)

		valueEnd, err := scanJSONValue(s, end)
		if err != nil {
			return valueEnd, err
		}
		end = valueEnd

		nextchar = runeAt(s, end)
		if isJSONWhitespace(nextchar) {
			end = skipJSONWhitespace(s, end+1)
			nextchar = runeAt(s, end)
		}
		commaPos := end
		end++

		if nextchar == '}' {
			return end, nil
		}
		if nextchar != ',' {
			return end - 1, &pyJSONError{Msg: "Expecting ',' delimiter", Pos: end - 1}
		}
		end = skipJSONWhitespace(s, end)
		nextchar = runeAt(s, end)
		if nextchar == '}' {
			return end, &pyJSONError{Msg: "Illegal trailing comma before end of object", Pos: commaPos}
		}
		end++
		if nextchar != '"' {
			return end - 1, &pyJSONError{Msg: "Expecting property name enclosed in double quotes", Pos: end - 1}
		}
	}
}

// scanJSONArray mirrors JSONArray (decoder.py), plus the same
// trailing-comma special case scanJSONObject documents.
func scanJSONArray(s []rune, idx int) (int, *pyJSONError) {
	end := idx
	nextchar := runeAt(s, end)
	if isJSONWhitespace(nextchar) {
		end = skipJSONWhitespace(s, end+1)
		nextchar = runeAt(s, end)
	}
	if nextchar == ']' {
		return end + 1, nil
	}
	for {
		valueEnd, err := scanJSONValue(s, end)
		if err != nil {
			return valueEnd, err
		}
		end = valueEnd

		nextchar = runeAt(s, end)
		if isJSONWhitespace(nextchar) {
			end = skipJSONWhitespace(s, end+1)
			nextchar = runeAt(s, end)
		}
		commaPos := end
		end++

		if nextchar == ']' {
			return end, nil
		}
		if nextchar != ',' {
			return end - 1, &pyJSONError{Msg: "Expecting ',' delimiter", Pos: end - 1}
		}
		end = skipJSONWhitespace(s, end)
		if runeAt(s, end) == ']' {
			return end, &pyJSONError{Msg: "Illegal trailing comma before end of array", Pos: commaPos}
		}
	}
}

// scanJSONString mirrors py_scanstring's error paths (decoder.py) --
// idx is the index just AFTER the opening quote. Only the error paths
// matter to this file (a valid string's decoded VALUE is never used,
// only where it ends), so escape sequences are walked structurally
// without unescaping.
func scanJSONString(s []rune, idx int) (int, *pyJSONError) {
	begin := idx - 1
	i := idx
	for {
		for i < len(s) {
			c := s[i]
			if c == '"' || c == '\\' || c < 0x20 {
				break
			}
			i++
		}
		if i >= len(s) {
			return i, &pyJSONError{Msg: "Unterminated string starting at", Pos: begin}
		}
		c := s[i]
		if c == '"' {
			return i + 1, nil
		}
		if c != '\\' {
			return i, &pyJSONError{Msg: "Invalid control character at", Pos: i}
		}
		i++
		if i >= len(s) {
			return i, &pyJSONError{Msg: "Unterminated string starting at", Pos: begin}
		}
		switch s[i] {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			i++
		case 'u':
			if !hasJSONHex4(s, i+1) {
				return i + 1, &pyJSONError{Msg: "Invalid \\uXXXX escape", Pos: i + 1}
			}
			i += 5
		default:
			return i, &pyJSONError{Msg: "Invalid \\escape", Pos: i}
		}
	}
}

func hasJSONHex4(s []rune, idx int) bool {
	if idx+4 > len(s) {
		return false
	}
	for i := idx; i < idx+4; i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func isJSONWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

// runeAt returns s[idx], or the sentinel rune -1 past the end -- every
// comparison above treats -1 as "not this character", matching Python's
// own out-of-range slice reads (s[end:end+1] == the empty string for every comparison).
func runeAt(s []rune, idx int) rune {
	if idx < 0 || idx >= len(s) {
		return -1
	}
	return s[idx]
}

// jsonSyntaxErrorDetail builds the full json_invalid detail for a body
// that failed to decode, at loc (e.g. []any{"body"}).
func jsonSyntaxErrorDetail(loc []any, body []byte) pydanticErrorDetail {
	classified := classifyJSONSyntaxError(body)
	fullLoc := make([]any, len(loc)+1)
	copy(fullLoc, loc)
	fullLoc[len(loc)] = classified.Pos
	return pydanticErrorDetail{
		Type:  "json_invalid",
		Loc:   fullLoc,
		Msg:   "JSON decode error",
		Input: map[string]any{},
		Ctx:   map[string]string{"error": classified.Msg},
	}
}
