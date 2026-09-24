package pythonparity

import (
	"bytes"
	"strconv"
	"unicode/utf8"
)

// JSONDecodeStatus says how Python's json.loads treats a response body.
type JSONDecodeStatus int

const (
	// JSONDecodeParses: json.loads accepts the body.
	JSONDecodeParses JSONDecodeStatus = iota
	// JSONDecodeError: json.loads raises JSONDecodeError; the message is set.
	JSONDecodeError
	// JSONDecodeUnmodelled: the body is not UTF-8 text (another encoding, or
	// bytes UTF-8 cannot decode), where Python raises a different exception
	// (UnicodeDecodeError) that this port does not model.
	JSONDecodeUnmodelled
)

// PythonJSONDecode reports what `json.loads(body)` does with a bytes body --
// the call httpx's Response.json makes -- and, for a JSONDecodeError, its
// exact message ("<msg>: line L column C (char N)"). It is a port of
// CPython's C scanner (Modules/_json.c), whose messages and positions
// (counted in code points, not bytes) differ from any Go decoder's.
//
// The body's encoding is detected as json.detect_encoding does; only UTF-8
// (with or without a BOM) is modelled, everything else is JSONDecodeUnmodelled.
func PythonJSONDecode(body []byte) (string, JSONDecodeStatus) {
	if len(body) >= 2 && (body[0] == 0 || body[1] == 0) {
		return "", JSONDecodeUnmodelled
	}
	if bytes.HasPrefix(body, []byte{0xff, 0xfe}) || bytes.HasPrefix(body, []byte{0xfe, 0xff}) {
		return "", JSONDecodeUnmodelled
	}
	body = bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf})
	if !utf8.Valid(body) {
		return "", JSONDecodeUnmodelled
	}
	scanner := jsonScanner{text: []rune(string(body))}
	if failure := scanner.decode(); failure != nil {
		return scanner.message(failure), JSONDecodeError
	}
	return "", JSONDecodeParses
}

type jsonFailure struct {
	message  string
	position int
}

type jsonScanner struct {
	text []rune
}

func (s *jsonScanner) message(f *jsonFailure) string {
	line, column := 1, f.position+1
	lastNewline := -1
	for index := 0; index < f.position && index < len(s.text); index++ {
		if s.text[index] == '\n' {
			line++
			lastNewline = index
		}
	}
	if lastNewline >= 0 {
		column = f.position - lastNewline
	}
	return f.message + ": line " + strconv.Itoa(line) + " column " + strconv.Itoa(column) + " (char " + strconv.Itoa(f.position) + ")"
}

func jsonSpace(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }

func (s *jsonScanner) skipSpace(index int) int {
	for index < len(s.text) && jsonSpace(s.text[index]) {
		index++
	}
	return index
}

// decode is JSONDecoder.decode: WHITESPACE.match, raw_decode, then the
// trailing-whitespace and "Extra data" check.
func (s *jsonScanner) decode() *jsonFailure {
	index := s.skipSpace(0)
	end, failure := s.scanOnce(index)
	if failure != nil {
		return failure
	}
	if end = s.skipSpace(end); end != len(s.text) {
		return &jsonFailure{"Extra data", end}
	}
	return nil
}

func (s *jsonScanner) hasAt(index int, literal string) bool {
	runes := []rune(literal)
	if index+len(runes) > len(s.text) {
		return false
	}
	for offset, r := range runes {
		if s.text[index+offset] != r {
			return false
		}
	}
	return true
}

// scanOnce is scan_once_unicode: it returns the index after the value, or the
// failure. A value that cannot start is StopIteration in C, which raw_decode
// turns into "Expecting value" at that index.
func (s *jsonScanner) scanOnce(index int) (int, *jsonFailure) {
	if index >= len(s.text) {
		return 0, &jsonFailure{"Expecting value", index}
	}
	switch s.text[index] {
	case '"':
		return s.scanString(index + 1)
	case '{':
		return s.scanObject(index + 1)
	case '[':
		return s.scanArray(index + 1)
	case 'n':
		if s.hasAt(index, "null") {
			return index + 4, nil
		}
	case 't':
		if s.hasAt(index, "true") {
			return index + 4, nil
		}
	case 'f':
		if s.hasAt(index, "false") {
			return index + 5, nil
		}
	case 'N':
		if s.hasAt(index, "NaN") {
			return index + 3, nil
		}
	case 'I':
		if s.hasAt(index, "Infinity") {
			return index + 8, nil
		}
	case '-':
		if s.hasAt(index, "-Infinity") {
			return index + 9, nil
		}
	}
	return s.scanNumber(index)
}

func digit(r rune) bool { return r >= '0' && r <= '9' }

// scanNumber is _match_number_unicode.
func (s *jsonScanner) scanNumber(start int) (int, *jsonFailure) {
	stop := &jsonFailure{"Expecting value", start}
	last := len(s.text) - 1
	index := start
	if s.text[index] == '-' {
		index++
		if index > last {
			return 0, stop
		}
	}
	switch {
	case s.text[index] >= '1' && s.text[index] <= '9':
		index++
		for index <= last && digit(s.text[index]) {
			index++
		}
	case s.text[index] == '0':
		index++
	default:
		return 0, stop
	}
	if index < last && s.text[index] == '.' && digit(s.text[index+1]) {
		index++
		for index <= last && digit(s.text[index]) {
			index++
		}
	}
	if index < last && (s.text[index] == 'e' || s.text[index] == 'E') {
		exponentStart := index
		index++
		if index < last && (s.text[index] == '-' || s.text[index] == '+') {
			index++
		}
		for index <= last && digit(s.text[index]) {
			index++
		}
		if !digit(s.text[index-1]) {
			index = exponentStart
		}
	}
	return index, nil
}

func hexDigit(r rune) bool {
	return digit(r) || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

// scanString is scanstring_unicode; end is the index after the opening quote.
func (s *jsonScanner) scanString(end int) (int, *jsonFailure) {
	begin := end - 1
	length := len(s.text)
	for {
		next := end
		var c rune
		for ; next < length; next++ {
			c = s.text[next]
			if c == '"' || c == '\\' {
				break
			}
			if c <= 0x1f {
				return 0, &jsonFailure{"Invalid control character at", next}
			}
		}
		if next >= length || !(c == '"' || c == '\\') {
			return 0, &jsonFailure{"Unterminated string starting at", begin}
		}
		end = next + 1
		if c == '"' {
			return end, nil
		}
		if next+1 >= length {
			return 0, &jsonFailure{"Unterminated string starting at", begin}
		}
		next++
		c = s.text[next]
		if c != 'u' {
			end = next + 1
			switch c {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			default:
				return 0, &jsonFailure{"Invalid \\escape", end - 2}
			}
			continue
		}
		next++
		end = next + 4
		if end > length {
			return 0, &jsonFailure{"Invalid \\uXXXX escape", next - 1}
		}
		var code rune
		for ; next < end; next++ {
			if !hexDigit(s.text[next]) {
				return 0, &jsonFailure{"Invalid \\uXXXX escape", end - 5}
			}
			code = code<<4 | jsonHexValue(s.text[next])
		}
		if code >= 0xd800 && code <= 0xdbff && end+6 < length && s.text[next] == '\\' && s.text[next+1] == 'u' {
			next += 2
			end += 6
			for ; next < end; next++ {
				if !hexDigit(s.text[next]) {
					return 0, &jsonFailure{"Invalid \\uXXXX escape", end - 5}
				}
			}
		}
	}
}

func jsonHexValue(r rune) rune {
	switch {
	case digit(r):
		return r - '0'
	case r >= 'a' && r <= 'f':
		return r - 'a' + 10
	}
	return r - 'A' + 10
}

// scanObject is _parse_object_unicode; index is after the "{".
func (s *jsonScanner) scanObject(index int) (int, *jsonFailure) {
	length := len(s.text)
	index = s.skipSpace(index)
	if index >= length || s.text[index] != '}' {
		for {
			if index >= length || s.text[index] != '"' {
				return 0, &jsonFailure{"Expecting property name enclosed in double quotes", index}
			}
			next, failure := s.scanString(index + 1)
			if failure != nil {
				return 0, failure
			}
			index = s.skipSpace(next)
			if index >= length || s.text[index] != ':' {
				return 0, &jsonFailure{"Expecting ':' delimiter", index}
			}
			index = s.skipSpace(index + 1)
			next, failure = s.scanOnce(index)
			if failure != nil {
				return 0, failure
			}
			index = s.skipSpace(next)
			if index < length && s.text[index] == '}' {
				break
			}
			if index >= length || s.text[index] != ',' {
				return 0, &jsonFailure{"Expecting ',' delimiter", index}
			}
			comma := index
			index = s.skipSpace(index + 1)
			if index < length && s.text[index] == '}' {
				return 0, &jsonFailure{"Illegal trailing comma before end of object", comma}
			}
		}
	}
	return index + 1, nil
}

// scanArray is _parse_array_unicode; index is after the "[".
func (s *jsonScanner) scanArray(index int) (int, *jsonFailure) {
	length := len(s.text)
	index = s.skipSpace(index)
	if index >= length || s.text[index] != ']' {
		for {
			next, failure := s.scanOnce(index)
			if failure != nil {
				return 0, failure
			}
			index = s.skipSpace(next)
			if index < length && s.text[index] == ']' {
				break
			}
			if index >= length || s.text[index] != ',' {
				return 0, &jsonFailure{"Expecting ',' delimiter", index}
			}
			comma := index
			index = s.skipSpace(index + 1)
			if index < length && s.text[index] == ']' {
				return 0, &jsonFailure{"Illegal trailing comma before end of array", comma}
			}
		}
	}
	return index + 1, nil
}
