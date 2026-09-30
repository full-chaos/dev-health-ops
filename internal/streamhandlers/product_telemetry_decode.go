package streamhandlers

import "encoding/json"

// decodeProductEvents reads a stream entry's `events` field. The Python producer writes
// it with json.dumps, which spells a non-finite float as the bare words NaN, Infinity and
// -Infinity; encoding/json refuses those, so one such value used to make the whole entry
// permanent and quarantine up to 500 otherwise valid events (CHAOS-6299).
//
// The text is rewritten lexically, never re-encoded: only a bare NaN, Infinity or -Infinity
// that is the direct value of a key of an event's `payload` object becomes the token null;
// every other byte is copied unchanged. encoding/json then decodes that text exactly as it
// always did, so every other decision (field types, key case, duplicate keys, unknown
// fields, number ranges, string repair, the payload value kinds validateProductEvent
// allows) stays encoding/json's and is identical for any document that has no such token.
// A non-finite word anywhere else (another event field, a nested payload value, the list
// or document root) is left in place and encoding/json refuses it as before: writing null
// there would turn, say, a NaN orgIdHash into an accepted empty string.
//
// A `payload` key is recognised only in its exact, unescaped spelling. encoding/json also
// matches other letter cases and escaped spellings; for those the word is left in place
// and the entry is refused, never stored differently.
//
// nonFinite is how many NaN or infinite payload numbers were read as null, so the caller can
// make each such value observable (North Star check 12: a value that was not finite is
// stored as missing, and the operator can still see that it happened).
func decodeProductEvents(raw string) (events []productEvent, nonFinite int, err error) {
	text, nonFinite := nullPayloadNonFinite(raw)
	if err := json.Unmarshal([]byte(text), &events); err != nil {
		return nil, 0, err
	}
	return events, nonFinite, nil
}

// frame is one open container on the scan stack.
type frame struct {
	object    bool
	event     bool // an object that is an element of the root list
	payload   bool // the object that is an event's payload value
	expectKey bool // object only: the next string is a key
	key       string
}

// nullPayloadNonFinite returns raw with each bare non-finite word that is a direct value of
// an event's payload object replaced by null, and how many it replaced. It does not
// validate: a malformed document comes back with whatever was copied, and encoding/json
// refuses it.
func nullPayloadNonFinite(raw string) (string, int) {
	var out []byte
	count, copied := 0, 0
	var stack []frame
	for i := 0; i < len(raw); {
		c := raw[i]
		switch {
		case c == '"':
			end := scanString(raw, i)
			if top := len(stack) - 1; top >= 0 && stack[top].object && stack[top].expectKey {
				body := raw[i+1 : end-1]
				stack[top].key = body
			}
			i = end
		case c == '{' || c == '[':
			f := frame{object: c == '{', expectKey: c == '{'}
			if top := len(stack) - 1; top >= 0 {
				parent := stack[top]
				f.event = f.object && !parent.object && top == 0
				f.payload = f.object && parent.object && parent.event && parent.key == "payload"
			}
			stack = append(stack, f)
			i++
		case c == '}' || c == ']':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			i++
		case c == ':':
			if top := len(stack) - 1; top >= 0 && stack[top].object {
				stack[top].expectKey = false
			}
			i++
		case c == ',':
			if top := len(stack) - 1; top >= 0 && stack[top].object {
				stack[top].expectKey = true
			}
			i++
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		default:
			end := i
			for end < len(raw) && !isDelimiter(raw[end]) {
				end++
			}
			if top := len(stack) - 1; top >= 0 && stack[top].payload && !stack[top].expectKey && isNonFiniteWord(raw[i:end]) {
				if out == nil {
					out = make([]byte, 0, len(raw))
				}
				out = append(out, raw[copied:i]...)
				out = append(out, "null"...)
				copied = end
				count++
			}
			i = end
		}
	}
	if count == 0 {
		return raw, 0
	}
	out = append(out, raw[copied:]...)
	return string(out), count
}

func isNonFiniteWord(word string) bool {
	return word == "NaN" || word == "Infinity" || word == "-Infinity"
}

func isDelimiter(c byte) bool {
	switch c {
	case ',', ':', '{', '}', '[', ']', '"', ' ', '\t', '\n', '\r':
		return true
	}
	return false
}

// scanString returns the index just past the string that opens at raw[start]; an
// unterminated string runs to the end of the text.
func scanString(raw string, start int) int {
	for i := start + 1; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return len(raw)
}
