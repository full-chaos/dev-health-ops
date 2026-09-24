package pybody

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// uuidGroupStarts are the byte offsets where each of the five groups of a
// hyphenated UUID begins.
var uuidGroupStarts = [5]int{0, 9, 14, 19, 24}

// uuidGroupLengths are the five groups' expected lengths.
var uuidGroupLengths = [5]int{8, 4, 4, 4, 12}

// ParsePydanticUUID is pydantic-core's str-to-UUID validation (the Rust
// uuid crate's parse_str): 32 hex digits, the 36-character hyphenated form,
// that form in braces, or it after a lower-case "urn:uuid:" prefix; hex
// digits in either case; no surrounding whitespace. On failure it returns
// the crate's error text, which pydantic reports as ctx.error.
//
// It is NOT Python's uuid.UUID(): that is pythonparity.ParseUUID, for the
// places a route calls uuid.UUID(value) itself.
func ParsePydanticUUID(raw string) (uuid.UUID, string) {
	if value, ok := pydanticUUIDAccept(raw); ok {
		return value, ""
	}
	return uuid.Nil, pydanticUUIDError(raw)
}

func pydanticUUIDAccept(raw string) (uuid.UUID, bool) {
	text := raw
	switch {
	case len(text) == 32:
		if !allHex(text) {
			return uuid.Nil, false
		}
	case len(text) == 36:
	case len(text) == 38 && text[0] == '{' && text[37] == '}':
		text = text[1:37]
	case len(text) == 45 && strings.HasPrefix(text, "urn:uuid:"):
		text = text[9:]
	default:
		return uuid.Nil, false
	}
	if len(text) == 36 {
		for index := 0; index < 36; index++ {
			hyphen := index == 8 || index == 13 || index == 18 || index == 23
			if hyphen != (text[index] == '-') || (!hyphen && !isHex(text[index])) {
				return uuid.Nil, false
			}
		}
	}
	value, err := uuid.Parse(text)
	return value, err == nil
}

// pydanticUUIDError is the uuid crate's (1.23.0, pinned by pydantic-core
// 2.46.4) diagnosis of a string it refused. The parser hands the
// diagnosis the INNER 36 bytes when the input had the braced (38-byte) or
// URN (45-byte) shape, and the whole input otherwise; the diagnosis then
// strips braces or the URN prefix itself (offsetting character positions
// by what it stripped) and measures the last group against the length of
// what it was handed.
func pydanticUUIDError(raw string) string {
	input := raw
	switch {
	case len(raw) == 38 && raw[0] == '{' && raw[37] == '}':
		input = raw[1:37]
	case len(raw) == 45 && strings.HasPrefix(raw, "urn:uuid:"):
		input = raw[9:]
	}
	body, offset, simple := input, 0, true
	switch {
	case len(input) >= 2 && input[0] == '{' && input[len(input)-1] == '}':
		body, offset, simple = input[1:len(input)-1], 1, false
	case strings.HasPrefix(input, "urn:uuid:"):
		body, offset, simple = input[9:], 9, false
	}
	hyphens := 0
	var bounds [4]int
	for index, character := range body {
		switch {
		case character == '-':
			if hyphens < 4 {
				bounds[hyphens] = index
			}
			hyphens++
		case character > 0x7f || !isHex(byte(character)):
			return fmt.Sprintf("invalid character: found `%c` at %d", character, index+offset+1)
		}
	}
	if hyphens == 0 && simple {
		return fmt.Sprintf("invalid length: expected length 32 for simple format, found %d", len(input))
	}
	if hyphens != 4 {
		return fmt.Sprintf("invalid group count: expected 5, found %d", hyphens+1)
	}
	for group := 0; group < 4; group++ {
		if bounds[group] != uuidGroupStarts[group+1]-1 {
			return fmt.Sprintf("invalid group length in group %d: expected %d, found %d",
				group, uuidGroupLengths[group], bounds[group]-uuidGroupStarts[group])
		}
	}
	return fmt.Sprintf("invalid group length in group 4: expected 12, found %d", len(input)-uuidGroupStarts[4])
}

func allHex(text string) bool {
	for index := 0; index < len(text); index++ {
		if !isHex(text[index]) {
			return false
		}
	}
	return true
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

// QueryUUID validates one `uuid.UUID | None = Query(default=None)`
// parameter: nil when absent. On failure the pydantic error is appended and
// ok is false.
func (e *Errors) QueryUUID(name string, raw *string) (*uuid.UUID, bool) {
	if raw == nil {
		return nil, true
	}
	value, failure := ParsePydanticUUID(*raw)
	if failure != "" {
		ctx := pyjson.NewObject()
		ctx.Set("error", failure)
		*e = append(*e, Error{Type: "uuid_parsing", Loc: []pyjson.Value{"query", name},
			Msg: "Input should be a valid UUID, " + failure, Input: *raw, Ctx: ctx})
		return nil, false
	}
	return &value, true
}
