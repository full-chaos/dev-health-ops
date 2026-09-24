package pybody

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// ParsePydanticUUID is pydantic-core's validation of a string as a UUID (a
// FastAPI path or query parameter typed uuid.UUID). It is not Python's
// uuid.UUID(): it accepts exactly the simple (32 hex), hyphenated (36),
// braced (38) and "urn:uuid:" (45) forms, ASCII hex in either case, and
// reports the failure the way the Rust uuid crate describes it.
func ParsePydanticUUID(text string) (uuid.UUID, *Error) {
	if parsed, ok := parseUUIDForms(text); ok {
		return parsed, nil
	}
	detail := uuidFailure(text)
	return uuid.UUID{}, &Error{Type: "uuid_parsing", Msg: "Input should be a valid UUID, " + detail, Ctx: uuidContext(detail)}
}

// UUIDError is the FastAPI error for a uuid.UUID parameter at loc.
func UUIDError(loc []pyjson.Value, input string, failure *Error) Error {
	problem := *failure
	problem.Loc, problem.Input = loc, input
	return problem
}

func uuidContext(detail string) *pyjson.Object {
	ctx := pyjson.NewObject()
	ctx.Set("error", detail)
	return ctx
}

func hexValue(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

func parseUUIDForms(text string) (uuid.UUID, bool) {
	switch {
	case len(text) == 32:
		return parseHexRun(text)
	case len(text) == 36:
		return parseHyphenated(text)
	case len(text) == 38 && text[0] == '{' && text[37] == '}':
		return parseHyphenated(text[1:37])
	case len(text) == 45 && strings.HasPrefix(text, "urn:uuid:"):
		return parseHyphenated(text[9:])
	}
	return uuid.UUID{}, false
}

func parseHexRun(text string) (uuid.UUID, bool) {
	var out uuid.UUID
	for index := 0; index < 16; index++ {
		high, low := hexValue(text[2*index]), hexValue(text[2*index+1])
		if high < 0 || low < 0 {
			return uuid.UUID{}, false
		}
		out[index] = byte(high<<4 | low)
	}
	return out, true
}

func parseHyphenated(text string) (uuid.UUID, bool) {
	if text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return uuid.UUID{}, false
	}
	return parseHexRun(text[:8] + text[9:13] + text[14:18] + text[19:23] + text[24:])
}

// uuidGroupLengths are the hyphenated form's group sizes.
var uuidGroupLengths = [5]int{8, 4, 4, 4, 12}

// uuidFailure is the uuid crate's account of why text is not a UUID.
func uuidFailure(text string) string {
	rest, offset := text, 0
	stripped := false
	switch {
	case strings.HasPrefix(text, "urn:uuid:"):
		rest, offset, stripped = text[9:], 9, true
		if len(text) == 45 {
			offset = 0
		}
	case len(text) >= 2 && text[0] == '{' && text[len(text)-1] == '}':
		rest, offset, stripped = text[1:len(text)-1], 1, true
		if len(text) == 38 {
			offset = 0
		}
	}
	hyphens := 0
	var bounds [4]int
	for index, character := range rest {
		if character >= utf8.RuneSelf || (character != '-' && hexValue(byte(character)) < 0) {
			return "invalid character: found `" + string(character) + "` at " + strconv.Itoa(offset+index+1)
		}
		if character == '-' {
			if hyphens < 4 {
				bounds[hyphens] = index
			}
			hyphens++
		}
	}
	switch {
	case hyphens == 0 && !stripped:
		return "invalid length: expected length 32 for simple format, found " + strconv.Itoa(len(rest))
	case hyphens != 4:
		return "invalid group count: expected 5, found " + strconv.Itoa(hyphens+1)
	}
	for group := 0; group < 4; group++ {
		start := 0
		if group > 0 {
			start = bounds[group-1] + 1
		}
		if found := bounds[group] - start; found != uuidGroupLengths[group] {
			return "invalid group length in group " + strconv.Itoa(group) + ": expected " + strconv.Itoa(uuidGroupLengths[group]) + ", found " + strconv.Itoa(found)
		}
	}
	return "invalid group length in group 4: expected 12, found " + strconv.Itoa(len(text)-bounds[3]-1)
}
